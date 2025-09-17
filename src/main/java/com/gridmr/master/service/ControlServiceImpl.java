package com.gridmr.master.service;
import com.gridmr.master.tasks.SchedulerState;
import com.gridmr.master.util.Env;
import com.gridmr.proto.*;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.*;
import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;

public class ControlServiceImpl extends ControlServiceGrpc.ControlServiceImplBase {
    
    private static final class JobContext {
        final String jobId;
        final List<String> mapInputSplits = new ArrayList<>();
        final AtomicInteger mapsCompleted = new AtomicInteger(0);
        volatile boolean reducersScheduled = false;
        volatile boolean finalConcatenated = false;
        int totalMaps = 0;
        int nReducers = 1;
        String mapBinUri = "";
        String reduceBinUri = "";
        final Map<Integer, BitSet> mapParts = new ConcurrentHashMap<>();
    final BitSet reducersDone = new BitSet();
        volatile boolean mapsEnqueued = false;
        long createdAtMs = System.currentTimeMillis();
        int minWorkersToStart;
        long startDelayMs;
        JobContext(String jobId, int minWorkersToStart, long startDelayMs){
            this.jobId = jobId; this.minWorkersToStart = minWorkersToStart; this.startDelayMs = startDelayMs;
        }
    }

    private final SchedulerState state;

    private final Map<String, StreamObserver<MasterToWorker>> clients = new ConcurrentHashMap<>();

    private final Map<String, JobContext> jobs = new ConcurrentHashMap<>();

    private final Map<String, Long> workerLastHeartbeat = new ConcurrentHashMap<>();
    private final Map<String, AssignTask> inFlight = new ConcurrentHashMap<>();
    private final ScheduledExecutorService hbMonitor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "hb-monitor");
                t.setDaemon(true);
                return t;
            });
    private final long hbTimeoutMs = Long.parseLong(Env.getEnvOrDefault("HEARTBEAT_TIMEOUT_MS", "15000"));
    private final long hbCheckPeriodMs = Long.parseLong(Env.getEnvOrDefault("HEARTBEAT_CHECK_PERIOD_MS", "3000"));
    private final long hbWarnStaleMs = Long.parseLong(Env.getEnvOrDefault("HEARTBEAT_LOG_STALE_MS", "5000"));

    private int defaultMinWorkersToStart = Integer.parseInt(Env.getEnvOrDefault("MR_MIN_WORKERS", "3"));
    private long defaultStartDelayMs = Long.parseLong(Env.getEnvOrDefault("MR_START_DELAY_MS", "0"));

    public ControlServiceImpl(SchedulerState state) {
        this.state = state;
        startHeartbeatMonitor();
    }

    public synchronized String submitJob(List<String> inputUris,
                                         Integer nReducers,
                                         String mapBin,
                                         String reduceBin,
                                         Integer desiredMaps,
                                         Boolean groupPartitioning,
                                         Integer minWorkers,
                                         Long startDelayMsOverride) {
        if (inputUris == null || inputUris.isEmpty()) {
            throw new IllegalArgumentException("inputUris vacío");
        }
        List<String> cleanedInputs = new ArrayList<>();
        for (String raw : inputUris) {
            if (raw == null) continue;
            String s = raw.trim();
            if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
                s = s.substring(1, s.length()-1).trim();
            }
            cleanedInputs.add(s);
        }
        String sharedRoot = Env.getEnvOrDefault("SHARED_DATA_ROOT", "/shared");
        for (String s : cleanedInputs) {
            if (s.isEmpty()) continue;
            if (s.startsWith("http://") || s.startsWith("https://")) continue;
            java.nio.file.Path p = s.startsWith("/") ? java.nio.file.Paths.get(s)
                    : java.nio.file.Paths.get(sharedRoot, s);
            if (!java.nio.file.Files.exists(p)) {
                throw new IllegalArgumentException("No existe input: " + p);
            }
        }

    String jobId = java.util.UUID.randomUUID().toString();
    int mw = (minWorkers != null && minWorkers > 0) ? minWorkers : defaultMinWorkersToStart;
    long sd = (startDelayMsOverride != null && startDelayMsOverride >= 0) ? startDelayMsOverride : defaultStartDelayMs;
    JobContext ctx = new JobContext(jobId, mw, sd);
    ctx.nReducers = (nReducers == null || nReducers <= 0) ? 1 : nReducers;
    ctx.mapBinUri = (mapBin == null ? "" : mapBin);
    ctx.reduceBinUri = (reduceBin == null ? "" : reduceBin);
    ctx.mapInputSplits.addAll(cleanedInputs);
    ctx.totalMaps = ctx.mapInputSplits.size();
    jobs.put(jobId, ctx);
    System.out.println("Initialized MAP splits: " + ctx.totalMaps + " (simple) for job " + jobId);

    maybeStartJobsIfReady();
    return jobId;
    }

    private void buildAndEnqueueMapTasks(String jobId, JobContext ctx) {
        List<AssignTask> tasks = new ArrayList<>();
        for (int i = 0; i < ctx.mapInputSplits.size(); i++) {
            AssignTask.Builder b = AssignTask.newBuilder()
                    .setTaskId("map-" + i)
                    .setJobId(jobId)
                    .setType(AssignTask.TaskType.MAP)
                    .addSplitUris(ctx.mapInputSplits.get(i))
                    .setReducerId(0)
                    .setNReducers(ctx.nReducers);
            if (!ctx.mapBinUri.isEmpty()) b.setBinaryUri(ctx.mapBinUri);
            tasks.add(b.build());
        }
        state.submitTasks(tasks);
        System.out.println("Enqueued " + tasks.size() + " MAP tasks for job " + jobId);
    tryAssignAll();
    }

    @Override
    public StreamObserver<WorkerToMaster> workerStream(StreamObserver<MasterToWorker> responseObserver) {

        return new StreamObserver<WorkerToMaster>() {
            private String workerId;

            @Override
            public void onNext(WorkerToMaster msg) {
                switch (msg.getPayloadCase()) {
                    case INFO:
                        handleInfo(msg.getInfo(), responseObserver);
                        break;
                    case HEARTBEAT:
                        handleHeartbeat(msg.getHeartbeat());
                        break;
                    case STATUS:
                        handleStatus(msg.getStatus());
                        break;
                    case PART:
                        handlePart(msg.getPart());
                        break;
                    case PAYLOAD_NOT_SET:
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Worker stream error (" + workerId + "): " + t.getMessage());
                if (workerId != null) cleanupWorker(workerId);
            }

            @Override
            public void onCompleted() {
                System.out.println("Worker stream completed: " + workerId);
                if (workerId != null) cleanupWorker(workerId);
                responseObserver.onCompleted();
            }

            private void handleInfo(WorkerInfo info, StreamObserver<MasterToWorker> obs) {
                this.workerId = info.getWorkerId();
                if (workerId == null || workerId.isEmpty()) {
                    System.err.println("Ignoring worker with empty worker_id");
                    return;
                }
                StreamObserver<MasterToWorker> prev = clients.put(workerId, obs);
                if (prev != null && prev != obs) {
                    AssignTask inflightPrev = inFlight.remove(workerId);
                    requeue(inflightPrev, "duplicate-worker");
                    state.markFinished(workerId);
                    try {
                        prev.onCompleted();
                    } catch (Exception ignore) {}
                    System.out.println("Replaced existing stream for worker " + workerId);
                }

                Heartbeat synthetic = Heartbeat.newBuilder()
                        .setWorkerId(workerId)
                        .setCpuUsage(0f)
                        .setRamUsage(0f)
                        .setTimestamp(Instant.now().toEpochMilli())
                        .build();
                state.updateHeartbeat(synthetic);
                workerLastHeartbeat.put(workerId, synthetic.getTimestamp());
                System.out.println("Worker connected: " + workerId + " cpu=" + info.getCpu());
                maybeStartJobsIfReady();
                tryAssign(workerId);
            }

            private void handleHeartbeat(Heartbeat hb) {
                state.updateHeartbeat(hb);
                workerLastHeartbeat.put(hb.getWorkerId(), hb.getTimestamp());
                tryAssign(hb.getWorkerId());
            }

            private void handleStatus(TaskStatus st) {
                if (workerId == null) return;
                String msg = st.getMessage();
                if (msg != null && msg.startsWith("result_uri=")) {
                    System.out.printf("Status %s %s %.1f%% result=%s%n",
                            st.getTaskId(), st.getState(), st.getProgress(), msg.substring("result_uri=".length()));
                } else {
                    System.out.printf("Status %s %s %.1f%%%s%n",
                            st.getTaskId(), st.getState(), st.getProgress(),
                            (msg == null || msg.isEmpty() ? "" : " msg=" + msg));
                }

                switch (st.getState()) {
                    case COMPLETED:
                        state.markFinished(workerId);
                        AssignTask done = inFlight.remove(workerId);
                        String jid = (done != null ? done.getJobId() : null);
                        if (st.getTaskId().startsWith("map-")) {
                            if (jid != null) {
                                JobContext ctx = jobs.get(jid);
                                if (ctx != null) {
                                    int d = ctx.mapsCompleted.incrementAndGet();
                                    System.out.println("MAP completed: " + d + "/" + ctx.totalMaps + " for job " + jid);
                                    maybeScheduleReducers(jid, ctx);
                                }
                            }
                        } else if (st.getTaskId().startsWith("reduce-")) {
                            if (jid != null) {
                                JobContext ctx = jobs.get(jid);
                                if (ctx != null) {
                                    // Marca reducer terminado y si ya terminaron todos, concatena
                                    try {
                                        int rid = Integer.parseInt(st.getTaskId().substring("reduce-".length()));
                                        synchronized (ControlServiceImpl.this) { ctx.reducersDone.set(rid); }
                                    } catch (Exception ignore) {}
                                    if (ctx.reducersDone.cardinality() >= ctx.nReducers) {
                                        // Intento inmediato
                                        maybeConcatenateFinalFor(jid, ctx);
                                        // Y 2 reintentos simples por si faltara visibilidad de archivos en NFS
                                        for (int i = 1; i <= 2; i++) {
                                            int delay = i * 1000;
                                            hbMonitor.schedule(() -> {
                                                try { maybeConcatenateFinalFor(jid, ctx); } catch (Exception ignored) {}
                                            }, delay, TimeUnit.MILLISECONDS);
                                        }
                                    }
                                }
                            }
                        }
                        tryAssign(workerId);
                        break;
                    case FAILED:
                        state.markFinished(workerId);
                        AssignTask original = inFlight.remove(workerId);
                        if (original == null && st.getTaskId().startsWith("map-")) {
                            // reconstrucción simple (buscar por cada job el índice)
                            for (Map.Entry<String, JobContext> e : jobs.entrySet()) {
                                JobContext ctx = e.getValue();
                                int idx;
                                try { idx = Integer.parseInt(st.getTaskId().substring("map-".length())); } catch (Exception ex) { idx = -1; }
                                if (idx >= 0 && idx < ctx.mapInputSplits.size()) {
                                    AssignTask.Builder b = AssignTask.newBuilder()
                                            .setTaskId("map-" + idx)
                                            .setJobId(ctx.jobId)
                                            .setType(AssignTask.TaskType.MAP)
                                            .addSplitUris(ctx.mapInputSplits.get(idx))
                                            .setReducerId(0)
                                            .setNReducers(ctx.nReducers);
                                    original = b.build();
                                    break;
                                }
                            }
                        }
                        requeue(original, "status-failed");
                        tryAssign(workerId);
                        break;
                    default:
                        break;
                }
            }

            private void handlePart(PartUploaded p) {
                System.out.printf("Part uploaded job=%s map=%d part=%d uri=%s%n",
                        p.getJobId(), p.getMapId(), p.getPartitionId(), p.getUri());
                JobContext ctx = jobs.get(p.getJobId());
                if (ctx != null) {
                    ctx.mapParts.computeIfAbsent(p.getMapId(), k -> new BitSet(ctx.nReducers)).set(p.getPartitionId());
                    maybeScheduleReducers(p.getJobId(), ctx);
                }
            }

            private void cleanupWorker(String wid) {
                AssignTask inflight = inFlight.remove(wid);
                requeue(inflight, "disconnect");
                state.markFinished(wid);
                clients.remove(wid);
                workerLastHeartbeat.remove(wid);
                System.out.println("Cleaned up worker: " + wid);
            }

            // Reemplazado por nueva versión con validación de partes

            // Sobrecarga: primero validar que todos los MAP completos y todas las partes subidas
            private synchronized void maybeScheduleReducers(String jobId, JobContext ctx) {
                if (ctx.reducersScheduled) return;
                if (ctx.mapsCompleted.get() < ctx.totalMaps) return;
                for (int mid = 0; mid < ctx.totalMaps; mid++) {
                    BitSet bs = ctx.mapParts.get(mid);
                    if (bs == null || bs.cardinality() < ctx.nReducers) {
                        return;
                    }
                }
                scheduleReducersInternal(jobId, ctx);
            }

            private synchronized void scheduleReducersInternal(String jobId, JobContext ctx) {
                if (ctx.reducersScheduled) return;
                ctx.reducersScheduled = true;
                int nReducersLocal = ctx.nReducers;
                String reduceBin = ctx.reduceBinUri;
                System.out.println("[Reducers] All MAP parts uploaded; scheduling " + nReducersLocal + " REDUCE tasks for job " + jobId + "...");
                String sharedRoot = Env.getEnvOrDefault("SHARED_DATA_ROOT", "/shared");
                List<AssignTask> reducers = new ArrayList<>();
                for (int r = 0; r < nReducersLocal; r++) {
                    AssignTask.Builder rb = AssignTask.newBuilder()
                            .setTaskId("reduce-" + r)
                            .setJobId(jobId)
                            .setType(AssignTask.TaskType.REDUCE)
                            .setReducerId(r)
                            .setNReducers(nReducersLocal);
                    if (!reduceBin.isEmpty()) rb.setBinaryUri(reduceBin);
                    for (int mid = 0; mid < ctx.totalMaps; mid++) {
                        String uri = sharedRoot + "/intermediate/" + jobId
                                + "/part-" + r + "-map-" + mid + ".txt";
                        rb.addSplitUris(uri);
                    }
                    reducers.add(rb.build());
                }
                state.submitTasks(reducers);
                tryAssignAll();
            }
        };
    }

    private void requeue(AssignTask t, String reason) {
        if (t == null) return;
        System.out.println("Requeue " + t.getTaskId() + " (" + reason + ")");
        state.submitTasks(List.of(t));
    }

    private void tryAssign(String workerId) {
        if (workerId == null) return;
        AssignTask t = state.tryAssignNext(workerId);
        if (t == null) return;
        StreamObserver<MasterToWorker> obs = clients.get(workerId);
        if (obs == null) {
            state.submitTasks(List.of(t));
            return;
        }
        inFlight.put(workerId, t);
        MasterToWorker out = MasterToWorker.newBuilder().setAssign(t).build();
        obs.onNext(out);
        System.out.printf("Assigned %s to %s (type=%s splits=%d)%n",
                t.getTaskId(), workerId, t.getType(), t.getSplitUrisCount());
    }

    private void tryAssignAll() {
        List<String> ordered = state.freeWorkersOrdered();
        for (String wid : ordered) {
            if (!state.hasPending()) break;
            if (state.isBusy(wid)) continue; // por seguridad
            AssignTask t = state.pollNext();
            if (t == null) break;
            StreamObserver<MasterToWorker> obs = clients.get(wid);
            if (obs == null) {
                state.submitTasks(List.of(t));
                continue;
            }
            state.markAssigned(wid, t);
            inFlight.put(wid, t);
            MasterToWorker out = MasterToWorker.newBuilder().setAssign(t).build();
            try {
                obs.onNext(out);
                System.out.printf("Assigned %s to %s (type=%s splits=%d)%n",
                        t.getTaskId(), wid, t.getType(), t.getSplitUrisCount());
            } catch (Exception e) {
                inFlight.remove(wid);
                state.markFinished(wid);
                state.submitTasks(List.of(t));
            }
        }
    }

    private synchronized void maybeConcatenateFinalFor(String jobId, JobContext ctx) {
        if (!ctx.reducersScheduled || ctx.finalConcatenated) return;
    String sharedRoot = Env.getEnvOrDefault("SHARED_DATA_ROOT", "/shared");
    String jobRoot = sharedRoot + "/results/" + jobId;
        try {
            java.nio.file.Path jobDir = java.nio.file.Paths.get(jobRoot);
            if (!java.nio.file.Files.isDirectory(jobDir)) return;
            List<java.nio.file.Path> parts = new ArrayList<>();
            for (int i = 0; i < ctx.nReducers; i++) {
                java.nio.file.Path p = jobDir.resolve("part-" + i + ".txt");
                if (!java.nio.file.Files.exists(p)) return; // not yet
                parts.add(p);
            }
            java.nio.file.Path finalOut = jobDir.resolve("final.txt");
            try (java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(finalOut, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING))) {
                byte[] buf = new byte[8192];
                for (java.nio.file.Path p : parts) {
                    try (java.io.InputStream is = java.nio.file.Files.newInputStream(p)) {
                        int r;
                        while ((r = is.read(buf)) != -1) {
                            bos.write(buf, 0, r);
                        }
                    }
                }
            }
            ctx.finalConcatenated = true;
            System.out.println("[Final] Concatenated results into: " + finalOut);
        } catch (Exception e) {
            System.err.println("[Final] Concatenation error: " + e.getMessage());
        }
    }

    private void startHeartbeatMonitor() {
        hbMonitor.scheduleAtFixedRate(this::scanHeartbeats, hbCheckPeriodMs, hbCheckPeriodMs, TimeUnit.MILLISECONDS);
        System.out.println("[HB] Monitor started timeout=" + hbTimeoutMs + "ms period=" + hbCheckPeriodMs + "ms");
    }

    private void scanHeartbeats() {
        long now = System.currentTimeMillis();
        for (String wid : new ArrayList<>(workerLastHeartbeat.keySet())) {
            long last = workerLastHeartbeat.getOrDefault(wid, 0L);
            long delta = now - last;
            if (delta >= hbTimeoutMs) {
                System.err.println("[HB] Worker " + wid + " expired (last " + delta + "ms ago)");
                AssignTask inflight = inFlight.remove(wid);
                requeue(inflight, "hb-expire");
                state.markFinished(wid);
                clients.remove(wid);
                workerLastHeartbeat.remove(wid);
            } else if (delta >= hbWarnStaleMs) {
                System.out.println("[HB] WARN worker " + wid + " heartbeat stale=" + delta + "ms");
            }
        }
    maybeStartJobsIfReady();
        tryAssignAll();
    }

    private synchronized void maybeStartJobsIfReady() {
        long now = System.currentTimeMillis();
        int connected = clients.size();
        for (JobContext ctx : jobs.values()) {
            if (ctx.mapsEnqueued) continue;
            boolean delayOk = (now - ctx.createdAtMs) >= ctx.startDelayMs;
            boolean workersOk = connected >= ctx.minWorkersToStart;
            if (delayOk && workersOk) {
                buildAndEnqueueMapTasks(ctx.jobId, ctx);
                ctx.mapsEnqueued = true;
            }
        }
    }

    public void shutdown() {
        hbMonitor.shutdownNow();
    }
}
