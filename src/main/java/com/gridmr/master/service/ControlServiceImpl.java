package com.gridmr.master.service;

import com.gridmr.master.Partitioner;
import com.gridmr.master.tasks.SchedulerState;
import com.gridmr.master.util.Env;
import com.gridmr.proto.*;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.*;
import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*; // <-- añadido para ScheduledExecutorService
import java.util.function.Consumer;

/**
 * ControlServiceImpl:
 * - Gestiona el stream bidireccional con cada worker.
 * - Registra workers (INFO).
 * - Actualiza métricas (HEARTBEAT).
 * - Asigna tareas (MAP / REDUCE) desde la TaskQueue vía SchedulerState.
 * - Recibe estados (STATUS) y partes generadas (PART).
 */
public class ControlServiceImpl extends ControlServiceGrpc.ControlServiceImplBase {

    // --- NUEVO: guarda grupos cuando se activa particionamiento agrupado ---
    private List<List<String>> groupedMapInputs = Collections.emptyList();

    private final SchedulerState state;

    // workerId -> stream para enviar MasterToWorker
    private final Map<String, StreamObserver<MasterToWorker>> clients = new ConcurrentHashMap<>();

    // Controles de job / tareas
    private final List<String> mapInputSplits = new ArrayList<>();
    // mapIdGen eliminado (no se usaba)
    private final AtomicInteger mapsCompleted = new AtomicInteger(0);
    private int totalMaps = 0;
    private volatile boolean reducersScheduled = false;
    private int nReducers = 1;

    // mapId -> bitset de reducers recibidos (particiones intermedias)
    private final Map<Integer, BitSet> mapParts = new ConcurrentHashMap<>();

    // === NUEVO: tracking de heartbeats y tareas en vuelo ===
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

    public ControlServiceImpl(SchedulerState state) {
        this.state = state;
        initializeMapSplits();
        buildAndEnqueueMapTasks();
        startHeartbeatMonitor(); // <-- iniciar monitor
    }

    /* Lee MR_INPUT_S3_URIS y genera la lista de splits */
    private void initializeMapSplits() {
        String urisEnv = Env.getEnvOrDefault("MR_INPUT_S3_URIS", "s3://gridmr/input.txt").trim();
        if (urisEnv.isEmpty()) {
            throw new IllegalStateException("MR_INPUT_S3_URIS está vacío");
        }
        List<String> inputs = new ArrayList<>();
        for (String raw : urisEnv.split(",")) {
            String u = raw.trim();
            if (!u.isEmpty()) inputs.add(u);
        }

        boolean groupMode = "1".equals(Env.getEnvOrDefault("MR_GROUP_PARTITIONING", "0"));
        int desiredMaps = Integer.parseInt(Env.getEnvOrDefault("MR_DESIRED_MAPS",
                String.valueOf(inputs.size())));

        if (!groupMode || desiredMaps <= 0 || desiredMaps >= inputs.size()) {
            // Comportamiento original (simple)
            groupedMapInputs = Collections.emptyList();
            mapInputSplits.clear();
            mapInputSplits.addAll(inputs);
            totalMaps = mapInputSplits.size();
            System.out.println("Initialized MAP splits: " + totalMaps + " (simple)");
            return;
        }

        // Agrupación: repartimos inputs en desiredMaps grupos
        List<List<String>> groups = Partitioner.partition(inputs, desiredMaps);
        groupedMapInputs = groups;
        mapInputSplits.clear(); // mantenemos limpio (usaremos groupedMapInputs)
        totalMaps = groups.size();
        System.out.println("Initialized MAP splits: " + totalMaps + " (grouped from " +
                inputs.size() + " inputs; desired=" + desiredMaps + ")");
    }

    /* Construye las tareas MAP iniciales y las encola */
    private void buildAndEnqueueMapTasks() {
        int nReducers = Integer.parseInt(Env.getEnvOrDefault("MR_N_REDUCERS", "1"));
        String binUri = Env.getEnvOrDefault("MR_MAP_BIN_URI", ""); // no forzamos por defecto

        List<AssignTask> tasks = new ArrayList<>();

        if (!groupedMapInputs.isEmpty()) {
            // Modo agrupado: cada grupo -> una tarea con múltiples split_uris
            for (int i = 0; i < groupedMapInputs.size(); i++) {
                List<String> group = groupedMapInputs.get(i);
                AssignTask.Builder b = AssignTask.newBuilder()
                        .setTaskId("map-" + i)
                        .setJobId(state.getJobId())
                        .setType(AssignTask.TaskType.MAP)
                        .setReducerId(0)
                        .setNReducers(nReducers);
                for (String uri : group) {
                    b.addSplitUris(uri);
                }
                if (!binUri.isEmpty()) b.setBinaryUri(binUri);
                tasks.add(b.build());
            }
        } else {
            // Comportamiento simple original: una URI por tarea
            for (int i = 0; i < mapInputSplits.size(); i++) {
                AssignTask.Builder b = AssignTask.newBuilder()
                        .setTaskId("map-" + i)
                        .setJobId(state.getJobId())
                        .setType(AssignTask.TaskType.MAP)
                        .addSplitUris(mapInputSplits.get(i))
                        .setReducerId(0)
                        .setNReducers(nReducers);
                if (!binUri.isEmpty()) b.setBinaryUri(binUri);
                tasks.add(b.build());
            }
        }

        state.submitTasks(tasks);
        System.out.println("Enqueued " + tasks.size() + " MAP tasks.");
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

            /* Registro inicial del worker */
            private void handleInfo(WorkerInfo info, StreamObserver<MasterToWorker> obs) {
                this.workerId = info.getWorkerId();
                if (workerId == null || workerId.isEmpty()) {
                    System.err.println("Ignoring worker with empty worker_id");
                    return;
                }
                // Duplicado: si ya existe, limpiar previa conexión
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
                        .setCpuUsage(100f)
                        .setRamUsage(100f)
                        .setTimestamp(Instant.now().toEpochMilli())
                        .build();
                state.updateHeartbeat(synthetic);
                workerLastHeartbeat.put(workerId, synthetic.getTimestamp());
                System.out.println("Worker connected: " + workerId + " cpu=" + info.getCpu());
                tryAssign(workerId); // asignar inmediatamente
            }

            /* Actualización de métricas (dispara intento de asignación si está libre) */
            private void handleHeartbeat(Heartbeat hb) {
                state.updateHeartbeat(hb);
                workerLastHeartbeat.put(hb.getWorkerId(), hb.getTimestamp());
                tryAssign(hb.getWorkerId());
            }

            /* Procesa estados de tareas */
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
                        inFlight.remove(workerId);
                        if (st.getTaskId().startsWith("map-")) {
                            int done = mapsCompleted.incrementAndGet();
                            System.out.println("MAP completed: " + done + "/" + totalMaps);
                            maybeScheduleReducers();
                        }
                        tryAssign(workerId);
                        break;
                    case FAILED:
                        state.markFinished(workerId);
                        AssignTask original = inFlight.remove(workerId);
                        if (original == null && st.getTaskId().startsWith("map-")) {
                            // reconstrucción simple
                            int idx = Integer.parseInt(st.getTaskId().substring("map-".length()));
                            if (idx >= 0 && idx < mapInputSplits.size()) {
                                AssignTask.Builder b = AssignTask.newBuilder()
                                        .setTaskId("map-" + idx)
                                        .setJobId(state.getJobId())
                                        .setType(AssignTask.TaskType.MAP)
                                        .addSplitUris(mapInputSplits.get(idx))
                                        .setReducerId(0)
                                        .setNReducers(nReducers);
                                original = b.build();
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
                mapParts.computeIfAbsent(p.getMapId(), k -> new BitSet(nReducers)).set(p.getPartitionId());
                maybeScheduleReducers();
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
            private synchronized void maybeScheduleReducers() {
                if (reducersScheduled) return;
                if (mapsCompleted.get() < totalMaps) return; // aún faltan MAPs
                // Validar que cada mapa tiene todas las partes registradas
                for (int mid = 0; mid < totalMaps; mid++) {
                    BitSet bs = mapParts.get(mid);
                    if (bs == null || bs.cardinality() < nReducers) {
                        return; // aún faltan uploads
                    }
                }
                // Todos listos -> delegar al original (renombrado) para construir reducers
                scheduleReducersInternal();
            }

            // La lógica original de programación la movemos aquí
            private synchronized void scheduleReducersInternal() {
                if (reducersScheduled) return;
                reducersScheduled = true;
                int nReducersLocal = nReducers; // usar campo externo
                String reduceBin = Env.getEnvOrDefault("MR_REDUCE_BIN_URI", "");
                System.out.println("[Reducers] All MAP parts uploaded; scheduling " + nReducersLocal + " REDUCE tasks...");
                String example = mapInputSplits.isEmpty() ? "s3://gridmr/input.txt" : mapInputSplits.get(0);
                String bucket = "gridmr";
                if (example.startsWith("s3://")) {
                    int slash = example.indexOf('/', 5);
                    if (slash > 5) bucket = example.substring(5, slash);
                }
                List<AssignTask> reducers = new ArrayList<>();
                for (int r = 0; r < nReducersLocal; r++) {
                    AssignTask.Builder rb = AssignTask.newBuilder()
                            .setTaskId("reduce-" + r)
                            .setJobId(state.getJobId())
                            .setType(AssignTask.TaskType.REDUCE)
                            .setReducerId(r)
                            .setNReducers(nReducersLocal);
                    if (!reduceBin.isEmpty()) rb.setBinaryUri(reduceBin);
                    for (int mid = 0; mid < totalMaps; mid++) {
                        String uri = "s3://" + bucket + "/intermediate/" + state.getJobId()
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

    /* Reencola una tarea (si no es null) */
    private void requeue(AssignTask t, String reason) {
        if (t == null) return;
        System.out.println("Requeue " + t.getTaskId() + " (" + reason + ")");
        state.submitTasks(List.of(t));
    }

    /* Intenta asignar usando SchedulerState.tryAssignNext */
    private void tryAssign(String workerId) {
        if (workerId == null) return;
        AssignTask t = state.tryAssignNext(workerId);
        if (t == null) return;
        StreamObserver<MasterToWorker> obs = clients.get(workerId);
        if (obs == null) {
            // Worker desapareció después de asignar: devolver
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
        for (String wid : new ArrayList<>(clients.keySet())) {
            tryAssign(wid);
        }
    }

    // === NUEVO: monitor de heartbeats ===
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
        tryAssignAll();
    }

    // (Opcional) llamar en shutdown del servidor si tienes hook
    public void shutdown() {
        hbMonitor.shutdownNow();
    }
}
