package com.gridmr.master;

import com.gridmr.proto.AssignTask;
import com.gridmr.proto.ControlServiceGrpc;
import com.gridmr.proto.MasterToWorker;
import com.gridmr.proto.WorkerInfo;
import com.gridmr.proto.WorkerToMaster;
import com.gridmr.proto.TaskStatus;
import com.gridmr.proto.PartUploaded;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal gRPC Master that accepts worker streams and assigns a single demo MAP task.
 * The MAP/REDUCE functions themselves are implemented in C++ workers (hardcoded there).
 */
public class MasterServer {
    private final int port;
    private Server server;

    public MasterServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new ControlServiceImpl())
                .build()
                .start();
        System.out.println("Master gRPC server started on port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down master...");
            MasterServer.this.stop();
            System.err.println("Master shut down.");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(getEnvOrDefault("MASTER_PORT", "50051"));
        new MasterServer(port).run();
    }

    private void run() throws Exception {
        start();
        blockUntilShutdown();
    }

    static String getEnvOrDefault(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }

    static class ControlServiceImpl extends ControlServiceGrpc.ControlServiceImplBase {
        // Global queue of pending map splits (URIs), shared across workers
        private static final Queue<String> PENDING_SPLITS = new ConcurrentLinkedQueue<>();
        private static final AtomicInteger NEXT_MAP_ID = new AtomicInteger(0);

        static {
            // Initialize split queue once from env
            String multi = getEnvOrDefault("MR_INPUT_S3_URIS", "").trim();
            if (!multi.isEmpty()) {
                Arrays.stream(multi.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .forEach(PENDING_SPLITS::add);
            } else {
                String single = getEnvOrDefault("MR_INPUT_S3_URI", "s3://gridmr/input.txt").trim();
                if (!single.isEmpty()) {
                    // Allow comma-separated in MR_INPUT_S3_URI as well
                    if (single.contains(",")) {
                        Arrays.stream(single.split(","))
                                .map(String::trim)
                                .filter(s -> !s.isEmpty())
                                .forEach(PENDING_SPLITS::add);
                    } else {
                        PENDING_SPLITS.add(single);
                    }
                }
            }
            System.out.println("Initialized splits: " + PENDING_SPLITS.size());
        }
        @Override
        public StreamObserver<WorkerToMaster> workerStream(StreamObserver<MasterToWorker> responseObserver) {
            final String jobId = UUID.randomUUID().toString();
            final String[] workerIdHolder = new String[1];
            final boolean[] busy = new boolean[]{false};

            return new StreamObserver<WorkerToMaster>() {
                @Override
                public void onNext(WorkerToMaster msg) {
                    if (msg.hasInfo()) {
                        WorkerInfo info = msg.getInfo();
                        workerIdHolder[0] = info.getWorkerId();
                        System.out.println("Worker connected: " + info.getWorkerId() + "@" + info.getHost());
                        tryAssignNext(responseObserver, jobId, workerIdHolder[0], busy);
                    }
                    if (msg.hasStatus()) {
                        TaskStatus st = msg.getStatus();
                        System.out.printf("Status from %s: %s %.1f%% %s%n", st.getTaskId(), st.getState(), st.getProgress(), st.getMessage());
                        if (st.getState() == TaskStatus.State.COMPLETED) {
                            // Mark worker idle and assign next split if available
                            busy[0] = false;
                            if (workerIdHolder[0] != null) {
                                tryAssignNext(responseObserver, jobId, workerIdHolder[0], busy);
                            }
                        }
                    }
                    if (msg.hasPart()) {
                        PartUploaded p = msg.getPart();
                        System.out.printf("Map part uploaded: job=%s map=%d part=%d uri=%s%n", p.getJobId(), p.getMapId(), p.getPartitionId(), p.getUri());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Worker stream error: " + t);
                }

                @Override
                public void onCompleted() {
                    System.out.println("Worker stream completed");
                    responseObserver.onCompleted();
                }

                private void tryAssignNext(StreamObserver<MasterToWorker> responseObserver, String jobId, String workerId, boolean[] busy) {
                    if (busy[0]) return;
                    String split = PENDING_SPLITS.poll();
                    if (split == null) {
                        return; // nothing to do
                    }
                    int mapId = NEXT_MAP_ID.getAndIncrement();
                    int nReducers = Integer.parseInt(getEnvOrDefault("MR_N_REDUCERS", "1"));
                    AssignTask.Builder ab = AssignTask.newBuilder()
                            .setTaskId("map-" + mapId)
                            .setJobId(jobId)
                            .setType(AssignTask.TaskType.MAP)
                            .addSplitUris(split)
                            .setReducerId(0)
                            .setNReducers(nReducers);
                    String binUri = getEnvOrDefault("MR_MAP_BIN_URI", "s3://gridmr/map.cc");
                    if (!binUri.isEmpty()) {
                        ab.setBinaryUri(binUri);
                    }
                    MasterToWorker out = MasterToWorker.newBuilder().setAssign(ab.build()).build();
                    responseObserver.onNext(out);
                    busy[0] = true;
                    System.out.println("Assigned MAP task " + ("map-" + mapId) + " split=" + split + " to " + workerId);
                }
            };
        }
    }
}
