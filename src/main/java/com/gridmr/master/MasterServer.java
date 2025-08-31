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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal gRPC Master that accepts worker streams and assigns a single demo MAP task.
 * The MAP/REDUCE functions themselves are implemented in C++ workers (hardcoded there).
 */
public class MasterServer {
    private final int port;
    private Server server;

    public MasterServer(int port) { this.port = port; }

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
        @Override
        public StreamObserver<WorkerToMaster> workerStream(StreamObserver<MasterToWorker> responseObserver) {
            final AtomicBoolean assignedOnce = new AtomicBoolean(false);
            final String jobId = UUID.randomUUID().toString();

            return new StreamObserver<WorkerToMaster>() {
                @Override
                public void onNext(WorkerToMaster msg) {
                    if (msg.hasInfo()) {
                        WorkerInfo info = msg.getInfo();
                        System.out.println("Worker connected: " + info.getWorkerId() + "@" + info.getHost());
                        // Assign a single demo MAP task once per stream
                        if (assignedOnce.compareAndSet(false, true)) {
                            String input = getEnvOrDefault("MR_INPUT_S3_URI", "s3://gridmr/input.txt");
                            int nReducers = Integer.parseInt(getEnvOrDefault("MR_N_REDUCERS", "1"));

                            AssignTask assign = AssignTask.newBuilder()
                                    .setTaskId("map-0")
                                    .setJobId(jobId)
                                    .setType(AssignTask.TaskType.MAP)
                                    .addSplitUris(input)
                                    .setReducerId(0)
                                    .setNReducers(nReducers)
                                    .build();
                            MasterToWorker out = MasterToWorker.newBuilder().setAssign(assign).build();
                            responseObserver.onNext(out);
                            System.out.println("Assigned demo MAP task to " + info.getWorkerId());
                        }
                    }
                    if (msg.hasStatus()) {
                        TaskStatus st = msg.getStatus();
                        System.out.printf("Status from %s: %s %.1f%% %s%n", st.getTaskId(), st.getState(), st.getProgress(), st.getMessage());
                        if (st.getState() == TaskStatus.State.COMPLETED) {
                            // In a real impl, we would track maps, then send REDUCE assignments.
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
            };
        }
    }
}
