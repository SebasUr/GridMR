package com.gridmr.master;

import com.gridmr.master.service.ControlServiceImpl;
import com.gridmr.master.tasks.SchedulerState;
import com.gridmr.master.util.Env;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.UUID;

/**
 * Minimal gRPC Master that wires the ControlService and delegates task scheduling.
 */
public class MasterServer {

    private final int port;
    private Server server;

    public MasterServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        SchedulerState state = new SchedulerState(UUID.randomUUID().toString());
    server = ServerBuilder.forPort(port)
        .addService(new ControlServiceImpl(state))
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
        int port = Integer.parseInt(Env.getEnvOrDefault("MASTER_PORT", "50051"));
        new MasterServer(port).run();
    }

    private void run() throws Exception {
        start();
        blockUntilShutdown();
    }
}
