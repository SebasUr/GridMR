package com.gridmr.master;

import com.gridmr.master.service.ControlServiceImpl;
import com.gridmr.master.tasks.SchedulerState;
import com.gridmr.master.util.Env;
import com.gridmr.master.http.HttpJobServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

/**
 * Minimal gRPC Master that wires the ControlService and delegates task scheduling.
 */
public class MasterServer {

    private final int port;
    private Server server;
    private HttpJobServer http;

    public MasterServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        SchedulerState state = new SchedulerState();
        ControlServiceImpl control = new ControlServiceImpl(state);
        server = ServerBuilder.forPort(port)
                .addService(control)
                .build()
                .start();
        System.out.println("Master gRPC server started on port " + port);
        int httpPort = Integer.parseInt(Env.getEnvOrDefault("MASTER_HTTP_PORT", "8080"));
        http = new HttpJobServer(control, httpPort);
        http.start();
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
        if (http != null) {
            http.stop();
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
