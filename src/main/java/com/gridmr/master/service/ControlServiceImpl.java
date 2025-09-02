package com.gridmr.master.service;

import com.gridmr.master.tasks.SchedulerState;
import com.gridmr.master.util.Env;
import com.gridmr.proto.AssignTask;
import com.gridmr.proto.ControlServiceGrpc;
import com.gridmr.proto.MasterToWorker;
import com.gridmr.proto.PartUploaded;
import com.gridmr.proto.TaskStatus;
import com.gridmr.proto.WorkerInfo;
import com.gridmr.proto.WorkerToMaster;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ControlServiceImpl extends ControlServiceGrpc.ControlServiceImplBase {
    // Inicializa la clase SchedulerState.
    private final SchedulerState state; 

    /* Explicación
     * final es para asegurar que la referencia a la lista no cambie
     * List<StreamObserver<MasterToWorker>> es una lista de observadores de flujo para los clientes conectados.
     * StreamObserver<MasterToWorker> es un observador de flujo que recibe mensajes del servidor.
     * (Mensajes tipo MasterToWorker, contiene onNext, onError, onCompleted)
     * El tipo de lista es para que sea thread-safe.
     */
    private final List<StreamObserver<MasterToWorker>> clients = new CopyOnWriteArrayList<>();
    private final List<String> clientIds = new CopyOnWriteArrayList<>();

    /* Explicación
     * Constructor de ControlServiceImpl
     */
    public ControlServiceImpl(SchedulerState state) {
        this.state = state;
        initializeSplits();
    }

    private void initializeSplits() {
        // MR_INPUT_S3_URIS. Admite uno o varios (separados por coma).
        /* Explicación
         * Son buckets uris que son agregadas a state.pendingSplits
         */
        String uris = Env.getEnvOrDefault("MR_INPUT_S3_URIS", "s3://gridmr/input.txt").trim();
        Arrays.stream(uris.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(state.pendingSplits::add);
        state.totalSplits = state.pendingSplits.size();
        System.out.println("Initialized splits: " + state.totalSplits);
    }

    /* Explicación
     * Recibe mensajes del worker y los procesa.
     * Retorna un StreamObserver que permite recibir mensajes del worker.
     * 
     * Este método se llama cuando un worker se conecta al servidor.
     * Sobreescribe el método workerStream de ControlServiceGrpc.ControlServiceImplBase
     * Cada vez que un worker envía un mensaje, se maneja en el método onNext.
     * Cada vez que un worker se desconecta, se maneja en el método onCompleted.
    */
    @Override
    public StreamObserver<WorkerToMaster> workerStream(StreamObserver<MasterToWorker> responseObserver) {

        // Estas variables actúan como cajas mutables que el StreamObserver puede modificar.
        final String[] workerIdHolder = new String[1];
        final boolean[] busy = new boolean[]{false};

        /*
         * Está devolviendo una clase anónima que implementa StreamObserver<WorkerToMaster>
         * El StreamObserver tiene 3 métodos: onNext, onError y onCompleted.
         */
        return new StreamObserver<WorkerToMaster>() {
            @Override
            public void onNext(WorkerToMaster msg) {

                // Se recibe información del worker, se establece el ID del worker y se imprime.
                if (msg.hasInfo()) {
                    WorkerInfo info = msg.getInfo();
                    workerIdHolder[0] = info.getWorkerId();

                    System.out.println("Worker connected: " + info.getWorkerId() + "@" + info.getHost());

                    // Si el worker no está en la lista de IDs de clientes, se agrega.
                    if (!clientIds.contains(info.getWorkerId())) {
                        clients.add(responseObserver);
                        clientIds.add(info.getWorkerId());
                    }

                    // Se intenta asignar la siguiente tarea al worker.
                    tryAssignNext(responseObserver, state.jobId, workerIdHolder[0], busy);
                }
                if (msg.hasStatus()) {
                    TaskStatus st = msg.getStatus();
                    String m = st.getMessage();

                    // Se procesa el mensaje de estado del worker.
                    if (m != null && m.startsWith("result_uri=")) {
                        System.out.printf("Status from %s: %s %.1f%% result=%s%n", st.getTaskId(), st.getState(), st.getProgress(), m.substring("result_uri=".length()));
                    } else {
                        System.out.printf("Status from %s: %s %.1f%%%s%n", st.getTaskId(), st.getState(), st.getProgress(), (m==null||m.isEmpty()?"":" msg="+m));
                    }

                    // Si el estado es COMPLETED, se intenta asignar la siguiente tarea.
                    if (st.getState() == TaskStatus.State.COMPLETED) {
                        busy[0] = false;

                        if (workerIdHolder[0] != null) {
                            tryAssignNext(responseObserver, state.jobId, workerIdHolder[0], busy);
                        }

                        if (st.getTaskId().startsWith("map-")) {
                            int done = ++state.completedMaps;
                            System.out.println("Map completed: " + done + "/" + state.totalSplits);
                        }

                        // Si el estado es COMPLETED, se intenta asignar la siguiente tarea (reduce).
                        maybeScheduleReducers();
                    }
                }

                // Se recibe información sobre la parte subida. (Obtener URI de la parte)
                if (msg.hasPart()) {
                    PartUploaded p = msg.getPart();
                    System.out.printf("Map part uploaded: job=%s map=%d part=%d uri=%s%n", p.getJobId(), p.getMapId(), p.getPartitionId(), p.getUri());
                }
            }

            // Si hay un error en el stream del worker, se imprime el error.
            @Override
            public void onError(Throwable t) {
                System.err.println("Worker stream error: " + t);
            }

            // Se completa el stream del worker.
            @Override
            public void onCompleted() {
                System.out.println("Worker stream completed");
                responseObserver.onCompleted();
            }

            private void tryAssignNext(StreamObserver<MasterToWorker> responseObserver, String jobId, String workerId, boolean[] busy) {
                if (busy[0]) return; // El worker está ocupado.

                // Se obtiene la siguiente parte pendiente.
                String split = state.pendingSplits.poll();
                if (split == null) return;

                // Se obtiene el ID del MAP y se incrementa el contador.
                int mapId = state.nextMapId.getAndIncrement();
                int nReducers = Integer.parseInt(Env.getEnvOrDefault("MR_N_REDUCERS", "1"));
                AssignTask.Builder ab = AssignTask.newBuilder()
                        .setTaskId("map-" + mapId)
                        .setJobId(jobId)
                        .setType(AssignTask.TaskType.MAP)
                        .addSplitUris(split)
                        .setReducerId(0)
                        .setNReducers(nReducers);

                // Se obtiene la URI del binario del mapper desde la variable de entorno.
                String binUri = Env.getEnvOrDefault("MR_MAP_BIN_URI", "s3://gridmr/map.cc");
                if (!binUri.isEmpty()) {
                    ab.setBinaryUri(binUri);
                }

                // Se construye el mensaje de asignación y se envía al worker.
                MasterToWorker out = MasterToWorker.newBuilder().setAssign(ab.build()).build();
                responseObserver.onNext(out);
                busy[0] = true;
                System.out.println("Assigned MAP task map-" + mapId + " split=" + split + " to " + workerId);
            }

            private synchronized void maybeScheduleReducers() {
                // Si ya se han programado reducers, no hacemos nada.
                if (state.reducersScheduled) return;
                if (state.completedMaps < state.totalSplits) return;

                state.reducersScheduled = true;
                System.out.println("All maps completed; scheduling reducers...");
                int nReducers = Integer.parseInt(Env.getEnvOrDefault("MR_N_REDUCERS", "1"));
                for (int pid = 0; pid < nReducers; ++pid) {
                    AssignTask.Builder rb = AssignTask.newBuilder()
                            .setTaskId("reduce-" + pid)
                            .setJobId(state.jobId)
                            .setType(AssignTask.TaskType.REDUCE)
                            .setReducerId(pid)
                            .setNReducers(nReducers);
                    String reduceBin = Env.getEnvOrDefault("MR_REDUCE_BIN_URI", "s3://gridmr/reduce.cc");
                    if (!reduceBin.isEmpty()) {
                        rb.setBinaryUri(reduceBin);
                    }
                    // Derivamos el bucket desde MR_INPUT_S3_URIS (toma el primer URI si hay varios)
                    String example = Env.getEnvOrDefault("MR_INPUT_S3_URIS", "s3://gridmr/input.txt");
                    if (example.contains(",")) example = example.split(",")[0].trim();
                    String bucket = "gridmr";
                    int s = example.indexOf("s3://");
                    if (s == 0) {
                        int slash = example.indexOf('/', 5);
                        if (slash > 5) bucket = example.substring(5, slash);
                    }
                    for (int mid = 0; mid < state.totalSplits; ++mid) {
                        String uri = "s3://" + bucket + "/intermediate/" + state.jobId + "/part-" + pid + "-map-" + mid + ".txt";
                        rb.addSplitUris(uri);
                    }
                    MasterToWorker out = MasterToWorker.newBuilder().setAssign(rb.build()).build();
                    StreamObserver<MasterToWorker> target = clients.isEmpty() ? null : clients.get(Math.abs(state.clientIdx.getAndIncrement()) % clients.size());
                    if (target != null) {
                        target.onNext(out);
                        System.out.println("Assigned REDUCE task reduce-" + pid + " to worker " + clientIds.get(Math.abs(state.clientIdx.get()-1) % clientIds.size()) + " with " + state.totalSplits + " inputs");
                    } else {
                        System.out.println("No connected workers to assign reducer " + pid);
                    }
                }
            }
        };
    }
}
