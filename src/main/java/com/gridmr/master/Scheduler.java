package com.gridmr.master;

import com.gridmr.proto.AssignTask;
import com.gridmr.proto.WorkerInfo;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Scheduler asigna tareas a los workers en orden de disponibilidad.
 */
public class Scheduler {
    private final WorkerManager workerManager;

    public Scheduler(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    /**
     * Asigna tareas a los workers más libres.
     * @param tasks Lista de tareas a asignar.
     * @return Mapa de worker_id a tarea asignada.
     */
    public java.util.Map<String, AssignTask> assignTasks(List<AssignTask> tasks) {
        List<WorkerInfo> workers = workerManager.getWorkersByAvailability();
        java.util.Map<String, AssignTask> assignments = new java.util.HashMap<>();
        int i = 0;
        for (AssignTask task : tasks) {
            if (i >= workers.size()) break;
            WorkerInfo worker = workers.get(i);
            assignments.put(worker.getWorkerId(), task);
            i++;
        }
        return assignments;
    }
}