package com.gridmr.master;

import com.gridmr.proto.WorkerInfo;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * WorkerManager mantiene el estado de los workers y permite listarlos ordenados por disponibilidad.
 */
public class WorkerManager {
    private final Map<String, WorkerInfo> workerStates = new ConcurrentHashMap<>();

    // Actualiza el estado de un worker
    public void updateWorkerState(WorkerInfo info) {
        workerStates.put(info.getWorkerId(), info);
    }

    // Devuelve una lista de workers ordenados por disponibilidad (más libre primero)
    public List<WorkerInfo> getWorkersByAvailability() {
        List<WorkerInfo> workers = new ArrayList<>(workerStates.values());
        workers.sort((w1, w2) -> {
            // Menor uso de CPU y RAM = más libre
            float w1Score = w1.getCpuUsage() + w1.getRamUsage();
            float w2Score = w2.getCpuUsage() + w2.getRamUsage();
            return Float.compare(w1Score, w2Score);
        });
        return workers;
    }

    public Map<String, WorkerInfo> getWorkerStates() {
        return workerStates;
    }
}