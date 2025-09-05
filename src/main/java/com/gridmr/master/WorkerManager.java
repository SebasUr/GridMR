package com.gridmr.master;

import com.gridmr.proto.Heartbeat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * WorkerManager mantiene el estado de los workers y permite listarlos ordenados por disponibilidad.
 */
public class WorkerManager {
    private final Map<String, Heartbeat> workerStates = new ConcurrentHashMap<>();

    // Actualiza el estado de un worker con Heartbeat
    public void updateWorkerState(Heartbeat hb) {
        workerStates.put(hb.getWorkerId(), hb);
    }

    // Devuelve una lista de workers ordenados por disponibilidad (más libre primero)
    public List<Heartbeat> getWorkersByAvailability() {
        List<Heartbeat> workers = new ArrayList<>(workerStates.values());
        workers.sort((w1, w2) -> {
            // Menor uso de CPU y RAM = más libre
            float w1Score = w1.getCpuUsage() + w1.getRamUsage();
            float w2Score = w2.getCpuUsage() + w2.getRamUsage();
            return Float.compare(w1Score, w2Score);
        });
        return workers;
    }

    public Map<String, Heartbeat> getWorkerStates() {
        return workerStates;
    }
}