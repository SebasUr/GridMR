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
    public void update(Heartbeat hb) {
        workerStates.put(hb.getWorkerId(), hb);
    }

    public void remove(String workerId) {
        workerStates.remove(workerId);
    }

    public Heartbeat get(String workerId) {
        return workerStates.get(workerId);
    }

    // Devuelve una lista de workers ordenados por disponibilidad (más libre primero)
    public List<Heartbeat> getWorkersByAvailability() {
        List<Heartbeat> workers = new ArrayList<>(workerStates.values());
        workers.sort((a,b) -> {
            // Menor uso de CPU y RAM = más libre
            float sa = a.getCpuUsage() + a.getRamUsage();
            float sb = b.getCpuUsage() + b.getRamUsage();
            return Float.compare(sa, sb); // menor score = más libre
        });
        return workers;
    }

    public boolean isKnown(String workerId) {
        return workerStates.containsKey(workerId);
    }
}