package com.gridmr.master;

import com.gridmr.proto.AssignTask;
import com.gridmr.proto.Heartbeat;
import com.gridmr.master.util.Env;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;

/**
 * Scheduler asigna tareas a los workers en orden de disponibilidad (más libre primero),
 * usando los heartbeats (cpu_usage + ram_usage).
 */
public class Scheduler {
    private final WorkerManager workerManager;
    private final float maxCpu; // porcentaje (0-100]
    private final float maxRam; // porcentaje (0-100]

    public Scheduler(WorkerManager workerManager) {
        this.workerManager = workerManager;
        // Leer thresholds de entorno, con defaults
        this.maxCpu = parseFloatEnv("SCHED_MAX_CPU_PCT", 90f);
        this.maxRam = parseFloatEnv("SCHED_MAX_RAM_PCT", 90f);
    }

    private float parseFloatEnv(String key, float def) {
        try {
            return Float.parseFloat(Env.getEnvOrDefault(key, Float.toString(def)));
        } catch (Exception e) {
            return def;
        }
    }

    private boolean withinThresholds(Heartbeat hb) {
        return hb.getCpuUsage() <= maxCpu && hb.getRamUsage() <= maxRam;
    }

    /**
     * Asigna una lista de tareas a los workers más libres (uno a uno).
     * Si hay más tareas que workers, las sobrantes no se asignan.
     * @param tasks lista de tareas candidatas.
     * @return mapa worker_id -> tarea asignada.
     */
    public Map<String, AssignTask> assignTasks(List<AssignTask> tasks) {
        List<Heartbeat> workers = workerManager.getWorkersByAvailability(); // ya ordenados (más libre primero)
        Map<String, AssignTask> assignments = new HashMap<>();
        int i = 0;
        for (AssignTask task : tasks) {
            if (i >= workers.size()) break;
            Heartbeat hb = workers.get(i);
            assignments.put(hb.getWorkerId(), task);
            i++;
        }
        return assignments;
    }

    /**
     * Asigna una sola tarea al worker más libre (si existe).
     * @param task tarea a asignar.
     * @return worker_id o null si no hay workers.
     */
    public String assignSingle(AssignTask task) {
        List<Heartbeat> workers = workerManager.getWorkersByAvailability();
        if (workers.isEmpty()) return null;
        return workers.get(0).getWorkerId();
    }

    // Retorna workerIds libres ordenados (más libre primero)
    public List<String> orderFreeWorkers(Set<String> busyWorkers) {
        List<String> ordered = new ArrayList<>();
        for (Heartbeat hb : workerManager.getWorkersByAvailability()) {
            if (!busyWorkers.contains(hb.getWorkerId()) && withinThresholds(hb)) {
                ordered.add(hb.getWorkerId());
            }
        }
        return ordered;
    }

    // Primer worker libre
    public String bestFreeWorker(Set<String> busyWorkers) {
        for (Heartbeat hb : workerManager.getWorkersByAvailability()) {
            if (!busyWorkers.contains(hb.getWorkerId()) && withinThresholds(hb)) {
                return hb.getWorkerId();
            }
        }
        return null;
    }
}