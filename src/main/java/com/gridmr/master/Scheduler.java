package com.gridmr.master;

import com.gridmr.proto.WorkerInfo;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Scheduler encargado de mantener el estado de los workers y seleccionar el mejor para asignar tareas,
 * basado en el porcentaje de uso de CPU y RAM reportado por cada worker.
 */
public class Scheduler {
    // Mapa de estado actual de los workers: worker_id -> WorkerInfo
    private final Map<String, WorkerInfo> workerStates = new ConcurrentHashMap<>();

    /**
     * Actualiza el estado de un worker.
     * @param info WorkerInfo con los datos actualizados.
     */
    public void updateWorkerState(WorkerInfo info) {
        workerStates.put(info.getWorkerId(), info);
    }

    /**
     * Selecciona el mejor worker disponible según los límites máximos de uso de CPU y RAM.
     * @param maxCpuUsage Porcentaje máximo de uso de CPU permitido.
     * @param maxRamUsage Porcentaje máximo de uso de RAM permitido.
     * @return Optional con el WorkerInfo del mejor candidato, o vacío si ninguno cumple.
     */
    public Optional<WorkerInfo> selectBestWorker(float maxCpuUsage, float maxRamUsage) {
        return workerStates.values().stream()
            .filter(w -> w.getCpuUsage() <= maxCpuUsage && w.getRamUsage() <= maxRamUsage)
            .sorted((w1, w2) -> {
                // Prioriza menor uso de CPU, luego menor uso de RAM
                int cmp = Float.compare(w1.getCpuUsage(), w2.getCpuUsage());
                if (cmp == 0) {
                    cmp = Float.compare(w1.getRamUsage(), w2.getRamUsage());
                }
                return cmp;
            })
            .findFirst();
    }

    /**
     * Obtiene el estado actual de todos los workers.
     * @return Mapa de worker_id a WorkerInfo.
     */
    public Map<String, WorkerInfo> getWorkerStates() {
        return workerStates;
    }
}