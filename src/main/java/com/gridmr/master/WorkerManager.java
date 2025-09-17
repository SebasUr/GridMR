package com.gridmr.master;

import com.gridmr.proto.Heartbeat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class WorkerManager {
    private static final float CPU_WEIGHT = 0.7f;
    private static final float RAM_WEIGHT = 0.3f;

    private final Map<String, Heartbeat> workerStates = new ConcurrentHashMap<>();

    
    public void update(Heartbeat hb) {
        workerStates.put(hb.getWorkerId(), hb);
    }

    public void remove(String workerId) {
        workerStates.remove(workerId);
    }

    public Heartbeat get(String workerId) {
        return workerStates.get(workerId);
    }

    
    public List<Heartbeat> getWorkersByAvailability() {
        List<Heartbeat> workers = new ArrayList<>(workerStates.values());
        workers.sort((a,b) -> {
            
            float sa = CPU_WEIGHT * a.getCpuUsage() + RAM_WEIGHT * a.getRamUsage();
            float sb = CPU_WEIGHT * b.getCpuUsage() + RAM_WEIGHT * b.getRamUsage();
            return Float.compare(sa, sb); 
        });
        return workers;
    }

    public boolean isKnown(String workerId) {
        return workerStates.containsKey(workerId);
    }
}