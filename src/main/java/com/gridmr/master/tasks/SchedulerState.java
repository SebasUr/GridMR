package com.gridmr.master.tasks;

import com.gridmr.master.*;
import com.gridmr.proto.AssignTask;
import com.gridmr.proto.Heartbeat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SchedulerState {
    private final WorkerManager workerManager = new WorkerManager();
    private final TaskQueue taskQueue = new TaskQueue();
    private final Scheduler scheduler = new Scheduler(workerManager);

    
    private final Map<String, AssignTask> running = new ConcurrentHashMap<>();

    public SchedulerState() {}

    public void updateHeartbeat(Heartbeat hb) {
        workerManager.update(hb);
    }

    public void submitTasks(List<AssignTask> tasks) {
        tasks.forEach(taskQueue::addTask);
    }

    public boolean hasPending() {
        return taskQueue.hasTasks();
    }

    public AssignTask pollNext() {
        return taskQueue.pollTask().orElse(null);
    }

    public boolean isBusy(String workerId) {
        return running.containsKey(workerId);
    }

    public void markAssigned(String workerId, AssignTask task) {
        running.put(workerId, task);
    }

    public AssignTask markFinished(String workerId) {
        return running.remove(workerId);
    }

    public List<String> freeWorkersOrdered() {
        return scheduler.orderFreeWorkers(running.keySet());
    }

    public String bestFreeWorker() {
        return scheduler.bestFreeWorker(running.keySet());
    }

    /**
     * Intenta asignar la siguiente tarea al worker (si no está ocupado).
     * Devuelve la tarea ya marcada como ASSIGNED o null si no hay.
     */
    public synchronized AssignTask tryAssignNext(String workerId) {
        if (workerId == null) return null;
        if (isBusy(workerId)) return null;
        if (!hasPending()) return null;
        AssignTask t = pollNext();
        if (t == null) return null;
        markAssigned(workerId, t);
        return t;
    }
}
