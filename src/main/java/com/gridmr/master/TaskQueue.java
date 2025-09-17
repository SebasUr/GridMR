package com.gridmr.master;

import com.gridmr.proto.AssignTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Optional;

public class TaskQueue {
    private final ConcurrentLinkedQueue<AssignTask> queue = new ConcurrentLinkedQueue<>();
 
    public void addTask(AssignTask task) {
        queue.add(task);
    }
 
    public Optional<AssignTask> pollTask() {
        return Optional.ofNullable(queue.poll());
    }
    
    public boolean hasTasks() {
        return !queue.isEmpty();
    }

}