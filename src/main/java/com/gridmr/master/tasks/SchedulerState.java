package com.gridmr.master.tasks;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerState {
    public final Queue<String> pendingSplits = new ConcurrentLinkedQueue<>();
    public final AtomicInteger nextMapId = new AtomicInteger(0);
    public volatile int totalSplits = 0;
    public volatile int completedMaps = 0;
    public volatile boolean reducersScheduled = false;
    public final AtomicInteger clientIdx = new AtomicInteger(0);
    public final String jobId;

    public SchedulerState(String jobId) {
        this.jobId = jobId;
    }
}
