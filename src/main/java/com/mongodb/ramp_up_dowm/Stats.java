package com.mongodb.ramp_up_dowm;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Stats {
    private final ReentrantLock lock = new ReentrantLock();
    Map<String, FrequencyCounter> stats = new HashMap<String, FrequencyCounter>();
    LocalDateTime lastLogTime = LocalDateTime.now();
    int currentReadThreads = 0;
    int currentWriteThreads = 0;

    public void registerEvent(String type) {
        lock.lock();
        try {
            FrequencyCounter counter = stats.getOrDefault(type, null);
            if (counter == null) {
                counter = new FrequencyCounter(1, TimeUnit.SECONDS);
                stats.put(type, counter);
            }
            counter.increment();
        } finally {
            lock.unlock();
        }
    }

    public void setThreadCount(int count, boolean readType) {
        lock.lock();
        try {
            if (readType) {
                currentReadThreads = count;
            } else {
                currentWriteThreads = count;
            }
        } finally {
            lock.unlock();
        }
    }

    public void logStatsIfNeeded() {
        StringBuffer sb = new StringBuffer();
        lock.lock();
        try {
            LocalDateTime now = LocalDateTime.now();
            if (ChronoUnit.MILLIS.between(lastLogTime, now) < 1000) {
                return;  // Too early
            }
            lastLogTime = now;
            sb.append(now).append(">   ");
            for (Map.Entry<String, FrequencyCounter> entry : stats.entrySet()) {
                sb.append(entry.getKey()).append(": ");
                sb.append(entry.getValue().getCount()).append(" events per second\n");
            }
            sb.append("Current read threads: ").append(currentReadThreads).append("\n");
            sb.append("Current write threads: ").append(currentWriteThreads);
        } finally {
            lock.unlock();
        }
        System.out.println(sb.toString());
    }

    int getValue(String key) {
        lock.lock();
        try {
            return stats.get(key).getCount();
        } catch (NullPointerException e) {
            return 0;
        } finally {
            lock.unlock();
        }
    }
}
