package com.mongodb.ramp_up_dowm;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Stats {
    public final static String READ_THREADS_KEY = "readThreads";
    public final static String WRITE_THREADS_KEY = "writeThreads";

    private final ReentrantLock lock = new ReentrantLock();
    private Map<String, FrequencyCounter> stats = new HashMap<String, FrequencyCounter>();
    private LocalDateTime lastLogTime = LocalDateTime.now();
    private int currentReadThreads = 0;
    private int currentWriteThreads = 0;
    private FileWriter report;
    private String[] columns;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

    public Stats(String report, String[] columns) throws IOException {
        this.report = new FileWriter(report);
        this.columns = columns;
        StringBuffer b = new StringBuffer();
        b.append("# ").append("time ");
        for (String col : columns) {
            b.append(col).append(" ");
        }
        b.append("\n");
        this.report.write(b.toString());
    }

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

    public void logStatsIfNeeded() throws IOException {
        StringBuffer sb = new StringBuffer();
        LocalDateTime now;
        lock.lock();
        try {
            now = LocalDateTime.now();
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

        // File report is using fixed columns.
        StringBuffer b = new StringBuffer();
        b.append(now.format(formatter)).append("\t");
        for (String col : columns) {
            b.append(getValue(col)).append("\t");
        }
        b.append("\n");
        report.write(b.toString());
    }

    public void close() throws IOException {
        report.close();
    }

    int getValue(String key) {
        lock.lock();
        try {
            if (key.equals(READ_THREADS_KEY)) {
                return currentReadThreads;
            }
            if (key.equals(WRITE_THREADS_KEY)) {
                return currentWriteThreads;
            }
            return stats.get(key).getCount();
        } catch (NullPointerException e) {
            return 0;
        } finally {
            lock.unlock();
        }
    }
}
