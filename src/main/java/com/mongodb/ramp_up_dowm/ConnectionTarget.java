package com.mongodb.ramp_up_dowm;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionTarget {
    static final int CONNECTION_RAMP_UP_PER_SEC = 10;
    static final int CONNECTION_RAMP_DOWN_PER_SEC = 10;
    static final Duration STRESS_STATE_LENGTH = Duration.ofSeconds(10);

    private final ReentrantLock lock = new ReentrantLock();
    private State state = State.RAMP_UP;
    private LocalDateTime lastAdjustTime = LocalDateTime.now();
    private LocalDateTime stressStateStart;
    private int targetThreadCount = 1;
    private Stats stats;
    private int lastReadQPS = 1;
    private int lastWriteQPS = 1;

    public enum State {
        RAMP_UP,
        STRESS,
        RAMP_DOWN
    }
    
    public ConnectionTarget(Stats stats, int initialTargetThreadCount) {
        this.stats = stats;
        this.targetThreadCount = initialTargetThreadCount;
    }

    int getThreadCountTarget() {
        lock.lock();
        try {
            LocalDateTime now = LocalDateTime.now();
            boolean doAdjust = false;
            if (ChronoUnit.MILLIS.between(lastAdjustTime, now) > 1000) {
                doAdjust = true;
                lastAdjustTime = now;
            }
            switch (state) {
                case RAMP_UP: {
                    if (doAdjust) {
                        checkIfRampUpDone();
                        if (state == State.RAMP_UP) {
                            targetThreadCount += CONNECTION_RAMP_UP_PER_SEC;
                        }
                        lastReadQPS = stats.getValue("read");
                        lastWriteQPS = stats.getValue("write");
                    }
                }
                break;
                case STRESS: {
                    if (ChronoUnit.MILLIS.between(stressStateStart, now) > 
                        STRESS_STATE_LENGTH.get(ChronoUnit.MILLIS)) {
                        state = State.RAMP_DOWN;
                        System.out.println("Detected stress completion state");
                    }
                }
                break;
                case RAMP_DOWN:
                default: {
                    if (doAdjust) {
                        targetThreadCount -= CONNECTION_RAMP_DOWN_PER_SEC;
                    }
                }
                break;
            }
            return targetThreadCount;
        } finally {
            lock.unlock();
        }
    }

    void checkIfRampUpDone() {
        assert(lock.isLocked());
        int currentReadQPS = stats.getValue("read");
        int currentWriteQPS = stats.getValue("write");
        if (currentReadQPS < lastReadQPS * 0.99 &&
            currentWriteQPS < lastWriteQPS * 0.99) {
                System.out.println("Detected ramp up completion state");
                state = State.STRESS;
                stressStateStart = LocalDateTime.now();
            }
    }
}
