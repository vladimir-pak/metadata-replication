package com.gpb.replication.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class MetricCounter {

    private final AtomicLong success = new AtomicLong();

    private final AtomicLong error = new AtomicLong();

    public void success() {
        success.incrementAndGet();
    }

    public void success(long count) {
        if (count < 0) {
            throw new IllegalArgumentException(
                    "count must be >= 0"
            );
        }
        success.addAndGet(count);
    }

    public void error() {
        error.incrementAndGet();
    }

    public void error(long count) {
        if (count < 0) {
            throw new IllegalArgumentException(
                    "count must be >= 0"
            );
        }
        error.addAndGet(count);
    }

    public long getSuccessCount() {
        return success.get();
    }

    public long getErrorCount() {
        return error.get();
    }
}
