package com.viettel.vocs.clickhouse.retry;

public class RetryPolicy {

    private final int maxAttempts;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final double multiplier;

    public RetryPolicy(
            int maxAttempts,
            long initialBackoffMs,
            long maxBackoffMs,
            double multiplier
    ) {
        this.maxAttempts = maxAttempts;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.multiplier = multiplier;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(
                5,        // attempts
                500,      // initial backoff
                10_000,   // max backoff
                2.0       // exponential
        );
    }
}
