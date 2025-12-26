package com.viettel.vocs.clickhouse.async;

public class AsyncBatchConfig {

    public final int batchSize;
    public final long flushIntervalMs;
    public final int queueCapacity;
    public final int workerThreads;

    public AsyncBatchConfig(
            int batchSize,
            long flushIntervalMs,
            int queueCapacity,
            int workerThreads
    ) {
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.queueCapacity = queueCapacity;
        this.workerThreads = workerThreads;
    }

    public static AsyncBatchConfig defaultConfig() {
        return new AsyncBatchConfig(
                2_000,     // batch size
                1_000,     // flush every 1s
                100_000,   // queue capacity
                1          // writer threads
        );
    }
}
