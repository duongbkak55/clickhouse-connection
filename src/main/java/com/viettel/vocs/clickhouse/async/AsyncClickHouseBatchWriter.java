package com.viettel.vocs.clickhouse.async;

import com.viettel.vocs.clickhouse.ClickHouseClient;
import com.viettel.vocs.clickhouse.retry.RetryPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncClickHouseBatchWriter implements AutoCloseable {

    private final ClickHouseClient client;
    private final String insertSql;
    private final RetryPolicy retryPolicy;
    private final AsyncBatchConfig config;

    private final BlockingQueue<Object[]> queue;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public AsyncClickHouseBatchWriter(
            ClickHouseClient client,
            String insertSql,
            RetryPolicy retryPolicy,
            AsyncBatchConfig config
    ) {
        this.client = client;
        this.insertSql = insertSql;
        this.retryPolicy = retryPolicy;
        this.config = config;

        this.queue = new ArrayBlockingQueue<>(config.queueCapacity);
        this.executor = Executors.newFixedThreadPool(config.workerThreads);

        startWorkers();
    }

    /**
     * Non-blocking enqueue (drops if full)
     */
    public boolean enqueue(Object[] row) {
        return queue.offer(row);
    }

    /**
     * Blocking enqueue (backpressure)
     */
    public void enqueueBlocking(Object[] row) throws InterruptedException {
        queue.put(row);
    }

    private void startWorkers() {
        for (int i = 0; i < config.workerThreads; i++) {
            executor.submit(this::runWorker);
        }
    }

    private void runWorker() {
        List<Object[]> batch = new ArrayList<>(config.batchSize);
        long lastFlush = System.currentTimeMillis();

        try {
            while (running.get() || !queue.isEmpty()) {

                Object[] row = queue.poll(
                        config.flushIntervalMs,
                        TimeUnit.MILLISECONDS
                );

                long now = System.currentTimeMillis();

                if (row != null) {
                    batch.add(row);
                }

                boolean timeFlush =
                        now - lastFlush >= config.flushIntervalMs;

                if (!batch.isEmpty() &&
                        (batch.size() >= config.batchSize || timeFlush)) {

                    flush(batch);
                    batch.clear();
                    lastFlush = now;
                }
            }

            // final flush
            if (!batch.isEmpty()) {
                flush(batch);
            }

        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private void flush(List<Object[]> batch) {
        client.batchUpdateWithRetry(
                insertSql,
                batch,
                retryPolicy
        );
    }

    @Override
    public void close() {
        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
