package com.viettel.vocs.clickhouse.sample;

import com.viettel.vocs.clickhouse.*;
import com.viettel.vocs.clickhouse.async.*;
import com.viettel.vocs.clickhouse.retry.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * FULL SAMPLE for ClickHouse internal library
 *
 * Covers:
 *  - Client creation
 *  - Query
 *  - QueryForObject
 *  - Update
 *  - Batch insert
 *  - Batch insert with retry
 *  - Chunked batch
 *  - Async batch writer
 *  - Graceful shutdown
 */
public class ClickHouseLibrarySamples {

    public static void main(String[] args) throws Exception {

        /* ============================================================
         * 1️⃣ CREATE CLIENT (BUILDER)
         * ============================================================ */
        ClickHouseClient client =
                new ClickHouseClientBuilder()
                        .withPropertiesFile("clickhouse.properties")
                        .build();

        /* ============================================================
         * 2️⃣ SIMPLE QUERY
         * ============================================================ */
        Long tableCount = client.queryForObject(
                "SELECT count() FROM system.tables",
                rs -> rs.getLong(1)
        );
        System.out.println("Table count = " + tableCount);

        /* ============================================================
         * 3️⃣ QUERY LIST
         * ============================================================ */
        List<String> tables = client.query(
                "SELECT name FROM system.tables WHERE database = ?",
                rs -> rs.getString("name"),
                "default"
        );

        System.out.println("Tables:");
        tables.forEach(System.out::println);

        /* ============================================================
         * 4️⃣ UPDATE (DDL / DML)
         * ============================================================ */
        client.update(
                "CREATE TABLE IF NOT EXISTS test_debug (" +
                        "id UInt64, " +
                        "name String, " +
                        "created_at DateTime" +
                        ") ENGINE = MergeTree ORDER BY id"
        );

        /* ============================================================
         * 5️⃣ SIMPLE BATCH INSERT
         * ============================================================ */
        List<Object[]> batch = new ArrayList<>();
        for (int i = 1; i <= 1_000; i++) {
            batch.add(new Object[]{
                    (long) i,
                    "name-" + i,
                    LocalDateTime.now()
            });
        }

        int inserted = client.batchUpdate(
                "INSERT INTO test_debug (id, name, created_at) VALUES (?, ?, ?)",
                batch
        );

        System.out.println("Inserted rows (no retry) = " + inserted);

        /* ============================================================
         * 6️⃣ BATCH INSERT WITH RETRY + BACKOFF
         * ============================================================ */
        RetryPolicy retryPolicy = new RetryPolicy(
                5,          // max attempts
                500,        // initial backoff ms
                10_000,     // max backoff ms
                2.0         // exponential multiplier
        );

        int retryInserted = client.batchUpdateWithRetry(
                "INSERT INTO test_debug (id, name, created_at) VALUES (?, ?, ?)",
                batch,
                retryPolicy
        );

        System.out.println("Inserted rows (with retry) = " + retryInserted);

        /* ============================================================
         * 7️⃣ CHUNKED BATCH INSERT (LARGE DATASET)
         * ============================================================ */
        List<Object[]> largeBatch = new ArrayList<>();
        for (int i = 1; i <= 50_000; i++) {
            largeBatch.add(new Object[]{
                    (long) (10_000 + i),
                    "bulk-" + i,
                    LocalDateTime.now()
            });
        }

        int chunkSize = 5_000;
        for (int i = 0; i < largeBatch.size(); i += chunkSize) {
            List<Object[]> chunk =
                    largeBatch.subList(i, Math.min(i + chunkSize, largeBatch.size()));

            client.batchUpdateWithRetry(
                    "INSERT INTO test_debug (id, name, created_at) VALUES (?, ?, ?)",
                    chunk,
                    RetryPolicy.defaultPolicy()
            );
        }

        System.out.println("Chunked batch insert done");

        /* ============================================================
         * 8️⃣ ASYNC BATCH WRITER
         * ============================================================ */
        AsyncBatchConfig asyncConfig = new AsyncBatchConfig(
                2_000,     // batch size
                1_000,     // flush interval ms
                100_000,   // queue capacity
                1          // worker threads
        );

        AsyncClickHouseBatchWriter asyncWriter =
                new AsyncClickHouseBatchWriter(
                        client,
                        "INSERT INTO test_debug (id, name, created_at) VALUES (?, ?, ?)",
                        RetryPolicy.defaultPolicy(),
                        asyncConfig
                );

        // Enqueue data (non-blocking)
        for (int i = 1; i <= 20_000; i++) {
            asyncWriter.enqueue(new Object[]{
                    (long) (100_000 + i),
                    "async-" + i,
                    LocalDateTime.now()
            });
        }

        System.out.println("Async enqueue done");

        /* ============================================================
         * 9️⃣ GRACEFUL SHUTDOWN HOOK
         * ============================================================ */
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println("Shutdown hook triggered");
                    asyncWriter.close();  // flush remaining
                    client.close();       // close pool
                })
        );

        /* ============================================================
         * 🔟 MANUAL CLOSE (FOR JOB / TEST)
         * ============================================================ */
        Thread.sleep(2_000); // let async writer flush
        asyncWriter.close();
        client.close();

        System.out.println("Done.");
    }
}
