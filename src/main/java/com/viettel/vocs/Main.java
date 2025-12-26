package com.viettel.vocs;

import com.viettel.vocs.clickhouse.ClickHouseClient;
import com.viettel.vocs.clickhouse.ClickHouseClientBuilder;
import com.viettel.vocs.clickhouse.async.AsyncBatchConfig;
import com.viettel.vocs.clickhouse.async.AsyncClickHouseBatchWriter;
import com.viettel.vocs.clickhouse.retry.RetryPolicy;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        ClickHouseClient clickHouseClient =
                new ClickHouseClientBuilder()
                        .withPropertiesFile("clickhouse.properties")
                        .build();

        Long count = clickHouseClient.queryForObject(
                "SELECT count() FROM system.tables",
                rs -> rs.getLong(1)
        );
        List<Object[]> allRows = new ArrayList<>();

        for (int i = 1; i <= 100_000; i++) {
            allRows.add(new Object[]{
                    (long) i,
                    "user-" + i,
                    LocalDateTime.now()
            });
        }

        for (List<Object[]> chunk : chunk(allRows, 5_000)) {
            clickHouseClient.batchUpdateWithRetry(
                    "INSERT INTO test_debug (id, name, created_at) VALUES (?, ?, ?)",
                    chunk,
                    RetryPolicy.defaultPolicy()
            );
        }

        AsyncClickHouseBatchWriter writer =
                new AsyncClickHouseBatchWriter(
                        clickHouseClient,
                        "INSERT INTO test_debug (id, name, created_at) VALUES (?, ?, ?)",
                        RetryPolicy.defaultPolicy(),
                        AsyncBatchConfig.defaultConfig()
                );

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println("Shutting down ClickHouse writer...");
                    writer.close();
                    clickHouseClient.close();
                })
        );

        writer.enqueue(new Object[]{100_001L, "user-100001", LocalDateTime.now()});


        clickHouseClient.close();

    }



    public static <T> List<List<T>> chunk(List<T> data, int size) {
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < data.size(); i += size) {
            chunks.add(data.subList(i, Math.min(i + size, data.size())));
        }
        return chunks;
    }

}