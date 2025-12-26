package com.viettel.vocs.clickhouse;

import com.viettel.vocs.clickhouse.jdbc.RowMapper;
import com.viettel.vocs.clickhouse.retry.RetryPolicy;

import java.util.List;

public interface ClickHouseClient extends AutoCloseable {

    <T> List<T> query(String sql, RowMapper<T> mapper, Object... params);

    <T> T queryForObject(String sql, RowMapper<T> mapper, Object... params);

    int update(String sql, Object... params);

    /**
     * Batch insert/update
     *
     * @param sql INSERT SQL with placeholders
     * @param batchArgs list of row parameters
     * @return total affected rows
     */
    int batchUpdate(String sql, List<Object[]> batchArgs);


    int batchUpdateWithRetry(
            String sql,
            List<Object[]> batchArgs,
            RetryPolicy retryPolicy
    );


    @Override
    void close();
}
