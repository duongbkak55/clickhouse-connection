package com.viettel.vocs.clickhouse;

import com.viettel.vocs.clickhouse.jdbc.ClickHouseJdbcTemplate;
import com.viettel.vocs.clickhouse.jdbc.RowMapper;
import com.viettel.vocs.clickhouse.retry.RetryPolicy;

import java.util.List;

class ClickHouseClientImpl implements ClickHouseClient {

    private final ClickHouseJdbcTemplate jdbcTemplate;
    private final Runnable shutdown;

    ClickHouseClientImpl(
            ClickHouseJdbcTemplate jdbcTemplate,
            Runnable shutdown
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.shutdown = shutdown;
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> mapper, Object... params) {
        return jdbcTemplate.query(sql, mapper, params);
    }

    @Override
    public <T> T queryForObject(String sql, RowMapper<T> mapper, Object... params) {
        return jdbcTemplate.queryForObject(sql, mapper, params);
    }

    @Override
    public int update(String sql, Object... params) {
        return jdbcTemplate.update(sql, params);
    }

    @Override
    public int batchUpdate(String sql, List<Object[]> batchArgs) {
        return jdbcTemplate.batchUpdate(sql, batchArgs);
    }

    @Override
    public int batchUpdateWithRetry(
            String sql,
            List<Object[]> batchArgs,
            RetryPolicy retryPolicy
    ) {
        return jdbcTemplate.batchUpdateWithRetry(
                sql,
                batchArgs,
                retryPolicy
        );
    }


    @Override
    public void close() {
        shutdown.run();
    }
}
