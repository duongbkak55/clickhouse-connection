package com.viettel.vocs.clickhouse.jdbc;

import com.viettel.vocs.clickhouse.retry.RetryExecutor;
import com.viettel.vocs.clickhouse.retry.RetryPolicy;
import com.viettel.vocs.clickhouse.retry.RetryableExceptionClassifier;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseJdbcTemplate {

    private final DataSource dataSource;

    public ClickHouseJdbcTemplate(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <T> List<T> query(String sql, RowMapper<T> mapper, Object... params) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)
        ) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapper.mapRow(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Query failed", e);
        }
    }

    public <T> T queryForObject(String sql, RowMapper<T> mapper, Object... params) {
        List<T> list = query(sql, mapper, params);
        return list.isEmpty() ? null : list.get(0);
    }

    public int update(String sql, Object... params) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)
        ) {
            bind(ps, params);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Update failed", e);
        }
    }

    public int batchUpdate(String sql, List<Object[]> batchArgs) {
        if (batchArgs == null || batchArgs.isEmpty()) {
            return 0;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (Object[] args : batchArgs) {
                bind(ps, args);
                ps.addBatch();
            }

            int[] results = ps.executeBatch();
            conn.commit();

            return sum(results);

        } catch (SQLException e) {
            throw new RuntimeException("Batch update failed", e);
        }
    }


    public int batchUpdateWithRetry(
            String sql,
            List<Object[]> batchArgs,
            RetryPolicy retryPolicy
    ) {
        return RetryExecutor.execute(retryPolicy, () -> {
            try {
                return batchUpdate(sql, batchArgs);
            } catch (RuntimeException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException) {
                    SQLException sqlEx = (SQLException) cause;
                    if (RetryableExceptionClassifier.isRetryable(sqlEx)) {
                        throw e; // retry
                    }
                }
                // Non-retryable → fail fast
                throw e;
            }
        });
    }


    private void bind(PreparedStatement ps, Object[] params) throws SQLException {
        if (params == null) return;
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private int sum(int[] arr) {
        int total = 0;
        for (int v : arr) {
            if (v > 0) total += v;
        }
        return total;
    }
}
