package com.viettel.vocs.clickhouse.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.Properties;

public class ClickHouseDataSourceFactory {

    private final HikariDataSource dataSource;

    public ClickHouseDataSourceFactory(Properties p) {

        String jdbcUrl = String.format(
                "jdbc:clickhouse://%s:%s/%s",
                require(p, "clickhouse.host"),
                require(p, "clickhouse.port"),
                require(p, "clickhouse.database")
        );

        Properties dsProps = new Properties();
        dsProps.setProperty("user", p.getProperty("clickhouse.user", "default"));
        dsProps.setProperty("password", p.getProperty("clickhouse.password", ""));

        setIfPresent(dsProps, "socket_timeout", "clickhouse.socketTimeout", p);
        setIfPresent(dsProps, "connection_timeout", "clickhouse.connectionTimeout", p);

        if ("true".equalsIgnoreCase(p.getProperty("clickhouse.compress"))) {
            dsProps.setProperty("compress", "1");
        }

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        cfg.setDataSourceProperties(dsProps);

        cfg.setMaximumPoolSize(intProp(p, "clickhouse.pool.maxSize", 20));
        cfg.setMinimumIdle(intProp(p, "clickhouse.pool.minIdle", 5));
        cfg.setConnectionTimeout(longProp(p, "clickhouse.pool.connectionTimeoutMs", 10000));
        cfg.setIdleTimeout(longProp(p, "clickhouse.pool.idleTimeoutMs", 600000));
        cfg.setMaxLifetime(longProp(p, "clickhouse.pool.maxLifetimeMs", 1800000));

        cfg.setPoolName("clickhouse-hikari");
        cfg.setAutoCommit(true);

        this.dataSource = new HikariDataSource(cfg);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void close() {
        dataSource.close();
    }

    private static String require(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing property: " + key);
        }
        return v;
    }

    private static void setIfPresent(Properties target, String jdbcKey, String propKey, Properties src) {
        String v = src.getProperty(propKey);
        if (v != null && !v.trim().isEmpty()) {
            target.setProperty(jdbcKey, v);
        }
    }

    private static int intProp(Properties p, String key, int def) {
        return Integer.parseInt(p.getProperty(key, String.valueOf(def)));
    }

    private static long longProp(Properties p, String key, long def) {
        return Long.parseLong(p.getProperty(key, String.valueOf(def)));
    }
}
