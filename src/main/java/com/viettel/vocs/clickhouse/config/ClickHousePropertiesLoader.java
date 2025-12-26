package com.viettel.vocs.clickhouse.config;

import java.io.InputStream;
import java.util.Properties;

public final class ClickHousePropertiesLoader {

    private ClickHousePropertiesLoader() {}

    public static Properties load(String configFile) {
        try (InputStream is = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(configFile)) {

            if (is == null) {
                throw new IllegalStateException("Cannot find " + configFile);
            }

            Properties props = new Properties();
            props.load(is);
            return props;

        } catch (Exception e) {
            throw new RuntimeException("Failed to load ClickHouse properties", e);
        }
    }
}
