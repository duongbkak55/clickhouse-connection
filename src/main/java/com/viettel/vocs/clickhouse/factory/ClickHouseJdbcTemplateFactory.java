package com.viettel.vocs.clickhouse.factory;

import com.viettel.vocs.clickhouse.config.ClickHouseDataSourceFactory;
import com.viettel.vocs.clickhouse.config.ClickHousePropertiesLoader;
import com.viettel.vocs.clickhouse.jdbc.ClickHouseJdbcTemplate;

import java.util.Properties;

public final class ClickHouseJdbcTemplateFactory {

    private static volatile ClickHouseJdbcTemplate INSTANCE;
    private static ClickHouseDataSourceFactory DS_FACTORY;

    private ClickHouseJdbcTemplateFactory() {}

//    public static ClickHouseJdbcTemplate get() {
//        if (INSTANCE == null) {
//            synchronized (ClickHouseJdbcTemplateFactory.class) {
//                if (INSTANCE == null) {
//                    Properties props = ClickHousePropertiesLoader.load();
//                    DS_FACTORY = new ClickHouseDataSourceFactory(props);
//                    INSTANCE = new ClickHouseJdbcTemplate(
//                            DS_FACTORY.getDataSource()
//                    );
//                }
//            }
//        }
//        return INSTANCE;
//    }

    public static void shutdown() {
        if (DS_FACTORY != null) {
            DS_FACTORY.close();
        }
    }
}
