package com.viettel.vocs.clickhouse;

import com.viettel.vocs.clickhouse.config.ClickHouseDataSourceFactory;
import com.viettel.vocs.clickhouse.jdbc.ClickHouseJdbcTemplate;

import java.util.Properties;

public class ClickHouseClientBuilder {

    private Properties properties;

    public ClickHouseClientBuilder withProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    public ClickHouseClientBuilder withPropertiesFile(String resourceName) {
        this.properties =
                com.viettel.vocs.clickhouse.config.ClickHousePropertiesLoader
                        .load(resourceName);
        return this;
    }


    public ClickHouseClient build() {

        if (properties == null) {
            throw new IllegalStateException("ClickHouse properties not set");
        }

        ClickHouseDataSourceFactory dsFactory =
                new ClickHouseDataSourceFactory(properties);

        ClickHouseJdbcTemplate jdbcTemplate =
                new ClickHouseJdbcTemplate(dsFactory.getDataSource());

        return new ClickHouseClientImpl(
                jdbcTemplate,
                dsFactory::close
        );
    }

}
