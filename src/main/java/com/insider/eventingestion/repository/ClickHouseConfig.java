package com.insider.eventingestion.repository;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

@Configuration
public class ClickHouseConfig {

    @Value("${app.clickhouse.url}")
    private String url;

    @Value("${app.clickhouse.user}")
    private String user;

    @Value("${app.clickhouse.password}")
    private String password;

    @Bean
    public DataSource clickHouseDataSource() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return new ClickHouseDataSource(url, properties);
    }
}
