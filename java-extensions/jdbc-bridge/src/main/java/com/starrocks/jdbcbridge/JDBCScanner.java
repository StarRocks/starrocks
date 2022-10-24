// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JDBCScanner {
    private String driverLocation;
    private HikariDataSource dataSource;
    private JDBCScanContext scanContext;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private ResultSetMetaData resultSetMetaData;
    private List<String> resultColumnClassNames;
    private List<Object[]> resultChunk;
    private int resultNumRows = 0;

    public JDBCScanner(String driverLocation, JDBCScanContext scanContext) {
        this.driverLocation = driverLocation;
        this.scanContext = scanContext;
    }

    public void open() throws Exception {

        dataSource = DataSourceCache.getInstance().getSource(scanContext.getJdbcURL(), () -> {
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(scanContext.getDriverClassName());
            config.setJdbcUrl(scanContext.getJdbcURL());
            config.setUsername(scanContext.getUser());
            config.setPassword(scanContext.getPassword());
            config.setMaximumPoolSize(scanContext.getConnectionPoolSize());
            config.setMinimumIdle(scanContext.getMinimumIdleConnections());
            config.setIdleTimeout(scanContext.getConnectionIdleTimeoutMs());
            dataSource = new HikariDataSource(config);
            return dataSource;
        });

        connection = dataSource.getConnection();
        statement = connection.createStatement();
        statement.setFetchSize(scanContext.getStatementFetchSize());
        statement.execute(scanContext.getSql());
        resultSet = statement.getResultSet();
        resultSetMetaData = resultSet.getMetaData();
        resultColumnClassNames = new ArrayList<>(resultSetMetaData.getColumnCount());
        resultChunk = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            resultColumnClassNames.add(resultSetMetaData.getColumnClassName(i));
            Class<?> clazz = Class.forName(resultSetMetaData.getColumnClassName(i));
            resultChunk.add((Object[]) Array.newInstance(clazz, scanContext.getStatementFetchSize()));
        }
    }

    // used for cpp interface
    public List<String> getResultColumnClassNames() {
        return resultColumnClassNames;
    }

    public boolean hasNext() throws Exception {
        return resultSet.next();
    }

    // return columnar chunk
    public List<Object[]> getNextChunk() throws Exception {
        int chunkSize = scanContext.getStatementFetchSize();
        int columnCount = resultSetMetaData.getColumnCount();
        resultNumRows = 0;
        do {
            for (int i = 0; i < columnCount; i++) {
                resultChunk.get(i)[resultNumRows] = resultSet.getObject(i + 1);
            }
            resultNumRows++;
        } while (resultNumRows < chunkSize && resultSet.next());
        return resultChunk;
    }

    public int getResultNumRows() {
        return resultNumRows;
    }


    public void close() throws Exception {
        if (resultSet != null) {
            resultSet.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
