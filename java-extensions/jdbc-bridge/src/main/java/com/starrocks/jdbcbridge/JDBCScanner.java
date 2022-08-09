// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

    public JDBCScanner(String driverLocation, JDBCScanContext scanContext) {
        this.driverLocation = driverLocation;
        this.scanContext = scanContext;
    }

    public void open() throws Exception {
        final String user = scanContext.getUser();
        final String pass = scanContext.getPassword();
        final String url = scanContext.getJdbcURL();

        dataSource = DataSourceCache.getInstance().getSource(driverLocation, user, pass, url);
        if (dataSource == null) {
            // create new datasource
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(scanContext.getDriverClassName());
            config.setJdbcUrl(scanContext.getJdbcURL());
            config.setUsername(scanContext.getUser());
            config.setPassword(scanContext.getPassword());
            config.setMaximumPoolSize(scanContext.getConnectionPoolSize());
            dataSource = new HikariDataSource(config);
            DataSourceCache.getInstance().put(driverLocation, user, pass, url, dataSource);
        }

        connection = dataSource.getConnection();
        statement = connection.createStatement();
        statement.setFetchSize(scanContext.getStatementFetchSize());
        statement.execute(scanContext.getSql());
        resultSet = statement.getResultSet();
        resultSetMetaData = resultSet.getMetaData();
        resultColumnClassNames = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            resultColumnClassNames.add(resultSetMetaData.getColumnClassName(i));
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
    public List<List<Object>> getNextChunk() throws Exception {
        int chunkSize = scanContext.getStatementFetchSize();
        int columnCount = resultSetMetaData.getColumnCount();
        List<List<Object>> chunk = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            chunk.add(new ArrayList<Object>(chunkSize));
        }
        int numRows = 0;
        do {
            for (int i = 0; i < columnCount; i++) {
                chunk.get(i).add(resultSet.getObject(i + 1));
            }
            numRows++;
        } while (numRows < chunkSize && resultSet.next());
        return chunk;
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
