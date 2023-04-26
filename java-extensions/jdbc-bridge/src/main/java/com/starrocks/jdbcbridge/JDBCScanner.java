// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        String key = scanContext.getUser() + "/" + scanContext.getJdbcURL();
        dataSource = DataSourceCache.getInstance().getSource(key, () -> {
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
                Object[] dataColumn = resultChunk.get(i);
                Object resultObject = resultSet.getObject(i + 1);
                // in some cases, the real java class type of result is not consistent with the type from
                // resultSetMetadata,
                // for example,FLOAT type in oracle gives java.lang.Double type in resultSetMetaData,
                // but the result type is BigDecimal when we getObject from resultSet.
                // So we choose to convert the value to the target type here.
                if (resultObject == null) {
                    dataColumn[resultNumRows] = null;
                } else if (dataColumn instanceof Short[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).shortValue();
                } else if (dataColumn instanceof Integer[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).intValue();
                } else if (dataColumn instanceof Long[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).longValue();
                } else if (dataColumn instanceof Float[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).floatValue();
                } else if (dataColumn instanceof Double[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).doubleValue();
                } else {
                    dataColumn[resultNumRows] = resultObject;
                }
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
