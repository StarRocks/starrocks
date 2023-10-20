// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
<<<<<<< HEAD
import java.sql.Statement;
=======
import java.sql.Timestamp;
import java.time.LocalDateTime;
>>>>>>> ce80eca948 ([BugFix] enable non-general jdbc class can be loaded (#32702))
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
<<<<<<< HEAD
=======
import java.util.Locale;
import java.util.Set;

>>>>>>> ce80eca948 ([BugFix] enable non-general jdbc class can be loaded (#32702))

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
    ClassLoader classLoader;


    public JDBCScanner(String driverLocation, JDBCScanContext scanContext) {
        this.driverLocation = driverLocation;
        this.scanContext = scanContext;
    }

    public void open() throws Exception {
        String key = scanContext.getUser() + "/" + scanContext.getJdbcURL();
        URL driverURL = new File(driverLocation).toURI().toURL();
        DataSourceCache.DataSourceCacheItem cacheItem = DataSourceCache.getInstance().getSource(key, () -> {
            ClassLoader classLoader = URLClassLoader.newInstance(new URL[] {
                    driverURL,
            });
            Thread.currentThread().setContextClassLoader(classLoader);
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(scanContext.getDriverClassName());
            config.setJdbcUrl(scanContext.getJdbcURL());
            config.setUsername(scanContext.getUser());
            config.setPassword(scanContext.getPassword());
            config.setMaximumPoolSize(scanContext.getConnectionPoolSize());
            config.setMinimumIdle(scanContext.getMinimumIdleConnections());
            config.setIdleTimeout(scanContext.getConnectionIdleTimeoutMs());
            HikariDataSource hikariDataSource = new HikariDataSource(config);
            // hikari doesn't support user-provided class loader, we should save them ourselves to ensure that
            // the classes of result data are loaded by the same class loader, otherwise we may encounter
            // ArrayStoreException in getNextChunk
            return new DataSourceCache.DataSourceCacheItem(hikariDataSource, classLoader);
        });
        dataSource = cacheItem.getHikariDataSource();
        classLoader = cacheItem.getClassLoader();

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
            Class<?> clazz = classLoader.loadClass(resultSetMetaData.getColumnClassName(i));
            if (isGeneralJDBCClassType(clazz)) {
                resultChunk.add((Object[]) Array.newInstance(clazz, scanContext.getStatementFetchSize()));
            } else {
                resultChunk.add((Object[]) Array.newInstance(String.class, scanContext.getStatementFetchSize()));
            }
        }
    }

    private static final Set<Class<?>> GENERAL_JDBC_CLASS_SET =  new HashSet<>(Arrays.asList(
            Boolean.class,
            Short.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            BigInteger.class,
            BigDecimal.class,
            java.sql.Date.class,
            Timestamp.class,
            LocalDateTime.class,
            String.class
    ));

    private boolean isGeneralJDBCClassType(Class<?> clazz) {
        return GENERAL_JDBC_CLASS_SET.contains(clazz);
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
                } else if (dataColumn instanceof String[] && resultObject instanceof String) {
                    // if both sides are String, assign value directly to avoid additional calls to getString
                    dataColumn[resultNumRows] = resultObject;
                } else if (!(dataColumn instanceof String[])) {
                    // for other general class type, assign value directly
                    dataColumn[resultNumRows] = resultObject;
                } else {
                    // for non-general class type, use string representation
                    dataColumn[resultNumRows] = resultSet.getString(i + 1);
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
