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

package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class JDBCMetadata implements ConnectorMetadata {

    private static Logger LOG = LogManager.getLogger(JDBCMetadata.class);

    private Map<String, String> properties;
    JDBCSchemaResolver schemaResolver;
    private String catalogName;

    private JDBCMetaCache<String, Database> dbCache;
    private JDBCMetaCache<JDBCTableName, List<String>> partitionNamesCache;
    private JDBCMetaCache<JDBCTableName, Integer> tableIdCache;
    private JDBCMetaCache<JDBCTableName, Table> tableInstanceCache;
    private JDBCMetaCache<JDBCTableName, List<Partition>> partitionInfoCache;

    private HikariDataSource dataSource;
    private static final ExecutorService NETWORK_TIMEOUT_EXECUTOR = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("jdbc-network-timeout-%d").build());

    public JDBCMetadata(Map<String, String> properties, String catalogName) {
        this(properties, catalogName, null);
    }

    public JDBCMetadata(Map<String, String> properties, String catalogName, HikariDataSource dataSource) {
        this.properties = properties;
        this.catalogName = catalogName;
        try {
            String driverName = getDriverName();
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new StarRocksConnectorException("doesn't find class: " + e.getMessage());
        }
        if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("mysql")) {
            schemaResolver = new MysqlSchemaResolver();
        } else if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("postgresql")) {
            schemaResolver = new PostgresSchemaResolver();
        } else if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("mariadb")) {
            schemaResolver = new MysqlSchemaResolver();
        } else if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("clickhouse")) {
            schemaResolver = new ClickhouseSchemaResolver(properties);
        } else if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("oracle")) {
            schemaResolver = new OracleSchemaResolver();
        } else if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("sqlserver")) {
            schemaResolver = new SqlServerSchemaResolver();
        } else {
            LOG.warn("{} not support yet", properties.get(JDBCResource.DRIVER_CLASS));
            throw new StarRocksConnectorException(properties.get(JDBCResource.DRIVER_CLASS) + " not support yet");
        }
        if (dataSource == null) {
            dataSource = createHikariDataSource();
        }
        this.dataSource = dataSource;
        checkAndSetSupportPartitionInformation();
        createMetaAsyncCacheInstances(properties);
    }

    private String getDriverName() {
        String driverName = properties.get(JDBCResource.DRIVER_CLASS);
        // use org.mariadb.jdbc.Driver for mysql because of gpl protocol
        if (driverName.contains("mysql")) {
            driverName = "org.mariadb.jdbc.Driver";
        }
        return driverName;
    }

    String getJdbcUrl() {
        String jdbcUrl = properties.get(JDBCResource.URI);
        // use org.mariadb.jdbc.Driver for mysql because of gpl protocol
        if (jdbcUrl.startsWith("jdbc:mysql")) {
            jdbcUrl = jdbcUrl.replaceFirst("jdbc:mysql", "jdbc:mariadb");
        }
        return jdbcUrl;
    }

    private void createMetaAsyncCacheInstances(Map<String, String> properties) {
        dbCache = new JDBCMetaCache<>(properties, false);
        partitionNamesCache = new JDBCMetaCache<>(properties, false);
        tableIdCache = new JDBCMetaCache<>(properties, true);
        tableInstanceCache = new JDBCMetaCache<>(properties, false);
        partitionInfoCache = new JDBCMetaCache<>(properties, false);
    }

    public void checkAndSetSupportPartitionInformation() {
        try (Connection connection = getConnection()) {
            schemaResolver.checkAndSetSupportPartitionInformation(connection);
        } catch (SQLException e) {
            throw new StarRocksConnectorException(
                    "check and set support partition information for JDBC catalog fail!", e);
        }
    }

    private HikariDataSource createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(getJdbcUrl());
        config.setUsername(properties.get(JDBCResource.USER));
        config.setPassword(properties.get(JDBCResource.PASSWORD));
        config.setDriverClassName(getDriverName());
        config.setMaximumPoolSize(Config.jdbc_connection_pool_size);
        config.setMinimumIdle(Config.jdbc_minimum_idle_connections);
        config.setIdleTimeout(Config.jdbc_connection_idle_timeout_ms);
        config.setConnectionTimeout(Config.jdbc_connection_timeout_ms);
        return new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            // Set network timeout only when it's configured (>=0)
            if (Config.jdbc_network_timeout_ms >= 0L) {
                int networkTimeoutMs = (int) Math.min(Config.jdbc_network_timeout_ms, (long) Integer.MAX_VALUE);
                connection.setNetworkTimeout(NETWORK_TIMEOUT_EXECUTOR, networkTimeoutMs);
            }
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.JDBC;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        try (Connection connection = getConnection()) {
            return Lists.newArrayList(schemaResolver.listSchemas(connection));
        } catch (SQLException e) {
            throw new StarRocksConnectorException("list db names for JDBC catalog fail!", e);
        }
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        // NOTE: We use manual cache control (getIfPresent + put) instead of the lambda-based approach
        // for the following reason:
        //
        // The lambda in getTable() can return null when the table doesn't exist, but JDBCMetaCache.get()
        // uses Objects.requireNonNull() which throws NullPointerException when the lambda returns null.
        //
        // For getDb(), we need to return null for non-existent databases (a valid result, not an error),
        // so we manually control the cache to avoid the NPE issue:
        // 1. Use getIfPresent() to check cache without triggering the lambda
        // 2. On cache miss, query directly and only cache successful (non-null) results
        // 3. Return null for non-existent databases or SQLException without caching

        // Check cache first
        Database cached = dbCache.getIfPresent(name);
        if (cached != null) {
            return cached;
        }

        // Cache miss - query directly
        try (Connection connection = getConnection()) {
            if (schemaResolver.databaseExists(connection, name)) {
                Database db = new Database(0, name);
                // Only cache on success to avoid caching null values
                dbCache.put(name, db);
                return db;
            }
            // Database doesn't exist - don't cache null
            return null;
        } catch (SQLException e) {
            // From getConnection() or databaseExists()
            LOG.warn("Failed to check database existence for {}.{}: {}",
                    catalogName, name, e.getMessage());
            // Exception occurred - don't cache null
            return null;
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try (Connection connection = getConnection()) {
            try (ResultSet resultSet = schemaResolver.getTables(connection, dbName)) {
                ImmutableList.Builder<String> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    list.add(tableName);
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw new StarRocksConnectorException("list table names for JDBC catalog fail!", e);
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        JDBCTableName jdbcTable = new JDBCTableName(null, dbName, tblName);
        return tableInstanceCache.get(jdbcTable,
                k -> {
                    try (Connection connection = getConnection();
                            ResultSet columnSet = schemaResolver.getColumns(connection, dbName, tblName)) {
                        List<Column> fullSchema = schemaResolver.convertToSRTable(columnSet);
                        List<Column> partitionColumns = Lists.newArrayList();
                        if (schemaResolver.isSupportPartitionInformation()) {
                            partitionColumns = listPartitionColumns(dbName, tblName, fullSchema);
                        }
                        if (fullSchema.isEmpty()) {
                            return null;
                        }

                        Integer tableId = tableIdCache.getPersistentCache(jdbcTable,
                                j -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
                        return schemaResolver.getTable(tableId, tblName, fullSchema,
                                partitionColumns, dbName, catalogName, properties);
                    } catch (SQLException | DdlException e) {
                        LOG.warn("get table for JDBC catalog fail!", e);
                        return null;
                    }
                });
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        return partitionNamesCache.get(new JDBCTableName(null, databaseName, tableName),
                k -> {
                    try (Connection connection = getConnection()) {
                        return schemaResolver.listPartitionNames(connection, databaseName, tableName);
                    } catch (SQLException e) {
                        throw new StarRocksConnectorException("list partition names for JDBC catalog fail!",
                                e);
                    }
                });
    }

    public List<Column> listPartitionColumns(String databaseName, String tableName, List<Column> fullSchema) {
        try (Connection connection = getConnection()) {
            Set<String> partitionColumnNames = schemaResolver.listPartitionColumns(connection, databaseName, tableName)
                    .stream().map(String::toLowerCase).collect(Collectors.toSet());
            if (!partitionColumnNames.isEmpty()) {
                return fullSchema.stream()
                        .filter(column -> partitionColumnNames.contains(column.getName().toLowerCase()))
                        .collect(Collectors.toList());
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException | StarRocksConnectorException e) {
            LOG.warn("list partition columns for JDBC catalog fail!", e);
            return Lists.newArrayList();
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        JDBCTable jdbcTable = (JDBCTable) table;
        List<Partition> partitions = partitionInfoCache.get(
                new JDBCTableName(null, jdbcTable.getCatalogDBName(), jdbcTable.getName()),
                k -> {
                    try (Connection connection = getConnection()) {
                        List<Partition> partitionsForCache = schemaResolver.getPartitions(connection, table);
                        if (!partitionsForCache.isEmpty()) {
                            return partitionsForCache;
                        }
                        return Lists.newArrayList();
                    } catch (SQLException e) {
                        throw new StarRocksConnectorException("get partitions for JDBC catalog fail!", e);
                    }
                });

        String maxInt = IntLiteral.createMaxValue(IntegerType.INT).getStringValue();
        String maxDate = DateLiteral.createMaxValue(DateType.DATE).getStringValue();

        ImmutableList.Builder<PartitionInfo> list = ImmutableList.builder();
        if (partitions.isEmpty()) {
            return Lists.newArrayList();
        }
        for (Partition partition : partitions) {
            String partitionName = partition.getPartitionName();
            if (partitionNames != null && partitionNames.contains(partitionName)) {
                list.add(partition);
            }
            // Determine boundary value
            if (partitionName.equalsIgnoreCase(PartitionUtil.MYSQL_PARTITION_MAXVALUE)) {
                if (partitionNames != null && (partitionNames.contains(maxInt)
                        || partitionNames.contains(maxDate))) {
                    list.add(partition);
                }
            }
        }
        return list.build();
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        JDBCTable jdbcTable = (JDBCTable) table;
        JDBCTableName jdbcTableName = new JDBCTableName(null, jdbcTable.getCatalogDBName(), jdbcTable.getName());
        if (!onlyCachedPartitions) {
            tableInstanceCache.invalidate(jdbcTableName);
        }
        partitionNamesCache.invalidate(jdbcTableName);
        partitionInfoCache.invalidate(jdbcTableName);
    }

    public void refreshCache(Map<String, String> properties) {
        createMetaAsyncCacheInstances(properties);
    }

    @Override
    public void shutdown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
