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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.statistics.ConnectorNdvEstimator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class JDBCMetadata implements ConnectorMetadata {

    private static Logger LOG = LogManager.getLogger(JDBCMetadata.class);

    private Map<String, String> properties;
    JDBCSchemaResolver schemaResolver;
    private String catalogName;

    private JDBCMetaCache<String, Database> dbCache;
    private JDBCMetaCache<JDBCTableName, List<String>> partitionNamesCache;
    private JDBCMetaCache<JDBCTableName, Long> tableIdCache;
    private JDBCMetaCache<JDBCTableName, Table> tableInstanceCache;
    private JDBCMetaCache<JDBCTableName, List<Partition>> partitionInfoCache;
    // Async statistics cache: never blocks planning. On cold start it reports "unknown" for this
    // planning round and loads in the background; refreshAfterWrite keeps the entry warm with an
    // async reload. One entry holds the table's row count *and* its column statistics, because a
    // dialect reads both over the same connection and acquiring that connection costs far more
    // than the catalog queries themselves (tenths of a millisecond).
    private AsyncLoadingCache<JDBCTableName, JdbcTableStats> tableStatsCache;

    private HikariDataSource dataSource;
    private static final ExecutorService NETWORK_TIMEOUT_EXECUTOR = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("jdbc-network-timeout-%d").build());
    private static final ExecutorService TABLE_STATS_EXECUTOR = Executors.newFixedThreadPool(
            2, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("jdbc-table-stats-%d").build());

    // A most-common-value list is treated as covering every non-null row when it comes this close
    // to doing so; the source reports frequencies as single-precision floats, which do not sum back
    // to exactly 1 even for a column whose values are all listed.

    // HikariCP connection lifecycle constants
    static final long MINIMUM_MAX_LIFETIME_MS = 30_000L;
    static final long DEFAULT_MAX_LIFETIME_MS = 300_000L;
    static final long MINIMUM_KEEPALIVE_TIME_MS = 30_000L;
    static final long KEEPALIVE_DISABLED = 0L;
    private static final List<String> SUPPORTED_SCHEMA_RESOLVERS =
            ImmutableList.of("postgresql", "mysql", "oracle", "sqlserver", "clickhouse");

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
        schemaResolver = createSchemaResolver();
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

    /**
     * Creates the appropriate SchemaResolver based on configuration.
     * Priority:
     * 1. If schema_resolver property is specified, use that resolver
     * 2. Otherwise, auto-detect based on driver class name
     */
    private JDBCSchemaResolver createSchemaResolver() {
        // Check for explicit schema_resolver property first
        String schemaResolverType = properties.get(JDBCResource.SCHEMA_RESOLVER);
        if (schemaResolverType != null && !schemaResolverType.trim().isEmpty()) {
            return createSchemaResolverFromProperty(schemaResolverType.trim());
        }

        // Fall back to driver class name detection
        String driverClass = properties.get(JDBCResource.DRIVER_CLASS).toLowerCase();
        if (driverClass.contains("mysql")) {
            return new MysqlSchemaResolver();
        } else if (driverClass.contains("postgresql")) {
            return new PostgresSchemaResolver();
        } else if (driverClass.contains("mariadb")) {
            return new MysqlSchemaResolver();
        } else if (driverClass.contains("clickhouse")) {
            return new ClickhouseSchemaResolver(properties);
        } else if (driverClass.contains("oracle")) {
            return new OracleSchemaResolver(properties);
        } else if (driverClass.contains("sqlserver")) {
            return new SqlServerSchemaResolver();
        } else {
            LOG.warn("{} not support yet", properties.get(JDBCResource.DRIVER_CLASS));
            throw new StarRocksConnectorException(properties.get(JDBCResource.DRIVER_CLASS) + " not support yet");
        }
    }

    /**
     * Creates a SchemaResolver from the explicitly specified resolver type.
     * @param resolverType the type of resolver (e.g., "postgresql", "mysql")
     * @return the appropriate JDBCSchemaResolver instance
     */
    private JDBCSchemaResolver createSchemaResolverFromProperty(String resolverType) {
        switch (resolverType.toLowerCase()) {
            case "postgresql":
                return new PostgresSchemaResolver();
            case "mysql":
                return new MysqlSchemaResolver();
            case "oracle":
                return new OracleSchemaResolver();
            case "sqlserver":
                return new SqlServerSchemaResolver();
            case "clickhouse":
                return new ClickhouseSchemaResolver(properties);
            default:
                throw new StarRocksConnectorException(
                        "Unknown schema_resolver: " + resolverType +
                        ". Supported values: " + String.join(", ", SUPPORTED_SCHEMA_RESOLVERS));
        }
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
        tableStatsCache = buildTableStatsCache(properties);
    }

    private AsyncLoadingCache<JDBCTableName, JdbcTableStats> buildTableStatsCache(Map<String, String> properties) {
        // The statistics cache is always enabled regardless of jdbc_meta_cache_enable.
        // jdbc_meta_cache_enable controls schema metadata freshness; statistics are a
        // separate concern and must never block planning — async loading is mandatory.
        // Each parameter can be overridden per-catalog via the JDBC catalog properties map.
        // The jdbc_row_count_cache_* names predate column statistics; they now govern the whole
        // statistics entry, which is loaded and expired as one unit.
        long refreshSec = Long.parseLong(properties.getOrDefault(
                "jdbc_row_count_cache_refresh_sec",
                String.valueOf(Config.jdbc_row_count_cache_refresh_sec)));
        long expireSec = Long.parseLong(properties.getOrDefault(
                "jdbc_row_count_cache_expire_sec",
                String.valueOf(Config.jdbc_row_count_cache_expire_sec)));
        long maxSize = Long.parseLong(properties.getOrDefault(
                "jdbc_row_count_cache_max_size",
                String.valueOf(Config.jdbc_row_count_cache_max_size)));
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshSec, TimeUnit.SECONDS)
                .expireAfterWrite(expireSec, TimeUnit.SECONDS)
                .executor(TABLE_STATS_EXECUTOR)
                .buildAsync(key -> loadTableStats(key));
    }

    /**
     * Load one table's statistics. Anything that goes wrong — an unreachable source, a table the
     * dialect cannot describe, a source that has never analyzed the table — yields
     * {@link JdbcTableStats#unknown()}, never a stand-in number. Reporting a default as though it
     * were read from the source is worse than reporting nothing: the optimizer has a sound answer
     * for "unknown" and none for "one row, trust me".
     */
    private JdbcTableStats loadTableStats(JDBCTableName key) {
        try (Connection connection = getConnection()) {
            return schemaResolver.getTableStatistics(connection, key.getDatabaseName(), key.getTableName())
                    .orElse(JdbcTableStats.unknown());
        } catch (Exception e) {
            LOG.warn("Failed to load statistics for {}.{}: {}", key.getDatabaseName(), key.getTableName(),
                    e.getMessage());
            return JdbcTableStats.unknown();
        }
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
        // Before the pool is built, not after: new HikariDataSource(config) creates the pool
        // eagerly and its fail-fast check opens a connection, so the cold-start wait happens here
        // rather than in getConnection(). This runs during JDBCMetadata construction, which for a
        // catalog restored from the journal happens on the first caller to touch it -- inside
        // whatever lock that caller holds.
        BlockingCallValidator.validateNotUnderLock("jdbc", catalogName);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(getJdbcUrl());
        config.setUsername(properties.get(JDBCResource.USER));
        config.setPassword(properties.get(JDBCResource.PASSWORD));
        config.setDriverClassName(getDriverName());
        config.setMaximumPoolSize(Config.jdbc_connection_pool_size);
        config.setMinimumIdle(Config.jdbc_minimum_idle_connections);
        config.setIdleTimeout(Config.jdbc_connection_idle_timeout_ms);
        config.setConnectionTimeout(Config.jdbc_connection_timeout_ms);

        applyLifecycleConfig(config);

        return new HikariDataSource(config);
    }

    // Package-visible for testing
    static void applyLifecycleConfig(HikariConfig config) {
        long maxLifetime = Config.jdbc_connection_max_lifetime_ms;
        if (maxLifetime < MINIMUM_MAX_LIFETIME_MS) {
            LOG.warn("jdbc_connection_max_lifetime_ms={} is below minimum {}, using default {}",
                    maxLifetime, MINIMUM_MAX_LIFETIME_MS, DEFAULT_MAX_LIFETIME_MS);
            maxLifetime = DEFAULT_MAX_LIFETIME_MS;
        }
        config.setMaxLifetime(maxLifetime);

        // keepaliveTime: 0 = disabled (HikariCP semantics), otherwise must be >= 30s and < maxLifetime
        long keepaliveTime = Config.jdbc_connection_keepalive_time_ms;
        if (keepaliveTime != KEEPALIVE_DISABLED) {
            if (keepaliveTime < MINIMUM_KEEPALIVE_TIME_MS || keepaliveTime >= maxLifetime) {
                LOG.warn("jdbc_connection_keepalive_time_ms={} is invalid (must be 0 or >= {} and < {}), disabling keepalive",
                        keepaliveTime, MINIMUM_KEEPALIVE_TIME_MS, maxLifetime);
                keepaliveTime = KEEPALIVE_DISABLED;
            }
        }
        config.setKeepaliveTime(keepaliveTime);

        // Connection leak detection (for debugging)
        if (Config.jdbc_connection_leak_detection_threshold_ms > 0) {
            config.setLeakDetectionThreshold(Config.jdbc_connection_leak_detection_threshold_ms);
        }
    }

    public Connection getConnection() throws SQLException {
        // The only door to the pool in this class. Hikari opens a socket here on a pool miss, and
        // the pool itself is built lazily on the first call.
        BlockingCallValidator.validateNotUnderLock("jdbc", catalogName);
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
                        Map<String, Integer> originalJdbcTypes = new HashMap<>();
                        Map<String, String> originalJdbcTypeNames = new HashMap<>();
                        Set<String> unboundedNumericColumns = new HashSet<>();
                        List<Column> fullSchema = schemaResolver.convertToSRTable(
                                columnSet, originalJdbcTypes, originalJdbcTypeNames, unboundedNumericColumns);
                        List<Column> partitionColumns = Lists.newArrayList();
                        if (schemaResolver.isSupportPartitionInformation()) {
                            partitionColumns = listPartitionColumns(dbName, tblName, fullSchema);
                        }
                        if (fullSchema.isEmpty()) {
                            return null;
                        }

                        Long tableId = tableIdCache.getPersistentCache(jdbcTable,
                                j -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong());
                        Table table = schemaResolver.getTable(tableId, tblName, fullSchema,
                                partitionColumns, dbName, catalogName, properties);
                        if (table != null) {
                            if (table instanceof JDBCTable && !originalJdbcTypes.isEmpty()) {
                                ((JDBCTable) table).setOriginalJdbcColumnTypes(originalJdbcTypes);
                                ((JDBCTable) table).setOriginalJdbcColumnTypeNames(originalJdbcTypeNames);
                                ((JDBCTable) table).setUnboundedNumericColumns(unboundedNumericColumns);
                            }
                        }
                        return table;
                    } catch (SQLException | DdlException e) {
                        LOG.warn("get table for JDBC catalog fail!", e);
                        return null;
                    }
                });
    }

    @Override
    public String getTableComment(ConnectContext context, String dbName, String tblName) {
        try (Connection connection = getConnection()) {
            return schemaResolver.getTableComment(connection, dbName, tblName);
        } catch (SQLException e) {
            LOG.warn("get table comment for JDBC catalog fail!", e);
            return "";
        }
    }

    @Override
    public Table getTableFromQuery(ConnectContext context, String dbName, String query) {
        String normalizedQuery = JDBCTable.normalizePassThroughQuery(query);
        String metadataQuery = "SELECT * FROM (" + normalizedQuery + ") starrocks_query WHERE 1 = 0";
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            int queryTimeoutSeconds = schemaResolver.getQueryTimeoutSeconds();
            if (queryTimeoutSeconds > 0) {
                statement.setQueryTimeout(queryTimeoutSeconds);
            }

            try (ResultSet resultSet = statement.executeQuery(metadataQuery)) {
                Map<String, Integer> originalJdbcTypes = new HashMap<>();
                Map<String, String> originalJdbcTypeNames = new HashMap<>();
                Set<String> unboundedNumericColumns = new HashSet<>();
                List<Column> fullSchema = schemaResolver.convertToSRTable(
                        resultSet.getMetaData(), originalJdbcTypes, originalJdbcTypeNames, unboundedNumericColumns);
                if (fullSchema.isEmpty()) {
                    throw new StarRocksConnectorException("pass-through query returned no columns");
                }

                long tableId = ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong();
                JDBCTable queryTable = new JDBCTable(tableId, "_query_" + tableId, fullSchema, dbName, catalogName,
                        properties);
                queryTable.setPassThroughQuery(normalizedQuery);
                if (!originalJdbcTypes.isEmpty()) {
                    queryTable.setOriginalJdbcColumnTypes(originalJdbcTypes);
                    queryTable.setOriginalJdbcColumnTypeNames(originalJdbcTypeNames);
                    queryTable.setUnboundedNumericColumns(unboundedNumericColumns);
                }
                return queryTable;
            }
        } catch (SQLException | DdlException e) {
            throw new StarRocksConnectorException("get query table for JDBC catalog fail!", e);
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           ConnectorMetadataRequestContext requestContext) {
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
    public Statistics getTableStatistics(OptimizerContext session, Table table,
            Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
            ScalarOperator predicate, long limit, TvrVersionRange tableVersionRange) {
        JDBCTable jdbcTable = (JDBCTable) table;
        if (tableStatsCache == null || jdbcTable.isInlineTable()) {
            // An inline table is a pushed-down join/aggregation (or a native_query pass-through):
            // its name and database no longer identify a table in the source, they are inherited
            // from whichever atom the merged scan was derived from. Looking that name up would
            // return some unrelated table's statistics and stamp them TABLE_METADATA — which is
            // how the same join came out estimated at 100 rows or at 1,000,000 depending only on
            // the order the two tables happened to appear in the SQL text.
            return unknownStatistics(columns);
        }
        JDBCTableName key = new JDBCTableName(null, jdbcTable.getCatalogDBName(), jdbcTable.getName());

        JdbcTableStats loaded = null;
        CompletableFuture<JdbcTableStats> future = tableStatsCache.getIfPresent(key);
        if (future == null) {
            // Cold start: fire the async load and report unknown for this planning round.
            tableStatsCache.get(key);
        } else if (future.isDone() && !future.isCompletedExceptionally()) {
            try {
                loaded = future.getNow(null);
            } catch (Exception e) {
                LOG.warn("Unexpected error reading statistics for {}.{}", key.getDatabaseName(),
                        key.getTableName(), e);
            }
        }
        // Future still in flight, completed exceptionally, or the source had nothing to report.
        if (loaded == null || !loaded.hasRowCount()) {
            return unknownStatistics(columns);
        }

        long rowCount = loaded.getRowCount().getAsLong();
        Statistics.Builder builder = Statistics.builder()
                .setOutputRowCount(rowCount)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA);
        if (!columns.isEmpty()) {
            // A dialect that describes its columns gets the honest treatment: what it reported, and
            // ColumnStatistic.unknown() for what it did not. A dialect that describes none keeps the
            // type-ratio estimate it has always been given -- MySQL and ClickHouse report a row
            // count but no column statistics, and reading nothing for PostgreSQL is not a reason to
            // change what they see. The session variable below restores that pre-feature behaviour
            // rather than blanking the columns.
            //
            // The test is whether the source was *asked*, not whether it answered. Those differ for
            // a table the source describes nothing about while still having a real row count -- a
            // partition parent, or a table row-level security hides the pg_stats rows for -- and the
            // difference matters, because the estimate below scales its NDV by the row count. Given
            // a real 20,000 it answered 10,000 distinct values for a column holding 5, which costed
            // an equality on that column at 2 rows against a true 12,000. Unknown is the honest
            // answer there, and the one Trino gives.
            boolean sourceDescribesColumns = isColumnStatisticsEnabled(session) && loaded.describesColumns();
            Map<ColumnRefOperator, ColumnStatistic> colStats = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
                colStats.put(entry.getKey(), sourceDescribesColumns
                        ? toColumnStatistic(loaded.getColumnStats(entry.getValue().getName()),
                                entry.getValue(), rowCount)
                        : estimatedColumnStatistic(entry.getValue(), rowCount));
            }
            builder.addColumnStatistics(colStats);
        }
        return builder.build();
    }

    /**
     * The type-ratio guess a JDBC catalog has always produced once it had a row count: NDV derived
     * from the StarRocks type, no nulls, stamped ESTIMATE. It is not a measurement and it is not
     * defended here -- it is kept so that a dialect this feature does not read statistics for sees
     * exactly what it saw before.
     */
    private static ColumnStatistic estimatedColumnStatistic(Column column, long rowCount) {
        ConnectorNdvEstimator.TypeCategory category =
                ConnectorNdvEstimator.fromStarRocksType(column.getType());
        double ndv = Math.max(1.0, Math.min(ConnectorNdvEstimator.typeNdv(category, rowCount), rowCount));
        return ColumnStatistic.builder()
                .setDistinctValuesCount(ndv)
                .setAverageRowSize(column.getType().getTypeSize())
                .setNullsFraction(0)
                .setType(ColumnStatistic.StatisticType.ESTIMATE)
                .build();
    }

    /**
     * Every requested column gets an entry — {@code Statistics.getColumnStatistic} throws on a
     * missing one — and every entry is honestly unknown.
     */
    private static Statistics unknownStatistics(Map<ColumnRefOperator, Column> columns) {
        Statistics.Builder builder = Statistics.builder()
                .setOutputRowCount(Config.default_statistics_output_row_count)
                .setStatsSource(Statistics.StatsSource.NONE);
        if (columns != null && !columns.isEmpty()) {
            Map<ColumnRefOperator, ColumnStatistic> colStats = new HashMap<>();
            columns.keySet().forEach(ref -> colStats.put(ref, ColumnStatistic.unknown()));
            builder.addColumnStatistics(colStats);
        }
        return builder.build();
    }

    private static boolean isColumnStatisticsEnabled(OptimizerContext session) {
        if (session != null && session.getSessionVariable() != null) {
            return session.getSessionVariable().isEnableJdbcColumnStatistics();
        }
        ConnectContext context = ConnectContext.get();
        return context == null || context.getSessionVariable() == null
                || context.getSessionVariable().isEnableJdbcColumnStatistics();
    }

    /**
     * Translate one column's source statistics into the optimizer's shape.
     *
     * <p>Only a column the dialect actually reported gets a real statistic; everything else stays
     * {@code UNKNOWN}. That is what bounds the blast radius of this change — a dialect that
     * reports nothing, or a column the source has not analyzed, behaves exactly as before.
     *
     * <p>The distinct-value count is the field the optimizer cannot do without, so a source row
     * that lacks it is treated as no statistic at all rather than as a half-filled one that would
     * read as trustworthy.
     */
    private static ColumnStatistic toColumnStatistic(JdbcColumnStats source, Column column, long rowCount) {
        if (source == null) {
            return ColumnStatistic.unknown();
        }
        double distinctValues;
        if (source.getDistinctValues().isPresent()) {
            distinctValues = Math.max(1.0, source.getDistinctValues().getAsDouble());
        } else if (isEntirelyNull(source)) {
            // A source that reports no distinct count has two very different reasons for it, and
            // the null fraction is what tells them apart. A column that is *entirely* NULL has no
            // distinct count to report because there are no values — but "every row is NULL" is
            // itself strong information, and throwing the whole row away to protect against a
            // missing NDV discards it. One distinct value is the floor the optimizer works in.
            //
            // The other reason — PostgreSQL's n_distinct is also 0 for a type with no equality
            // operator (json, xml, point) — must keep producing unknown: there the column may be
            // fully populated, and reading its zero as "all NULL" would assert something false.
            // The null fraction separates the two cases exactly.
            distinctValues = 1.0;
        } else {
            return ColumnStatistic.unknown();
        }
        ColumnStatistic.Builder builder = ColumnStatistic.builder()
                .setDistinctValuesCount(distinctValues)
                .setNullsFraction(source.getNullsFraction().orElse(0))
                .setAverageRowSize(averageRowSize(source, column))
                .setType(ColumnStatistic.StatisticType.ESTIMATE);
        // No min/max: this path reads no value bounds from the source, so range selectivity keeps
        // falling back to the optimizer's own defaults rather than to an invented interval.
        return builder.build();
    }

    private static boolean isEntirelyNull(JdbcColumnStats source) {
        return source.getNullsFraction().isPresent() && source.getNullsFraction().getAsDouble() >= 1.0;
    }

    /**
     * How many bytes one row of this column costs the engine.
     *
     * <p>The source reports an average width <em>per row</em>, which is exactly what
     * {@code averageRowSize} means here — it must not be multiplied by the row count. But the
     * source measures its own storage, and for a decimal the two engines do not agree: PostgreSQL
     * stores {@code numeric} as a variable-length value averaging around four bytes, while
     * StarRocks materializes the mapped type at a fixed width — eight bytes for a DECIMAL64, and
     * sixteen for the DECIMAL128(38,18) an undeclared {@code numeric} is narrowed to. Believing the
     * source there under-counts memory and network cost by two to four times, which is enough to
     * pick a broadcast join where a shuffle was wanted.
     *
     * <p>Only decimals are overridden. The obvious generalization — every fixed-width type takes
     * its StarRocks type size — is wrong in this engine: {@link Type#getTypeSize()} is the tuple
     * slot size, which is 16 for DATE and DATETIME, so it would inflate a PostgreSQL date from the
     * measured 4 bytes to 16 and a timestamp from 8 to 16. On the integer and floating-point types
     * the two numbers already agree, so there is nothing to gain there either. A missing width
     * still falls back to the type size, as before.
     */
    private static double averageRowSize(JdbcColumnStats source, Column column) {
        Type type = column.getType();
        if (type.isDecimalOfAnyVersion()) {
            return type.getTypeSize();
        }
        return source.getAverageWidth().isPresent() ? source.getAverageWidth().getAsInt() : type.getTypeSize();
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
        if (tableStatsCache != null) {
            tableStatsCache.synchronous().invalidate(jdbcTableName);
        }
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
