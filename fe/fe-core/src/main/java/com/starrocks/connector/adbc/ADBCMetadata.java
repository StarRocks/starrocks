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

package com.starrocks.connector.adbc;

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ADBCMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(ADBCMetadata.class);

    private final String catalogName;
    private final Map<String, String> properties;
    private final AdbcDatabase adbcDatabase;
    private final ADBCSchemaResolver schemaResolver;

    // Caches (mirror JDBCMetadata structure)
    private final ADBCMetaCache<ADBCTableName, Integer> tableIdCache;
    private final ADBCMetaCache<ADBCTableName, Table> tableInstanceCache;
    private final ADBCMetaCache<String, List<String>> dbNamesCache;
    private final ADBCMetaCache<String, List<String>> tableNamesCache;

    public ADBCMetadata(Map<String, String> properties, String catalogName) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.adbcDatabase = initDatabase(properties);
        this.schemaResolver = createSchemaResolver(properties.get(ADBCConnector.PROP_DRIVER));

        // Initialize caches
        this.tableIdCache = new ADBCMetaCache<>(properties, true);       // permanent
        this.tableInstanceCache = new ADBCMetaCache<>(properties, false);
        this.dbNamesCache = new ADBCMetaCache<>(properties, false);
        this.tableNamesCache = new ADBCMetaCache<>(properties, false);
    }

    // Visible for testing: allows injecting a mocked AdbcDatabase
    ADBCMetadata(Map<String, String> properties, String catalogName, AdbcDatabase adbcDatabase) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.adbcDatabase = adbcDatabase;
        this.schemaResolver = createSchemaResolver(properties.get(ADBCConnector.PROP_DRIVER));

        this.tableIdCache = new ADBCMetaCache<>(properties, true);
        this.tableInstanceCache = new ADBCMetaCache<>(properties, false);
        this.dbNamesCache = new ADBCMetaCache<>(properties, false);
        this.tableNamesCache = new ADBCMetaCache<>(properties, false);
    }

    private AdbcDatabase initDatabase(Map<String, String> properties) {
        BufferAllocator allocator = new RootAllocator();
        FlightSqlDriver driver = new FlightSqlDriver(allocator);
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, properties.get(ADBCConnector.PROP_URL));
        if (properties.containsKey("adbc.username")) {
            AdbcDriver.PARAM_USERNAME.set(params, properties.get("adbc.username"));
        }
        if (properties.containsKey("adbc.password")) {
            AdbcDriver.PARAM_PASSWORD.set(params, properties.get("adbc.password"));
        }

        // TLS options
        String uri = properties.get(ADBCConnector.PROP_URL);
        if (uri != null && uri.toLowerCase().startsWith("grpc+tls://")) {
            // Read cert/key files into byte arrays and wrap in resettable streams.
            // The ADBC driver stores the params map and reads the InputStream on every
            // adbcDatabase.connect() call. A plain FileInputStream (or ByteArrayInputStream)
            // is consumed on the first read, causing "does not contain valid certificates"
            // on subsequent connections. Auto-resetting before each read fixes this.
            String caCertFile = properties.get(ADBCConnector.PROP_TLS_CA_CERT_FILE);
            if (caCertFile != null) {
                try {
                    FlightSqlConnectionProperties.TLS_ROOT_CERTS.set(params,
                            rereadableStream(Files.readAllBytes(Paths.get(caCertFile))));
                } catch (IOException e) {
                    throw new StarRocksConnectorException(
                            "Cannot read CA cert file: " + caCertFile + ": " + e.getMessage(), e);
                }
            }

            String clientCertFile = properties.get(ADBCConnector.PROP_TLS_CLIENT_CERT_FILE);
            String clientKeyFile = properties.get(ADBCConnector.PROP_TLS_CLIENT_KEY_FILE);
            if (clientCertFile != null && clientKeyFile != null) {
                try {
                    FlightSqlConnectionProperties.MTLS_CERT_CHAIN.set(params,
                            rereadableStream(Files.readAllBytes(Paths.get(clientCertFile))));
                    FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.set(params,
                            rereadableStream(Files.readAllBytes(Paths.get(clientKeyFile))));
                } catch (IOException e) {
                    throw new StarRocksConnectorException(
                            "Cannot read mTLS cert/key file: " + e.getMessage(), e);
                }
            }

            String verify = properties.getOrDefault(ADBCConnector.PROP_TLS_VERIFY, "true");
            if ("false".equalsIgnoreCase(verify)) {
                FlightSqlConnectionProperties.TLS_SKIP_VERIFY.set(params, true);
                LOG.warn("ADBC catalog '{}': TLS certificate verification DISABLED (insecure mode)",
                        properties.getOrDefault("catalog_name", "unknown"));
            }
        }

        try {
            return driver.open(params);
        } catch (AdbcException e) {
            throw new StarRocksConnectorException(
                    "Failed to connect to ADBC catalog '" + properties.getOrDefault("catalog_name", "unknown")
                            + "' at " + properties.getOrDefault(ADBCConnector.PROP_URL, "<unknown URL>")
                            + ". Check that the server is running and the URL is correct. Detail: "
                            + e.getMessage(), e);
        }
    }

    // Returns a ByteArrayInputStream that auto-resets before the ADBC driver reads it.
    // The driver calls available() to size a buffer, then readFully() to fill it.
    // Each adbcDatabase.connect() re-reads the same stream from the params map,
    // so we must reset to position 0 before each available() call.
    private static ByteArrayInputStream rereadableStream(byte[] data) {
        return new ByteArrayInputStream(data) {
            @Override
            public synchronized int available() {
                reset();
                return super.available();
            }
        };
    }

    private ADBCSchemaResolver createSchemaResolver(String driver) {
        if ("flight_sql".equalsIgnoreCase(driver)) {
            return new FlightSQLSchemaResolver();
        }
        throw new StarRocksConnectorException(
                "Unsupported ADBC driver: '" + driver + "'. Supported drivers: [flight_sql]");
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.ADBC;
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        try {
            List<String> dbNames = listDbNames(context);
            if (dbNames.contains(name)) {
                return new Database(0, name);
            }
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to check database existence for {}.{}: {}",
                    catalogName, name, e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return dbNamesCache.get(catalogName, k -> {
            try (AdbcConnection conn = adbcDatabase.connect()) {
                try (ArrowReader reader = conn.getObjects(
                        AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                        null, null, null, null, null)) {
                    return parseSchemaNames(reader);
                }
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "Failed to list databases from ADBC catalog '" + catalogName
                                + "'. Check that the remote server at "
                                + properties.getOrDefault(ADBCConnector.PROP_URL, "<unknown URL>")
                                + " is reachable. Detail: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return tableNamesCache.get(dbName, k -> {
            try (AdbcConnection conn = adbcDatabase.connect()) {
                // First try with table type filter
                try (ArrowReader reader = conn.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES,
                        null, dbName, null,
                        new String[] {"TABLE", "VIEW"},
                        null)) {
                    List<String> result = parseTableNames(reader);
                    if (!result.isEmpty()) {
                        return result;
                    }
                }
                // Retry without table type filter (some servers use non-standard type strings)
                try (ArrowReader reader = conn.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES,
                        null, dbName, null,
                        null,
                        null)) {
                    return parseTableNames(reader);
                }
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "Failed to list tables from ADBC catalog '" + catalogName
                                + "', database '" + dbName + "'. Check that the remote server at "
                                + properties.getOrDefault(ADBCConnector.PROP_URL, "<unknown URL>")
                                + " is reachable. Detail: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        ADBCTableName key = ADBCTableName.of(catalogName, dbName, tblName);
        try {
            return tableInstanceCache.get(key, k -> {
                try (AdbcConnection conn = adbcDatabase.connect()) {
                    Schema arrowSchema = getTableSchema(conn, dbName, tblName);
                    if (arrowSchema == null) {
                        throw new StarRocksConnectorException(
                                "No schema returned for table " + dbName + "." + tblName);
                    }
                    List<Column> fullSchema = schemaResolver.convertToSRTable(arrowSchema);
                    if (fullSchema.isEmpty()) {
                        throw new StarRocksConnectorException(
                                "Empty schema for table " + dbName + "." + tblName);
                    }
                    int tableId = tableIdCache.getPersistentCache(k,
                            j -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
                    return new ADBCTable(tableId, tblName, fullSchema, dbName, catalogName, properties);
                } catch (StarRocksConnectorException e) {
                    throw e;
                } catch (Exception e) {
                    throw new StarRocksConnectorException(
                            "Failed to get table '" + dbName + "." + tblName
                                    + "' from ADBC catalog '" + catalogName
                                    + "'. Check that the remote server is reachable. Detail: "
                                    + e.getMessage(), e);
                }
            });
        } catch (StarRocksConnectorException e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    /**
     * Get table schema, trying getTableSchema() first, then falling back to a query.
     */
    private Schema getTableSchema(AdbcConnection conn, String dbName, String tblName) throws Exception {
        // Try the standard ADBC getTableSchema API first
        try {
            Schema schema = conn.getTableSchema(null, dbName, tblName);
            if (schema != null) {
                return schema;
            }
        } catch (Exception e) {
            LOG.debug("getTableSchema not supported, falling back to query for {}.{}: {}",
                    dbName, tblName, e.getMessage());
        }

        // Fallback: infer schema from an empty query result
        String sql = "SELECT * FROM \"" + dbName + "\".\"" + tblName + "\" WHERE 1=0";
        try (AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(sql);
            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                reader.loadNextBatch();
                return reader.getVectorSchemaRoot().getSchema();
            }
        }
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TvrVersionRange tableVersionRange) {
        Statistics.Builder builder = Statistics.builder();

        // Default: unknown stats for all columns
        for (ColumnRefOperator colRef : columns.keySet()) {
            builder.addColumnStatistic(colRef, ColumnStatistic.unknown());
        }

        // Try to get row count from remote via COUNT(*)
        ADBCTable adbcTable = (ADBCTable) table;
        try {
            long rowCount = getRemoteRowCount(adbcTable);
            builder.setOutputRowCount(rowCount);
        } catch (Exception e) {
            LOG.warn("Failed to get row count for ADBC table {}: {}",
                    adbcTable.getName(), e.getMessage());
            builder.setOutputRowCount(1);  // Fallback
        }

        return builder.build();
    }

    private long getRemoteRowCount(ADBCTable table) throws Exception {
        String sql = "SELECT COUNT(*) FROM \"" + table.getDbName() + "\".\"" + table.getName() + "\"";
        try (AdbcConnection conn = adbcDatabase.connect()) {
            try (AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery(sql);
                AdbcStatement.QueryResult result = stmt.executeQuery();
                try (ArrowReader reader = result.getReader()) {
                    if (reader.loadNextBatch()) {
                        BigIntVector countVec = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
                        return countVec.get(0);
                    }
                }
            }
        }
        throw new IOException("No result returned from COUNT(*) query");
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        ADBCTable adbcTable = (ADBCTable) table;
        ADBCTableName tableName = ADBCTableName.of(catalogName, adbcTable.getDbName(), adbcTable.getName());
        if (!onlyCachedPartitions) {
            tableInstanceCache.invalidate(tableName);
        }
    }

    @Override
    public void clear() {
        tableInstanceCache.invalidateAll();
        dbNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
    }

    @Override
    public void shutdown() {
        try {
            if (adbcDatabase != null) {
                adbcDatabase.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing ADBC database in catalog {}: {}", catalogName, e.getMessage());
        }
    }

    /**
     * Parse schema names from getObjects(DB_SCHEMAS) result.
     * The result contains a nested structure: catalog -> db_schema list.
     * We extract db_schema_name from the nested catalog_db_schemas list.
     */
    private List<String> parseSchemaNames(ArrowReader reader) throws IOException {
        List<String> results = new ArrayList<>();
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // getObjects result has: catalog_name (Utf8), catalog_db_schemas (List<Struct>)
            // Each struct has: db_schema_name (Utf8), db_schema_tables (List)
            ListVector dbSchemasListVec = (ListVector) root.getVector("catalog_db_schemas");
            if (dbSchemasListVec == null) {
                // Fallback: try flat structure
                VarCharVector nameVec = (VarCharVector) root.getVector("db_schema_name");
                if (nameVec != null) {
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (!nameVec.isNull(i)) {
                            results.add(new String(nameVec.get(i)));
                        }
                    }
                }
                continue;
            }

            for (int i = 0; i < root.getRowCount(); i++) {
                if (dbSchemasListVec.isNull(i)) {
                    continue;
                }
                List<?> schemas = (List<?>) dbSchemasListVec.getObject(i);
                if (schemas == null) {
                    continue;
                }
                StructVector dataVec = (StructVector) dbSchemasListVec.getDataVector();
                int start = dbSchemasListVec.getElementStartIndex(i);
                int end = dbSchemasListVec.getElementEndIndex(i);
                VarCharVector nameVec = (VarCharVector) dataVec.getChild("db_schema_name");
                for (int j = start; j < end; j++) {
                    if (!nameVec.isNull(j)) {
                        results.add(new String(nameVec.get(j)));
                    }
                }
            }
        }
        return results;
    }

    /**
     * Parse table names from getObjects(TABLES) result.
     * The result contains a nested structure: catalog -> db_schema -> tables list.
     */
    private List<String> parseTableNames(ArrowReader reader) throws IOException {
        List<String> results = new ArrayList<>();
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // Navigate nested structure: catalog_db_schemas -> db_schema_tables -> table_name
            ListVector dbSchemasListVec = (ListVector) root.getVector("catalog_db_schemas");
            if (dbSchemasListVec == null) {
                continue;
            }

            for (int catalogRow = 0; catalogRow < root.getRowCount(); catalogRow++) {
                if (dbSchemasListVec.isNull(catalogRow)) {
                    continue;
                }
                StructVector schemaStructVec = (StructVector) dbSchemasListVec.getDataVector();
                int schemaStart = dbSchemasListVec.getElementStartIndex(catalogRow);
                int schemaEnd = dbSchemasListVec.getElementEndIndex(catalogRow);

                for (int schemaIdx = schemaStart; schemaIdx < schemaEnd; schemaIdx++) {
                    ListVector tablesListVec =
                            (ListVector) schemaStructVec.getChild("db_schema_tables");
                    if (tablesListVec == null || tablesListVec.isNull(schemaIdx)) {
                        continue;
                    }
                    StructVector tableStructVec = (StructVector) tablesListVec.getDataVector();
                    int tableStart = tablesListVec.getElementStartIndex(schemaIdx);
                    int tableEnd = tablesListVec.getElementEndIndex(schemaIdx);
                    VarCharVector tableNameVec =
                            (VarCharVector) tableStructVec.getChild("table_name");
                    for (int tableIdx = tableStart; tableIdx < tableEnd; tableIdx++) {
                        if (!tableNameVec.isNull(tableIdx)) {
                            results.add(new String(tableNameVec.get(tableIdx)));
                        }
                    }
                }
            }
        }
        return results;
    }
}
