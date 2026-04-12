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
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ADBCMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(ADBCMetadata.class);

    // SQL constants for metadata discovery via information_schema
    private static final String SQL_LIST_SCHEMAS =
            "SELECT schema_name FROM information_schema.schemata";
    private static final String SQL_LIST_TABLES =
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'";
    private static final String SQL_GET_SCHEMA =
            "SELECT * FROM \"%s\".\"%s\" WHERE 1=0";

    // SQLite fallback (sqlite has no information_schema)
    private static final String SQL_LIST_SCHEMAS_SQLITE =
            "SELECT DISTINCT 'main' AS schema_name";
    private static final String SQL_LIST_TABLES_SQLITE =
            "SELECT name AS table_name FROM sqlite_master WHERE type IN ('table', 'view')";
    private static final String SQL_GET_SCHEMA_SQLITE =
            "SELECT * FROM \"%s\" WHERE 1=0";

    private final String catalogName;
    private final Map<String, String> properties;
    private final AdbcDatabase adbcDatabase;
    private final BufferAllocator allocator;
    private final ADBCSchemaResolver schemaResolver;

    // Caches (mirror JDBCMetadata structure)
    private final ADBCMetaCache<ADBCTableName, Integer> tableIdCache;
    private final ADBCMetaCache<ADBCTableName, Table> tableInstanceCache;
    private final ADBCMetaCache<String, List<String>> dbNamesCache;
    private final ADBCMetaCache<String, List<String>> tableNamesCache;

    public ADBCMetadata(Map<String, String> properties, String catalogName,
                        BufferAllocator allocator, AdbcDatabase adbcDatabase) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.allocator = allocator;
        this.adbcDatabase = adbcDatabase;
        this.schemaResolver = createSchemaResolver();

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
        this.allocator = new RootAllocator();
        this.adbcDatabase = adbcDatabase;
        this.schemaResolver = createSchemaResolver();

        this.tableIdCache = new ADBCMetaCache<>(properties, true);
        this.tableInstanceCache = new ADBCMetaCache<>(properties, false);
        this.dbNamesCache = new ADBCMetaCache<>(properties, false);
        this.tableNamesCache = new ADBCMetaCache<>(properties, false);
    }

    private ADBCSchemaResolver createSchemaResolver() {
        return new FlightSQLSchemaResolver();  // driver-agnostic despite the name
    }

    private boolean isSQLiteDriver() {
        String driverUrl = properties.getOrDefault("driver_url", "");
        return driverUrl.toLowerCase().contains("sqlite");
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
            String sql = isSQLiteDriver() ? SQL_LIST_SCHEMAS_SQLITE : SQL_LIST_SCHEMAS;
            try (AdbcConnection conn = adbcDatabase.connect();
                 AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery(sql);
                try (AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                    ArrowReader reader = qr.getReader();
                    return extractStringColumn(reader, "schema_name");
                }
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "Failed to list databases from ADBC catalog '" + catalogName
                                + "'. Detail: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return tableNamesCache.get(dbName, k -> {
            String sql = isSQLiteDriver()
                    ? SQL_LIST_TABLES_SQLITE
                    : String.format(SQL_LIST_TABLES, dbName);
            try (AdbcConnection conn = adbcDatabase.connect();
                 AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery(sql);
                try (AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                    ArrowReader reader = qr.getReader();
                    return extractStringColumn(reader, "table_name");
                }
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "Failed to list tables from ADBC catalog '" + catalogName
                                + "', database '" + dbName + "'. Detail: " + e.getMessage(), e);
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
     * Get table schema by executing a WHERE 1=0 query to retrieve column metadata.
     */
    private Schema getTableSchema(AdbcConnection conn, String dbName, String tblName) throws Exception {
        String sql;
        if (isSQLiteDriver()) {
            sql = String.format(SQL_GET_SCHEMA_SQLITE, tblName);
        } else {
            sql = String.format(SQL_GET_SCHEMA, dbName, tblName);
        }
        try (AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(sql);
            try (AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                ArrowReader reader = qr.getReader();
                reader.loadNextBatch();
                return reader.getVectorSchemaRoot().getSchema();
            }
        }
    }

    /**
     * Extract a string column from an ArrowReader result set.
     * Tries the named column first, falls back to the first column.
     */
    private List<String> extractStringColumn(ArrowReader reader, String columnName) throws IOException {
        List<String> results = new ArrayList<>();
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // Try by column name first, fall back to first column
            VarCharVector vec = null;
            try {
                vec = (VarCharVector) root.getVector(columnName);
            } catch (Exception e) {
                vec = (VarCharVector) root.getVector(0);
            }
            if (vec == null && root.getFieldVectors().size() > 0) {
                vec = (VarCharVector) root.getFieldVectors().get(0);
            }
            if (vec != null) {
                for (int i = 0; i < root.getRowCount(); i++) {
                    if (!vec.isNull(i)) {
                        results.add(new String(vec.get(i)));
                    }
                }
            }
        }
        return results;
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
}
