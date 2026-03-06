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

import java.io.IOException;
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
        try {
            return driver.open(params);
        } catch (AdbcException e) {
            throw new StarRocksConnectorException("Failed to open ADBC database: " + e.getMessage(), e);
        }
    }

    private ADBCSchemaResolver createSchemaResolver(String driver) {
        // Only flight_sql supported in Phase 1; more drivers added in future
        if ("flight_sql".equalsIgnoreCase(driver)) {
            return new FlightSQLSchemaResolver();
        }
        LOG.warn("Unknown ADBC driver '{}', falling back to FlightSQLSchemaResolver", driver);
        return new FlightSQLSchemaResolver();
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
                        "ADBC listDbNames failed: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return tableNamesCache.get(dbName, k -> {
            try (AdbcConnection conn = adbcDatabase.connect()) {
                try (ArrowReader reader = conn.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES,
                        null, dbName, null,
                        new String[] {"TABLE", "VIEW"},
                        null)) {
                    return parseTableNames(reader);
                }
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "ADBC listTableNames failed for db " + dbName + ": " + e.getMessage(), e);
            }
        });
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        ADBCTableName key = ADBCTableName.of(catalogName, dbName, tblName);
        return tableInstanceCache.get(key, k -> {
            try (AdbcConnection conn = adbcDatabase.connect()) {
                // CRITICAL: first arg is null (catalog); dbName is the "schema" in Arrow Flight SQL
                Schema arrowSchema = conn.getTableSchema(null, dbName, tblName);
                if (arrowSchema == null) {
                    return null;
                }
                List<Column> fullSchema = schemaResolver.convertToSRTable(arrowSchema);
                if (fullSchema.isEmpty()) {
                    return null;
                }
                int tableId = tableIdCache.getPersistentCache(k,
                        j -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
                return new ADBCTable(tableId, tblName, fullSchema, dbName, catalogName, properties);
            } catch (Exception e) {
                LOG.warn("ADBC getTable failed for {}.{}: {}", dbName, tblName, e.getMessage());
                return null;
            }
        });
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
