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
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Metadata provider for ADBC external catalogs.
 *
 * <h3>Hierarchy mapping</h3>
 * ADBC drivers expose a 3-level hierarchy: {@code catalog → db_schema → table}.
 * Different drivers populate these levels differently:
 * <ul>
 *   <li><b>SQLite:</b> catalog="main", db_schema="" → schemas are empty, catalogs carry meaning</li>
 *   <li><b>PostgreSQL:</b> catalog="testdb", db_schema="public" → schemas carry meaning</li>
 *   <li><b>DuckDB:</b> catalog="my.db", db_schema="main" → schemas carry meaning</li>
 * </ul>
 *
 * StarRocks maps to a 2-level model: {@code external_catalog.database.table}. This class
 * probes the driver's hierarchy once via {@link #resolveHierarchy} and caches the result
 * so that {@code listDbNames}, {@code listTableNames}, and {@code getTableSchema} always
 * map StarRocks' "database" concept to the correct ADBC level.
 */
public class ADBCMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(ADBCMetadata.class);

    /**
     * Which ADBC hierarchy level maps to StarRocks' "database" concept.
     * <ul>
     *   <li>{@code CATALOG} — driver has meaningful catalog names but empty schemas
     *       (e.g. SQLite: "main" is a catalog).</li>
     *   <li>{@code SCHEMA} — driver has meaningful schema names
     *       (e.g. PostgreSQL: "public" is a schema).</li>
     * </ul>
     */
    enum HierarchyModel {
        CATALOG,
        SCHEMA
    }

    private final String catalogName;
    private final Map<String, String> properties;
    private final AdbcDatabase adbcDatabase;
    private final BufferAllocator allocator;
    private final ADBCSchemaResolver schemaResolver;

    // Caches
    private final ADBCMetaCache<ADBCTableName, Integer> tableIdCache;
    private final ADBCMetaCache<ADBCTableName, Table> tableInstanceCache;
    private final ADBCMetaCache<String, List<String>> dbNamesCache;
    private final ADBCMetaCache<String, List<String>> tableNamesCache;

    // Resolved once on first metadata call, then cached for the catalog's lifetime.
    private volatile HierarchyModel hierarchyModel;

    public ADBCMetadata(Map<String, String> properties, String catalogName,
                        BufferAllocator allocator, AdbcDatabase adbcDatabase) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.allocator = allocator;
        this.adbcDatabase = adbcDatabase;
        this.schemaResolver = createSchemaResolver();

        this.tableIdCache = new ADBCMetaCache<>(properties, true);
        this.tableInstanceCache = new ADBCMetaCache<>(properties, false);
        this.dbNamesCache = new ADBCMetaCache<>(properties, false);
        this.tableNamesCache = new ADBCMetaCache<>(properties, false);
    }

    // Visible for testing
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
        return new ADBCSchemaResolver();
    }

    // -------------------------------------------------------------------
    // Hierarchy resolution
    // -------------------------------------------------------------------

    /**
     * Probe the ADBC driver's catalog/schema structure and decide which level
     * maps to StarRocks' "database".
     *
     * <p>Logic: call {@code getObjects(DB_SCHEMAS)} and inspect the results.
     * If every schema name is null or empty, the driver uses catalogs as the
     * meaningful level (e.g. SQLite). Otherwise it uses schemas (e.g. PostgreSQL).
     *
     * <p>Called once per catalog lifetime, result is cached in {@link #hierarchyModel}.
     */
    private HierarchyModel resolveHierarchy(AdbcConnection conn) throws Exception {
        boolean foundNonEmptySchema = false;

        try (ArrowReader reader = conn.getObjects(
                AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                null, null, null, null, null)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                ListVector dbSchemasVec = (ListVector) root.getVector("catalog_db_schemas");
                if (dbSchemasVec == null) {
                    continue;
                }
                for (int i = 0; i < root.getRowCount(); i++) {
                    if (dbSchemasVec.isNull(i)) {
                        continue;
                    }
                    List<?> schemaList = dbSchemasVec.getObject(i);
                    if (schemaList == null) {
                        continue;
                    }
                    for (Object obj : schemaList) {
                        if (obj instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, ?> m = (Map<String, ?>) obj;
                            Object nameObj = m.get("db_schema_name");
                            if (nameObj != null && !nameObj.toString().isEmpty()) {
                                foundNonEmptySchema = true;
                            }
                        }
                    }
                }
            }
        } catch (AdbcException e) {
            if (e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED) {
                LOG.info("ADBC driver does not implement getObjects(); defaulting to CATALOG model");
                return HierarchyModel.CATALOG;
            }
            throw e;
        }

        HierarchyModel model = foundNonEmptySchema ? HierarchyModel.SCHEMA : HierarchyModel.CATALOG;
        LOG.info("ADBC catalog '{}': resolved hierarchy model = {}", catalogName, model);
        return model;
    }

    /**
     * Return the cached hierarchy model, resolving it on first call.
     */
    private HierarchyModel getHierarchyModel(AdbcConnection conn) throws Exception {
        if (hierarchyModel == null) {
            synchronized (this) {
                if (hierarchyModel == null) {
                    hierarchyModel = resolveHierarchy(conn);
                }
            }
        }
        return hierarchyModel;
    }

    // -------------------------------------------------------------------
    // ConnectorMetadata interface
    // -------------------------------------------------------------------

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
                HierarchyModel model = getHierarchyModel(conn);
                return listDbNamesFromGetObjects(conn, model);
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "Failed to list databases from ADBC catalog '" + catalogName
                                + "'. Detail: " + e.getMessage(), e);
            }
        });
    }

    /**
     * List "databases" by reading the correct hierarchy level.
     *
     * <ul>
     *   <li>{@code CATALOG model}: returns catalog names (e.g. SQLite → ["main"])</li>
     *   <li>{@code SCHEMA model}: returns schema names (e.g. PostgreSQL → ["public", "pg_catalog"])</li>
     * </ul>
     */
    private List<String> listDbNamesFromGetObjects(AdbcConnection conn, HierarchyModel model)
            throws Exception {
        List<String> result = new ArrayList<>();

        try (ArrowReader reader = conn.getObjects(
                AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                null, null, null, null, null)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                org.apache.arrow.vector.VarCharVector catNameVec =
                        (org.apache.arrow.vector.VarCharVector) root.getVector("catalog_name");
                ListVector dbSchemasVec = (ListVector) root.getVector("catalog_db_schemas");

                for (int catIdx = 0; catIdx < root.getRowCount(); catIdx++) {
                    if (model == HierarchyModel.CATALOG) {
                        // Use catalog names as databases
                        String catName = catNameVec != null && !catNameVec.isNull(catIdx)
                                ? new String(catNameVec.get(catIdx)) : null;
                        if (catName != null && !catName.isEmpty()) {
                            result.add(catName);
                        }
                    } else {
                        // Use schema names as databases
                        if (dbSchemasVec == null || dbSchemasVec.isNull(catIdx)) {
                            continue;
                        }
                        List<?> schemaList = dbSchemasVec.getObject(catIdx);
                        if (schemaList == null) {
                            continue;
                        }
                        for (Object obj : schemaList) {
                            if (!(obj instanceof Map)) {
                                continue;
                            }
                            @SuppressWarnings("unchecked")
                            Map<String, ?> m = (Map<String, ?>) obj;
                            Object nameObj = m.get("db_schema_name");
                            if (nameObj != null && !nameObj.toString().isEmpty()) {
                                result.add(nameObj.toString());
                            }
                        }
                    }
                }
            }
        } catch (AdbcException e) {
            if (e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED) {
                LOG.info("getObjects() not implemented; returning 'main' as default database");
                result.add("main");
            } else {
                throw e;
            }
        }

        if (result.isEmpty()) {
            result.add("main");
        }
        return result;
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return tableNamesCache.get(dbName, k -> {
            try (AdbcConnection conn = adbcDatabase.connect()) {
                HierarchyModel model = getHierarchyModel(conn);
                return listTableNamesFromGetObjects(conn, dbName, model);
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        "Failed to list tables from ADBC catalog '" + catalogName
                                + "', database '" + dbName + "'. Detail: " + e.getMessage(), e);
            }
        });
    }

    /**
     * List table names, filtering by the correct hierarchy level.
     *
     * <p>For CATALOG model: pass dbName as catalog filter (e.g. SQLite "main").
     * For SCHEMA model: fetch all objects without server-side filtering, then
     * match schemas client-side. This is necessary because some drivers (e.g.
     * FlightSQL/sqlflite) have a non-trivial catalog name (e.g. "TPC-H-small")
     * that we don't know, so passing only schema filter may return nothing.
     */
    private List<String> listTableNamesFromGetObjects(
            AdbcConnection conn, String dbName, HierarchyModel model) throws Exception {
        String catalogFilter = (model == HierarchyModel.CATALOG) ? dbName : null;
        // For SCHEMA model, don't filter server-side — filter client-side below
        String schemaFilter = null;

        List<String> tables = new ArrayList<>();
        try (ArrowReader reader = conn.getObjects(
                AdbcConnection.GetObjectsDepth.TABLES,
                catalogFilter, schemaFilter, null,
                null, null)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                ListVector dbSchemasVec = (ListVector) root.getVector("catalog_db_schemas");
                if (dbSchemasVec == null) {
                    continue;
                }
                for (int catIdx = 0; catIdx < root.getRowCount(); catIdx++) {
                    if (dbSchemasVec.isNull(catIdx)) {
                        continue;
                    }
                    List<?> schemaList = dbSchemasVec.getObject(catIdx);
                    if (schemaList == null) {
                        continue;
                    }
                    for (Object schemaObj : schemaList) {
                        if (!(schemaObj instanceof Map)) {
                            continue;
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, ?> schemaMap = (Map<String, ?>) schemaObj;

                        // Client-side schema filter for SCHEMA model
                        if (model == HierarchyModel.SCHEMA) {
                            Object schemaName = schemaMap.get("db_schema_name");
                            String sName = schemaName != null ? schemaName.toString() : "";
                            if (!dbName.equals(sName)) {
                                continue;
                            }
                        }

                        Object tablesObj = schemaMap.get("db_schema_tables");
                        if (!(tablesObj instanceof List)) {
                            continue;
                        }
                        for (Object tableObj : (List<?>) tablesObj) {
                            if (tableObj instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, ?> tableMap = (Map<String, ?>) tableObj;
                                Object nameObj = tableMap.get("table_name");
                                if (nameObj != null) {
                                    tables.add(nameObj.toString());
                                }
                            }
                        }
                    }
                }
            }
        } catch (AdbcException e) {
            if (e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED) {
                LOG.warn("ADBC driver does not implement getObjects() for table listing");
            } else {
                throw e;
            }
        }
        return tables;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        ADBCTableName key = ADBCTableName.of(catalogName, dbName, tblName);
        try {
            return tableInstanceCache.get(key, k -> {
                try (AdbcConnection conn = adbcDatabase.connect()) {
                    HierarchyModel model = getHierarchyModel(conn);
                    Schema arrowSchema = getTableSchemaViaADBC(conn, dbName, tblName, model);
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
     * Get table schema using the native ADBC {@code getTableSchema()} API.
     *
     * <p>The hierarchy model determines how {@code dbName} is passed:
     * <ul>
     *   <li>{@code CATALOG model}: {@code getTableSchema(catalog=dbName, schema=null, table)}</li>
     *   <li>{@code SCHEMA model}: {@code getTableSchema(catalog=null, schema=dbName, table)}</li>
     * </ul>
     *
     * <p>This replaces the previous approach of {@code SELECT * FROM table WHERE 1=0} which
     * failed for SQLite (returns int64 for all columns when no rows are present).
     */
    private Schema getTableSchemaViaADBC(AdbcConnection conn, String dbName,
                                          String tblName, HierarchyModel model)
            throws Exception {
        String catalogArg = (model == HierarchyModel.CATALOG) ? dbName : null;
        String schemaArg = (model == HierarchyModel.SCHEMA) ? dbName : null;

        try {
            return conn.getTableSchema(catalogArg, schemaArg, tblName);
        } catch (AdbcException e) {
            if (e.getStatus() == AdbcStatusCode.NOT_IMPLEMENTED) {
                LOG.warn("ADBC driver does not implement getTableSchema(), "
                        + "falling back to query-based inference for {}.{}", dbName, tblName);
                return getTableSchemaViaQuery(conn, dbName, tblName);
            }
            throw e;
        }
    }

    /**
     * Fallback: infer schema by executing {@code SELECT * FROM table LIMIT 1}.
     * Uses LIMIT 1 (not WHERE 1=0) because SQLite returns int64 for all columns
     * when the result set is empty.
     */
    private Schema getTableSchemaViaQuery(AdbcConnection conn, String dbName, String tblName)
            throws Exception {
        String sql = String.format("SELECT * FROM \"%s\".\"%s\" LIMIT 1", dbName, tblName);
        try (org.apache.arrow.adbc.core.AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(sql);
            try (org.apache.arrow.adbc.core.AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                ArrowReader reader = qr.getReader();
                reader.loadNextBatch();
                return reader.getVectorSchemaRoot().getSchema();
            }
        }
    }

    @Override
    public void refreshTable(String srDbName, Table table,
                              List<String> partitionNames, boolean onlyCachedPartitions) {
        ADBCTable adbcTable = (ADBCTable) table;
        ADBCTableName tableName = ADBCTableName.of(
                catalogName, adbcTable.getDbName(), adbcTable.getName());
        if (!onlyCachedPartitions) {
            tableInstanceCache.invalidate(tableName);
        }
    }

    @Override
    public void clear() {
        tableInstanceCache.invalidateAll();
        dbNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        synchronized (this) {
            hierarchyModel = null;
        }
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
