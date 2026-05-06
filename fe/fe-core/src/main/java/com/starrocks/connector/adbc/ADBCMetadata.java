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
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata provider for ADBC external catalogs.
 *
 * <h3>Hierarchy mapping</h3>
 * ADBC drivers expose a 3-level hierarchy: {@code catalog -> db_schema -> table}.
 * Different drivers populate these levels differently:
 * <ul>
 *   <li><b>SQLite:</b> catalog="main", db_schema="" -> schemas are empty, catalogs carry meaning</li>
 *   <li><b>PostgreSQL:</b> catalog="testdb", db_schema="public" -> schemas carry meaning</li>
 *   <li><b>DuckDB:</b> catalog="my.db", db_schema="main" -> schemas carry meaning</li>
 * </ul>
 *
 * StarRocks maps to a 2-level model: {@code external_catalog.database.table}. This class
 * probes the driver's hierarchy once via {@link #resolveHierarchy} and caches the result
 * so that {@code listDbNames}, {@code listTableNames}, and {@code getTableSchema} always
 * map StarRocks' "database" concept to the correct ADBC level.
 */
public class ADBCMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(ADBCMetadata.class);

    // Pool size = 8: matches a typical FE metadata RPC concurrency for a single catalog.
    // FE serves metadata calls from the query-planning thread pool, which rarely fans out
    // beyond ~8 concurrent operations against the same external catalog. We set min=0 so
    // an idle catalog releases native handles after the eviction interval.
    // Override path (future): expose via Config.adbc_metadata_pool_max_total when usage data
    // shows real workloads need a different ceiling. Tracked as a follow-up; not added now
    // because the user explicitly delegated the choice and a single constant is the smaller
    // surface area today.
    private static final int DEFAULT_POOL_MAX_TOTAL = 8;

    /**
     * Which ADBC hierarchy level maps to StarRocks' "database" concept.
     * <ul>
     *   <li>{@code CATALOG} -- driver has meaningful catalog names but empty schemas
     *       (e.g. SQLite: "main" is a catalog).</li>
     *   <li>{@code SCHEMA} -- driver has meaningful schema names
     *       (e.g. PostgreSQL: "public" is a schema).</li>
     * </ul>
     */
    enum HierarchyModel {
        CATALOG,
        SCHEMA,
        /** Driver does not implement getObjects(); hierarchy must be probed at query time. */
        UNKNOWN
    }

    private final String catalogName;
    private final Map<String, String> properties;
    private final AdbcDatabase adbcDatabase;
    private final BufferAllocator allocator;
    private final ADBCSchemaResolver schemaResolver;

    // One pool per ADBCMetadata == one pool per (driver, uri, credentials, adbc.* params).
    // Different credentials are physically isolated by living in different ADBCMetadata instances.
    private final GenericObjectPool<AdbcConnection> connectionPool;

    // Stable table ID assignment -- IDs persist for the catalog's lifetime so that
    // the same table always gets the same ID within a session.
    private final ConcurrentHashMap<ADBCTableName, Integer> tableIdMap;

    // Resolved once on first metadata call, then cached for the catalog's lifetime.
    private volatile HierarchyModel hierarchyModel;

    public ADBCMetadata(Map<String, String> properties, String catalogName,
                        BufferAllocator allocator, AdbcDatabase adbcDatabase) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.allocator = allocator;
        this.adbcDatabase = adbcDatabase;
        this.schemaResolver = createSchemaResolver();
        this.tableIdMap = new ConcurrentHashMap<>();
        this.connectionPool = createConnectionPool();
    }

    // Visible for testing
    ADBCMetadata(Map<String, String> properties, String catalogName, AdbcDatabase adbcDatabase) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.allocator = new RootAllocator();
        this.adbcDatabase = adbcDatabase;
        this.schemaResolver = createSchemaResolver();
        this.tableIdMap = new ConcurrentHashMap<>();
        this.connectionPool = createConnectionPool();
    }

    private ADBCSchemaResolver createSchemaResolver() {
        return new ADBCSchemaResolver();
    }

    private GenericObjectPool<AdbcConnection> createConnectionPool() {
        @SuppressWarnings("rawtypes")
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(DEFAULT_POOL_MAX_TOTAL);
        poolConfig.setMaxIdle(DEFAULT_POOL_MAX_TOTAL);
        poolConfig.setMinIdle(0);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(5000);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(60_000);
        poolConfig.setMinEvictableIdleTimeMillis(300_000);
        poolConfig.setJmxEnabled(false);
        return new GenericObjectPool<>(new AdbcConnectionFactory(), poolConfig);
    }

    private AdbcConnection borrowConnection() throws Exception {
        try {
            return connectionPool.borrowObject();
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    "ADBC catalog '" + catalogName + "': connection pool exhausted (max="
                            + DEFAULT_POOL_MAX_TOTAL + ", waited 5s)", e);
        }
    }

    private void returnOrInvalidate(AdbcConnection conn, boolean invalidate) {
        if (conn == null) {
            return;
        }
        if (invalidate) {
            try {
                connectionPool.invalidateObject(conn);
            } catch (Exception e) {
                LOG.warn("Failed to invalidate pooled AdbcConnection for catalog {}: {}",
                        catalogName, e.getMessage());
            }
        } else {
            connectionPool.returnObject(conn);
        }
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
                LOG.info("ADBC driver does not implement getObjects(); hierarchy unknown, will probe at schema lookup");
                return HierarchyModel.UNKNOWN;
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
        AdbcConnection conn = null;
        boolean invalidate = false;
        try {
            conn = borrowConnection();
            HierarchyModel model = getHierarchyModel(conn);
            return listDbNamesFromGetObjects(conn, model);
        } catch (AdbcException e) {
            invalidate = true;
            throw new StarRocksConnectorException(
                    "Failed to list databases from ADBC catalog '" + catalogName
                            + "'. Detail: " + e.getMessage(), e);
        } catch (Exception e) {
            invalidate = true;
            throw new StarRocksConnectorException(
                    "Failed to list databases from ADBC catalog '" + catalogName
                            + "'. Detail: " + e.getMessage(), e);
        } finally {
            returnOrInvalidate(conn, invalidate);
        }
    }

    /**
     * List "databases" by reading the correct hierarchy level.
     *
     * <ul>
     *   <li>{@code CATALOG model}: returns catalog names (e.g. SQLite -> ["main"])</li>
     *   <li>{@code SCHEMA model}: returns schema names (e.g. PostgreSQL -> ["public", "pg_catalog"])</li>
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
                LOG.info("getObjects() not implemented; falling back to SQL discovery");
                return listDbNamesViaSql(conn);
            } else {
                throw e;
            }
        }

        if (result.isEmpty()) {
            return listDbNamesViaSql(conn);
        }
        return result;
    }

    /**
     * SQL fallback for listing databases when {@code getObjects()} is not implemented.
     * Tries standard {@code information_schema} queries in order of portability.
     */
    private List<String> listDbNamesViaSql(AdbcConnection conn) throws Exception {
        String[] queries = {
            "SELECT DISTINCT schema_name FROM information_schema.schemata",
            "SELECT DISTINCT table_schema FROM information_schema.tables",
        };

        for (String sql : queries) {
            try (AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery(sql);
                AdbcStatement.QueryResult qr = stmt.executeQuery();
                try (ArrowReader reader = qr.getReader()) {
                    List<String> result = new ArrayList<>();
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        org.apache.arrow.vector.VarCharVector nameVec =
                                (org.apache.arrow.vector.VarCharVector) root.getVector(0);
                        for (int i = 0; i < root.getRowCount(); i++) {
                            if (!nameVec.isNull(i)) {
                                result.add(new String(nameVec.get(i)));
                            }
                        }
                    }
                    if (!result.isEmpty()) {
                        LOG.info("ADBC catalog '{}': discovered {} databases via SQL fallback",
                                catalogName, result.size());
                        return result;
                    }
                }
            } catch (Exception ex) {
                LOG.debug("SQL fallback '{}' failed for catalog '{}': {}",
                        sql, catalogName, ex.getMessage());
            }
        }

        LOG.warn("ADBC catalog '{}': all SQL fallbacks failed, returning 'main'", catalogName);
        return List.of("main");
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        AdbcConnection conn = null;
        boolean invalidate = false;
        try {
            conn = borrowConnection();
            HierarchyModel model = getHierarchyModel(conn);
            return listTableNamesFromGetObjects(conn, dbName, model);
        } catch (AdbcException e) {
            invalidate = true;
            throw new StarRocksConnectorException(
                    "Failed to list tables from ADBC catalog '" + catalogName
                            + "', database '" + dbName + "'. Detail: " + e.getMessage(), e);
        } catch (Exception e) {
            invalidate = true;
            throw new StarRocksConnectorException(
                    "Failed to list tables from ADBC catalog '" + catalogName
                            + "', database '" + dbName + "'. Detail: " + e.getMessage(), e);
        } finally {
            returnOrInvalidate(conn, invalidate);
        }
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
        // For SCHEMA model, don't filter server-side -- filter client-side below
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
                LOG.info("getObjects() not implemented for table listing; falling back to SQL");
                return listTableNamesViaSql(conn, dbName);
            } else {
                throw e;
            }
        }
        return tables;
    }

    /**
     * SQL fallback for listing tables when {@code getObjects()} is not implemented.
     */
    private List<String> listTableNamesViaSql(AdbcConnection conn, String dbName) throws Exception {
        String escaped = dbName.replace("'", "''");
        String[] queries = {
            String.format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", escaped),
            String.format("SELECT table_name FROM information_schema.tables WHERE table_catalog = '%s'", escaped),
        };

        for (String sql : queries) {
            try (AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery(sql);
                AdbcStatement.QueryResult qr = stmt.executeQuery();
                try (ArrowReader reader = qr.getReader()) {
                    List<String> result = new ArrayList<>();
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        org.apache.arrow.vector.VarCharVector nameVec =
                                (org.apache.arrow.vector.VarCharVector) root.getVector(0);
                        for (int i = 0; i < root.getRowCount(); i++) {
                            if (!nameVec.isNull(i)) {
                                result.add(new String(nameVec.get(i)));
                            }
                        }
                    }
                    if (!result.isEmpty()) {
                        return result;
                    }
                }
            } catch (Exception ex) {
                LOG.debug("SQL fallback '{}' failed for catalog '{}': {}",
                        sql, catalogName, ex.getMessage());
            }
        }

        return List.of();
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        ADBCTableName key = ADBCTableName.of(catalogName, dbName, tblName);
        AdbcConnection conn = null;
        boolean invalidate = false;
        try {
            conn = borrowConnection();
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
            int tableId = tableIdMap.computeIfAbsent(key,
                    k -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
            return new ADBCTable(tableId, tblName, fullSchema, dbName, catalogName, properties);
        } catch (StarRocksConnectorException e) {
            invalidate = true;
            LOG.warn(e.getMessage());
            return null;
        } catch (Exception e) {
            invalidate = true;
            LOG.warn("Failed to get table '{}' from ADBC catalog '{}': {}",
                    dbName + "." + tblName, catalogName, e.getMessage());
            return null;
        } finally {
            returnOrInvalidate(conn, invalidate);
        }
    }

    /**
     * Get table schema using the native ADBC {@code getTableSchema()} API.
     *
     * <p>The hierarchy model determines how {@code dbName} is passed:
     * <ul>
     *   <li>{@code CATALOG model}: {@code getTableSchema(catalog=dbName, schema=null, table)}</li>
     *   <li>{@code SCHEMA model}: {@code getTableSchema(catalog=null, schema=dbName, table)}</li>
     *   <li>{@code UNKNOWN model}: tries schema first, then catalog, then query fallback</li>
     * </ul>
     *
     * <p>This replaces the previous approach of {@code SELECT * FROM table WHERE 1=0} which
     * failed for SQLite (returns int64 for all columns when no rows are present).
     */
    private Schema getTableSchemaViaADBC(AdbcConnection conn, String dbName,
                                          String tblName, HierarchyModel model)
            throws Exception {
        if (model == HierarchyModel.UNKNOWN) {
            return getTableSchemaProbing(conn, dbName, tblName);
        }

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
     * Probe both catalog and schema positions when the hierarchy model is unknown.
     * Tries schema first (more common), then catalog, then falls back to SQL query.
     */
    private Schema getTableSchemaProbing(AdbcConnection conn, String dbName, String tblName)
            throws Exception {
        // Try as schema first (PostgreSQL, StarRocks, DuckDB)
        try {
            Schema schema = conn.getTableSchema(null, dbName, tblName);
            if (schema != null) {
                LOG.info("ADBC catalog '{}': getTableSchema succeeded with schema='{}', "
                        + "upgrading hierarchy to SCHEMA", catalogName, dbName);
                setHierarchyModel(HierarchyModel.SCHEMA);
                return schema;
            }
        } catch (AdbcException e) {
            if (e.getStatus() != AdbcStatusCode.NOT_IMPLEMENTED
                    && e.getStatus() != AdbcStatusCode.NOT_FOUND) {
                throw e;
            }
            LOG.debug("getTableSchema(schema='{}') failed: {}", dbName, e.getMessage());
        }

        // Try as catalog (SQLite)
        try {
            Schema schema = conn.getTableSchema(dbName, null, tblName);
            if (schema != null) {
                LOG.info("ADBC catalog '{}': getTableSchema succeeded with catalog='{}', "
                        + "upgrading hierarchy to CATALOG", catalogName, dbName);
                setHierarchyModel(HierarchyModel.CATALOG);
                return schema;
            }
        } catch (AdbcException e) {
            if (e.getStatus() != AdbcStatusCode.NOT_IMPLEMENTED
                    && e.getStatus() != AdbcStatusCode.NOT_FOUND) {
                throw e;
            }
            LOG.debug("getTableSchema(catalog='{}') failed: {}", dbName, e.getMessage());
        }

        // Both failed, fall back to SQL
        LOG.warn("ADBC catalog '{}': getTableSchema failed for both schema and catalog positions, "
                + "falling back to query-based inference for {}.{}", catalogName, dbName, tblName);
        return getTableSchemaViaQuery(conn, dbName, tblName);
    }

    private void setHierarchyModel(HierarchyModel model) {
        synchronized (this) {
            this.hierarchyModel = model;
        }
    }

    /**
     * Fallback: infer schema by executing {@code SELECT * FROM db.table LIMIT 1}.
     * Uses LIMIT 1 (not WHERE 1=0) because SQLite returns int64 for all columns
     * when the result set is empty.
     *
     * <p>Tries the configured identifier quote first, then unquoted if that fails.
     * This handles FlightSQL where the transport uses ANSI quotes but the remote
     * database (e.g. StarRocks/MySQL) expects backticks or no quotes.
     */
    private Schema getTableSchemaViaQuery(AdbcConnection conn, String dbName, String tblName)
            throws Exception {
        String q = properties.getOrDefault("_sr_identifier_quote", "\"");
        String escapedDb = dbName.replace(q, q + q);
        String escapedTbl = tblName.replace(q, q + q);

        // Try with configured quoting first
        String quotedSql = String.format("SELECT * FROM %s%s%s.%s%s%s LIMIT 1",
                q, escapedDb, q, q, escapedTbl, q);
        try (AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(quotedSql);
            AdbcStatement.QueryResult qr = stmt.executeQuery();
            try (ArrowReader reader = qr.getReader()) {
                reader.loadNextBatch();
                return reader.getVectorSchemaRoot().getSchema();
            }
        } catch (Exception e) {
            LOG.debug("Schema query with quote '{}' failed: {}", q, e.getMessage());
        }

        // Try unquoted (works for simple identifiers on most databases)
        String unquotedSql = String.format("SELECT * FROM %s.%s LIMIT 1", dbName, tblName);
        try (AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(unquotedSql);
            AdbcStatement.QueryResult qr = stmt.executeQuery();
            try (ArrowReader reader = qr.getReader()) {
                reader.loadNextBatch();
                return reader.getVectorSchemaRoot().getSchema();
            }
        }
    }

    @Override
    public void refreshTable(String srDbName, Table table,
                              List<String> partitionNames, boolean onlyCachedPartitions) {
        // No cache to invalidate -- next getTable() fetches fresh metadata from driver
    }

    @Override
    public void clear() {
        tableIdMap.clear();
        synchronized (this) {
            hierarchyModel = null;
        }
    }

    @Override
    public void shutdown() {
        try {
            if (connectionPool != null) {
                connectionPool.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing ADBC connection pool in catalog {}: {}", catalogName, e.getMessage());
        }
        try {
            if (adbcDatabase != null) {
                adbcDatabase.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing ADBC database in catalog {}: {}", catalogName, e.getMessage());
        }
    }

    private final class AdbcConnectionFactory extends BasePooledObjectFactory<AdbcConnection> {
        @Override
        public AdbcConnection create() throws Exception {
            return adbcDatabase.connect();
        }

        @Override
        public PooledObject<AdbcConnection> wrap(AdbcConnection conn) {
            return new DefaultPooledObject<>(conn);
        }

        @Override
        public void destroyObject(PooledObject<AdbcConnection> p) {
            try {
                p.getObject().close();
            } catch (Exception e) {
                LOG.warn("Failed to close pooled AdbcConnection for catalog {}: {}",
                        catalogName, e.getMessage());
            }
        }

        @Override
        public boolean validateObject(PooledObject<AdbcConnection> p) {
            try (ArrowReader reader = p.getObject().getInfo()) {
                while (reader.loadNextBatch()) {
                    // drain to release Arrow buffers
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
