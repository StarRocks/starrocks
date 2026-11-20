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

package com.starrocks.service.arrow.flight.sql;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.PatternMatcher;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.KeysType;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Builds Arrow {@link VectorSchemaRoot}s for the Flight SQL metadata commands
 * ({@code GetCatalogs}, {@code GetDbSchemas}, {@code GetTables}, ...), backed by the FE's in-memory
 * catalog/database/table metadata.
 *
 * <p>The roots produced here conform exactly to the {@code FlightSqlProducer.Schemas.GET_*_SCHEMA}
 * definitions, which are the schemas advertised by the matching {@code getFlightInfo*} handlers in
 * {@link ArrowFlightSqlServiceImpl}. Each builder maps StarRocks' three-level
 * {@code catalog -> database -> table} namespace onto Flight SQL's
 * {@code catalog_name -> db_schema_name -> table_name} columns.
 *
 * <p>The caller owns the returned root and must {@link VectorSchemaRoot#close()} it after streaming.
 */
public final class ArrowFlightSqlMetadataHandler {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlMetadataHandler.class);

    private ArrowFlightSqlMetadataHandler() {
    }

    // ------------------------------------------------------------------
    // Catalogs
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildCatalogs(BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator);
        VarCharVector catalogVector = (VarCharVector) root.getVector("catalog_name");
        catalogVector.allocateNew();

        int row = 0;
        for (String catalog : listCatalogNames()) {
            setVarChar(catalogVector, row++, catalog);
        }
        root.setRowCount(row);
        return root;
    }

    // ------------------------------------------------------------------
    // DB schemas
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildSchemas(FlightSql.CommandGetDbSchemas command, BufferAllocator allocator,
                                                 ConnectContext context) {
        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator);
        VarCharVector catalogVector = (VarCharVector) root.getVector("catalog_name");
        VarCharVector dbSchemaVector = (VarCharVector) root.getVector("db_schema_name");
        catalogVector.allocateNew();
        dbSchemaVector.allocateNew();

        PatternMatcher dbPattern = createPattern(
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null);

        int row = 0;
        for (String catalog : catalogsForCommand(command.hasCatalog() ? command.getCatalog() : null)) {
            for (String db : listDbNames(context, catalog)) {
                if (dbPattern != null && !dbPattern.match(db)) {
                    continue;
                }
                setVarChar(catalogVector, row, catalog);
                setVarChar(dbSchemaVector, row, db);
                row++;
            }
        }
        root.setRowCount(row);
        return root;
    }

    // ------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildTables(FlightSql.CommandGetTables command, BufferAllocator allocator,
                                               ConnectContext context) {
        boolean includeSchema = command.getIncludeSchema();
        Schema arrowSchema = includeSchema ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);

        VarCharVector catalogVector = (VarCharVector) root.getVector("catalog_name");
        VarCharVector dbSchemaVector = (VarCharVector) root.getVector("db_schema_name");
        VarCharVector tableNameVector = (VarCharVector) root.getVector("table_name");
        VarCharVector tableTypeVector = (VarCharVector) root.getVector("table_type");
        VarBinaryVector tableSchemaVector = includeSchema ? (VarBinaryVector) root.getVector("table_schema") : null;
        for (Field field : arrowSchema.getFields()) {
            root.getVector(field.getName()).allocateNew();
        }

        PatternMatcher dbPattern = createPattern(
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null);
        PatternMatcher tablePattern = createPattern(
                command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null);
        List<String> tableTypeFilter = command.getTableTypesList();

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        int row = 0;
        for (String catalog : catalogsForCommand(command.hasCatalog() ? command.getCatalog() : null)) {
            for (String db : listDbNames(context, catalog)) {
                if (dbPattern != null && !dbPattern.match(db)) {
                    continue;
                }
                for (String tableName : listTableNames(context, catalog, db)) {
                    if (tablePattern != null && !tablePattern.match(tableName)) {
                        continue;
                    }
                    Table table;
                    try {
                        table = metadataMgr.getTable(context, catalog, db, tableName);
                    } catch (Exception e) {
                        LOG.warn("[ARROW] Failed to load table {}.{}.{} for GetTables, skipping",
                                catalog, db, tableName, e);
                        continue;
                    }
                    if (table == null) {
                        continue;
                    }
                    String tableType = table.getMysqlType();
                    if (!tableTypeFilter.isEmpty() && !tableTypeFilter.contains(tableType)) {
                        continue;
                    }

                    setVarChar(catalogVector, row, catalog);
                    setVarChar(dbSchemaVector, row, db);
                    setVarChar(tableNameVector, row, tableName);
                    setVarChar(tableTypeVector, row, tableType);
                    if (tableSchemaVector != null) {
                        tableSchemaVector.setSafe(row, serializeTableSchema(table));
                    }
                    row++;
                }
            }
        }
        root.setRowCount(row);
        return root;
    }

    // ------------------------------------------------------------------
    // Table types
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildTableTypes(BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLE_TYPES_SCHEMA, allocator);
        VarCharVector tableTypeVector = (VarCharVector) root.getVector("table_type");
        tableTypeVector.allocateNew();

        // The distinct set of values that Table#getMysqlType() can emit.
        String[] tableTypes = {"BASE TABLE", "VIEW", "SYSTEM VIEW"};
        for (int i = 0; i < tableTypes.length; i++) {
            setVarChar(tableTypeVector, i, tableTypes[i]);
        }
        root.setRowCount(tableTypes.length);
        return root;
    }

    // ------------------------------------------------------------------
    // Primary keys
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, BufferAllocator allocator,
                                                    ConnectContext context) {
        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_PRIMARY_KEYS_SCHEMA, allocator);
        VarCharVector catalogVector = (VarCharVector) root.getVector("catalog_name");
        VarCharVector dbSchemaVector = (VarCharVector) root.getVector("db_schema_name");
        VarCharVector tableNameVector = (VarCharVector) root.getVector("table_name");
        VarCharVector columnNameVector = (VarCharVector) root.getVector("column_name");
        IntVector keySequenceVector = (IntVector) root.getVector("key_sequence");
        VarCharVector keyNameVector = (VarCharVector) root.getVector("key_name");
        for (Field field : Schemas.GET_PRIMARY_KEYS_SCHEMA.getFields()) {
            root.getVector(field.getName()).allocateNew();
        }

        String catalog = command.hasCatalog() ? command.getCatalog() : InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        String db = command.hasDbSchema() ? command.getDbSchema() : null;
        String tableName = command.getTable();

        int row = 0;
        for (Column column : primaryKeyColumns(context, catalog, db, tableName)) {
            setVarChar(catalogVector, row, catalog);
            setVarChar(dbSchemaVector, row, db);
            setVarChar(tableNameVector, row, tableName);
            setVarChar(columnNameVector, row, column.getName());
            keySequenceVector.setSafe(row, row + 1);
            keyNameVector.setNull(row);
            row++;
        }
        root.setRowCount(row);
        return root;
    }

    // ------------------------------------------------------------------
    // Imported / exported / cross-reference keys
    //
    // StarRocks does not model foreign-key relationships, so these always return an empty stream that
    // still conforms to the (shared) imported/exported/cross-reference key schema.
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildImportedKeys(BufferAllocator allocator) {
        return emptyRoot(Schemas.GET_IMPORTED_KEYS_SCHEMA, allocator);
    }

    public static VectorSchemaRoot buildExportedKeys(BufferAllocator allocator) {
        return emptyRoot(Schemas.GET_EXPORTED_KEYS_SCHEMA, allocator);
    }

    public static VectorSchemaRoot buildCrossReference(BufferAllocator allocator) {
        return emptyRoot(Schemas.GET_CROSS_REFERENCE_SCHEMA, allocator);
    }

    // ------------------------------------------------------------------
    // XDBC type info
    // ------------------------------------------------------------------

    public static VectorSchemaRoot buildTypeInfo(FlightSql.CommandGetXdbcTypeInfo command, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TYPE_INFO_SCHEMA, allocator);
        for (Field field : Schemas.GET_TYPE_INFO_SCHEMA.getFields()) {
            root.getVector(field.getName()).allocateNew();
        }

        VarCharVector typeNameVector = (VarCharVector) root.getVector("type_name");
        IntVector dataTypeVector = (IntVector) root.getVector("data_type");
        IntVector columnSizeVector = (IntVector) root.getVector("column_size");
        VarCharVector literalPrefixVector = (VarCharVector) root.getVector("literal_prefix");
        VarCharVector literalSuffixVector = (VarCharVector) root.getVector("literal_suffix");
        ListVector createParamsVector = (ListVector) root.getVector("create_params");
        IntVector nullableVector = (IntVector) root.getVector("nullable");
        BitVector caseSensitiveVector = (BitVector) root.getVector("case_sensitive");
        IntVector searchableVector = (IntVector) root.getVector("searchable");
        BitVector unsignedVector = (BitVector) root.getVector("unsigned_attribute");
        BitVector fixedPrecScaleVector = (BitVector) root.getVector("fixed_prec_scale");
        BitVector autoIncrementVector = (BitVector) root.getVector("auto_increment");
        VarCharVector localTypeNameVector = (VarCharVector) root.getVector("local_type_name");
        IntVector minScaleVector = (IntVector) root.getVector("minimum_scale");
        IntVector maxScaleVector = (IntVector) root.getVector("maximum_scale");
        IntVector sqlDataTypeVector = (IntVector) root.getVector("sql_data_type");
        IntVector datetimeSubcodeVector = (IntVector) root.getVector("datetime_subcode");
        IntVector numPrecRadixVector = (IntVector) root.getVector("num_prec_radix");
        IntVector intervalPrecisionVector = (IntVector) root.getVector("interval_precision");

        Integer dataTypeFilter = command.hasDataType() ? command.getDataType() : null;

        int row = 0;
        for (TypeInfo type : STARROCKS_TYPE_INFO) {
            if (dataTypeFilter != null && dataTypeFilter != type.jdbcType) {
                continue;
            }
            setVarChar(typeNameVector, row, type.name);
            dataTypeVector.setSafe(row, type.jdbcType);
            if (type.columnSize == null) {
                columnSizeVector.setNull(row);
            } else {
                columnSizeVector.setSafe(row, type.columnSize);
            }
            setVarChar(literalPrefixVector, row, type.stringType ? "'" : null);
            setVarChar(literalSuffixVector, row, type.stringType ? "'" : null);
            // create_params is informational; StarRocks does not surface it, so leave it null.
            createParamsVector.setNull(row);
            nullableVector.setSafe(row, DatabaseMetaData.typeNullable);
            caseSensitiveVector.setSafe(row, type.stringType ? 1 : 0);
            searchableVector.setSafe(row, DatabaseMetaData.typeSearchable);
            unsignedVector.setSafe(row, 0);
            fixedPrecScaleVector.setSafe(row, 0);
            autoIncrementVector.setNull(row);
            setVarChar(localTypeNameVector, row, type.name);
            minScaleVector.setNull(row);
            maxScaleVector.setNull(row);
            sqlDataTypeVector.setSafe(row, type.jdbcType);
            datetimeSubcodeVector.setNull(row);
            numPrecRadixVector.setSafe(row, 10);
            intervalPrecisionVector.setNull(row);
            row++;
        }
        root.setRowCount(row);
        return root;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static VectorSchemaRoot emptyRoot(Schema schema, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.setRowCount(0);
        return root;
    }

    /**
     * The internal {@code default_catalog} plus every registered external catalog, sorted for a stable order.
     * {@link CatalogMgr#getCatalogs()} only returns external catalogs, so the internal one is added explicitly.
     */
    private static List<String> listCatalogNames() {
        TreeSet<String> catalogs = new TreeSet<>();
        catalogs.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        catalogs.addAll(GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs().keySet());
        return new ArrayList<>(catalogs);
    }

    /**
     * Resolve the catalogs a command should enumerate: the single requested catalog when {@code catalog} is set,
     * otherwise every catalog. A {@code null} value means "no filter" in the Flight SQL protocol.
     */
    private static List<String> catalogsForCommand(String catalog) {
        if (catalog == null) {
            return listCatalogNames();
        }
        return Lists.newArrayList(catalog);
    }

    private static List<String> listDbNames(ConnectContext context, String catalog) {
        try {
            return GlobalStateMgr.getCurrentState().getMetadataMgr().listDbNames(context, catalog);
        } catch (Exception e) {
            LOG.warn("[ARROW] Failed to list databases for catalog {}, skipping", catalog, e);
            return Lists.newArrayList();
        }
    }

    private static List<String> listTableNames(ConnectContext context, String catalog, String db) {
        try {
            return GlobalStateMgr.getCurrentState().getMetadataMgr().listTableNames(context, catalog, db);
        } catch (Exception e) {
            LOG.warn("[ARROW] Failed to list tables for {}.{}, skipping", catalog, db, e);
            return Lists.newArrayList();
        }
    }

    /**
     * Returns the primary-key columns of a table, or an empty list when the table is missing or is not a
     * StarRocks PRIMARY KEY table (the only model that maps to a SQL primary key).
     */
    private static List<Column> primaryKeyColumns(ConnectContext context, String catalog, String db, String tableName) {
        if (db == null || tableName == null || tableName.isEmpty()) {
            return Lists.newArrayList();
        }
        Table table;
        try {
            table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, catalog, db, tableName);
        } catch (Exception e) {
            LOG.warn("[ARROW] Failed to load table {}.{}.{} for GetPrimaryKeys", catalog, db, tableName, e);
            return Lists.newArrayList();
        }
        if (!(table instanceof OlapTable)) {
            return Lists.newArrayList();
        }
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
            return Lists.newArrayList();
        }
        return olapTable.getKeyColumns();
    }

    private static PatternMatcher createPattern(String filterPattern) {
        if (filterPattern == null || filterPattern.isEmpty()) {
            return null;
        }
        // Database and table names are matched case-insensitively, consistent with SR's identifier handling.
        return PatternMatcher.createMysqlPattern(filterPattern, false);
    }

    private static byte[] serializeTableSchema(Table table) {
        List<Field> fields = Lists.newArrayList();
        try {
            for (Column column : table.getBaseSchema()) {
                fields.add(ArrowUtils.convertToArrowType(column.getType(), column.getName(), column.isAllowNull()));
            }
        } catch (Exception e) {
            // A single column with a type Arrow can't represent must not fail the whole GetTables stream;
            // fall back to an empty schema for this table.
            LOG.warn("[ARROW] Failed to convert schema of table {} for GetTables, emitting empty schema",
                    table.getName(), e);
            fields.clear();
        }
        return ArrowFlightSqlServiceImpl.serializeMetadata(new Schema(fields)).array();
    }

    private static void setVarChar(VarCharVector vector, int index, String value) {
        if (value == null) {
            vector.setNull(index);
        } else {
            vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class TypeInfo {
        private final String name;
        private final int jdbcType;
        private final Integer columnSize;
        private final boolean stringType;

        private TypeInfo(String name, int jdbcType, Integer columnSize, boolean stringType) {
            this.name = name;
            this.jdbcType = jdbcType;
            this.columnSize = columnSize;
            this.stringType = stringType;
        }
    }

    // The set of StarRocks SQL types exposed to clients, mapped onto their closest JDBC type codes.
    private static final List<TypeInfo> STARROCKS_TYPE_INFO = List.of(
            new TypeInfo("BOOLEAN", Types.BOOLEAN, 1, false),
            new TypeInfo("TINYINT", Types.TINYINT, 3, false),
            new TypeInfo("SMALLINT", Types.SMALLINT, 5, false),
            new TypeInfo("INT", Types.INTEGER, 10, false),
            new TypeInfo("BIGINT", Types.BIGINT, 19, false),
            new TypeInfo("LARGEINT", Types.DECIMAL, 39, false),
            new TypeInfo("FLOAT", Types.REAL, 7, false),
            new TypeInfo("DOUBLE", Types.DOUBLE, 15, false),
            new TypeInfo("DECIMAL", Types.DECIMAL, 38, false),
            new TypeInfo("CHAR", Types.CHAR, 255, true),
            new TypeInfo("VARCHAR", Types.VARCHAR, 1048576, true),
            new TypeInfo("STRING", Types.VARCHAR, 1048576, true),
            new TypeInfo("JSON", Types.VARCHAR, null, true),
            new TypeInfo("DATE", Types.DATE, 10, false),
            new TypeInfo("DATETIME", Types.TIMESTAMP, 26, false),
            new TypeInfo("VARBINARY", Types.VARBINARY, null, false));
}
