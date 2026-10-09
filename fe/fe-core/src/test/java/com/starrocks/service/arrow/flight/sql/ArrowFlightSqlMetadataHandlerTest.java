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

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlMetadataHandlerTest {

    private BufferAllocator allocator;
    private ConnectContext context;
    private MockedStatic<GlobalStateMgr> mockedGlobalState;
    private MetadataMgr metadataMgr;
    private CatalogMgr catalogMgr;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        context = mock(ConnectContext.class);

        GlobalStateMgr globalStateMgr = mock(GlobalStateMgr.class);
        metadataMgr = mock(MetadataMgr.class);
        catalogMgr = mock(CatalogMgr.class);
        when(globalStateMgr.getMetadataMgr()).thenReturn(metadataMgr);
        when(globalStateMgr.getCatalogMgr()).thenReturn(catalogMgr);
        when(catalogMgr.getCatalogs()).thenReturn(Map.of());

        mockedGlobalState = mockStatic(GlobalStateMgr.class);
        mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
    }

    @AfterEach
    public void tearDown() {
        mockedGlobalState.close();
        allocator.close();
    }

    private static String str(VectorSchemaRoot root, String field, int row) {
        VarCharVector vector = (VarCharVector) root.getVector(field);
        return vector.isNull(row) ? null : vector.getObject(row).toString();
    }

    @Test
    public void testBuildCatalogsIncludesInternalAndExternalSorted() {
        Map<String, Catalog> catalogs = new LinkedHashMap<>();
        catalogs.put("zeta_catalog", mock(Catalog.class));
        catalogs.put("alpha_catalog", mock(Catalog.class));
        when(catalogMgr.getCatalogs()).thenReturn(catalogs);

        try (VectorSchemaRoot root = ArrowFlightSqlMetadataHandler.buildCatalogs(allocator)) {
            assertEquals(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, root.getSchema());
            assertEquals(3, root.getRowCount());
            // TreeSet ordering: alpha_catalog, default_catalog, zeta_catalog
            assertEquals("alpha_catalog", str(root, "catalog_name", 0));
            assertEquals("default_catalog", str(root, "catalog_name", 1));
            assertEquals("zeta_catalog", str(root, "catalog_name", 2));
        }
    }

    @Test
    public void testBuildSchemasFiltersByCatalogAndPattern() {
        when(metadataMgr.listDbNames(any(), eq("default_catalog")))
                .thenReturn(List.of("sales_db", "ops_db", "sandbox"));

        FlightSql.CommandGetDbSchemas command = FlightSql.CommandGetDbSchemas.newBuilder()
                .setCatalog("default_catalog")
                .setDbSchemaFilterPattern("s%")
                .build();

        try (VectorSchemaRoot root = ArrowFlightSqlMetadataHandler.buildSchemas(command, allocator, context)) {
            assertEquals(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, root.getSchema());
            // "sales_db" and "sandbox" match "s%"; "ops_db" does not.
            assertEquals(2, root.getRowCount());
            assertEquals("default_catalog", str(root, "catalog_name", 0));
            assertEquals("sales_db", str(root, "db_schema_name", 0));
            assertEquals("sandbox", str(root, "db_schema_name", 1));
        }
    }

    @Test
    public void testBuildTablesAppliesTypeFilterAndNamespaceColumns() {
        when(metadataMgr.listDbNames(any(), eq("default_catalog"))).thenReturn(List.of("db1"));
        when(metadataMgr.listTableNames(any(), eq("default_catalog"), eq("db1")))
                .thenReturn(List.of("t_table", "t_view"));

        Table baseTable = mock(Table.class);
        when(baseTable.getMysqlType()).thenReturn("BASE TABLE");
        Table viewTable = mock(Table.class);
        when(viewTable.getMysqlType()).thenReturn("VIEW");
        when(metadataMgr.getTable(any(), eq("default_catalog"), eq("db1"), eq("t_table"))).thenReturn(baseTable);
        when(metadataMgr.getTable(any(), eq("default_catalog"), eq("db1"), eq("t_view"))).thenReturn(viewTable);

        FlightSql.CommandGetTables command = FlightSql.CommandGetTables.newBuilder()
                .setCatalog("default_catalog")
                .addTableTypes("BASE TABLE")
                .setIncludeSchema(false)
                .build();

        try (VectorSchemaRoot root = ArrowFlightSqlMetadataHandler.buildTables(command, allocator, context)) {
            assertEquals(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, root.getSchema());
            assertEquals(1, root.getRowCount());
            assertEquals("default_catalog", str(root, "catalog_name", 0));
            assertEquals("db1", str(root, "db_schema_name", 0));
            assertEquals("t_table", str(root, "table_name", 0));
            assertEquals("BASE TABLE", str(root, "table_type", 0));
        }
    }

    @Test
    public void testBuildPrimaryKeysReturnsKeyColumnsForPrimaryKeyTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.getKeysType()).thenReturn(KeysType.PRIMARY_KEYS);
        Column id = new Column("id", IntegerType.BIGINT, true);
        Column region = new Column("region", IntegerType.INT, true);
        when(table.getKeyColumns()).thenReturn(List.of(id, region));
        when(metadataMgr.getTable(any(), eq("default_catalog"), eq("db1"), eq("orders"))).thenReturn(table);

        FlightSql.CommandGetPrimaryKeys command = FlightSql.CommandGetPrimaryKeys.newBuilder()
                .setCatalog("default_catalog")
                .setDbSchema("db1")
                .setTable("orders")
                .build();

        try (VectorSchemaRoot root = ArrowFlightSqlMetadataHandler.buildPrimaryKeys(command, allocator, context)) {
            assertEquals(FlightSqlProducer.Schemas.GET_PRIMARY_KEYS_SCHEMA, root.getSchema());
            assertEquals(2, root.getRowCount());
            assertEquals("orders", str(root, "table_name", 0));
            assertEquals("id", str(root, "column_name", 0));
            assertEquals(1, ((IntVector) root.getVector("key_sequence")).get(0));
            assertEquals("region", str(root, "column_name", 1));
            assertEquals(2, ((IntVector) root.getVector("key_sequence")).get(1));
        }
    }

    @Test
    public void testBuildPrimaryKeysEmptyForNonPrimaryKeyTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        when(metadataMgr.getTable(any(), eq("default_catalog"), eq("db1"), eq("logs"))).thenReturn(table);

        FlightSql.CommandGetPrimaryKeys command = FlightSql.CommandGetPrimaryKeys.newBuilder()
                .setCatalog("default_catalog")
                .setDbSchema("db1")
                .setTable("logs")
                .build();

        try (VectorSchemaRoot root = ArrowFlightSqlMetadataHandler.buildPrimaryKeys(command, allocator, context)) {
            assertEquals(0, root.getRowCount());
        }
    }

    @Test
    public void testBuildTypeInfoFilterByDataType() {
        FlightSql.CommandGetXdbcTypeInfo command = FlightSql.CommandGetXdbcTypeInfo.newBuilder()
                .setDataType(java.sql.Types.INTEGER)
                .build();

        try (VectorSchemaRoot root = ArrowFlightSqlMetadataHandler.buildTypeInfo(command, allocator)) {
            assertEquals(FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA, root.getSchema());
            assertEquals(1, root.getRowCount());
            assertEquals("INT", str(root, "type_name", 0));
            assertEquals(java.sql.Types.INTEGER, ((IntVector) root.getVector("data_type")).get(0));
        }
    }
}
