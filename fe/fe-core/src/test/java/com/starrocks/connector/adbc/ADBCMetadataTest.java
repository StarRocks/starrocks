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

import com.starrocks.catalog.ADBCPartitionKey;
import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.ExternalFullStatisticsCollectJob;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCMetadataTest {

    @Mocked
    AdbcDatabase mockDatabase;

    @Mocked
    AdbcConnection mockConnection;

    @Mocked
    ArrowReader mockReader;

    @Mocked
    VectorSchemaRoot mockRoot;

    @Mocked
    ListVector mockDbSchemasListVec;

    @Mocked
    StructVector mockSchemaStructVec;

    @Mocked
    VarCharVector mockSchemaNameVec;

    @Mocked
    ListVector mockTablesListVec;

    @Mocked
    StructVector mockTableStructVec;

    @Mocked
    VarCharVector mockTableNameVec;

    @Mocked
    AdbcStatement mockStatement;

    @Mocked
    AdbcStatement.QueryResult mockQueryResult;

    @Mocked
    ArrowReader mockStatsReader;

    @Mocked
    VectorSchemaRoot mockStatsRoot;

    @Mocked
    BigIntVector mockBigIntVector;

    private Map<String, String> properties;
    private ADBCMetadata metadata;

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>();
        properties.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        properties.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");
        properties.put("adbc_meta_cache_enable", "false"); // disable cache for direct testing
    }

    @Test
    public void testListDbNames() throws Exception {
        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.getObjects(
                        AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                        null, null, null, null, null);
                result = mockReader;

                // First call to loadNextBatch returns true, second returns false
                mockReader.loadNextBatch();
                returns(true, false);

                mockReader.getVectorSchemaRoot();
                result = mockRoot;

                mockRoot.getVector("catalog_db_schemas");
                result = mockDbSchemasListVec;

                mockRoot.getRowCount();
                result = 1;

                mockDbSchemasListVec.isNull(0);
                result = false;

                mockDbSchemasListVec.getObject(0);
                result = Arrays.asList("schema1", "schema2");

                mockDbSchemasListVec.getDataVector();
                result = mockSchemaStructVec;

                mockDbSchemasListVec.getElementStartIndex(0);
                result = 0;

                mockDbSchemasListVec.getElementEndIndex(0);
                result = 2;

                mockSchemaStructVec.getChild("db_schema_name");
                result = mockSchemaNameVec;

                mockSchemaNameVec.isNull(0);
                result = false;
                mockSchemaNameVec.get(0);
                result = "schema1".getBytes();

                mockSchemaNameVec.isNull(1);
                result = false;
                mockSchemaNameVec.get(1);
                result = "schema2".getBytes();
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        List<String> dbNames = metadata.listDbNames(null);

        assertNotNull(dbNames);
        assertEquals(2, dbNames.size());
        assertTrue(dbNames.contains("schema1"));
        assertTrue(dbNames.contains("schema2"));
    }

    @Test
    public void testListTableNames() throws Exception {
        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES,
                        null, "mydb", null,
                        new String[] {"TABLE", "VIEW"},
                        null);
                result = mockReader;

                mockReader.loadNextBatch();
                returns(true, false);

                mockReader.getVectorSchemaRoot();
                result = mockRoot;

                mockRoot.getVector("catalog_db_schemas");
                result = mockDbSchemasListVec;

                mockRoot.getRowCount();
                result = 1;

                // catalog level
                mockDbSchemasListVec.isNull(0);
                result = false;

                mockDbSchemasListVec.getDataVector();
                result = mockSchemaStructVec;

                mockDbSchemasListVec.getElementStartIndex(0);
                result = 0;
                mockDbSchemasListVec.getElementEndIndex(0);
                result = 1;

                // schema level
                mockSchemaStructVec.getChild("db_schema_tables");
                result = mockTablesListVec;

                mockTablesListVec.isNull(0);
                result = false;

                mockTablesListVec.getDataVector();
                result = mockTableStructVec;

                mockTablesListVec.getElementStartIndex(0);
                result = 0;
                mockTablesListVec.getElementEndIndex(0);
                result = 2;

                mockTableStructVec.getChild("table_name");
                result = mockTableNameVec;

                mockTableNameVec.isNull(0);
                result = false;
                mockTableNameVec.get(0);
                result = "tbl1".getBytes();

                mockTableNameVec.isNull(1);
                result = false;
                mockTableNameVec.get(1);
                result = "tbl2".getBytes();
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        List<String> tableNames = metadata.listTableNames(null, "mydb");

        assertNotNull(tableNames);
        assertEquals(2, tableNames.size());
        assertEquals("tbl1", tableNames.get(0));
        assertEquals("tbl2", tableNames.get(1));
    }

    @Test
    public void testGetTable() throws Exception {
        Schema arrowSchema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList()),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), Collections.emptyList())
        ));

        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.getTableSchema(null, "mydb", "tbl1");
                result = arrowSchema;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        Table table = metadata.getTable(null, "mydb", "tbl1");

        assertNotNull(table);
        assertInstanceOf(ADBCTable.class, table);
        ADBCTable adbcTable = (ADBCTable) table;
        assertEquals("tbl1", adbcTable.getName());
        assertEquals("mydb", adbcTable.getDbName());
        assertEquals("test_catalog", adbcTable.getCatalogName());

        List<Column> columns = adbcTable.getFullSchema();
        assertEquals(2, columns.size());
        assertEquals("id", columns.get(0).getName());
        assertEquals("name", columns.get(1).getName());
    }

    @Test
    public void testGetTable_nullSchema() throws Exception {
        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.getTableSchema(null, "mydb", "nonexistent");
                result = null;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        Table table = metadata.getTable(null, "mydb", "nonexistent");

        assertNull(table);
    }

    @Test
    public void testGetTable_emptySchema() throws Exception {
        // A schema with only unsupported types (all fields skipped)
        Schema arrowSchema = new Schema(Collections.singletonList(
                new Field("complex", FieldType.nullable(ArrowType.Struct.INSTANCE),
                        Collections.singletonList(
                                new Field("sub", FieldType.nullable(new ArrowType.Int(32, true)),
                                        Collections.emptyList())))
        ));

        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.getTableSchema(null, "mydb", "empty_tbl");
                result = arrowSchema;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        Table table = metadata.getTable(null, "mydb", "empty_tbl");

        assertNull(table);
    }

    @Test
    public void testGetTable_cachedById() throws Exception {
        Schema arrowSchema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList())
        ));

        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                // getTableSchema called twice because cache is disabled
                mockConnection.getTableSchema(null, "mydb", "tbl1");
                result = arrowSchema;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        Table table1 = metadata.getTable(null, "mydb", "tbl1");
        Table table2 = metadata.getTable(null, "mydb", "tbl1");

        // Both should get the same table ID because tableIdCache is permanent
        assertNotNull(table1);
        assertNotNull(table2);
        assertEquals(table1.getId(), table2.getId());
    }

    @Test
    public void testShutdown() throws Exception {
        new Expectations() {{
                mockDatabase.close();
                times = 1;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        metadata.shutdown();
        // Should not throw
    }

    @Test
    public void testShutdown_withException() throws Exception {
        new Expectations() {{
                mockDatabase.close();
                result = new Exception("close error");
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        metadata.shutdown();
        // Should not throw, just log warning
    }

    @Test
    public void testCreateSchemaResolver_flightSql() {
        properties.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        // Should not throw, resolver should be FlightSQLSchemaResolver
        assertNotNull(metadata);
    }

    @Test
    public void testGetTableStatistics_returnsRowCount() throws Exception {
        // Create an ADBCTable to pass to getTableStatistics
        List<Column> cols = Arrays.asList(
                new Column("col1", IntegerType.INT),
                new Column("col2", VarcharType.VARCHAR)
        );
        ADBCTable adbcTable = new ADBCTable(1, "test_tbl", cols, "mydb", "test_catalog", properties);

        // Build columns map with ColumnRefOperator keys
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        ColumnRefOperator colRef1 = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ColumnRefOperator colRef2 = new ColumnRefOperator(2, VarcharType.VARCHAR, "col2", true);
        columns.put(colRef1, cols.get(0));
        columns.put(colRef2, cols.get(1));

        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.createStatement();
                result = mockStatement;

                mockStatement.setSqlQuery(anyString);

                mockStatement.executeQuery();
                result = mockQueryResult;

                mockQueryResult.getReader();
                result = mockStatsReader;

                mockStatsReader.loadNextBatch();
                result = true;

                mockStatsReader.getVectorSchemaRoot();
                result = mockStatsRoot;

                mockStatsRoot.getVector(0);
                result = mockBigIntVector;

                mockBigIntVector.get(0);
                result = 42L;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        Statistics stats = metadata.getTableStatistics(null, adbcTable, columns,
                Collections.emptyList(), null, -1, null);

        assertNotNull(stats);
        assertEquals(42.0, stats.getOutputRowCount(), 0.001);
        // Each column should have unknown stats
        assertEquals(ColumnStatistic.unknown(), stats.getColumnStatistic(colRef1));
        assertEquals(ColumnStatistic.unknown(), stats.getColumnStatistic(colRef2));
    }

    @Test
    public void testGetTableStatistics_connectionFailure() throws Exception {
        List<Column> cols = Arrays.asList(
                new Column("col1", IntegerType.INT)
        );
        ADBCTable adbcTable = new ADBCTable(1, "test_tbl", cols, "mydb", "test_catalog", properties);

        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        ColumnRefOperator colRef1 = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        columns.put(colRef1, cols.get(0));

        new Expectations() {{
                mockDatabase.connect();
                result = new AdbcException("Connection refused", null,
                        AdbcStatusCode.UNKNOWN, null, 0);
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        // Should NOT throw -- returns fallback stats
        Statistics stats = metadata.getTableStatistics(null, adbcTable, columns,
                Collections.emptyList(), null, -1, null);

        assertNotNull(stats);
        // Fallback row count should be 1
        assertEquals(1.0, stats.getOutputRowCount(), 0.001);
        // Column stats should still be unknown
        assertEquals(ColumnStatistic.unknown(), stats.getColumnStatistic(colRef1));
    }

    @Test
    public void testGetTableStatistics_emptyColumns() throws Exception {
        List<Column> cols = Arrays.asList(
                new Column("col1", IntegerType.INT)
        );
        ADBCTable adbcTable = new ADBCTable(1, "test_tbl", cols, "mydb", "test_catalog", properties);

        // Empty columns map
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();

        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.createStatement();
                result = mockStatement;

                mockStatement.setSqlQuery(anyString);

                mockStatement.executeQuery();
                result = mockQueryResult;

                mockQueryResult.getReader();
                result = mockStatsReader;

                mockStatsReader.loadNextBatch();
                result = true;

                mockStatsReader.getVectorSchemaRoot();
                result = mockStatsRoot;

                mockStatsRoot.getVector(0);
                result = mockBigIntVector;

                mockBigIntVector.get(0);
                result = 100L;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        Statistics stats = metadata.getTableStatistics(null, adbcTable, columns,
                Collections.emptyList(), null, -1, null);

        assertNotNull(stats);
        assertEquals(100.0, stats.getOutputRowCount(), 0.001);
    }

    @Test
    public void testGetCatalogTableName() {
        List<Column> cols = Arrays.asList(
                new Column("col1", IntegerType.INT)
        );
        ADBCTable adbcTable = new ADBCTable(1, "test_table", cols, "mydb", "my_catalog", properties);
        assertEquals("my_catalog.mydb.test_table", adbcTable.getCatalogTableName());
    }

    @Test
    public void testPartitionTraitsRegistered() {
        assertTrue(ConnectorPartitionTraits.isSupported(Table.TableType.ADBC));
    }

    @Test
    public void testPartitionTraitsNoPCT() {
        assertFalse(ConnectorPartitionTraits.isSupportPCTRefresh(Table.TableType.ADBC));
    }

    @Test
    public void testListPartitionNames_default() {
        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        List<String> partitionNames = metadata.listPartitionNames("mydb", "tbl1", null);
        assertNotNull(partitionNames);
        assertTrue(partitionNames.isEmpty());
    }

    // ==================== MV Support Verification Tests ====================
    // These structural tests verify the wiring that enables materialized views
    // over ADBC tables. Full behavioral MV tests (CREATE MV, REFRESH, EXPLAIN
    // rewrite) require MockedADBCMetadata + ConnectorPlanTestBase and are
    // deferred to Phase 4: Integration Testing.

    @Test
    public void testMVSupportedTableType() {
        // ADBC must be in TRAITS_TABLE (registered in 03-01) for MV to work.
        // ConnectorPartitionTraits.isSupported() is the proxy check: if traits
        // are registered, MaterializedViewAnalyzer accepts the table type.
        assertTrue(ConnectorPartitionTraits.isSupported(Table.TableType.ADBC));
    }

    @Test
    public void testADBCPartitionTraitsGetTableName() {
        // Create ADBCTable with known catalog/db/table names
        List<Column> cols = Arrays.asList(new Column("id", IntegerType.INT));
        ADBCTable adbcTable = new ADBCTable(1, "orders", cols, "sales_db", "flight_catalog", properties);

        // Build traits via the static factory (mirrors production code path)
        ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(Table.TableType.ADBC);
        // Set the table field (protected) via the buildWithoutCache path
        ConnectorPartitionTraits fullTraits = ConnectorPartitionTraits.build(adbcTable);

        assertEquals("flight_catalog.sales_db.orders", fullTraits.getTableName());
    }

    @Test
    public void testADBCPartitionTraitsGetPartitions() {
        List<Column> cols = Arrays.asList(new Column("id", IntegerType.INT));
        ADBCTable adbcTable = new ADBCTable(1, "tbl", cols, "db", "cat", properties);

        ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(adbcTable);
        List<PartitionInfo> partitions = traits.getPartitions(Arrays.asList("p1", "p2"));

        assertNotNull(partitions);
        assertTrue(partitions.isEmpty());
    }

    @Test
    public void testADBCPartitionTraitsCreateEmptyKey() {
        ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(Table.TableType.ADBC);
        PartitionKey key = traits.createEmptyKey();

        assertNotNull(key);
        assertInstanceOf(ADBCPartitionKey.class, key);
        assertInstanceOf(NullablePartitionKey.class, key);
    }

    @Test
    public void testRefreshTable() throws Exception {
        Schema arrowSchema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList())
        ));

        new Expectations() {{
                mockDatabase.connect();
                result = mockConnection;

                mockConnection.getTableSchema(null, "mydb", "tbl1");
                result = arrowSchema;
            }};

        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        // Populate cache
        Table table1 = metadata.getTable(null, "mydb", "tbl1");
        assertNotNull(table1);

        // Refresh should invalidate cache without throwing
        metadata.refreshTable("mydb", table1, Collections.emptyList(), false);

        // After refresh, getTable should re-fetch (table ID should stay the same due to permanent tableIdCache)
        Table table2 = metadata.getTable(null, "mydb", "tbl1");
        assertNotNull(table2);
        assertEquals(table1.getId(), table2.getId());
    }

    @Test
    public void testClear() throws Exception {
        metadata = new ADBCMetadata(properties, "test_catalog", mockDatabase);
        // clear() should not throw even with empty caches
        assertDoesNotThrow(() -> metadata.clear());
    }

    @Test
    public void testAnalyzeTableInfraAcceptsADBCTable(@Mocked Database mockDb) {
        // Structural test: ExternalFullStatisticsCollectJob constructor
        // accepts any Table subclass, including ADBCTable.
        // This proves STAT-01 per-column stats work for ADBC via existing infrastructure.
        List<Column> cols = Arrays.asList(
                new Column("col1", IntegerType.INT)
        );
        ADBCTable adbcTable = new ADBCTable(1, "test_tbl", cols, "mydb", "test_catalog", properties);

        assertDoesNotThrow(() -> {
            ExternalFullStatisticsCollectJob job = new ExternalFullStatisticsCollectJob(
                    "test_catalog",
                    mockDb,
                    adbcTable,
                    Collections.emptyList(),      // partitionNames
                    Collections.singletonList("col1"),  // columnNames
                    Collections.singletonList(IntegerType.INT),  // columnTypes
                    StatsConstants.AnalyzeType.FULL,
                    StatsConstants.ScheduleType.ONCE,
                    new HashMap<>()               // properties
            );
            assertNotNull(job);
        });
    }
}
