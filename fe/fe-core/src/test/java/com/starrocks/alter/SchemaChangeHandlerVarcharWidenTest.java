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

package com.starrocks.alter;

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.TestWithFeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaChangeHandlerVarcharWidenTest extends TestWithFeService {
    private static final String DB_NAME = "test";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB_NAME);
        useDatabase(DB_NAME);
    }

    @Override
    protected void runBeforeEach() {
        connectContext.setThreadLocalInfo();
        useDatabase(DB_NAME);
    }

    @Override
    protected void runAfterEach() {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    @Test
    public void testPartitionColumnWidenUsesFastPath() throws Exception {
        String tableName = "t_partition_distribution_fast";
        createPartitionAndDistributionTable(tableName);

        AlterJobV2 job = analyzeAndCreateJob(
                "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 VARCHAR(30) KEY NOT NULL",
                tableName);

        Assertions.assertNull(job);
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
    }

    @Test
    public void testPartitionColumnFinalNonWidenRejected() throws Exception {
        String tableName = "t_partition_distribution_reject";
        createPartitionAndDistributionTable(tableName);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 INT KEY NOT NULL",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains("Can not modify partition column[k1]"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
    }

    @Test
    public void testDistributionColumnFinalNonWidenRejected() throws Exception {
        String tableName = "t_distribution_reject";
        createDistributionTable(tableName, false);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 INT KEY NOT NULL",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains("Can not modify distribution column[k1]"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
    }

    @Test
    public void testKeyColumnWidenUsesFastPath() throws Exception {
        String tableName = "t_key_column_fast";
        createKeyColumnTable(tableName);

        AlterJobV2 job = analyzeAndCreateJob(
                "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 VARCHAR(30) KEY NOT NULL",
                tableName);

        Assertions.assertNull(job);
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
    }

    @Test
    public void testKeyColumnNonWidenCreatesJob() throws Exception {
        String tableName = "t_key_column_job";
        createKeyColumnTable(tableName);

        AlterJobV2 job = analyzeAndCreateJob(
                "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 INT KEY NOT NULL",
                tableName);

        Assertions.assertNotNull(job);
        Assertions.assertInstanceOf(SchemaChangeJobV2.class, job);
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
    }

    @Test
    public void testBaseClauseRollupColumnWidenUsesFastPath() throws Exception {
        String tableName = "t_rollup_fast";
        createRollupTable(tableName);

        AlterJobV2 job = analyzeAndCreateJob(
                "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 VARCHAR(30) KEY NOT NULL",
                tableName);

        Assertions.assertNull(job);
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 30);
        assertColumnType(tableName, "r1", "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
        assertIndexColumns(tableName, "r1", "k2", "k1");
    }

    @Test
    public void testRollupClauseNonWidenRejectedByBaseDistribution() throws Exception {
        String tableName = "t_rollup_reject";
        createRollupTable(tableName);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 INT KEY NOT NULL FIRST FROM r1",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains("Can not modify distribution column[k1]"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
        assertColumnType(tableName, "r1", "k1", PrimitiveType.VARCHAR, 10);
        assertIndexColumns(tableName, "r1", "k2", "k1");
    }

    @Test
    public void testColocateDistributionColumnWidenRejected() throws Exception {
        ColocateTableIndex originalColocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        try {
            GlobalStateMgr.getCurrentState().setColocateTableIndex(new ColocateTableIndex());
            String tableName = "t_colocate_reject";
            createDistributionTable(tableName, true);

            OlapTable table = getTable(tableName);
            Assertions.assertTrue(
                    GlobalStateMgr.getCurrentState().getColocateTableIndex().isColocateTable(table.getId()));

            Exception exception = Assertions.assertThrows(Exception.class,
                    () -> analyzeAndCreateJob(
                            "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 VARCHAR(20) KEY NOT NULL",
                            tableName));

            Assertions.assertTrue(exception.getMessage().contains("for colocate table"),
                    exception.getMessage());
            assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
            assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
        } finally {
            GlobalStateMgr.getCurrentState().setColocateTableIndex(originalColocateIndex);
        }
    }

    @Test
    public void testPrimaryKeyColumnWidenUsesFastPath() throws Exception {
        String tableName = "t_pk_key_fast";
        createPrimaryKeyTable(tableName);

        AlterJobV2 job = analyzeAndCreateJob(
                "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 VARCHAR(30)",
                tableName);

        Assertions.assertNull(job);
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(tableName, tableName, "k1", "v1");
    }

    @Test
    public void testPrimaryKeySortColumnWidenUsesFastPath() throws Exception {
        String tableName = "t_pk_sort_fast";
        createPrimaryKeySortTable(tableName);

        AlterJobV2 job = analyzeAndCreateJob(
                "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 VARCHAR(30)",
                tableName);

        Assertions.assertNull(job);
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(tableName, tableName, "k0", "k1", "v1");
    }

    @Test
    public void testPrimaryKeyColumnNonWidenRejected() throws Exception {
        String tableName = "t_pk_key_reject";
        createPrimaryKeyTable(tableName);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 INT",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains(
                        "Can not modify key column: k1 for primary key table except for increasing varchar length"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
    }

    @Test
    public void testPrimaryKeySortColumnNonWidenRejected() throws Exception {
        String tableName = "t_pk_sort_reject";
        createPrimaryKeySortTable(tableName);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 BIGINT",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains(
                        "Can not modify sort column in primary data model table except for increasing varchar length"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
    }

    @Test
    public void testPrimaryKeyKeyReorderRejected() throws Exception {
        String tableName = "t_pk_reorder_reject";
        createPrimaryKeyReorderTable(tableName);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k2 VARCHAR(30) FIRST",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains("Can not reorder pk keys by modify column"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k2", PrimitiveType.VARCHAR, 10);
        assertIndexColumns(tableName, tableName, "k1", "k2", "v1");
    }

    @Test
    public void testRollupBaseClauseNonWidenRejected() throws Exception {
        String tableName = "t_rollup_base_non_widen_reject";
        createRollupTable(tableName);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(
                        "ALTER TABLE test." + tableName + " MODIFY COLUMN k1 INT KEY NOT NULL",
                        tableName));

        Assertions.assertTrue(exception.getMessage().contains("Can not modify distribution column[k1]"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
        assertColumnType(tableName, "r1", "k1", PrimitiveType.VARCHAR, 10);
    }

    @Test
    public void testPrimaryKeyColumnWidenToNullableRejectedByHandler() throws Exception {
        String tableName = "t_pk_nullable_reject";
        createPrimaryKeyTable(tableName);

        ColumnDef columnDef = new ColumnDef(
                "k1",
                new TypeDef(TypeFactory.createVarcharType(20)),
                false,
                null,
                null,
                true,
                ColumnDef.DefaultValueDef.NOT_SET,
                "");
        ModifyColumnClause clause = new ModifyColumnClause(columnDef, null, null, Map.of());

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> analyzeAndCreateJob(List.of(clause), tableName));

        Assertions.assertTrue(exception.getMessage().contains("primary key column[k1] cannot be nullable"),
                exception.getMessage());
        assertColumnType(tableName, tableName, "k1", PrimitiveType.VARCHAR, 10);
    }

    private AlterJobV2 analyzeAndCreateJob(String sql, String tableName) throws Exception {
        connectContext.setThreadLocalInfo();
        AlterTableStmt alterStmt = (AlterTableStmt) parseAndAnalyzeStmt(sql, connectContext);
        return analyzeAndCreateJob(alterStmt.getAlterClauseList(), tableName);
    }

    private AlterJobV2 analyzeAndCreateJob(List<AlterClause> alterClauses, String tableName) throws Exception {
        connectContext.setThreadLocalInfo();
        Database db = getDatabase();
        OlapTable table = getTable(tableName);
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(alterClauses, db, table);
    }

    private Database getDatabase() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
    }

    private OlapTable getTable(String tableName) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(getDatabase().getFullName(), tableName);
    }

    private Column getColumn(String tableName, String indexName, String columnName) {
        OlapTable table = getTable(tableName);
        Long indexMetaId = table.getIndexMetaIdByName(indexName);
        Assertions.assertNotNull(indexMetaId, indexName);
        return table.getSchemaByIndexMetaId(indexMetaId).stream()
                .filter(column -> column.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "column " + columnName + " not found in index " + indexName));
    }

    private void assertColumnType(String tableName, String indexName, String columnName,
                                  PrimitiveType primitiveType, int expectedLength) {
        Column column = getColumn(tableName, indexName, columnName);
        Assertions.assertEquals(primitiveType, column.getPrimitiveType());
        if (primitiveType == PrimitiveType.VARCHAR) {
            Assertions.assertEquals(expectedLength, column.getStrLen());
        }
    }

    private void assertIndexColumns(String tableName, String indexName, String... expectedColumns) {
        OlapTable table = getTable(tableName);
        Long indexMetaId = table.getIndexMetaIdByName(indexName);
        Assertions.assertNotNull(indexMetaId, indexName);
        List<String> actualColumns = table.getSchemaByIndexMetaId(indexMetaId).stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(List.of(expectedColumns), actualColumns);
    }

    private void createPartitionAndDistributionTable(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1, k2)\n"
                + "PARTITION BY LIST(k1) (\n"
                + "  PARTITION p1 VALUES IN (\"a\"),\n"
                + "  PARTITION p2 VALUES IN (\"b\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");
    }

    private void createDistributionTable(String tableName, boolean colocate) throws Exception {
        String colocateProperty = colocate ? ",\n  \"colocate_with\" = \"cg_varchar_widen\"" : "";
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\""
                + colocateProperty + "\n"
                + ");");
    }

    private void createKeyColumnTable(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k2) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");
    }

    private void createRollupTable(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "ROLLUP (\n"
                + "  r1(k2, k1)\n"
                + ")\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");
    }

    private void createPrimaryKeyTable(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "PRIMARY KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");
    }

    private void createPrimaryKeySortTable(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k0 INT NOT NULL,\n"
                + "  k1 VARCHAR(10),\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "PRIMARY KEY(k0)\n"
                + "DISTRIBUTED BY HASH(k0) BUCKETS 1\n"
                + "ORDER BY(k1)\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");
    }

    private void createPrimaryKeyReorderTable(String tableName) throws Exception {
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 VARCHAR(10) NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "PRIMARY KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"fast_schema_evolution\" = \"true\"\n"
                + ");");
    }
}
