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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableStatementAnalyzer;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests for the "compression_dict_columns" table property: the CREATE TABLE path, SHOW CREATE TABLE
 * echo, the OlapTable copy/accessor helpers, the SchemaInfo -> BE plumbing and the ALTER TABLE path.
 */
public class CompressionDictColumnsTest {
    private static final String DB_NAME = "test_compression_dict";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    private static String createTableSql(String tableName, String extraProperties) {
        return "CREATE TABLE " + DB_NAME + "." + tableName + " (\n"
                + "  k1 int,\n"
                + "  v1 string,\n"
                + "  v2 json,\n"
                + "  v3 int\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\"" + extraProperties + ");";
    }

    private static OlapTable getTable(String tableName) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, tableName);
        Assertions.assertNotNull(table, "table " + tableName + " not found");
        return (OlapTable) table;
    }

    private static String showCreateTable(String tableName) throws Exception {
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create table " + DB_NAME + "." + tableName, connectContext);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
        return resultSet.getResultRows().get(0).toString();
    }

    /**
     * Waits for the schema change job created for {@code table} by the preceding ALTER to reach a
     * final state. Also asserts that a job was created at all, which is what proves that the
     * compression-dict change forced {@code needAlter} in SchemaChangeHandler#finalAnalyze: without
     * that, the resolved schema map stays empty and no job is produced.
     */
    private static void waitForSchemaChangeJob(OlapTable table) throws InterruptedException {
        AlterJobV2 job = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values().stream()
                .filter(j -> j.getTableId() == table.getId())
                .max(Comparator.comparingLong(AlterJobV2::getJobId))
                .orElse(null);
        Assertions.assertNotNull(job, "no schema change job was created for table " + table.getName());
        long deadlineMs = System.currentTimeMillis() + 120_000L;
        while (!job.getJobState().isFinalState() && System.currentTimeMillis() < deadlineMs) {
            Thread.sleep(100L);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    // ------------------------------------------------------------------
    // CREATE TABLE (OlapTableFactory) + SHOW CREATE TABLE (OlapTable#getCommonProperties)
    // ------------------------------------------------------------------

    @Test
    public void testCreateTableWithCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_create",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1, v2\""));

        OlapTable table = getTable("t_cdict_create");
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), table.getCompressionDictColumnNames());

        Set<ColumnId> columnIds = table.getCompressionDictColumnIds();
        Assertions.assertNotNull(columnIds);
        Assertions.assertEquals(2, columnIds.size());
        Assertions.assertTrue(columnIds.contains(ColumnId.create("v1")));
        Assertions.assertTrue(columnIds.contains(ColumnId.create("v2")));
    }

    @Test
    public void testShowCreateTableEchoesCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_show",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1, v2\""));

        String createTableSql = showCreateTable("t_cdict_show");
        Assertions.assertTrue(
                createTableSql.contains("\"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1, v2\""),
                createTableSql);
    }

    @Test
    public void testCreateTableWithoutCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_absent", ""));

        OlapTable table = getTable("t_cdict_absent");
        Assertions.assertNull(table.getCompressionDictColumnIds());
        Assertions.assertNull(table.getCompressionDictColumnNames());

        String createTableSql = showCreateTable("t_cdict_absent");
        Assertions.assertFalse(createTableSql.contains(PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS),
                createTableSql);
    }

    // ------------------------------------------------------------------
    // OlapTable#copyOnlyForQuery
    // ------------------------------------------------------------------

    @Test
    public void testCopyOnlyForQueryCopiesCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_copy",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1, v2\""));

        OlapTable table = getTable("t_cdict_copy");
        OlapTable copied = new OlapTable();
        table.copyOnlyForQuery(copied);

        Set<ColumnId> original = table.getCompressionDictColumnIds();
        Set<ColumnId> copy = copied.getCompressionDictColumnIds();
        Assertions.assertNotNull(copy);
        Assertions.assertNotSame(original, copy);
        Assertions.assertEquals(original, copy);
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), copied.getCompressionDictColumnNames());

        // the copy is deep enough that mutating it does not affect the source table
        copy.add(ColumnId.create("v3"));
        Assertions.assertEquals(2, table.getCompressionDictColumnIds().size());
    }

    @Test
    public void testCopyOnlyForQueryClearsCompressionDictColumnsWhenSourceHasNone() {
        OlapTable source = new OlapTable();
        Assertions.assertNull(source.getCompressionDictColumnIds());

        OlapTable target = new OlapTable();
        target.setCompressionDictColumns(Sets.newHashSet(ColumnId.create("stale")));

        source.copyOnlyForQuery(target);
        Assertions.assertNull(target.getCompressionDictColumnIds());
    }

    // ------------------------------------------------------------------
    // OlapTable#getCompressionDictColumnNames edge cases
    // ------------------------------------------------------------------

    @Test
    public void testGetCompressionDictColumnNamesSkipsDroppedColumn() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_dropped",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1\""));

        // work on a query copy so the catalog table itself is not corrupted
        OlapTable copied = new OlapTable();
        getTable("t_cdict_dropped").copyOnlyForQuery(copied);

        Set<ColumnId> columnIds = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        columnIds.add(ColumnId.create("v1"));
        columnIds.add(ColumnId.create("already_dropped"));
        copied.setCompressionDictColumns(columnIds);

        // the id that no longer resolves to a column is skipped, the remaining one is returned
        Assertions.assertEquals(Sets.newHashSet("v1"), copied.getCompressionDictColumnNames());
    }

    @Test
    public void testGetCompressionDictColumnNamesReturnsNullWhenAllColumnsDropped() {
        OlapTable table = new OlapTable();
        Assertions.assertNull(table.getCompressionDictColumnNames());

        table.setCompressionDictColumns(Sets.newHashSet(ColumnId.create("already_dropped")));
        // every id was dropped -> the resolved name set is empty -> null
        Assertions.assertNull(table.getCompressionDictColumnNames());
    }

    // ------------------------------------------------------------------
    // SchemaInfo (FE -> BE plumbing)
    // ------------------------------------------------------------------

    @Test
    public void testSchemaInfoCarriesCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_schema",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1, v2\""));

        OlapTable table = getTable("t_cdict_schema");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId));

        Assertions.assertEquals(table.getCompressionDictColumnIds(), schemaInfo.getCompressionDictColumnNames());

        TTabletSchema tabletSchema = schemaInfo.toTabletSchema();
        Set<String> flagged = Sets.newHashSet();
        for (TColumn tColumn : tabletSchema.getColumns()) {
            if (tColumn.isUse_compression_dict()) {
                flagged.add(tColumn.getColumn_name());
            }
        }
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), flagged);
    }

    @Test
    public void testSchemaInfoWithoutCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_schema_absent", ""));

        OlapTable table = getTable("t_cdict_schema_absent");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId));

        Assertions.assertNull(schemaInfo.getCompressionDictColumnNames());
        for (TColumn tColumn : schemaInfo.toTabletSchema().getColumns()) {
            Assertions.assertFalse(tColumn.isUse_compression_dict(), tColumn.getColumn_name());
        }
    }

    // ------------------------------------------------------------------
    // ALTER TABLE
    // ------------------------------------------------------------------

    @Test
    public void testCompressionDictPropertyIsAnIndexClause() {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS, "v1"));
        Assertions.assertTrue(AlterTableStatementAnalyzer.indexClause(clause));

        ModifyTablePropertiesClause other = new ModifyTablePropertiesClause(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1"));
        Assertions.assertFalse(AlterTableStatementAnalyzer.indexClause(other));
    }

    @Test
    public void testAlterTableSetCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_alter", ""));
        OlapTable table = getTable("t_cdict_alter");
        Assertions.assertNull(table.getCompressionDictColumnNames());

        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_alter SET (\""
                + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v2\")");
        waitForSchemaChangeJob(table);

        Assertions.assertEquals(Sets.newHashSet("v2"), table.getCompressionDictColumnNames());
        Assertions.assertEquals(Sets.newHashSet(ColumnId.create("v2")), table.getCompressionDictColumnIds());
        Assertions.assertTrue(showCreateTable("t_cdict_alter")
                .contains("\"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v2\""));
    }

    @Test
    public void testAlterTableClearCompressionDictColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_clear",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1\""));
        OlapTable table = getTable("t_cdict_clear");
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getCompressionDictColumnNames());

        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_clear SET (\""
                + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"\")");
        waitForSchemaChangeJob(table);

        Assertions.assertNull(table.getCompressionDictColumnIds());
        Assertions.assertNull(table.getCompressionDictColumnNames());
        Assertions.assertFalse(showCreateTable("t_cdict_clear")
                .contains(PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS));
    }

    @Test
    public void testAlterTableWithoutCompressionDictPropertyKeepsIt() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_keep",
                ", \"" + PropertyAnalyzer.PROPERTIES_COMPRESSION_DICT_COLUMNS + "\" = \"v1\""));
        OlapTable table = getTable("t_cdict_keep");
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getCompressionDictColumnNames());

        // an unrelated schema change (bloom filter) must not disturb the compression dict set
        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_keep SET (\""
                + PropertyAnalyzer.PROPERTIES_BF_COLUMNS + "\" = \"k1\")");
        waitForSchemaChangeJob(table);

        Assertions.assertEquals(Sets.newHashSet("k1"), table.getBfColumnNames());
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getCompressionDictColumnNames());
    }
}
