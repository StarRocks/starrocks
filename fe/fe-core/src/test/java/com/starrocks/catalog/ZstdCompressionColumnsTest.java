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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests for the "zstd_compression_columns" table property: the CREATE TABLE path, SHOW CREATE TABLE
 * echo, the OlapTable copy/accessor helpers, the SchemaInfo -> BE plumbing and the ALTER TABLE path.
 */
public class ZstdCompressionColumnsTest {
    private static final String DB_NAME = "test_zstd_compression_dict";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    @Test
    public void testDropColumnRemovesItFromZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_drop_col",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1, v2\""));
        OlapTable table = getTable("t_cdict_drop_col");
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), table.getZstdCompressionColumnNames());

        // Drop one of them WITHOUT restating the property. A ColumnId is only a name, so
        // if the drop left v1 behind, re-creating a column called v1 later would silently
        // get the dictionary again.
        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_drop_col DROP COLUMN v1");
        waitForSchemaChangeJob(table);

        Assertions.assertEquals(Sets.newHashSet("v2"), table.getZstdCompressionColumnNames());
        Assertions.assertEquals(Sets.newHashSet(ColumnId.create("v2")), table.getZstdCompressionColumnIds());
        Assertions.assertFalse(showCreateTable("t_cdict_drop_col").contains("v1"));

        // Re-adding a column with the dropped name must NOT resurrect the dictionary flag.
        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_drop_col ADD COLUMN v1 string");
        waitForSchemaChangeJob(table);
        Assertions.assertEquals(Sets.newHashSet("v2"), table.getZstdCompressionColumnNames());
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
    public void testCreateTableWithZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_create",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1, v2\""));

        OlapTable table = getTable("t_cdict_create");
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), table.getZstdCompressionColumnNames());

        Set<ColumnId> columnIds = table.getZstdCompressionColumnIds();
        Assertions.assertNotNull(columnIds);
        Assertions.assertEquals(2, columnIds.size());
        Assertions.assertTrue(columnIds.contains(ColumnId.create("v1")));
        Assertions.assertTrue(columnIds.contains(ColumnId.create("v2")));
    }

    @Test
    public void testShowCreateTableEchoesZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_show",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1, v2\""));

        String createTableSql = showCreateTable("t_cdict_show");
        Assertions.assertTrue(
                createTableSql.contains("\"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1, v2\""),
                createTableSql);
    }

    @Test
    public void testCreateTableWithoutZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_absent", ""));

        OlapTable table = getTable("t_cdict_absent");
        Assertions.assertNull(table.getZstdCompressionColumnIds());
        Assertions.assertNull(table.getZstdCompressionColumnNames());

        String createTableSql = showCreateTable("t_cdict_absent");
        Assertions.assertFalse(createTableSql.contains(PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS),
                createTableSql);
    }

    // ------------------------------------------------------------------
    // OlapTable#copyOnlyForQuery
    // ------------------------------------------------------------------

    @Test
    public void testCopyOnlyForQueryCopiesZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_copy",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1, v2\""));

        OlapTable table = getTable("t_cdict_copy");
        OlapTable copied = new OlapTable();
        table.copyOnlyForQuery(copied);

        Set<ColumnId> original = table.getZstdCompressionColumnIds();
        Set<ColumnId> copy = copied.getZstdCompressionColumnIds();
        Assertions.assertNotNull(copy);
        Assertions.assertNotSame(original, copy);
        Assertions.assertEquals(original, copy);
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), copied.getZstdCompressionColumnNames());

        // the copy is deep enough that mutating it does not affect the source table
        copy.add(ColumnId.create("v3"));
        Assertions.assertEquals(2, table.getZstdCompressionColumnIds().size());
    }

    @Test
    public void testCopyOnlyForQueryClearsZstdCompressionColumnsWhenSourceHasNone() {
        OlapTable source = new OlapTable();
        Assertions.assertNull(source.getZstdCompressionColumnIds());

        OlapTable target = new OlapTable();
        target.setZstdCompressionColumns(Sets.newHashSet(ColumnId.create("stale")));

        source.copyOnlyForQuery(target);
        Assertions.assertNull(target.getZstdCompressionColumnIds());
    }

    // ------------------------------------------------------------------
    // OlapTable#getZstdCompressionColumnNames edge cases
    // ------------------------------------------------------------------

    @Test
    public void testGetZstdCompressionColumnNamesSkipsDroppedColumn() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_dropped",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1\""));

        // work on a query copy so the catalog table itself is not corrupted
        OlapTable copied = new OlapTable();
        getTable("t_cdict_dropped").copyOnlyForQuery(copied);

        Set<ColumnId> columnIds = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        columnIds.add(ColumnId.create("v1"));
        columnIds.add(ColumnId.create("already_dropped"));
        copied.setZstdCompressionColumns(columnIds);

        // the id that no longer resolves to a column is skipped, the remaining one is returned
        Assertions.assertEquals(Sets.newHashSet("v1"), copied.getZstdCompressionColumnNames());
    }

    @Test
    public void testGetZstdCompressionColumnNamesReturnsNullWhenAllColumnsDropped() {
        OlapTable table = new OlapTable();
        Assertions.assertNull(table.getZstdCompressionColumnNames());

        table.setZstdCompressionColumns(Sets.newHashSet(ColumnId.create("already_dropped")));
        // every id was dropped -> the resolved name set is empty -> null
        Assertions.assertNull(table.getZstdCompressionColumnNames());
    }

    // ------------------------------------------------------------------
    // SchemaInfo (FE -> BE plumbing)
    // ------------------------------------------------------------------

    @Test
    public void testSchemaInfoCarriesZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_schema",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1, v2\""));

        OlapTable table = getTable("t_cdict_schema");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId));

        Assertions.assertEquals(table.getZstdCompressionColumnIds(), schemaInfo.getZstdCompressionColumnNames());

        TTabletSchema tabletSchema = schemaInfo.toTabletSchema();
        Set<String> flagged = Sets.newHashSet();
        for (TColumn tColumn : tabletSchema.getColumns()) {
            if (tColumn.isUse_zstd_compression()) {
                flagged.add(tColumn.getColumn_name());
            }
        }
        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), flagged);
    }

    @Test
    public void testSchemaInfoWithoutZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_schema_absent", ""));

        OlapTable table = getTable("t_cdict_schema_absent");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId));

        Assertions.assertNull(schemaInfo.getZstdCompressionColumnNames());
        for (TColumn tColumn : schemaInfo.toTabletSchema().getColumns()) {
            Assertions.assertFalse(tColumn.isUse_zstd_compression(), tColumn.getColumn_name());
        }
    }

    // ------------------------------------------------------------------
    // ALTER TABLE
    // ------------------------------------------------------------------

    @Test
    public void testZstdCompressionPropertyIsAnIndexClause() {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS, "v1"));
        Assertions.assertTrue(AlterTableStatementAnalyzer.indexClause(clause));

        ModifyTablePropertiesClause other = new ModifyTablePropertiesClause(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1"));
        Assertions.assertFalse(AlterTableStatementAnalyzer.indexClause(other));
    }

    @Test
    public void testAlterTableSetZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_alter", ""));
        OlapTable table = getTable("t_cdict_alter");
        Assertions.assertNull(table.getZstdCompressionColumnNames());

        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_alter SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v2\")");
        waitForSchemaChangeJob(table);

        Assertions.assertEquals(Sets.newHashSet("v2"), table.getZstdCompressionColumnNames());
        Assertions.assertEquals(Sets.newHashSet(ColumnId.create("v2")), table.getZstdCompressionColumnIds());
        Assertions.assertTrue(showCreateTable("t_cdict_alter")
                .contains("\"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v2\""));
    }

    @Test
    public void testAlterTableClearZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_clear",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1\""));
        OlapTable table = getTable("t_cdict_clear");
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getZstdCompressionColumnNames());

        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_clear SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"\")");
        waitForSchemaChangeJob(table);

        Assertions.assertNull(table.getZstdCompressionColumnIds());
        Assertions.assertNull(table.getZstdCompressionColumnNames());
        Assertions.assertFalse(showCreateTable("t_cdict_clear")
                .contains(PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS));
    }

    @Test
    public void testAlterTableWithoutZstdCompressionDictPropertyKeepsIt() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_keep",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1\""));
        OlapTable table = getTable("t_cdict_keep");
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getZstdCompressionColumnNames());

        // an unrelated schema change (bloom filter) must not disturb the compression dict set
        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_keep SET (\""
                + PropertyAnalyzer.PROPERTIES_BF_COLUMNS + "\" = \"k1\")");
        waitForSchemaChangeJob(table);

        Assertions.assertEquals(Sets.newHashSet("k1"), table.getBfColumnNames());
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getZstdCompressionColumnNames());
    }

    @Test
    public void testPerColumnPageSizeIsParsedAndEchoed() throws Exception {
        // A page size may be attached to any nominated column; columns without one
        // keep the BE default. Sizes are per column because the size that pays off
        // depends on the column's row length.
        starRocksAssert.withTable("CREATE TABLE test.t_page_size (\n" +
                "k BIGINT NOT NULL, v STRING, j JSON\n" +
                ") ENGINE=OLAP DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 " +
                "PROPERTIES (\"replication_num\"=\"1\", \"zstd_compression_columns\"=\"v:4m, j\")");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test").getTable("t_page_size");

        Assertions.assertEquals(Sets.newHashSet("v", "j"), table.getZstdCompressionColumnNames());
        Map<ColumnId, Integer> pageSizes = table.getZstdCompressionPageSizes();
        Assertions.assertNotNull(pageSizes);
        Assertions.assertEquals(Integer.valueOf(4 * 1024 * 1024), pageSizes.get(ColumnId.create("v")));
        Assertions.assertNull(pageSizes.get(ColumnId.create("j")));

        // SHOW CREATE TABLE has to round-trip the size, or the statement it prints
        // would silently create a differently-encoded table.
        String show = table.getProperties().get(PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS);
        Assertions.assertTrue(show.contains("v:" + (4 * 1024 * 1024)), show);
    }

    @Test
    public void testPageSizeOutOfRangeIsRejected() {
        // 1KB is below the floor and 64MB above the ceiling: a page that small
        // holds nothing to compress, and one that large would be decompressed in
        // full for a single row.
        Assertions.assertThrows(Exception.class, () -> starRocksAssert.withTable(
                "CREATE TABLE test.t_page_small (k BIGINT NOT NULL, v STRING) ENGINE=OLAP DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\", " +
                        "\"zstd_compression_columns\"=\"v:1k\")"));
        Assertions.assertThrows(Exception.class, () -> starRocksAssert.withTable(
                "CREATE TABLE test.t_page_big (k BIGINT NOT NULL, v STRING) ENGINE=OLAP DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\", " +
                        "\"zstd_compression_columns\"=\"v:64m\")"));
        Assertions.assertThrows(Exception.class, () -> starRocksAssert.withTable(
                "CREATE TABLE test.t_page_junk (k BIGINT NOT NULL, v STRING) ENGINE=OLAP DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\", " +
                        "\"zstd_compression_columns\"=\"v:abc\")"));
    }
}
