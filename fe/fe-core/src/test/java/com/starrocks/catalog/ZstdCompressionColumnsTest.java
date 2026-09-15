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
import com.google.common.collect.Lists;
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

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.List;
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
    private static AlterJobV2 waitForSchemaChangeJob(OlapTable table) throws InterruptedException {
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
        return job;
    }

    private static int schemaChangeJobCount(OlapTable table) {
        return (int) GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values().stream()
                .filter(j -> j.getTableId() == table.getId())
                .count();
    }

    /** Reads a private field, so a job's internals can be asserted without widening its API. */
    private static Object fieldOf(Object object, String name) throws Exception {
        for (Class<?> clazz = object.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            try {
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(object);
            } catch (NoSuchFieldException ignored) {
                // keep walking up
            }
        }
        throw new NoSuchFieldException(name + " on " + object.getClass());
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
        // A page size may be attached to any nominated column; columns without one keep
        // the BE default. The size is per column because what pays off depends on how
        // large that column's rows are relative to a page.
        starRocksAssert.withTable(createTableSql("t_page_size",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:1m, v2\""));
        OlapTable table = getTable("t_page_size");

        Assertions.assertEquals(Sets.newHashSet("v1", "v2"), table.getZstdCompressionColumnNames());
        Map<ColumnId, Integer> pageSizes = table.getZstdCompressionPageSizes();
        Assertions.assertNotNull(pageSizes);
        Assertions.assertEquals(Integer.valueOf(1024 * 1024), pageSizes.get(ColumnId.create("v1")));
        Assertions.assertNull(pageSizes.get(ColumnId.create("v2")));

        // SHOW CREATE TABLE has to round-trip the size, or the statement it prints would
        // silently create a differently-encoded table.
        String show = showCreateTable("t_page_size");
        Assertions.assertTrue(show.contains("v1:" + (1024 * 1024)), show);

        // And the size has to reach the BE, not just sit in the catalog.
        long baseIndexMetaId = table.getBaseIndexMetaId();
        SchemaInfo schemaInfo = SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId));
        for (TColumn tColumn : schemaInfo.toTabletSchema().getColumns()) {
            if ("v1".equalsIgnoreCase(tColumn.getColumn_name())) {
                Assertions.assertTrue(tColumn.isUse_zstd_compression());
                Assertions.assertEquals(1024 * 1024, tColumn.getZstd_compression_page_size());
            } else if ("v2".equalsIgnoreCase(tColumn.getColumn_name())) {
                Assertions.assertTrue(tColumn.isUse_zstd_compression());
                Assertions.assertEquals(0, tColumn.getZstd_compression_page_size());
            }
        }
    }

    @Test
    public void testPageSizeOnlyAlterIsDetected() throws Exception {
        // Same column set, different page size. Comparing column names alone would
        // accept this and drop it on the floor: no index marked for alteration, and
        // the catalog map only updated when the change flag is set.
        starRocksAssert.withTable(createTableSql("t_page_alter",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:64k\""));
        OlapTable table = getTable("t_page_alter");
        Assertions.assertEquals(Integer.valueOf(64 * 1024),
                table.getZstdCompressionPageSizes().get(ColumnId.create("v1")));

        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_page_alter SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:1m\")");
        waitForSchemaChangeJob(table);

        Assertions.assertEquals(Integer.valueOf(1024 * 1024),
                table.getZstdCompressionPageSizes().get(ColumnId.create("v1")));
        Assertions.assertTrue(showCreateTable("t_page_alter").contains("v1:" + (1024 * 1024)),
                showCreateTable("t_page_alter"));
    }

    @Test
    public void testAddColumnCarryingThePropertyPersistsIt() throws Exception {
        // A column clause can carry table properties, and that keeps the fast-schema-evolution
        // path eligible. Those jobs finish by rebuilding the schema and the indexes and never
        // persist this table-level property, so the change would reach the tablets and then
        // vanish from FE metadata. (The property names an existing column: the column set is
        // resolved against the table's current schema, so a column added by the same statement
        // is not yet nameable -- the same holds for bloom_filter_columns.)
        starRocksAssert.withTable(createTableSql("t_add_col_prop", ""));
        OlapTable table = getTable("t_add_col_prop");
        Assertions.assertNull(table.getZstdCompressionColumnNames());

        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_add_col_prop ADD COLUMN v9 STRING PROPERTIES (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:256k\")");
        waitForSchemaChangeJob(table);

        Assertions.assertTrue(table.getZstdCompressionColumnNames().contains("v1"),
                String.valueOf(table.getZstdCompressionColumnNames()));
        Assertions.assertEquals(Integer.valueOf(256 * 1024),
                table.getZstdCompressionPageSizes().get(ColumnId.create("v1")));
        Assertions.assertTrue(showCreateTable("t_add_col_prop").contains("v1:" + (256 * 1024)),
                showCreateTable("t_add_col_prop"));
    }

    @Test
    public void testPageSizeReachesEveryTabletSchemaPath() throws Exception {
        // The page size has to travel on every path that builds a tablet schema, not
        // just the one the create path happens to use: a schema that carries the
        // column flag without the size tells the BE "default page size" while SHOW
        // CREATE TABLE still reports what was asked for.
        starRocksAssert.withTable(createTableSql("t_page_paths",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:256k\""));
        OlapTable table = getTable("t_page_paths");
        long baseIndexMetaId = table.getBaseIndexMetaId();

        List<TTabletSchema> schemas = Lists.newArrayList();
        schemas.add(SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId)).toTabletSchema());
        schemas.add(SchemaInfo.newBuilder()
                .setId(baseIndexMetaId)
                .setKeysType(table.getKeysType())
                .setShortKeyColumnCount(table.getIndexMetaByMetaId(baseIndexMetaId).getShortKeyColumnCount())
                .setSchemaHash(0)
                .setStorageType(table.getStorageType())
                .addColumns(table.getBaseSchema())
                .setBloomFilterColumnNames(table.getBfColumnIds())
                .setBloomFilterFpp(table.getBfFpp())
                .setZstdCompressionColumns(table.getZstdCompressionColumnIds(),
                        table.getZstdCompressionPageSizes())
                .build()
                .toTabletSchema());

        for (TTabletSchema schema : schemas) {
            boolean seen = false;
            for (TColumn tColumn : schema.getColumns()) {
                if ("v1".equalsIgnoreCase(tColumn.getColumn_name())) {
                    seen = true;
                    Assertions.assertTrue(tColumn.isUse_zstd_compression());
                    Assertions.assertEquals(256 * 1024, tColumn.getZstd_compression_page_size());
                }
            }
            Assertions.assertTrue(seen, "v1 missing from tablet schema");
        }
    }

    @Test
    public void testPageSizeOutOfRangeIsRejected() {
        // 1KB is below the floor and 4MB above the ceiling: a page that small holds
        // nothing to compress, and one that large would be decompressed in full for a
        // single row -- measured at ~2.6ms on 9KB rows, 45x the default page.
        assertCreateTableFails("t_page_small", "v1:1k", "must be between");
        assertCreateTableFails("t_page_big", "v1:4m", "must be between");
        assertCreateTableFails("t_page_junk", "v1:abc", "Invalid page size");
        assertCreateTableFails("t_page_empty", "v1:", "missing value");
    }

    // ------------------------------------------------------------------
    // RENAME COLUMN: a ColumnId keeps the ORIGINAL name, so anything that re-derives
    // an id from the column's current name loses track of the renamed column
    // ------------------------------------------------------------------

    @Test
    public void testRenameColumnKeepsPerColumnPageSize() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_rename",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:1m\""));
        OlapTable table = getTable("t_cdict_rename");

        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_rename RENAME COLUMN v1 TO w1");

        // The set and the page-size map are both keyed by the id, and the id still resolves to the
        // renamed column, so the property survives -- under its NEW name and with its page size.
        Assertions.assertEquals(Sets.newHashSet("w1"), table.getZstdCompressionColumnNames());
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 1024 * 1024),
                table.getZstdCompressionPageSizes());
        String ddl = showCreateTable("t_cdict_rename");
        Assertions.assertTrue(
                ddl.contains("\"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"w1:1048576\""),
                ddl);

        // What BE is told is unchanged by the rename: the tablet schema names columns by id.
        TTabletSchema schema = SchemaInfo.newBuilder()
                .setId(table.getBaseIndexMetaId())
                .setKeysType(table.getKeysType())
                .setShortKeyColumnCount(
                        table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getShortKeyColumnCount())
                .setSchemaHash(0)
                .setStorageType(table.getStorageType())
                .addColumns(table.getBaseSchema())
                .setZstdCompressionColumns(table.getZstdCompressionColumnIds(),
                        table.getZstdCompressionPageSizes())
                .build()
                .toTabletSchema();
        boolean seen = false;
        for (TColumn tColumn : schema.getColumns()) {
            if ("v1".equalsIgnoreCase(tColumn.getColumn_name())) {
                seen = true;
                Assertions.assertTrue(tColumn.isUse_zstd_compression());
                Assertions.assertEquals(1024 * 1024, tColumn.getZstd_compression_page_size());
            }
        }
        Assertions.assertTrue(seen, "the renamed column is missing from the tablet schema");
    }

    @Test
    public void testRestatingPropertyAfterRenameIsNotAChange() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_rename_restate",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:1m\""));
        OlapTable table = getTable("t_cdict_rename_restate");
        starRocksAssert.alterTable(
                "ALTER TABLE " + DB_NAME + ".t_cdict_rename_restate RENAME COLUMN v1 TO w1");
        int jobsBefore = schemaChangeJobCount(table);

        // Restating exactly what the table already has must be a no-op. The table keys page sizes
        // by id ("v1") and the property names the column as it is called now ("w1"), so comparing
        // the two raw reports a change and rewrites every tablet for nothing.
        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_rename_restate SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"w1:1m\")");

        Assertions.assertEquals(jobsBefore, schemaChangeJobCount(table));
        Assertions.assertEquals(Sets.newHashSet("w1"), table.getZstdCompressionColumnNames());
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 1024 * 1024),
                table.getZstdCompressionPageSizes());

        // A real change on the renamed column is still detected.
        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_rename_restate SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"w1:256k\")");
        waitForSchemaChangeJob(table);
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 256 * 1024),
                table.getZstdCompressionPageSizes());
    }

    // ------------------------------------------------------------------
    // The job has to carry the setting: it is what the shadow tablets are created from,
    // and what copyForPersist() hands to the followers and to the next leader
    // ------------------------------------------------------------------

    @Test
    public void testCopyForPersistCarriesZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_persist",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:256k\""));
        OlapTable table = getTable("t_cdict_persist");

        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_persist SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:1m, v2\")");
        AlterJobV2 job = waitForSchemaChangeJob(table);

        // A field missing from the copy is a field the edit log never carries, so a follower or a
        // restarted leader finishes this job without applying the property at all.
        AlterJobV2 persisted = job.copyForPersist();
        Assertions.assertEquals(Boolean.TRUE, fieldOf(persisted, "hasZstdCompressionChange"));
        Assertions.assertEquals(Sets.newHashSet(ColumnId.create("v1"), ColumnId.create("v2")),
                fieldOf(persisted, "zstdCompressionColumns"));
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 1024 * 1024),
                fieldOf(persisted, "zstdCompressionPageSizes"));
    }

    @Test
    public void testModifySortKeyJobCarriesZstdCompressionColumns() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_sortkey",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:256k\""));
        OlapTable table = getTable("t_cdict_sortkey");

        // ORDER BY rewrites every tablet through a shadow index built from the sets handed to the
        // job, not from the table, so the existing setting has to travel with it -- otherwise the
        // rewritten data comes out with the table codec while the property stays on the table.
        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_sortkey ORDER BY (k1, v3)");
        AlterJobV2 job = waitForSchemaChangeJob(table);

        Assertions.assertEquals(Sets.newHashSet(ColumnId.create("v1")), fieldOf(job, "zstdCompressionColumns"));
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 256 * 1024),
                fieldOf(job, "zstdCompressionPageSizes"));
        // Not a change to the property itself, so the job must not write it back at finish.
        Assertions.assertEquals(Boolean.FALSE, fieldOf(job, "hasZstdCompressionChange"));
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getZstdCompressionColumnNames());
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 256 * 1024),
                table.getZstdCompressionPageSizes());
    }

    @Test
    public void testModifyColumnToUnsupportedTypeIsRejectedWhileNominated() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_modify_type",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1\""));
        OlapTable table = getTable("t_cdict_modify_type");

        // The property is not restated by this statement and survives it untouched, so letting the
        // type change through would leave the table naming a BIGINT column -- a SHOW CREATE TABLE
        // that CREATE TABLE rejects.
        Exception e = Assertions.assertThrows(Exception.class, () -> starRocksAssert.alterTable(
                "ALTER TABLE " + DB_NAME + ".t_cdict_modify_type MODIFY COLUMN v1 bigint"));
        Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains("can no longer be a zstd compression"),
                "unexpected message: " + e.getMessage());
        Assertions.assertEquals(Sets.newHashSet("v1"), table.getZstdCompressionColumnNames());

        // Dropping it from the property first is the way through, and then the type change is fine.
        starRocksAssert.alterTableProperties("ALTER TABLE " + DB_NAME + ".t_cdict_modify_type SET (\""
                + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v2\")");
        waitForSchemaChangeJob(table);
        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_modify_type MODIFY COLUMN v1 bigint");
        waitForSchemaChangeJob(table);
        Assertions.assertEquals(Sets.newHashSet("v2"), table.getZstdCompressionColumnNames());

        // A type change on a column the property does not name was never affected.
        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_modify_type MODIFY COLUMN v3 bigint");
        waitForSchemaChangeJob(table);
    }

    @Test
    public void testSchemaInfoSnapshotSurvivesDropColumn() throws Exception {
        starRocksAssert.withTable(createTableSql("t_cdict_snapshot",
                ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"v1:1m, v2\""));
        OlapTable table = getTable("t_cdict_snapshot");
        long baseIndexMetaId = table.getBaseIndexMetaId();

        // What a fast schema evolution keeps for the transactions still writing the old schema.
        SchemaInfo snapshot = SchemaInfo.fromMaterializedIndex(table, baseIndexMetaId,
                table.getIndexMetaByMetaId(baseIndexMetaId));
        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 1024 * 1024),
                snapshot.getZstdCompressionPageSizes());

        // Dropping the column prunes the table's own map IN PLACE (rebuildFullSchema). A snapshot
        // that aliased that map would silently lose the page size it was taken to remember.
        starRocksAssert.alterTable("ALTER TABLE " + DB_NAME + ".t_cdict_snapshot DROP COLUMN v1");
        waitForSchemaChangeJob(table);
        Assertions.assertEquals(Sets.newHashSet("v2"), table.getZstdCompressionColumnNames());

        Assertions.assertEquals(ImmutableMap.of(ColumnId.create("v1"), 1024 * 1024),
                snapshot.getZstdCompressionPageSizes());
        Assertions.assertTrue(snapshot.getZstdCompressionColumnNames().contains(ColumnId.create("v1")));
    }

    private static void assertCreateTableFails(String tableName, String spec, String expectedMessage) {
        Exception e = Assertions.assertThrows(Exception.class,
                () -> starRocksAssert.withTable(createTableSql(tableName,
                        ", \"" + PropertyAnalyzer.PROPERTIES_ZSTD_COMPRESSION_COLUMNS + "\" = \"" + spec + "\"")));
        Assertions.assertTrue(e.getMessage() != null && e.getMessage().contains(expectedMessage),
                "unexpected message for '" + spec + "': " + e.getMessage());
    }
}
