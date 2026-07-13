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

import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Covers {@link LakeRangeRewriteSchemaChangeJob#rewriteSelectColumnNames} and
 * {@code rewriteTargetColumnNames}'s surviving-subset projection for a column-set-shrinking (DROP key
 * column) range rewrite, and confirms the reorder/keyness-flip path stays byte-identical.
 */
public class LakeRangeRewriteSchemaChangeJobKeyColumnTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_range_rewrite_test";
    private static Database db;
    private OlapTable table;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        Config.enable_range_distribution = true;
    }

    @BeforeEach
    public void before() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(DB_NAME);
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        // A range-distribution table: data is routed by the range sort key (k1, k2).
        String sql = "create table t_range (k1 int, k2 int, v1 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_range");
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    private static Column columnNamed(List<Column> schema, String name) {
        return schema.stream().filter(column -> column.getName().equals(name)).findFirst().orElseThrow();
    }

    /** An added (shadow-prefixed) key column with a constant default, absent from the base table. */
    private static Column addedKeyColumn(String name, String defaultValue) {
        return new Column(name, IntegerType.INT, /*isKey=*/ true, null, defaultValue, "");
    }

    /** An added (shadow-prefixed) VARCHAR key column with a constant default, absent from the base table. */
    private static Column addedVarcharKeyColumn(String name, String defaultValue) {
        return new Column(name, VarcharType.VARCHAR, /*isKey=*/ true, null, defaultValue, "");
    }

    /** Builds a job carrying the given new schema / new sort key, wired to the base index as origin. */
    private LakeRangeRewriteSchemaChangeJob newJob(List<Column> newSchema, List<Column> newSortKeyColumns,
                                                    List<Integer> newSortKeyIdxes) {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        long shadowIndexMetaId = GlobalStateMgr.getCurrentState().getNextId();
        LakeRangeRewriteSchemaChangeJob job = new LakeRangeRewriteSchemaChangeJob(
                jobId, db.getId(), table.getId(), table.getName(), 3600_000L);
        job.setNewSchema(newSchema);
        job.setNewKeysType(KeysType.DUP_KEYS);
        job.setNewSortKeyIdxes(newSortKeyIdxes);
        job.setNewSortKeyColumns(newSortKeyColumns);
        job.setShadowIndex(shadowIndexMetaId, table.getBaseIndexMetaId(),
                SchemaChangeHandler.SHADOW_NAME_PREFIX + table.getName(), (short) newSortKeyColumns.size());
        return job;
    }

    @Test
    public void testDropProjectsSurvivingSubset() {
        // DROP k2 from the base (k1, k2, v1): newSchema keeps only the survivors (k1, v1) and the sort
        // key shrinks to (k1).
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column v1 = columnNamed(baseSchema, "v1");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k1, v1), List.of(k1), List.of(0));

        Assertions.assertEquals(List.of(ParseUtil.backquote("k1"), ParseUtil.backquote("v1")),
                job.rewriteSelectColumnNames(table),
                "SELECT list must project only the surviving columns");
        Assertions.assertEquals(List.of("k1", "v1"), job.rewriteTargetColumnNames(table),
                "a column-set-shrinking rewrite needs an explicit (raw, unquoted) target column list");
    }

    @Test
    public void testReorderKeepsFullSchemaAndEmptyTarget() {
        // A sort-key reorder to (k2, k1, v1): the column set is unchanged, only reordered.
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column k2 = columnNamed(baseSchema, "k2");
        Column v1 = columnNamed(baseSchema, "v1");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k2, k1, v1), List.of(k2, k1), List.of(0, 1));

        Assertions.assertEquals(
                List.of(ParseUtil.backquote("k2"), ParseUtil.backquote("k1"), ParseUtil.backquote("v1")),
                job.rewriteSelectColumnNames(table),
                "SELECT list must still project the full (reordered) column set");
        Assertions.assertTrue(job.rewriteTargetColumnNames(table).isEmpty(),
                "an unchanged column set must keep the default empty target list");
    }

    @Test
    public void testAddKeyColumnBuildsPrefixedShadowSchemaFreshUniqueIdNoAggKey() throws Exception {
        // AGG range table: v1 SUM (no explicit key), so ADD COLUMN k2 INT KEY joins a key-derived range
        // sort key and is routed to the range rewrite job with a prefixed shadow schema.
        String sql = "create table t_range_agg (k1 int, v1 int sum)\n"
                + "aggregate key(k1)\n"
                + "order by(k1)\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        OlapTable agg = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_range_agg");

        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_range_agg add column k2 int key default '0'", connectContext);
        AlterJobV2 job = GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(alterStmt.getAlterClauseList(), db, agg);
        LakeRangeRewriteSchemaChangeJob r = (LakeRangeRewriteSchemaChangeJob) job;

        String shadowName = SchemaChangeHandler.SHADOW_NAME_PREFIX + "k2";
        Column added = r.getNewSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(shadowName)).findFirst().orElseThrow();
        Assertions.assertTrue(added.isKey());
        Assertions.assertNull(added.getAggregationType(), "key column carries no aggregation");
        Assertions.assertTrue(added.getUniqueId() > Column.COLUMN_UNIQUE_ID_INIT_VALUE, "fresh id");
        Assertions.assertTrue(r.getNewSortKeyColumns().stream().anyMatch(c -> c.getName().equalsIgnoreCase(shadowName)));
        // Unprefixed name must NOT appear in the shadow schema during the window.
        Assertions.assertTrue(r.getNewSchema().stream().noneMatch(c -> c.getName().equalsIgnoreCase("k2")));
    }

    @Test
    public void testRegisterShadowExposesPrefixedAddedColumnHiddenFromUser() throws Exception {
        // AGG range table (v1 SUM, no explicit key): ADD COLUMN k2 INT KEY routes to the range rewrite
        // job with a prefixed shadow schema (see testAddKeyColumnBuildsPrefixedShadowSchemaFreshUniqueIdNoAggKey).
        String sql = "create table t_range_agg_add (k1 int, v1 int sum)\n"
                + "aggregate key(k1)\n"
                + "order by(k1)\n"
                + "properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        OlapTable agg = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_range_agg_add");

        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_range_agg_add add column k2 int key default '0'", connectContext);
        AlterJobV2 job = GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .analyzeAndCreateJob(alterStmt.getAlterClauseList(), db, agg);
        LakeRangeRewriteSchemaChangeJob r = (LakeRangeRewriteSchemaChangeJob) job;

        r.registerShadowIndexMeta(agg);

        String shadowName = SchemaChangeHandler.SHADOW_NAME_PREFIX + "k2";
        Assertions.assertTrue(agg.getFullSchema().stream().anyMatch(c -> c.getName().equalsIgnoreCase(shadowName)),
                "the prefixed added column must enter getFullSchema() during the rewrite window");
        Assertions.assertNull(agg.getColumn("k2"), "the real name must not be resolvable until flip");
        Assertions.assertFalse(agg.getBaseSchema().stream().anyMatch(c -> c.getName().equalsIgnoreCase("k2")),
                "the real name must not appear in the base schema until flip");
    }

    @Test
    public void testRegisterShadowIsNoOpForSameColumnSet() {
        // A sort-key reorder to (k2, k1, v1): the column set is unchanged, so registering the shadow
        // index meta (which rebuilds the full schema) must not change getFullSchema()'s column set.
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column k2 = columnNamed(baseSchema, "k2");
        Column v1 = columnNamed(baseSchema, "v1");
        List<String> before = table.getFullSchema().stream().map(Column::getName).collect(Collectors.toList());
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k2, k1, v1), List.of(k2, k1), List.of(0, 1));

        job.registerShadowIndexMeta(table);

        Assertions.assertEquals(before, table.getFullSchema().stream().map(Column::getName).collect(Collectors.toList()));
    }

    @Test
    public void testDropColumnIsAffectedForMvInactivation() {
        // DROP k2 from the base (k1, k2, v1): newSchema keeps only the survivors (k1, v1). A dependent
        // async MV referencing k2 must be inactivated, so the dropped column must show up as affected.
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column v1 = columnNamed(baseSchema, "v1");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k1, v1), List.of(k1), List.of(0));
        table.addRelatedMaterializedView(new MvId(db.getId(), GlobalStateMgr.getCurrentState().getNextId()));

        Assertions.assertTrue(job.affectedColumnsForMvInactivation(table).contains("k2"),
                "a dropped column must be reported as affected so dependent MVs referencing it inactivate");
    }

    @Test
    public void testAddKeyColumnIsNotAffectedForMvInactivation() {
        // ADD a shadow-prefixed key column __starrocks_shadow_k3 to the base (k1, k2, v1): newSchema is
        // the base plus the added column, and the new sort key is (k1, k2, __starrocks_shadow_k3). An
        // added key column with a constant default leaves historical base data unchanged, so a dependent
        // async MV built on the pre-existing sort key (e.g. GROUP BY k1) must not be inactivated.
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column k2 = columnNamed(baseSchema, "k2");
        Column v1 = columnNamed(baseSchema, "v1");
        Column addedK3 = addedKeyColumn("__starrocks_shadow_k3", "0");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k1, k2, v1, addedK3),
                List.of(k1, k2, addedK3), List.of(0, 1, 2));
        table.addRelatedMaterializedView(new MvId(db.getId(), GlobalStateMgr.getCurrentState().getNextId()));

        Assertions.assertTrue(job.affectedColumnsForMvInactivation(table).isEmpty(),
                "a pure add-key must not inactivate dependent MVs on the pre-existing sort key");
    }

    @Test
    public void testAddSamplingProjectsLiteralForAbsentColumn() {
        // newSortKeyColumns = [k1 (source), __starrocks_shadow_k2 (added, default "0")]; the source
        // (base) schema has only k1 and v1 — the added column cannot be projected by name.
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column addedK2 = addedKeyColumn("__starrocks_shadow_k2", "0");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k1, addedK2), List.of(k1, addedK2), List.of(0, 1));

        List<String> projection = job.buildSampleProjectionForTest(Set.of("k1", "v1"));

        Assertions.assertEquals(ParseUtil.backquote("k1"), projection.get(0),
                "a source-present sort-key column is projected by its backquoted name");
        Assertions.assertEquals(
                "CAST(\"0\" AS " + addedK2.getType().toSql() + ") AS " + ParseUtil.backquote("__starrocks_shadow_k2"),
                projection.get(1),
                "a source-absent (added) sort-key column is projected as a raw CAST literal, not a column reference");
    }

    @Test
    public void testAddSamplingEscapesBackslashAndQuoteInDefaultLiteral() {
        // The added column's default carries both a backslash and a double quote. The StarRocks
        // double-quoted-string lexer decodes backslash escapes, so the backslash must be escaped
        // first, then the quote -- otherwise a backslash could swallow the closing quote or corrupt
        // the literal's value.
        List<Column> baseSchema = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        Column k1 = columnNamed(baseSchema, "k1");
        Column addedK2 = addedVarcharKeyColumn("__starrocks_shadow_k2", "back\\slash\"quote");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(k1, addedK2), List.of(k1, addedK2), List.of(0, 1));

        List<String> projection = job.buildSampleProjectionForTest(Set.of("k1", "v1"));

        Assertions.assertEquals(
                "CAST(\"back\\\\slash\\\"quote\" AS " + addedK2.getType().toSql() + ") AS "
                        + ParseUtil.backquote("__starrocks_shadow_k2"),
                projection.get(1),
                "the backslash must be escaped before the double quote in the CAST literal");
    }

    @Test
    public void testAddSoleKeyCollapsesToSingleTablet() {
        // The added column is the SOLE new sort key; a real sampler would see the same constant
        // literal (the CAST(default) projection) on every row, so the boundary planner must collapse
        // to NO_SPLIT -> an empty boundary list (a single full-range shadow tablet).
        Column addedK1 = addedKeyColumn("__starrocks_shadow_k1", "0");
        LakeRangeRewriteSchemaChangeJob job = newJob(List.of(addedK1), List.of(addedK1), List.of(0));
        List<Tuple> constantSample = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            constantSample.add(new Tuple(List.of(Variant.of(IntegerType.INT, "0"))));
        }
        job.setSampler(request -> new SampleSet(constantSample, new Estimates(1024L, constantSample.size())));

        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        List<Tuple> boundaries = job.planBoundariesForTest(table, db.getFullName(), table.getName(), "p0",
                physicalPartitionId, /*partitionDataSize=*/ 1024L, /*requestedTabletCount=*/ 4);

        Assertions.assertTrue(boundaries.isEmpty(),
                "a sole-added-key ADD must collapse to a single full-range tablet");
    }
}
