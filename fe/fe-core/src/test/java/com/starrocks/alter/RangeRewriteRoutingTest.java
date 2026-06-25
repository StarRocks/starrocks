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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Routing tests for {@link SchemaChangeHandler#needsRangeRewriteSchemaChange}: an eligible
 * range-distribution key change on a shared-data table is routed to
 * {@link LakeRangeRewriteSchemaChangeJob}; everything else keeps the existing rejection.
 */
public class RangeRewriteRoutingTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
    }

    @AfterAll
    public static void tearDown() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    private static SchemaChangeHandler handler() {
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
    }

    private static Database db() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
    }

    /** Run analyzeAndCreateJob (without running the job) and return the created AlterJobV2. */
    private static AlterJobV2 createJob(String tableName, String alterSql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        return handler().analyzeAndCreateJob(stmt.getAlterClauseList(), db(), table(tableName));
    }

    /** Extract the underlying DdlException from a (possibly wrapped) throwable. */
    private static DdlException assertThrowsDdlException(Executable executable) {
        Throwable thrown = assertThrows(Throwable.class, executable);
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (current instanceof DdlException) {
                return (DdlException) current;
            }
        }
        fail("Expected DdlException in cause chain of " + thrown);
        return null; // unreachable
    }

    // (a) Lake range DUP table: ALTER ... ORDER BY (reordered) routes to the rewrite job.
    @Test
    public void testReorderSortKeyRoutesToRangeRewriteJob() throws Exception {
        starRocksAssert.withTable("create table t_route_reorder (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        AlterJobV2 job = createJob("t_route_reorder", "alter table t_route_reorder order by (k2, k1)");
        assertInstanceOf(LakeRangeRewriteSchemaChangeJob.class, job);
    }

    // (b) Lake range DUP table: MODIFY COLUMN keyness flip that shifts the (key-derived) sort key
    // routes to the rewrite job. DUP is used because all DUP columns carry the same (NONE)
    // aggregation type, so promoting a value column to a key is a valid schema change (an AGG
    // value->key flip is independently rejected by the aggregation-type compatibility check).
    @Test
    public void testKeynessFlipShiftingSortKeyRoutesToRangeRewriteJob() throws Exception {
        // DUP range table with explicit keys but no explicit ORDER BY -> the range sort key is
        // derived from the key columns, so promoting v1 to a key shifts it.
        starRocksAssert.withTable("create table t_route_keyflip (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_keyflip");
        org.junit.jupiter.api.Assertions.assertTrue(table.isRangeDistribution(),
                "expected a range-distribution table");
        AlterJobV2 job = createJob("t_route_keyflip", "alter table t_route_keyflip modify column v1 int key");
        assertInstanceOf(LakeRangeRewriteSchemaChangeJob.class, job);
        // Regression (e2e-found): the rewrite schema must carry un-prefixed (base) column names.
        // processModifyColumn shadow-prefixes the flipped column (__starrocks_shadow_v1); if that name
        // leaks into the rewrite job, the INSERT ... SELECT (over base names) cannot resolve the routing
        // column and the job cancels at RUNNING. The reorder path already passes the base schema.
        for (com.starrocks.catalog.Column c : ((LakeRangeRewriteSchemaChangeJob) job).getNewSchema()) {
            org.junit.jupiter.api.Assertions.assertFalse(
                    c.getName().startsWith(SchemaChangeHandler.SHADOW_NAME_PREFIX),
                    "rewrite schema column must be un-prefixed, got: " + c.getName());
        }
    }

    // (c) ADD COLUMN ... KEY on a range table stays rejected (column-set change, out of scope).
    @Test
    public void testAddKeyColumnStillRejected() throws Exception {
        starRocksAssert.withTable("create table t_route_addkey (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        DdlException e = assertThrowsDdlException(() ->
                createJob("t_route_addkey", "alter table t_route_addkey add column k_new int key default '0'"));
        org.junit.jupiter.api.Assertions.assertTrue(
                e.getMessage().toLowerCase().contains("range distribution"), e.getMessage());
    }

    // (d) OPTIMIZE on a range table stays rejected.
    @Test
    public void testOptimizeStillRejected() throws Exception {
        starRocksAssert.withTable("create table t_route_optimize (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        // OPTIMIZE is triggered by `alter table ... distributed by ...`; the analyzer rejects it
        // before a job is created, so this asserts on the (wrapped) message.
        Throwable e = assertThrows(Throwable.class, () ->
                starRocksAssert.alterTable("alter table t_route_optimize distributed by hash(k1)"));
        org.junit.jupiter.api.Assertions.assertTrue(
                e.getMessage().toLowerCase().contains("range distribution"), e.getMessage());
    }

    // (e) A non-lake (shared-nothing) range table must NOT route to the lake rewrite job. The lake
    // gate is exercised by making isCloudNativeTable() report false for the table; both a reorder and
    // a keyness flip must then keep the existing rejection rather than building a
    // LakeRangeRewriteSchemaChangeJob.
    @Test
    public void testNonLakeRangeTableNotRouted() throws Exception {
        starRocksAssert.withTable("create table t_route_nonlake (k1 int, v1 int sum)\n"
                + "aggregate key(k1)\n"
                + "properties('replication_num' = '1');");
        starRocksAssert.withTable("create table t_route_nonlake_dup (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");

        new MockUp<OlapTable>() {
            @Mock
            public boolean isCloudNativeTable() {
                return false;
            }
        };

        // Predicate is false for both the keyness flip and the reorder once the table is not lake.
        AlterTableStmt flipStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_nonlake modify column v1 int key", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table("t_route_nonlake"), flipStmt.getAlterClauseList().get(0)));
        AlterTableStmt reorderStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_nonlake_dup order by (k2, k1)", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table("t_route_nonlake_dup"), reorderStmt.getAlterClauseList().get(0)));

        // And the dispatch keeps the existing rejection (it throws rather than building any job, so
        // in particular no LakeRangeRewriteSchemaChangeJob is created).
        assertThrows(Throwable.class, () ->
                createJob("t_route_nonlake", "alter table t_route_nonlake modify column v1 int key"));
        assertThrows(Throwable.class, () ->
                createJob("t_route_nonlake_dup", "alter table t_route_nonlake_dup order by (k2, k1)"));
    }

    // (f) Regression guard: a keyness flip BUNDLED with a type change (here a VARCHAR shorten) is NOT
    // a pure keyness flip, so it must NOT be routed. It instead falls into the existing range-
    // distribution rejection path rather than building a rewrite job that would have bypassed
    // validation -- the gap this gate closes.
    @Test
    public void testKeynessFlipBundledWithTypeChangeNotRouted() throws Exception {
        starRocksAssert.withTable("create table t_route_keyflip_shorten (k1 int, v1 varchar(10))\n"
                + "duplicate key(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_keyflip_shorten");
        org.junit.jupiter.api.Assertions.assertTrue(table.isRangeDistribution(),
                "expected a range-distribution table");
        // The flip alone would shift the sort key, but the bundled VARCHAR(10)->VARCHAR(5) shorten
        // means it is no longer a pure keyness flip, so the predicate is false.
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_keyflip_shorten modify column v1 varchar(5) key", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, stmt.getAlterClauseList().get(0)));
        // And the dispatch rejects it with a DdlException rather than building any rewrite job.
        DdlException e = assertThrowsDdlException(() -> createJob("t_route_keyflip_shorten",
                "alter table t_route_keyflip_shorten modify column v1 varchar(5) key"));
        org.junit.jupiter.api.Assertions.assertTrue(
                e.getMessage().toLowerCase().contains("range distribution"), e.getMessage());
    }

    // (g) An AGG value->key flip inherently changes the column's aggregation state (a value column
    // carries an aggregation function, a key does not), so it is not a pure keyness flip and is not
    // routed; it falls through to the existing aggregation-type-compatibility rejection.
    @Test
    public void testAggValueToKeyFlipNotRouted() throws Exception {
        starRocksAssert.withTable("create table t_route_agg_flip (k1 int, v1 int sum)\n"
                + "aggregate key(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_agg_flip");
        org.junit.jupiter.api.Assertions.assertTrue(table.isRangeDistribution(),
                "expected a range-distribution table");
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_agg_flip modify column v1 int key", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, stmt.getAlterClauseList().get(0)));
        // The dispatch keeps the existing rejection rather than building a rewrite job.
        assertThrows(Throwable.class, () ->
                createJob("t_route_agg_flip", "alter table t_route_agg_flip modify column v1 int key"));
    }

    // (h) A lake range-distribution table that has rollup / synchronous-MV indexes beyond the base
    // index must NOT be routed to the range-rewrite job: createRangeRewriteJob / buildRangeRewriteJob
    // only rebuild the base index, so rollup indexes would be left stale. The presence of extra
    // indexes is simulated by mocking getIndexMetaIdToMeta() to return a two-entry map (base + rollup).
    @Test
    public void testRollupTableNotRouted() throws Exception {
        starRocksAssert.withTable("create table t_route_rollup (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_rollup");
        org.junit.jupiter.api.Assertions.assertTrue(table.isRangeDistribution(),
                "expected a range-distribution table");

        // Simulate a rollup index by making getIndexMetaIdToMeta() report two entries.
        MaterializedIndexMeta fakeMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        new MockUp<OlapTable>() {
            @Mock
            public Map<Long, MaterializedIndexMeta> getIndexMetaIdToMeta() {
                return ImmutableMap.of(
                        table.getBaseIndexMetaId(), fakeMeta,
                        table.getBaseIndexMetaId() + 1L, fakeMeta);
            }
        };

        // Both a sort-key reorder and a keyness flip must be blocked when a rollup exists.
        AlterTableStmt reorderStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_rollup order by (k2, k1)", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, reorderStmt.getAlterClauseList().get(0)),
                "reorder must not route to range-rewrite when rollup exists");

        AlterTableStmt flipStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_rollup modify column v1 int key", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, flipStmt.getAlterClauseList().get(0)),
                "keyness flip must not route to range-rewrite when rollup exists");
    }

    // (i) A pure keyness flip whose sort-key shift would leave NO key column (the only/last key demoted to
    // a value) must NOT be routed. createRangeRewriteJob would otherwise build a shadow with zero key
    // columns and the job would fail asynchronously at validateRewriteConfig. The routing re-checks the
    // post-flip key invariants and declines, so the flip is rejected synchronously at DDL time instead.
    @Test
    public void testKeynessFlipLeavingNoKeyRejectedSynchronously() throws Exception {
        starRocksAssert.withTable("create table t_route_demote_only_key (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_demote_only_key");
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_demote_only_key modify column k1 int", connectContext);
        // The flip shifts the (key-derived) sort key, so the routing predicate still matches...
        org.junit.jupiter.api.Assertions.assertTrue(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, stmt.getAlterClauseList().get(0)),
                "demoting the only key shifts the sort key, so the predicate matches");
        // ...but the post-flip schema has no key column, so it must be rejected synchronously (no
        // LakeRangeRewriteSchemaChangeJob is built) rather than failing later inside the rewrite job.
        assertThrowsDdlException(() ->
                createJob("t_route_demote_only_key", "alter table t_route_demote_only_key modify column k1 int"));
    }

    // (j) A pure keyness flip whose sort-key shift would place a VALUE column before a KEY column
    // (demoting a non-last key) must NOT be routed: the post-flip schema violates the key-prefix
    // invariant finalAnalyze enforces. The routing declines and the flip is rejected synchronously.
    @Test
    public void testKeynessFlipPuttingValueBeforeKeyRejectedSynchronously() throws Exception {
        starRocksAssert.withTable("create table t_route_demote_first_key (k1 int, k2 int, v1 int)\n"
                + "duplicate key(k1, k2)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_demote_first_key");
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_demote_first_key modify column k1 int", connectContext);
        org.junit.jupiter.api.Assertions.assertTrue(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, stmt.getAlterClauseList().get(0)),
                "demoting the first of two keys shifts the sort key, so the predicate matches");
        assertThrowsDdlException(() ->
                createJob("t_route_demote_first_key", "alter table t_route_demote_first_key modify column k1 int"));
    }

    // (k) A cloud-native MATERIALIZED VIEW must NOT route to the range-rewrite job, even though it is a
    // range-distribution lake object. The predicate gates on isCloudNativeTable() (table-only), not
    // isCloudNativeTableOrMaterializedView(): a key/sort-key ALTER TABLE on an MV is already rejected
    // upstream by AlterTableStatementAnalyzer, and the rewrite drives an internal INSERT that
    // InsertAnalyzer rejects on a non-system MV target -- so the MV must be declined here regardless.
    // The MV nature is simulated by making the table report as a cloud-native MV (not a plain table).
    @Test
    public void testMaterializedViewNotRouted() throws Exception {
        starRocksAssert.withTable("create table t_route_mv_guard (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "properties('replication_num' = '1');");
        OlapTable table = table("t_route_mv_guard");
        org.junit.jupiter.api.Assertions.assertTrue(table.isRangeDistribution(),
                "expected a range-distribution table");

        // Report as a cloud-native MV: isCloudNativeTable() is false, but the broad
        // isCloudNativeTableOrMaterializedView() (its default OR) still reports true.
        new MockUp<OlapTable>() {
            @Mock
            public boolean isCloudNativeTable() {
                return false;
            }

            @Mock
            public boolean isCloudNativeMaterializedView() {
                return true;
            }
        };
        org.junit.jupiter.api.Assertions.assertTrue(table.isCloudNativeTableOrMaterializedView(),
                "guard models an MV: the broad predicate still matches");

        // A keyness flip that would otherwise route must be declined because the target is an MV.
        AlterTableStmt flipStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table t_route_mv_guard modify column v1 int key", connectContext);
        assertFalse(SchemaChangeHandler.needsRangeRewriteSchemaChange(
                table, flipStmt.getAlterClauseList().get(0)),
                "a cloud-native materialized view must not route to the range-rewrite job");
    }
}
