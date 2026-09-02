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

package com.starrocks.sql.plan;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.ChangesScanNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Planner proof that a cloud-native CHANGES scan advertises its table's distribution as an
 * output property. A HASH-distributed table surfaces as a LOCAL hash property, so a single-phase
 * aggregation runs with no pre-agg shuffle and a join to a co-bucketed table colocates; a
 * RANDOM-distributed table advertises nothing and still shuffles. Two CHANGES scans over
 * different selected partition subsets must not be judged colocate-compatible.
 */
public class ChangesScanDistributionPlanTest extends BookmarkTestBase {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    @BeforeAll
    public static void beforeAllPlan() {
        // The plan-test framework wraps each SQL as CREATE VIEW for a round-trip
        // probe; CHANGES is rejected on views, so disable that probe.
        FeConstants.unitTestView = false;
    }

    @AfterAll
    public static void afterAllPlan() {
        FeConstants.unitTestView = true;
    }

    /**
     * Activation proof (no colocate group needed): a HASH-distributed CHANGES
     * scan advertises a LOCAL hash property on its bucket key, so aggregation
     * grouped by that key collapses to a single-phase aggregate with no shuffle
     * EXCHANGE below it. Before activation the scan advertised nothing and the
     * aggregate was forced into a two-phase shuffle.
     */
    @Test
    public void testChangesSingleTableAggIsOnePhase() throws Exception {
        String name = "ch_agg_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = getTable(tableId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("agg_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("agg_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, count(*) FROM %s [_CHANGES_%d_%d_] GROUP BY k",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = getFragmentPlan(sql);
            // One-phase aggregate directly over the scan: the LOCAL hash property on
            // the bucket key elides the pre-agg shuffle. A two-phase aggregate would
            // instead show "update serialize" + "merge finalize" around a shuffle
            // EXCHANGE. (The only remaining EXCHANGE is the top-level result gather,
            // above the aggregate, present in both shapes.)
            assertContains(plan, "AGGREGATE (update finalize)");
            assertNotContains(plan, "merge finalize");
            assertNotContains(plan, "update serialize");
        } finally {
            release(bm, tableId, base, hBase, head, hHead);
        }
    }

    /**
     * Negative control: a RANDOM-distributed CHANGES scan advertises no
     * distribution (ANY), so aggregation grouped by a column still needs a
     * shuffle EXCHANGE. This is invariant across activation.
     */
    @Test
    public void testRandomDistributedChangesStillShuffles() throws Exception {
        String name = "ch_rand_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY RANDOM BUCKETS 8 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = getTable(tableId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rand_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rand_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, count(*) FROM %s [_CHANGES_%d_%d_] GROUP BY k",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = getFragmentPlan(sql);
            // RANDOM distribution advertises no property, so the pre-agg shuffle
            // EXCHANGE forces a two-phase aggregate: "merge finalize" above the
            // shuffle. This is the inverse of testChangesSingleTableAggIsOnePhase,
            // which asserts "merge finalize" absent for the HASH one-phase case.
            assertContains(plan, "merge finalize");
            assertContains(plan, "EXCHANGE");
        } finally {
            release(bm, tableId, base, hBase, head, hHead);
        }
    }

    /**
     * Join payoff (RANGE colocate): a CHANGES scan over a range-colocate table
     * joined to a co-bucketed base table in the same colocate group produces a
     * colocate join with no shuffle. Before activation the CHANGES side
     * advertised nothing and the join fell back to shuffle.
     */
    @Test
    public void testChangesJoinCoBucketedTableColocatesRange() throws Exception {
        boolean savedConfig = Config.enable_range_distribution;
        boolean savedSessionVar = connectContext.getSessionVariable().isEnableRangeDistribution();
        Config.enable_range_distribution = true;
        connectContext.getSessionVariable().setEnableRangeDistribution(true);

        String group = "ch_grp_" + COUNTER.getAndIncrement();
        String delta = "ch_rc_delta_" + COUNTER.getAndIncrement();
        String dim = "ch_rc_dim_" + COUNTER.getAndIncrement();
        long deltaId = createTable("CREATE TABLE " + delta + " (k1 int, k2 int) ORDER BY(k1, k2) "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + ":k1');");
        createTable("CREATE TABLE " + dim + " (k1 int, k2 int) ORDER BY(k1, k2) "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + ":k1');");
        OlapTable deltaTable = getTable(deltaId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rc_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rc_head");
        Bookmark base = bm.create(dbId, deltaId, hBase);
        bumpVisibleVersion(deltaTable, 5L);
        Bookmark head = bm.create(dbId, deltaId, hHead);

        try {
            String sql = String.format(
                    "SELECT count(*) FROM %s c [_CHANGES_%d_%d_] JOIN %s d ON c.k1 = d.k1",
                    delta, base.getBookmarkId(), head.getBookmarkId(), dim);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "colocate: true");
        } finally {
            release(bm, deltaId, base, hBase, head, hHead);
            Config.enable_range_distribution = savedConfig;
            connectContext.getSessionVariable().setEnableRangeDistribution(savedSessionVar);
        }
    }

    /**
     * Join payoff (HASH colocate): a CHANGES scan over a hash-colocate table
     * joined to a co-bucketed base table in the same colocate group produces a
     * colocate join with no shuffle. Before activation the CHANGES side
     * advertised nothing and the join fell back to shuffle.
     */
    @Test
    public void testChangesJoinCoBucketedTableColocatesHash() throws Exception {
        String group = "ch_hgrp_" + COUNTER.getAndIncrement();
        String delta = "ch_hc_delta_" + COUNTER.getAndIncrement();
        String dim = "ch_hc_dim_" + COUNTER.getAndIncrement();
        long deltaId = createTable("CREATE TABLE " + delta + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + "');");
        createTable("CREATE TABLE " + dim + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + "');");
        OlapTable deltaTable = getTable(deltaId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("hc_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("hc_head");
        Bookmark base = bm.create(dbId, deltaId, hBase);
        bumpVisibleVersion(deltaTable, 5L);
        Bookmark head = bm.create(dbId, deltaId, hHead);

        try {
            String sql = String.format(
                    "SELECT count(*) FROM %s c [_CHANGES_%d_%d_] JOIN %s d ON c.k = d.k",
                    delta, base.getBookmarkId(), head.getBookmarkId(), dim);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "colocate: true");
        } finally {
            release(bm, deltaId, base, hBase, head, hHead);
        }
    }

    /**
     * Group-execution payoff on top of the HASH colocate join: with group
     * execution enabled, the colocate join over a CHANGES scan and a
     * co-bucketed base table runs bucket-parallel, so EXPLAIN prints the
     * "colocate exec groups:" marker (only emitted when
     * Config.show_execution_groups is true) alongside "COLOCATE". Before the
     * CHANGES scan's ExecGroup was allowed to join a colocate group, the
     * fragment's ExecGroup was force-disabled, so the join was still
     * "colocate: true" but never became a group-execution colocate exec
     * group.
     */
    @Test
    public void testChangesColocateJoinUsesGroupExecution() throws Exception {
        boolean savedShowExecGroups = Config.show_execution_groups;
        boolean savedEnableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        Config.show_execution_groups = true;
        connectContext.getSessionVariable().setEnableGroupExecution(true);

        String group = "ch_gx_grp_" + COUNTER.getAndIncrement();
        String delta = "ch_gx_delta_" + COUNTER.getAndIncrement();
        String dim = "ch_gx_dim_" + COUNTER.getAndIncrement();
        long deltaId = createTable("CREATE TABLE " + delta + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + "');");
        createTable("CREATE TABLE " + dim + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + "');");
        OlapTable deltaTable = getTable(deltaId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("gx_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("gx_head");
        Bookmark base = bm.create(dbId, deltaId, hBase);
        bumpVisibleVersion(deltaTable, 5L);
        Bookmark head = bm.create(dbId, deltaId, hHead);

        try {
            String sql = String.format(
                    "SELECT count(*) FROM %s c [_CHANGES_%d_%d_] JOIN %s d ON c.k = d.k",
                    delta, base.getBookmarkId(), head.getBookmarkId(), dim);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "colocate exec groups:");
            assertContains(plan, "COLOCATE");
        } finally {
            // Restore global/session settings before release: a throwing release must not
            // leak show_execution_groups or the group-execution session var into later tests.
            Config.show_execution_groups = savedShowExecGroups;
            connectContext.getSessionVariable().setEnableGroupExecution(savedEnableGroupExecution);
            release(bm, deltaId, base, hBase, head, hHead);
        }
    }

    /**
     * Single-table group-execution payoff (no colocate-OlapScan sibling): a
     * one-phase aggregation grouped by the bucket key, directly over a single
     * CHANGES scan of a colocate-bucketed table, is flagged as a colocate node.
     * That flag is what makes the enclosing fragment schedule bucket-aware
     * (ColocatedBackendSelector) so group execution actually engages per bucket;
     * without it the fragment would print the group-execution marker but fall
     * back to the normal, non-bucketed scan assignment. Because there is no
     * co-bucketed OlapScan in the fragment, the colocate flag is set only
     * because a colocate-table CHANGES scan is recognized as a colocate scan
     * child of the aggregate.
     */
    @Test
    public void testChangesSingleTableColocateAggUsesGroupExecution() throws Exception {
        boolean savedEnableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        connectContext.getSessionVariable().setEnableGroupExecution(true);

        String group = "ch_sgx_grp_" + COUNTER.getAndIncrement();
        String delta = "ch_sgx_delta_" + COUNTER.getAndIncrement();
        long deltaId = createTable("CREATE TABLE " + delta + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + "');");
        OlapTable deltaTable = getTable(deltaId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("sgx_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("sgx_head");
        Bookmark base = bm.create(dbId, deltaId, hBase);
        bumpVisibleVersion(deltaTable, 5L);
        Bookmark head = bm.create(dbId, deltaId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, count(*) FROM %s [_CHANGES_%d_%d_] GROUP BY k",
                    delta, base.getBookmarkId(), head.getBookmarkId());
            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            AggregationNode agg = findAggregationNode(plan);
            assertNotNull(agg, "expected an AggregationNode in the plan:\n"
                    + plan.getExplainString(TExplainLevel.NORMAL));
            assertTrue(agg.isColocate(),
                    "aggregate over a single colocate CHANGES scan must be a colocate node:\n"
                            + plan.getExplainString(TExplainLevel.NORMAL));
        } finally {
            connectContext.getSessionVariable().setEnableGroupExecution(savedEnableGroupExecution);
            release(bm, deltaId, base, hBase, head, hHead);
        }
    }

    /**
     * Inverse of testChangesSingleTableColocateAggUsesGroupExecution: a CHANGES scan whose
     * bookmark range crosses a tablet reshard must never land in a bucket-scheduled fragment, even
     * on a colocate table. It mixes generations, so it advertises no distribution and numbers no
     * buckets; the scheduler picks ColocatedBackendSelector for a fragment holding any colocate
     * node (ExecutionFragment.isColocated), and that selector derives its buckets from the scan's
     * empty bucket-sequence map -- assigning it no scan range at all on a hash-colocate table, and
     * failing closed on a range-colocate one. Asserted over the whole fragment holding the scan,
     * which is the unit the selector is chosen for.
     */
    @Test
    public void testChangesAcrossReshardIsNotColocateScheduled() throws Exception {
        boolean savedEnableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        connectContext.getSessionVariable().setEnableGroupExecution(true);

        String group = "ch_rsx_grp_" + COUNTER.getAndIncrement();
        String delta = "ch_rsx_delta_" + COUNTER.getAndIncrement();
        long deltaId = createTable("CREATE TABLE " + delta + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = '" + group + "');");
        OlapTable deltaTable = getTable(deltaId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsx_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsx_head");
        bumpVisibleVersion(deltaTable, 3L);
        Bookmark base = bm.create(dbId, deltaId, hBase);
        bumpVisibleVersion(deltaTable, 5L);
        installNewGeneration(deltaTable, 6, 6L);
        bumpVisibleVersion(deltaTable, 9L);
        Bookmark head = bm.create(dbId, deltaId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, count(*) FROM %s [_CHANGES_%d_%d_] GROUP BY k",
                    delta, base.getBookmarkId(), head.getBookmarkId());
            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            PlanFragment scanFragment = null;
            ChangesScanNode scan = null;
            for (PlanFragment fragment : plan.getFragments()) {
                scan = findChangesScanNode(fragment.getPlanRoot());
                if (scan != null) {
                    scanFragment = fragment;
                    break;
                }
            }
            assertNotNull(scanFragment, "expected a fragment holding the CHANGES scan:\n"
                    + plan.getExplainString(TExplainLevel.NORMAL));
            assertTrue(scan.getDelta().hasReshardedChanges(),
                    "the installed generation should make the bookmark range cross a reshard");
            assertFalse(hasColocateNode(scanFragment.getPlanRoot()),
                    "the fragment holding a generation-crossing CHANGES scan must not be scheduled "
                            + "bucket-aware:\n" + plan.getExplainString(TExplainLevel.NORMAL));
        } finally {
            connectContext.getSessionVariable().setEnableGroupExecution(savedEnableGroupExecution);
            release(bm, deltaId, base, hBase, head, hHead);
        }
    }

    /**
     * Red-line guard: two CHANGES scans over the same table but selecting
     * DIFFERENT partition subsets must not be judged colocate-compatible, so
     * their self-join shuffles; the same query restricted to the SAME partition
     * subset self-colocates. Proves the advertised property carries the
     * CHANGES-selected partition ids, not the table's full partition set.
     */
    @Test
    public void testSelfJoinPartitionSubsetGating() throws Exception {
        String name = "ch_self_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (\n"
                + "    k bigint NOT NULL,\n"
                + "    dt date NOT NULL,\n"
                + "    v bigint\n"
                + ") DUPLICATE KEY(k, dt)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "    PARTITION p1 VALUES LESS THAN ('2024-02-01'),\n"
                + "    PARTITION p2 VALUES LESS THAN ('2024-03-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = getTable(tableId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("self_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("self_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);
        String window = String.format("[_CHANGES_%d_%d_]", base.getBookmarkId(), head.getBookmarkId());

        try {
            // Same partition subset (both sides p1) -> self-colocate, no shuffle.
            String same = String.format(
                    "SELECT count(*) FROM %s c %s JOIN %s d %s ON c.k = d.k "
                            + "WHERE c.dt < '2024-02-01' AND d.dt < '2024-02-01'",
                    name, window, name, window);
            assertContains(getFragmentPlan(same), "colocate: true");

            // Different partition subsets (c=p1, d=p2) -> not colocate -> shuffle.
            String diff = String.format(
                    "SELECT count(*) FROM %s c %s JOIN %s d %s ON c.k = d.k "
                            + "WHERE c.dt < '2024-02-01' AND d.dt >= '2024-02-01'",
                    name, window, name, window);
            String diffPlan = getFragmentPlan(diff);
            assertFalse(diffPlan.contains("colocate: true"),
                    "CHANGES scans over different partition subsets must not colocate:\n" + diffPlan);
            assertContains(diffPlan, "EXCHANGE");
        } finally {
            release(bm, tableId, base, hBase, head, hHead);
        }
    }

    // ---------- helpers ----------

    private static AggregationNode findAggregationNode(ExecPlan plan) {
        for (PlanFragment fragment : plan.getFragments()) {
            AggregationNode agg = findAggregationNode(fragment.getPlanRoot());
            if (agg != null) {
                return agg;
            }
        }
        return null;
    }

    private static AggregationNode findAggregationNode(PlanNode root) {
        if (root instanceof AggregationNode aggregationNode) {
            return aggregationNode;
        }
        for (PlanNode child : root.getChildren()) {
            AggregationNode agg = findAggregationNode(child);
            if (agg != null) {
                return agg;
            }
        }
        return null;
    }

    /**
     * Simulates a tablet reshard on {@code table}'s only physical partition: installs a new
     * base-index generation (fresh index id, same meta id) over {@code tabletCount} real StarOS
     * shards and stamped with the reshard lineage, leaving the old generation installed. Real
     * shards matter because the scan resolves every tablet's queryable replicas through the
     * shard-to-worker mapping.
     */
    private static void installNewGeneration(OlapTable table, int tabletCount, long takeoverVersion)
            throws Exception {
        PhysicalPartition pp = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        MaterializedIndex oldBase = pp.getLatestBaseIndex();
        MaterializedIndex newBase = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                oldBase.getMetaId(), MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        List<Long> shardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                tabletCount, table.getPartitionFilePathInfo(pp.getId()),
                table.getPartitionFileCacheInfo(pp.getId()), pp.getShardGroupId(),
                null, Map.of(), WarehouseManager.DEFAULT_RESOURCE);
        for (Long shardId : shardIds) {
            newBase.addTablet(new LakeTablet(shardId), null, false);
        }
        newBase.setTakeoverVersion(takeoverVersion);
        newBase.setPredecessorIndexId(oldBase.getId());
        pp.addMaterializedIndex(newBase, true);
    }

    private static ChangesScanNode findChangesScanNode(PlanNode root) {
        if (root instanceof ChangesScanNode changesScanNode) {
            return changesScanNode;
        }
        for (PlanNode child : root.getChildren()) {
            ChangesScanNode scan = findChangesScanNode(child);
            if (scan != null) {
                return scan;
            }
        }
        return null;
    }

    /**
     * Whether any node of this fragment is flagged colocate -- the same walk (stopping at exchanges)
     * {@code ExecutionFragment.isColocated} makes to decide between ColocatedBackendSelector and the
     * normal one.
     */
    private static boolean hasColocateNode(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return false;
        }
        if (root.isColocate()) {
            return true;
        }
        return root.getChildren().stream().anyMatch(ChangesScanDistributionPlanTest::hasColocateNode);
    }

    private static OlapTable getTable(long tableId) {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);
        return table;
    }

    private String getFragmentPlan(String sql) throws Exception {
        return UtFrameUtils.getFragmentPlan(connectContext, sql);
    }

    private static void bumpVisibleVersion(OlapTable t, long newVersion) {
        for (Partition p : t.getPartitions()) {
            for (PhysicalPartition pp : p.getSubPartitions()) {
                pp.setVisibleVersion(newVersion, System.currentTimeMillis());
            }
        }
    }

    private void release(BookmarkManager bm, long tableId, Bookmark base, BookmarkHolder hBase,
                         Bookmark head, BookmarkHolder hHead) throws Exception {
        bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
        bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
    }

    private static void assertContains(String plan, String marker) {
        assertTrue(plan.contains(marker),
                "plan should contain `" + marker + "`:\n" + plan);
    }

    private static void assertNotContains(String plan, String marker) {
        assertFalse(plan.contains(marker),
                "plan should NOT contain `" + marker + "`:\n" + plan);
    }
}
