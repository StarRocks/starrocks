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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.planner.ChangesScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end planner test for [_CHANGES_base_head_] on a cloud-native DUP table.
 * Runs the optimizer's physical stage and fragment builder against a real
 * SHARED_DATA mini-cluster to cover ChangesScanImplementationRule.transform,
 * PhysicalChangesScanOperator, PlanFragmentBuilder.visitPhysicalChangesScan,
 * ChangesScanNode.computeScanRanges / getNodeExplainString / toThrift, and
 * StatisticsCalculator.visitLogicalChangesScan.
 */
public class CloudNativeChangesPlanTest extends BookmarkTestBase {

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

    @Test
    public void testChangesDataChangedPlan() throws Exception {
        String name = "ch_dc_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("plan_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("plan_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan should include ChangesScanNode:\n" + plan);
            assertTrue(plan.contains("TABLE: " + name),
                    "plan should reference " + name + ":\n" + plan);
            assertTrue(plan.contains("partitions=1/1"),
                    "plan should record partition selection:\n" + plan);
            assertTrue(plan.contains("tabletRatio=1/1"),
                    "plan should record tablet selection:\n" + plan);

            // Thrift conversion runs ChangesScanNode.toThrift; the FE → BE
            // contract carries the live schema key plus the FE-resolved
            // CHANGES metadata descriptors BE uses for classification.
            String thrift = UtFrameUtils.getPlanThriftString(connectContext, sql);
            assertTrue(thrift.contains("CHANGES_SCAN_NODE"),
                    "thrift plan should reference CHANGES_SCAN_NODE node type:\n" + thrift);
            assertTrue(thrift.contains("changes_scan_node"),
                    "thrift plan should attach changes_scan_node payload:\n" + thrift);
            assertTrue(thrift.contains("kind:CHANGE_TYPE"),
                    "thrift plan should carry a CHANGE_TYPE meta descriptor:\n" + thrift);
            assertTrue(thrift.contains("kind:ROW_VERSION"),
                    "thrift plan should carry a ROW_VERSION meta descriptor:\n" + thrift);
            assertTrue(thrift.contains("name:__CHANGE_TYPE__"),
                    "thrift plan should carry the CHANGE_TYPE default name:\n" + thrift);
            assertTrue(thrift.contains("name:__ROW_VERSION__"),
                    "thrift plan should carry the ROW_VERSION default name:\n" + thrift);

            // Direct accessor coverage: the scheduler reads these on every
            // CHANGES scan node, and runtime adaptive DOP is unsupported.
            Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            ChangesScanNode scan = null;
            for (ScanNode node : planPair.second.getScanNodes()) {
                if (node instanceof ChangesScanNode) {
                    scan = (ChangesScanNode) node;
                    break;
                }
            }
            assertNotNull(scan, "ExecPlan should contain a ChangesScanNode");
            assertFalse(scan.canUseRuntimeAdaptiveDop(),
                    "ChangesScanNode never opts into runtime adaptive DOP");
            assertFalse(scan.getScanRangeLocations(0).isEmpty(),
                    "ChangesScanNode should emit at least one scan range");
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesPredicatePushedDownToScan() throws Exception {
        String name = "ch_pred_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("pred_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("pred_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, v FROM %s [_CHANGES_%d_%d_] WHERE k > 1",
                    name, base.getBookmarkId(), head.getBookmarkId());
            Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            ChangesScanNode scan = null;
            for (ScanNode node : planPair.second.getScanNodes()) {
                if (node instanceof ChangesScanNode) {
                    scan = (ChangesScanNode) node;
                    break;
                }
            }
            assertNotNull(scan, "ExecPlan should contain a ChangesScanNode");
            // The WHERE predicate must be pushed into the scan operator and land as a
            // ChangesScanNode conjunct, mirroring how OLAP scan absorbs its filter.
            assertFalse(scan.getConjuncts().isEmpty(),
                    "WHERE predicate should be pushed into the ChangesScanNode conjuncts:\n"
                            + planPair.first);
            // Verify the pushed predicate is the expected one and shows in EXPLAIN the
            // same way OLAP scan renders it (a PREDICATES line on the CHANGES scan node).
            String fragmentPlan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(fragmentPlan.contains("PREDICATES:") && fragmentPlan.contains("k > 1"),
                    "CHANGES scan EXPLAIN should show the pushed predicate content (k > 1):\n"
                            + fragmentPlan);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesVerbosePredicateFormat() throws Exception {
        String name = "ch_vpred_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("vpred_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("vpred_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, v FROM %s [_CHANGES_%d_%d_] WHERE k > 1",
                    name, base.getBookmarkId(), head.getBookmarkId());
            // EXPLAIN VERBOSE must render the CHANGES scan predicate the same way OLAP scan
            // does: a lowercase "Predicates:" line carrying the typed verbose expression
            // ("[.. INT ..]"), not the NORMAL-level "PREDICATES:" form.
            String verbose = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
            assertTrue(verbose.contains("Predicates: ") && verbose.contains("k, INT, true]"),
                    "CHANGES scan EXPLAIN VERBOSE should render the predicate in OLAP verbose "
                            + "format (Predicates: [.. INT ..]):\n" + verbose);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesScanColumnPruning() throws Exception {
        String name = "ch_prune_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name
                + " (k int, c varchar(10), v int, dim_id int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("prune_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("prune_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);
        String window = String.format("[_CHANGES_%d_%d_]", base.getBookmarkId(), head.getBookmarkId());

        try {
            // A bare projection prunes every unreferenced table column and both CDC
            // metadata columns; the scan materializes only k.
            assertEquals(Set.of("k"),
                    changesScanColumns(String.format("SELECT k FROM %s %s", name, window)),
                    "only the selected column should be materialized");

            // A selected CDC metadata column survives; the other CDC column and the
            // unreferenced table columns are pruned.
            assertEquals(Set.of("k", "__CHANGE_TYPE__"),
                    changesScanColumns(String.format("SELECT k, __CHANGE_TYPE__ FROM %s %s", name, window)),
                    "selected CDC metadata column should be kept, the rest pruned");

            // count(*) needs no specific column; canUseAnyColumn keeps exactly one
            // (smallest real) column and never a CDC metadata column.
            Set<String> countCols = changesScanColumns(String.format("SELECT count(*) FROM %s %s", name, window));
            assertEquals(1, countCols.size(), "count(*) should materialize exactly one column: " + countCols);
            assertFalse(countCols.contains("__CHANGE_TYPE__") || countCols.contains("__ROW_VERSION__"),
                    "count(*) must not pick a CDC metadata column: " + countCols);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /** Column names the CHANGES scan in {@code sql}'s plan actually materializes. */
    private Set<String> changesScanColumns(String sql) throws Exception {
        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        for (ScanNode node : planPair.second.getScanNodes()) {
            if (node instanceof ChangesScanNode) {
                return node.getDesc().getSlots().stream()
                        .map(slot -> slot.getColumn().getName())
                        .collect(Collectors.toSet());
            }
        }
        throw new AssertionError("ExecPlan should contain a ChangesScanNode:\n" + planPair.first);
    }

    @Test
    public void testChangesPartitionAddedPlan() throws Exception {
        // RANGE-partitioned table so ALTER TABLE ADD PARTITION can mint a
        // PartitionAdded delta between base and head; this exercises
        // ChangesScanNode.versionPair's PartitionAdded branch.
        String name = "ch_pa_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (\n"
                + "    k bigint NOT NULL,\n"
                + "    dt date NOT NULL,\n"
                + "    v bigint\n"
                + ") DUPLICATE KEY(k, dt)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "    PARTITION p1 VALUES LESS THAN ('2024-02-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("pa_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("pa_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        addPartition(tableId, "p2", "2024-03-01");
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, dt, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan should include ChangesScanNode:\n" + plan);
            // Only the new partition is in the delta, so partitions=1/1.
            assertTrue(plan.contains("partitions=1/1"),
                    "plan should report only the added partition:\n" + plan);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesPlanWithNameConflict() throws Exception {
        // A base table whose schema already uses one of the default CHANGES
        // metadata names must still plan: the analyzer picks the alternate
        // query name, and PlanFragmentBuilder still wires the metadata
        // descriptor through to the thrift node.
        String name = "ch_conflict_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, `__CHANGE_TYPE__` int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("conf_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("conf_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            // Real __CHANGE_TYPE__ column is queryable; CHANGES metadata uses
            // __CHANGE_TYPE_1__.
            String sql = String.format(
                    "SELECT k, `__CHANGE_TYPE__`, __CHANGE_TYPE_1__, __ROW_VERSION__ "
                            + "FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan should include ChangesScanNode:\n" + plan);

            String thrift = UtFrameUtils.getPlanThriftString(connectContext, sql);
            assertTrue(thrift.contains("name:__CHANGE_TYPE_1__"),
                    "thrift plan should carry the alternate __CHANGE_TYPE_1__ name under conflict:\n"
                            + thrift);
            assertTrue(thrift.contains("name:__ROW_VERSION__"),
                    "thrift plan should still carry the default __ROW_VERSION__ name:\n"
                            + thrift);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesPartitionPruningRangeListExpr() throws Exception {
        // Range partition: a predicate on the partition column drops whole partitions from the delta.
        String rangePlan = changesPrunePlan(
                "CREATE TABLE ${name} (k bigint NOT NULL, dt date NOT NULL, v bigint) "
                        + "DUPLICATE KEY(k, dt) PARTITION BY RANGE(dt) ("
                        + "PARTITION p1 VALUES LESS THAN ('2026-02-01'), "
                        + "PARTITION p2 VALUES LESS THAN ('2026-03-01'), "
                        + "PARTITION p3 VALUES LESS THAN ('2026-04-01')) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1');",
                "k, dt, v", "dt >= '2026-02-01'");
        assertTrue(rangePlan.contains("partitions=2/3"),
                "RANGE partition prune should keep 2 of 3 delta partitions:\n" + rangePlan);

        // List partition: an IN predicate on the partition column drops whole partitions from the delta.
        String listPlan = changesPrunePlan(
                "CREATE TABLE ${name} (k int NOT NULL, city varchar(16) NOT NULL, v int) "
                        + "DUPLICATE KEY(k, city) PARTITION BY LIST(city) ("
                        + "PARTITION p_bj VALUES IN ('bj'), "
                        + "PARTITION p_sh VALUES IN ('sh'), "
                        + "PARTITION p_gz VALUES IN ('gz')) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1');",
                "k, city, v", "city IN ('bj', 'sh')");
        assertTrue(listPlan.contains("partitions=2/3"),
                "LIST partition prune should keep 2 of 3 delta partitions:\n" + listPlan);

        // Expression (date_trunc) partition: drives OptOlapPartitionPruner.doFurtherPartitionPrune.
        String exprPlan = changesPrunePlan(
                "CREATE TABLE ${name} (k bigint NOT NULL, dt datetime NOT NULL, v bigint) "
                        + "DUPLICATE KEY(k, dt) PARTITION BY date_trunc('day', dt) ("
                        + "START ('2026-01-01') END ('2026-01-04') EVERY (INTERVAL 1 DAY)) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1');",
                "k, dt, v", "dt >= '2026-01-02 00:00:00'");
        // The delta holds the 3 day partitions plus the automatic shadow partition (4 total); the
        // predicate keeps the 2026-01-02/03 days and prunes 2026-01-01 and the shadow.
        assertTrue(exprPlan.contains("partitions=2/4"),
                "expression partition prune should keep 2 of 4 delta partitions:\n" + exprPlan);
    }

    @Test
    public void testChangesTabletPruningHashRandom() throws Exception {
        // Hash distribution: an equality on the bucket key prunes to a single tablet.
        String hashPlan = changesPrunePlan(
                "CREATE TABLE ${name} (k int, v int) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num'='1');",
                "k, v", "k = 7");
        assertTrue(hashPlan.contains("partitions=1/1"),
                "single-partition table keeps its one partition:\n" + hashPlan);
        assertTrue(hashPlan.contains("tabletRatio=1/8"),
                "HASH bucket-key equality should prune to one tablet:\n" + hashPlan);

        // Random distribution cannot map a value to a bucket; every tablet stays.
        String randomPlan = changesPrunePlan(
                "CREATE TABLE ${name} (k int, v int) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY RANDOM BUCKETS 4 PROPERTIES ('replication_num'='1');",
                "k, v", "k = 7");
        assertTrue(randomPlan.contains("tabletRatio=4/4"),
                "RANDOM distribution cannot prune tablets:\n" + randomPlan);
    }

    @Test
    public void testChangesPruningParityWithOlapScan() throws Exception {
        // HASH: the CHANGES scan must prune to the same tabletRatio as an equivalent OLAP scan.
        assertChangesOlapTabletParity(
                "CREATE TABLE ${name} (k int, v int) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num'='1');",
                "k = 7");

        // RANGE distribution: exercises RangeDistributionPruner through the same shared core the
        // OLAP scan uses. A freshly created range table has a single (-inf,+inf) tablet, so both
        // scans select it; this checks the CHANGES path routes RANGE distribution identically.
        boolean prev = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        try {
            assertChangesOlapTabletParity(
                    "CREATE TABLE ${name} (k int, v int) ORDER BY (k) "
                            + "PROPERTIES ('replication_num'='1');",
                    "k >= 3");
        } finally {
            Config.enable_range_distribution = prev;
        }
    }

    /**
     * Builds a CHANGES window over a freshly created table whose every partition is DATA_CHANGED
     * between base and head, then returns the fragment plan for {@code SELECT cols ... WHERE pred}.
     */
    private String changesPrunePlan(String createDdl, String selectCols, String wherePred) throws Exception {
        String name = "ch_pr_" + COUNTER.getAndIncrement();
        long tableId = createTable(createDdl.replace("${name}", name));
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("pr_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("pr_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);
        try {
            String sql = String.format("SELECT %s FROM %s [_CHANGES_%d_%d_] WHERE %s",
                    selectCols, name, base.getBookmarkId(), head.getBookmarkId(), wherePred);
            return UtFrameUtils.getFragmentPlan(connectContext, sql);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /** Asserts the CHANGES scan and an equivalent OLAP scan prune to the same tabletRatio. */
    private void assertChangesOlapTabletParity(String createDdl, String wherePred) throws Exception {
        String name = "ch_par_" + COUNTER.getAndIncrement();
        long tableId = createTable(createDdl.replace("${name}", name));
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("par_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("par_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);
        try {
            String changesPlan = UtFrameUtils.getFragmentPlan(connectContext, String.format(
                    "SELECT k, v FROM %s [_CHANGES_%d_%d_] WHERE %s",
                    name, base.getBookmarkId(), head.getBookmarkId(), wherePred));
            String olapPlan = UtFrameUtils.getFragmentPlan(connectContext,
                    String.format("SELECT k, v FROM %s WHERE %s", name, wherePred));
            assertEquals(tabletRatioOf(olapPlan), tabletRatioOf(changesPlan),
                    "CHANGES scan should prune to the same tabletRatio as OLAP scan\nCHANGES:\n"
                            + changesPlan + "\nOLAP:\n" + olapPlan);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    private static String tabletRatioOf(String plan) {
        Matcher m = Pattern.compile("tabletRatio=(\\d+/\\d+)").matcher(plan);
        assertTrue(m.find(), "plan should contain a tabletRatio:\n" + plan);
        return m.group(1);
    }

    private static void bumpVisibleVersion(OlapTable t, long newVersion) {
        for (Partition p : t.getPartitions()) {
            for (PhysicalPartition pp : p.getSubPartitions()) {
                pp.setVisibleVersion(newVersion, System.currentTimeMillis());
            }
        }
    }
}
