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

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.planner.ChangesScanNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TChangeDerivationMode;
import com.starrocks.thrift.TChangeScanSpec;
import com.starrocks.thrift.TChangesScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    public void testChangesTopNRuntimeFilter() throws Exception {
        String name = "ch_topn_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("topn_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("topn_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        // A freshly created test table has a tiny estimated cardinality, and
        // globalRuntimeFilterProbeMinSize's default (100 * 1024) would reject the TopN runtime
        // filter as not worth probing regardless of scan-node eligibility. Force-accept it like
        // IcebergTopNRuntimeFilterTest does, so this test isolates the eligibility gate
        // (ChangesScanNode.supportTopNRuntimeFilter) from the unrelated cardinality heuristic.
        long savedProbeMinSize = connectContext.getSessionVariable().getGlobalRuntimeFilterProbeMinSize();
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
        try {
            String sql = String.format(
                    "SELECT k FROM %s [_CHANGES_%d_%d_] ORDER BY k DESC LIMIT 10",
                    name, base.getBookmarkId(), head.getBookmarkId());
            // "build runtime filters:" / "probe runtime filters:" only render at VERBOSE detail
            // level (PlanNode.getNodeVerboseExplain), mirroring IcebergTopNRuntimeFilterTest.
            String verbose = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
            assertTrue(verbose.contains("ChangesScanNode"),
                    "plan should include ChangesScanNode:\n" + verbose);
            assertTrue(verbose.contains("build runtime filters:"),
                    "TopN sort should build a runtime filter:\n" + verbose);
            assertTrue(verbose.contains("probe runtime filters:"),
                    "CHANGES scan should probe the TopN runtime filter (requires "
                            + "ChangesScanNode.supportTopNRuntimeFilter() to accept it):\n" + verbose);
        } finally {
            connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(savedProbeMinSize);
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

    @Test
    public void testChangesScanSlotIsOutputColumn() throws Exception {
        String name = "ch_oc_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("oc_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("oc_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);
        try {
            // k is projected; v is referenced ONLY by the WHERE predicate, so it is
            // materialized as a slot but must NOT be flagged as an output column.
            String sql = String.format(
                    "SELECT k FROM %s [_CHANGES_%d_%d_] WHERE v > 1",
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
            Map<String, Boolean> outputByColumn = scan.getDesc().getSlots().stream()
                    .collect(Collectors.toMap(s -> s.getColumn().getName(), SlotDescriptor::isOutputColumn));
            assertTrue(outputByColumn.containsKey("k") && outputByColumn.containsKey("v"), outputByColumn.toString());
            assertTrue(outputByColumn.get("k"), "projected k must be output: " + outputByColumn);
            assertFalse(outputByColumn.get("v"), "WHERE-only v must be non-output: " + outputByColumn);
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

            // The added partition's scan range must base at its initial (empty) version, not 0: the
            // BE walks each tablet's metadata-ancestor chain down to base, and a freshly-created
            // tablet's chain bottoms out at PARTITION_INIT_VERSION (its empty initial metadata), so
            // base 0 is unreachable and the scan fails with "cannot reach base version 0". Diffing
            // head against that empty initial version surfaces every rowset as an insert.
            Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            ChangesScanNode scan = null;
            for (ScanNode node : planPair.second.getScanNodes()) {
                if (node instanceof ChangesScanNode) {
                    scan = (ChangesScanNode) node;
                    break;
                }
            }
            assertNotNull(scan, "ExecPlan should contain a ChangesScanNode");
            var ranges = scan.getScanRangeLocations(0);
            assertFalse(ranges.isEmpty(), "added partition should emit a scan range");
            for (var loc : ranges) {
                TChangeScanSpec spec = loc.getScan_range().getChanges_scan_range().getScan_spec();
                assertEquals(TChangeDerivationMode.FULL_SCAN, spec.getDerivation_mode(),
                        "added-partition scan range must use FULL_SCAN (reads head only, no base)");
                assertFalse(spec.isSetBase_version(), "FULL_SCAN range must not carry a base_version");
            }
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

    @Test
    public void testChangesTabletHintNarrows() throws Exception {
        String name = "ch_th_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("th_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("th_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        long aTablet = table.getPartition(name).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(0).getId();
        try {
            String sql = String.format(
                    "SELECT k, v FROM %s TABLET(%d) [_CHANGES_%d_%d_]",
                    name, aTablet, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("tabletRatio=1/8"),
                    "TABLET hint should keep 1 of 8 tablets:\n" + plan);

            String bogus = String.format(
                    "SELECT k, v FROM %s TABLET(%d) [_CHANGES_%d_%d_]",
                    name, 1L, base.getBookmarkId(), head.getBookmarkId());
            SemanticException e = assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getFragmentPlan(connectContext, bogus));
            assertTrue(e.getMessage().contains("not trackable") && e.getMessage().contains("not present"),
                    "tablet id not in changeset should be rejected as not-trackable: " + e.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesPartitionAndTabletHintCombined() throws Exception {
        String name = "ch_pt_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name
                + " (k int NOT NULL, city varchar(16) NOT NULL, v int) DUPLICATE KEY(k, city) "
                + "PARTITION BY LIST(city) ("
                + "PARTITION p_bj VALUES IN ('bj'), PARTITION p_sh VALUES IN ('sh')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("pt_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("pt_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        long bjTablet = table.getPartition("p_bj").getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(0).getId();
        try {
            // PARTITION keeps p_bj (1/2); TABLET keeps one of p_bj's 4 tablets.
            String sql = String.format(
                    "SELECT k, city, v FROM %s PARTITION(p_bj) TABLET(%d) [_CHANGES_%d_%d_]",
                    name, bjTablet, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("partitions=1/2"),
                    "PARTITION hint should keep 1 of 2 partitions:\n" + plan);
            assertTrue(plan.contains("tabletRatio=1/4"),
                    "TABLET hint should keep 1 of p_bj's 4 tablets:\n" + plan);

            // A tablet from the non-named partition p_sh is outside the scan scope.
            long shTablet = table.getPartition("p_sh").getDefaultPhysicalPartition()
                    .getLatestBaseIndex().getTablets().get(0).getId();
            String crossPart = String.format(
                    "SELECT k, city, v FROM %s PARTITION(p_bj) TABLET(%d) [_CHANGES_%d_%d_]",
                    name, shTablet, base.getBookmarkId(), head.getBookmarkId());
            SemanticException e = assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getFragmentPlan(connectContext, crossPart));
            assertTrue(e.getMessage().contains("not trackable") && e.getMessage().contains("not present"),
                    "tablet outside named partitions should be not-trackable: " + e.getMessage());

            // REPLICA hint stays rejected on CHANGES.
            String withReplica = String.format(
                    "SELECT k, city, v FROM %s REPLICA(1) [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            SemanticException re = assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getFragmentPlan(connectContext, withReplica));
            assertTrue(re.getMessage().contains("REPLICA"),
                    "REPLICA hint should still be rejected: " + re.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testChangesPartitionHintNarrows() throws Exception {
        String name = "ch_ph_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name
                + " (k bigint NOT NULL, dt date NOT NULL, v bigint) DUPLICATE KEY(k, dt) "
                + "PARTITION BY RANGE(dt) ("
                + "PARTITION p1 VALUES LESS THAN ('2026-02-01'), "
                + "PARTITION p2 VALUES LESS THAN ('2026-03-01'), "
                + "PARTITION p3 VALUES LESS THAN ('2026-04-01')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("ph_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("ph_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);
        try {
            // PARTITION hint keeps 1 of the 3 changed partitions.
            String sql = String.format(
                    "SELECT k, dt, v FROM %s PARTITION(p2) [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("partitions=1/3"),
                    "PARTITION hint should keep 1 of 3 delta partitions:\n" + plan);

            // Naming a partition that has no changes in the window is not trackable.
            String absent = String.format(
                    "SELECT k, dt, v FROM %s PARTITION(p1) [_CHANGES_%d_%d_]",
                    name, head.getBookmarkId(), head.getBookmarkId());
            SemanticException e = assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getFragmentPlan(connectContext, absent));
            assertTrue(e.getMessage().contains("not trackable") && e.getMessage().contains("not present"),
                    "absent partition hint should be rejected as not-trackable: " + e.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * A bookmark range crossing two tablet reshards must be read one generation at a time: the
     * scan emits a range for every generation's tablets, each carrying that generation's own
     * version sub-range. The reshard commit version belongs to neither side, so the retiring
     * generation's slice ends one version below the takeover.
     */
    @Test
    public void testChangesAcrossReshardEmitsPerEpochRanges() throws Exception {
        String name = "ch_rs_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rs_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rs_head");
        bumpVisibleVersion(table, 3L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        List<Long> gen0Tablets = currentGenerationTabletIds(table);

        // Two successive reshards: gen1 takes over at 6, gen2 at 9.
        bumpVisibleVersion(table, 5L);
        List<Long> gen1Tablets = installNewGeneration(table, 8, 6L);
        bumpVisibleVersion(table, 8L);
        List<Long> gen2Tablets = installNewGeneration(table, 2, 9L);
        bumpVisibleVersion(table, 12L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            Map<Long, TChangeScanSpec> specByTablet = scanSpecsByTablet(changesScanOf(sql));

            assertEquals(gen0Tablets.size() + gen1Tablets.size() + gen2Tablets.size(), specByTablet.size(),
                    "every generation's tablets must get a scan range: " + specByTablet.keySet());
            assertEpochSpecs(specByTablet, gen0Tablets, 3L, 5L);
            assertEpochSpecs(specByTablet, gen1Tablets, 6L, 8L);
            assertEpochSpecs(specByTablet, gen2Tablets, 9L, 12L);

            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(plan.contains("tabletRatio=14/14"),
                    "tabletRatio should count every generation's tablets:\n" + plan);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * Silent-filter hazard: with an equality predicate on the distribution column, tablet pruning
     * must run per generation and keep the matching OLD-generation tablet too. Pruning only the
     * head generation would leave the pre-reshard epoch with no surviving tablet, silently
     * dropping its changes from an otherwise successful query.
     */
    @Test
    public void testChangesAcrossReshardPrunesEveryGeneration() throws Exception {
        String name = "ch_rsp_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsp_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsp_head");
        bumpVisibleVersion(table, 3L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        List<Long> gen0Tablets = currentGenerationTabletIds(table);

        bumpVisibleVersion(table, 5L);
        List<Long> gen1Tablets = installNewGeneration(table, 8, 6L);
        bumpVisibleVersion(table, 9L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_] WHERE k = 7",
                    name, base.getBookmarkId(), head.getBookmarkId());
            Map<Long, TChangeScanSpec> specByTablet = scanSpecsByTablet(changesScanOf(sql));

            // A bucket-key equality keeps one tablet per generation: each generation hashes the
            // value over its own bucket count (4 before the reshard, 8 after).
            assertEquals(2, specByTablet.size(),
                    "bucket-key equality should keep one tablet per generation: " + specByTablet.keySet());
            assertEquals(1, gen0Tablets.stream().filter(specByTablet::containsKey).count(),
                    "the matching pre-reshard tablet must survive pruning: " + specByTablet.keySet());
            assertEquals(1, gen1Tablets.stream().filter(specByTablet::containsKey).count(),
                    "the matching post-reshard tablet must survive pruning: " + specByTablet.keySet());
            assertEpochSpecs(specByTablet, gen0Tablets, 3L, 5L);
            assertEpochSpecs(specByTablet, gen1Tablets, 6L, 9L);

            // EXPLAIN reports the pruning: 1 of 4 pre-reshard plus 1 of 8 post-reshard tablets.
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertEquals("2/12", tabletRatioOf(plan),
                    "a crossing scan should report the pruned count over every generation's tablets:\n" + plan);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * A scan mixing generations has no single bucket layout, so it advertises no distribution and
     * builds no bucket-sequence map: the colocation dispatch map stays empty instead of tripping
     * over an unnumbered old-generation tablet.
     */
    @Test
    public void testChangesAcrossReshardBuildsNoBucketSeq() throws Exception {
        String name = "ch_rsb_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsb_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsb_head");
        bumpVisibleVersion(table, 3L);
        Bookmark base = bm.create(dbId, tableId, hBase);

        bumpVisibleVersion(table, 5L);
        installNewGeneration(table, 8, 6L);
        bumpVisibleVersion(table, 9L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            ChangesScanNode scan = changesScanOf(sql);
            assertFalse(scan.getScanRangeLocations(0).isEmpty(), "the crossing scan should emit ranges");
            assertTrue(scan.getBucketSeqToLocations().isEmpty(),
                    "a generation-crossing scan must not build a bucket-sequence map: "
                            + scan.getBucketSeqToLocations());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * Net-change fold with an empty base folds to head's live rows even across a reshard, so the
     * scan stays on the single-spec FULL_SCAN path over the head generation only -- no
     * old-generation tablets, no per-epoch diff.
     */
    @Test
    public void testChangesAcrossReshardNetChangeReadsHeadOnly() throws Exception {
        String name = "ch_rsn_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsn_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsn_head");
        // Base at the partition's initial (empty) version: the precondition for the head-only fold.
        Bookmark base = bm.create(dbId, tableId, hBase);
        List<Long> gen0Tablets = currentGenerationTabletIds(table);

        bumpVisibleVersion(table, 5L);
        List<Long> gen1Tablets = installNewGeneration(table, 8, 6L);
        bumpVisibleVersion(table, 9L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        boolean savedNetChange = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            Map<Long, TChangeScanSpec> specByTablet = scanSpecsByTablet(changesScanOf(sql));

            assertEquals(Set.copyOf(gen1Tablets), specByTablet.keySet(),
                    "the head-only fold must read exactly the head generation's tablets");
            assertTrue(gen0Tablets.stream().noneMatch(specByTablet::containsKey),
                    "no pre-reshard tablet may be read: " + specByTablet.keySet());
            for (TChangeScanSpec spec : specByTablet.values()) {
                assertEquals(TChangeDerivationMode.FULL_SCAN, spec.getDerivation_mode());
                assertEquals(9L, spec.getHead_version());
                assertFalse(spec.isSetBase_version(), "FULL_SCAN range must not carry a base_version");
            }
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(savedNetChange);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * A resolved reshard whose epochs are all empty crosses nothing. base = S-1 and head = S for a
     * split with no load in between collapses both sub-ranges, so the scan reads no tablet at all.
     * The protections that exist for a generation-crossing scan -- ANY distribution, the fragment
     * colocate veto, and rejecting a TABLET hint -- must not fire for it; rejecting a hint here
     * told the user "the range crosses a tablet reshard" about a range that crosses nothing.
     */
    @Test
    public void testEmptyEpochReshardIsNotTreatedAsCrossing() throws Exception {
        String name = "ch_rse_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rse_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rse_head");
        // Base at S-1, then the split takes over at S with no load in between: (S-1, S-1] and
        // (S, S] are both empty, so the resolved change carries no epochs. The partition version
        // has to advance to S with the takeover -- a reshard commits at S -- or the generation is
        // installed ahead of the version and the delta does not resolve at all.
        bumpVisibleVersion(table, 5L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        List<Long> gen1Tablets = installNewGeneration(table, 4, 6L);
        bumpVisibleVersion(table, 6L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            assertTrue(scanSpecsByTablet(changesScanOf(sql)).isEmpty(),
                    "an empty-epoch reshard must produce no scan ranges");

            // The hint names a head-generation tablet, which is what the scan would read if it read
            // anything. It must be accepted rather than rejected as generation-crossing.
            String hinted = String.format("SELECT k, v FROM %s TABLET(%d) [_CHANGES_%d_%d_]",
                    name, gen1Tablets.get(0), base.getBookmarkId(), head.getBookmarkId());
            assertTrue(scanSpecsByTablet(changesScanOf(hinted)).isEmpty(),
                    "a TABLET hint must be accepted for a range that crosses nothing");
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * Net change on a PK table across a reshard must shuffle. The fold is
     * MIN/MAX(rv) OVER (PARTITION BY pk), so every row of a key has to reach one fragment instance;
     * the two generations bucket the same key differently, so the ANY-distribution downgrade is
     * what forces the exchange that makes the fold correct. Without it a key inserted at an
     * old-generation version and deleted at a new-generation version folds per instance and yields
     * a spurious INSERT plus a spurious DELETE, with no error to notice.
     */
    @Test
    public void testNetChangeAcrossReshardOnPkShuffles() throws Exception {
        String name = "ch_rsnpk_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int, v int) PRIMARY KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsnpk_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsnpk_head");
        // Base above the initial version so this is a real multi-version diff rather than the
        // head-only FULL_SCAN shortcut, which needs no fold and so would not exercise the shuffle.
        bumpVisibleVersion(table, 3L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        installNewGeneration(table, 4, 6L);
        bumpVisibleVersion(table, 9L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        boolean savedNetChange = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;

            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            assertFalse(analyticNodes.isEmpty(), "crossing PK net change must fold:\n" + plan);

            List<ExchangeNode> exchanges = new ArrayList<>();
            analyticNodes.get(0).collect(ExchangeNode.class, exchanges);
            assertFalse(exchanges.isEmpty(),
                    "the fold must be fed by an exchange, or a key split across generations is "
                            + "netted per fragment instance:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(savedNetChange);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * Range distribution -- what a tablet reshard actually splits -- across a reshard: the split
     * halves the parent's key space, so pruning by the distribution column must run against each
     * generation's own tablet ranges. The pre-reshard generation's single all-range tablet always
     * matches; the post-reshard generation keeps only the half covering the value.
     */
    @Test
    public void testChangesAcrossReshardPrunesRangeDistribution() throws Exception {
        boolean savedRangeDistribution = Config.enable_range_distribution;
        try {
            Config.enable_range_distribution = true;

            String name = "ch_rsr_" + COUNTER.getAndIncrement();
            long tableId = createTable("CREATE TABLE " + name + " (k int, v int) ORDER BY (k) "
                    + "PROPERTIES ('replication_num'='1');");
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getDb(dbId).getTable(tableId);
            table.maySetDatabaseId(dbId);

            BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
            BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsr_base");
            BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsr_head");
            bumpVisibleVersion(table, 3L);
            Bookmark base = bm.create(dbId, tableId, hBase);
            List<Long> gen0Tablets = currentGenerationTabletIds(table);

            bumpVisibleVersion(table, 5L);
            Type keyType = table.getColumn("k").getType();
            List<Long> gen1Tablets = installNewGeneration(table, 6L,
                    List.of(keyRange(keyType, null, "100"), keyRange(keyType, "100", null)));
            bumpVisibleVersion(table, 9L);
            Bookmark head = bm.create(dbId, tableId, hHead);

            try {
                String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_] WHERE k = 7",
                        name, base.getBookmarkId(), head.getBookmarkId());
                Map<Long, TChangeScanSpec> specByTablet = scanSpecsByTablet(changesScanOf(sql));

                assertEquals(1, gen0Tablets.size(), "a fresh range-distributed partition has one tablet");
                assertTrue(specByTablet.containsKey(gen0Tablets.get(0)),
                        "the pre-reshard all-range tablet must survive pruning: " + specByTablet.keySet());
                assertEquals(1, gen1Tablets.stream().filter(specByTablet::containsKey).count(),
                        "only the post-reshard range covering k = 7 should survive: " + specByTablet.keySet());
                assertEpochSpecs(specByTablet, gen0Tablets, 3L, 5L);
                assertEpochSpecs(specByTablet, gen1Tablets, 6L, 9L);
            } finally {
                bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
                bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
            }
        } finally {
            Config.enable_range_distribution = savedRangeDistribution;
        }
    }

    /**
     * A delta mixing a resharded partition with a plainly changed one: the crossing partition emits
     * one range per generation, while the plain partition keeps the single-spec whole-range path and
     * its bucket numbering -- which the crossing partition's tablets stay out of.
     */
    @Test
    public void testChangesMixedReshardedAndPlainPartitions() throws Exception {
        String name = "ch_rsm_" + COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name
                + " (k int NOT NULL, city varchar(16) NOT NULL, v int) DUPLICATE KEY(k, city) "
                + "PARTITION BY LIST(city) ("
                + "PARTITION p_bj VALUES IN ('bj'), PARTITION p_sh VALUES IN ('sh')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num'='1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("rsm_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("rsm_head");
        bumpVisibleVersion(table, 3L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        PhysicalPartition reshardedPartition = table.getPartition("p_bj").getDefaultPhysicalPartition();
        List<Long> reshardedGen0 = currentGenerationTabletIds(reshardedPartition);
        List<Long> plainTablets = currentGenerationTabletIds(
                table.getPartition("p_sh").getDefaultPhysicalPartition());

        // Only p_bj reshards; p_sh just takes writes, so its change stays DATA_CHANGED.
        bumpVisibleVersion(table, 5L);
        List<Long> reshardedGen1 = installNewGeneration(
                table, reshardedPartition, 6L, Collections.nCopies(8, null));
        bumpVisibleVersion(table, 9L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format("SELECT k, city, v FROM %s [_CHANGES_%d_%d_]",
                    name, base.getBookmarkId(), head.getBookmarkId());
            ChangesScanNode scan = changesScanOf(sql);
            Map<Long, TChangeScanSpec> specByTablet = scanSpecsByTablet(scan);

            assertEquals(reshardedGen0.size() + reshardedGen1.size() + plainTablets.size(),
                    specByTablet.size(),
                    "both partitions must contribute scan ranges: " + specByTablet.keySet());
            assertEpochSpecs(specByTablet, reshardedGen0, 3L, 5L);
            assertEpochSpecs(specByTablet, reshardedGen1, 6L, 9L);
            // The plain partition diffs the whole bookmark range in a single spec.
            assertEpochSpecs(specByTablet, plainTablets, 3L, 9L);

            assertEquals(Set.copyOf(plainTablets), bucketNumberedTablets(scan),
                    "only the plainly changed partition should be bucket-numbered");
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * Simulates a tablet reshard on {@code table}'s only physical partition: installs a new
     * base-index generation (fresh index id, same meta id) carrying {@code tabletCount} tablets and
     * stamped with the reshard lineage, while leaving the old generation installed -- the parked
     * state a reshard leaves until the recycle bin erases the predecessor. Returns the new
     * generation's tablet ids.
     */
    private List<Long> installNewGeneration(OlapTable table, int tabletCount, long takeoverVersion)
            throws Exception {
        return installNewGeneration(table, takeoverVersion, Collections.nCopies(tabletCount, null));
    }

    /**
     * {@link #installNewGeneration(OlapTable, int, long)} with one tablet per entry of
     * {@code tabletRanges}; a null entry leaves the tablet without a range, as a hash-distributed
     * table's tablets are.
     */
    private List<Long> installNewGeneration(OlapTable table, long takeoverVersion,
                                            List<TabletRange> tabletRanges) throws Exception {
        return installNewGeneration(table, onlyPhysicalPartition(table), takeoverVersion, tabletRanges);
    }

    /** {@link #installNewGeneration(OlapTable, long, List)} on one physical partition of {@code table}. */
    private List<Long> installNewGeneration(OlapTable table, PhysicalPartition pp, long takeoverVersion,
                                            List<TabletRange> tabletRanges) throws Exception {
        MaterializedIndex oldBase = pp.getLatestBaseIndex();
        MaterializedIndex newBase = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                oldBase.getMetaId(), MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        // Real StarOS shards: the scan resolves every tablet's queryable replicas through the
        // shard-to-worker mapping, which a fabricated tablet id would not have. The inverted index
        // is left alone (nothing on the planning path reads it for a CHANGES scan).
        List<Long> shardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                tabletRanges.size(), table.getPartitionFilePathInfo(pp.getId()),
                table.getPartitionFileCacheInfo(pp.getId()), pp.getShardGroupId(),
                null, Map.of(), WarehouseManager.DEFAULT_RESOURCE);
        for (int i = 0; i < shardIds.size(); i++) {
            Tablet tablet = new LakeTablet(shardIds.get(i));
            if (tabletRanges.get(i) != null) {
                tablet.setRange(tabletRanges.get(i));
            }
            newBase.addTablet(tablet, null, false);
        }
        newBase.setTakeoverVersion(takeoverVersion);
        newBase.setPredecessorIndexId(oldBase.getId());
        pp.addMaterializedIndex(newBase, true);
        return currentGenerationTabletIds(pp);
    }

    /** The tablet range [{@code lower}, {@code upper}) on one distribution column; null is unbounded. */
    private static TabletRange keyRange(Type keyType, String lower, String upper) {
        Tuple lowerBound = lower == null ? null : new Tuple(Lists.newArrayList(Variant.of(keyType, lower)));
        Tuple upperBound = upper == null ? null : new Tuple(Lists.newArrayList(Variant.of(keyType, upper)));
        return new TabletRange(Range.of(lowerBound, upperBound, lower != null, false));
    }

    private static PhysicalPartition onlyPhysicalPartition(OlapTable table) {
        return table.getPartitions().iterator().next().getDefaultPhysicalPartition();
    }

    /** Tablet ids of the current base-index generation of {@code table}'s only physical partition. */
    private static List<Long> currentGenerationTabletIds(OlapTable table) {
        return currentGenerationTabletIds(onlyPhysicalPartition(table));
    }

    /** Tablet ids of {@code pp}'s current base-index generation. */
    private static List<Long> currentGenerationTabletIds(PhysicalPartition pp) {
        return pp.getLatestBaseIndex().getTablets().stream().map(Tablet::getId).toList();
    }

    /** The ChangesScanNode of {@code sql}'s plan. */
    private ChangesScanNode changesScanOf(String sql) throws Exception {
        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        for (ScanNode node : planPair.second.getScanNodes()) {
            if (node instanceof ChangesScanNode changesScanNode) {
                return changesScanNode;
            }
        }
        throw new AssertionError("ExecPlan should contain a ChangesScanNode:\n" + planPair.first);
    }

    /** Scan spec per scanned tablet id; a tablet must not be scanned by two ranges. */
    private static Map<Long, TChangeScanSpec> scanSpecsByTablet(ChangesScanNode scan) {
        Map<Long, TChangeScanSpec> specByTablet = new HashMap<>();
        for (TScanRangeLocations locations : scan.getScanRangeLocations(0)) {
            TChangesScanRange range = locations.getScan_range().getChanges_scan_range();
            assertNull(specByTablet.put(range.getTablet_id(), range.getScan_spec()),
                    "tablet " + range.getTablet_id() + " should be scanned by one range only");
            assertFalse(locations.getLocations().isEmpty(),
                    "tablet " + range.getTablet_id() + " should resolve to a location");
        }
        return specByTablet;
    }

    /** Tablet ids that got a bucket sequence, i.e. that entered the colocation dispatch map. */
    private static Set<Long> bucketNumberedTablets(ChangesScanNode scan) {
        return scan.getBucketSeqToLocations().values().stream()
                .map(locations -> locations.getScan_range().getChanges_scan_range().getTablet_id())
                .collect(Collectors.toSet());
    }

    /** Every scanned tablet of {@code generationTablets} must diff exactly {@code (base, head]}. */
    private static void assertEpochSpecs(Map<Long, TChangeScanSpec> specByTablet,
                                        List<Long> generationTablets, long baseVersion, long headVersion) {
        for (Long tabletId : generationTablets) {
            TChangeScanSpec spec = specByTablet.get(tabletId);
            if (spec == null) {
                continue;
            }
            assertEquals(TChangeDerivationMode.VERSION_CHAIN_DIFF, spec.getDerivation_mode(),
                    "tablet " + tabletId + " should diff its own generation's version chain");
            assertEquals(baseVersion, spec.getBase_version(), "base version of tablet " + tabletId);
            assertEquals(headVersion, spec.getHead_version(), "head version of tablet " + tabletId);
        }
    }
}
