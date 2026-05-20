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

import java.util.concurrent.atomic.AtomicInteger;

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

    private static void bumpVisibleVersion(OlapTable t, long newVersion) {
        for (Partition p : t.getPartitions()) {
            for (PhysicalPartition pp : p.getSubPartitions()) {
                pp.setVisibleVersion(newVersion, System.currentTimeMillis());
            }
        }
    }
}
