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

package com.starrocks.planner;

import com.google.common.collect.ArrayListMultimap;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coordinator-side scan-range dispatch tests for range-distribution
 * colocate tables. Exercises the observable behavior of
 * {@link OlapScanNode#getBucketNums()} and the optimizer-built
 * {@link com.starrocks.sql.plan.PlanFragmentBuilder} path against real
 * shared-data tables.
 *
 * <p>Lower-level tests for the {@link RangeColocateScanDispatch} facade
 * (forTable / bucketCount / requireAligned / computeBucketSeq) live in
 * {@link RangeColocateScanDispatchTest}.
 */
public class RangeColocateScanRangeDispatchTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("p4_dispatch_test").useDatabase("p4_dispatch_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("p4_dispatch_test");
    }

    private static Tuple makeTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    private static OlapScanNode newOlapScanNode(OlapTable table, int planNodeIdSeq) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(planNodeIdSeq));
        desc.setTable(table);
        return new OlapScanNode(new PlanNodeId(planNodeIdSeq), desc,
                "OlapScanNode", table.getBaseIndexMetaId());
    }

    // ---- OlapScanNode.getBucketNums against a real range colocate table ----

    @Test
    public void testGetBucketNumsSingleRange() throws Exception {
        starRocksAssert.withTable(
                "create table t_dispatch_single (k1 int, k2 int, v1 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_dispatch_single:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_dispatch_single");
        Assertions.assertInstanceOf(RangeDistributionInfo.class, table.getDefaultDistributionInfo());

        OlapScanNode scanNode = newOlapScanNode(table, 1);
        scanNode.setSelectedPartitionIds(new ArrayList<>(table.getAllPartitionIds()));

        // In production the bucketSeq fill runs before getBucketNums() (getScanRangeLocations precedes
        // backend selection); mirror that here by populating the aligned assignment the fill would build.
        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        Assertions.assertNotNull(dispatch);
        var physicalPartition = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        scanNode.setTabletId2BucketSeq(new HashMap<>(
                dispatch.computeBucketSeq(physicalPartition.getLatestIndex(table.getBaseIndexMetaId()))));

        // Initial state: ColocateRangeMgr seeded with [MIN, MAX) -> 1 PACK shard group,
        // and createRangeColocateLakeTablets created exactly 1 tablet per partition.
        Assertions.assertEquals(1, scanNode.getBucketNums());
    }

    @Test
    public void testGetBucketNumsForNonColocateRangeTable() throws Exception {
        starRocksAssert.withTable(
                "create table t_dispatch_nonloc (k1 int, k2 int, v1 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_dispatch_nonloc");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getColocateTableIndex()
                .isColocateTable(table.getId()));

        OlapScanNode scanNode = newOlapScanNode(table, 3);
        scanNode.setSelectedPartitionIds(new ArrayList<>(table.getAllPartitionIds()));

        // Range-distribution non-colocate falls through and returns
        // RangeDistributionInfo.getBucketNum() == 1.
        Assertions.assertEquals(1, scanNode.getBucketNums());
    }

    @Test
    public void testGetBucketNumsThrowsWhenColocateGroupIsUnaligned() throws Exception {
        starRocksAssert.withTable(
                "create table t_dispatch_throw (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_dispatch_throw:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_dispatch_throw");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long groupId = colocateTableIndex.getGroup(table.getId()).grpId;

        colocateTableIndex.getColocateRangeMgr().setColocateRanges(groupId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9002L),
                new ColocateRange(Range.ge(makeTuple(200)), 9003L)));

        OlapScanNode scanNode = newOlapScanNode(table, 9);
        scanNode.setSelectedPartitionIds(new ArrayList<>(table.getAllPartitionIds()));

        // getBucketNums() is invoked from ExecutionFragment.getOrCreateColocatedAssignment,
        // which BackendSelectorFactory only calls on the colocate-dispatch path. The
        // throw therefore fires only there, not on single-table reads.
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                scanNode::getBucketNums);
        Assertions.assertTrue(exception.getMessage().contains("unaligned state"),
                "actual: " + exception.getMessage());
    }

    // ---- PlanFragmentBuilder end-to-end + availability regression ----

    @Test
    public void testPlanFragmentBuilderAlignsBucketSeqToColocateRange() throws Exception {
        starRocksAssert.withTable(
                "create table t_e2e_left (k1 int, k2 int, v1 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_e2e:k1');");
        starRocksAssert.withTable(
                "create table t_e2e_right (k1 int, k2 int, v2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_e2e:k1');");

        // Drive the real PlanFragmentBuilder path. The query is unfiltered so every
        // ColocateRange should appear as a key in bucketSeq2locations.
        com.starrocks.sql.plan.ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext,
                "select l.v1, r.v2 from t_e2e_left l "
                        + "join t_e2e_right r on l.k1 = r.k1 and l.k2 = r.k2").second;

        List<OlapScanNode> olapScans = new ArrayList<>();
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            if (scanNode instanceof OlapScanNode) {
                olapScans.add((OlapScanNode) scanNode);
            }
        }
        Assertions.assertEquals(2, olapScans.size(), "expected two OlapScanNodes for the join");

        for (OlapScanNode scan : olapScans) {
            ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2Locations = scan.bucketSeq2locations;
            Assertions.assertFalse(bucketSeq2Locations.isEmpty(),
                    "scan node " + scan.getTableName() + " has no bucketSeq2locations entries");
            Assertions.assertEquals(1, bucketSeq2Locations.keySet().size(),
                    "bucketSeq2locations should have one bucket key for a single-range colocate group");
            Assertions.assertTrue(bucketSeq2Locations.containsKey(0),
                    "bucketSeq 0 missing from " + bucketSeq2Locations.keySet());
        }
    }

    @Test
    public void testSingleTableSelectDuringUnalignedStatePlansSuccessfully() throws Exception {
        starRocksAssert.withTable(
                "create table t_avail (k1 int, k2 int, v1 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_avail:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_avail");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long groupId = colocateTableIndex.getGroup(table.getId()).grpId;

        // 3-range ColocateRangeMgr without aligning the underlying tablet — single-table
        // SELECT must still plan because it does not need colocate dispatch
        // (NormalBackendSelector is chosen, getBucketNums() is not invoked).
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(groupId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9002L),
                new ColocateRange(Range.ge(makeTuple(200)), 9003L)));

        Assertions.assertDoesNotThrow(() -> UtFrameUtils.getPlanAndFragment(connectContext,
                "select v1 from t_avail"));
    }

    @Test
    public void testGetBucketNumsFailsClosedOnStaleAssignment() throws Exception {
        // Even when the group is currently ALIGNED (a fresh recompute returns a bucket count), a scan
        // whose built bucketSeq does not match the aligned mapping must make getBucketNums() fail closed.
        // This closes the fill-vs-dispatch TOCTOU that let a position-based assignment (built while the
        // group was momentarily unaligned) reach a colocate join and silently return wrong results.
        starRocksAssert.withTable(
                "create table t_dispatch_flag (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_dispatch_flag:k1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_dispatch_flag");
        OlapScanNode scanNode = newOlapScanNode(table, 11);
        scanNode.setSelectedPartitionIds(new ArrayList<>(table.getAllPartitionIds()));

        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        Assertions.assertNotNull(dispatch);
        var physicalPartition = table.getPartitions().iterator().next().getDefaultPhysicalPartition();
        Map<Long, Integer> aligned =
                dispatch.computeBucketSeq(physicalPartition.getLatestIndex(table.getBaseIndexMetaId()));
        Assertions.assertNotNull(aligned, "single default range should be aligned");

        // Built assignment matches the aligned mapping -> getBucketNums returns the bucket count.
        scanNode.setTabletId2BucketSeq(new HashMap<>(aligned));
        Assertions.assertEquals(1, scanNode.getBucketNums());

        // Stale assignment (bucketSeq perturbed, e.g. a position fallback) -> fail closed.
        Map<Long, Integer> stale = new HashMap<>();
        aligned.forEach((tabletId, seq) -> stale.put(tabletId, seq + 1));
        scanNode.setTabletId2BucketSeq(stale);
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                scanNode::getBucketNums);
        Assertions.assertTrue(exception.getMessage().contains("stale bucket assignment"),
                "actual: " + exception.getMessage());
    }

    @Test
    public void testPlanFragmentBuilderUnalignedScanFailsClosed() throws Exception {
        // When computeBucketSeq returns null, the optimizer/PlanFragmentBuilder bucketSeq fill falls back
        // to position-based bucketSeq (so a non-colocate scan still works); the later getBucketNums()
        // colocate-dispatch guard must fail closed rather than pairing by that position fallback.
        starRocksAssert.withTable(
                "create table t_pfb_unaligned (k1 int, k2 int, v1 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_pfb_unaligned:k1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_pfb_unaligned");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateTableIndex.getGroup(table.getId()).grpId;
        // 3 ColocateRanges but the single tablet still spans [MIN, MAX) -> computeBucketSeq == null.
        // The group is left stable so PlanFragmentBuilder's colocate fill path (not shuffle) runs.
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(grpId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9101L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9102L),
                new ColocateRange(Range.ge(makeTuple(200)), 9103L)));

        com.starrocks.sql.plan.ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext,
                "select k1, k2 from t_pfb_unaligned").second;
        OlapScanNode scan = null;
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            if (scanNode instanceof OlapScanNode && scanNode.getTableName().contains("t_pfb_unaligned")) {
                scan = (OlapScanNode) scanNode;
            }
        }
        Assertions.assertNotNull(scan, "expected an OlapScanNode for t_pfb_unaligned");
        scan.setSelectedPartitionIds(new ArrayList<>(table.getAllPartitionIds()));
        // The single tablet spans all 3 ColocateRanges, so the group is unaligned: getBucketNums() on
        // the colocate-dispatch path must fail closed rather than pairing by the position fallback.
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                scan::getBucketNums);
        Assertions.assertTrue(exception.getMessage().contains("unaligned state"),
                "actual: " + exception.getMessage());
    }
}
