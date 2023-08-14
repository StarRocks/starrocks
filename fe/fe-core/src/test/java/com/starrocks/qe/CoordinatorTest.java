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

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.planner.BinlogScanNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.stream.StreamAggNode;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TBinlogOffset;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CoordinatorTest extends PlanTestBase {
    ConnectContext ctx;
    DefaultCoordinator coordinator;
    CoordinatorPreprocessor coordinatorPreprocessor;

    @Before
    public void setUp() throws IOException {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setExecutionId(new TUniqueId(0xdeadbeef, 0xdeadbeef));
        ConnectContext.threadLocalInfo.set(ctx);

        coordinator = new DefaultCoordinator.Factory().createQueryScheduler(ctx, Lists.newArrayList(), Lists.newArrayList(),
                new TDescriptorTable());
        coordinatorPreprocessor = coordinator.getPrepareInfo();
    }

    private PlanFragment genFragment() {
        ArrayList<TupleId> tupleIdArrayList = new ArrayList<>();
        tupleIdArrayList.add(new TupleId(1));
        PlanFragment fragment =
                new PlanFragment(new PlanFragmentId(1), new EmptySetNode(new PlanNodeId(1), tupleIdArrayList),
                        new DataPartition(TPartitionType.RANDOM));
        return fragment;
    }

    private void testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode mode) throws IOException {
        PlanFragment fragment = genFragment();
        ExecutionFragment execFragment = new ExecutionFragment(null, fragment, 0);
        FragmentInstance instance0 = new FragmentInstance(null, execFragment);
        FragmentInstance instance1 = new FragmentInstance(null, execFragment);
        FragmentInstance instance2 = new FragmentInstance(null, execFragment);
        instance0.addBucketSeq(2);
        instance0.addBucketSeq(0);
        instance1.addBucketSeq(1);
        instance1.addBucketSeq(4);
        instance2.addBucketSeq(3);
        instance2.addBucketSeq(5);

        execFragment.addInstance(instance0);
        execFragment.addInstance(instance1);
        execFragment.addInstance(instance2);

        OlapTable table = new OlapTable();
        table.setDefaultDistributionInfo(new HashDistributionInfo(6, Collections.emptyList()));
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        OlapScanNode scanNode = new OlapScanNode(new PlanNodeId(0), desc, "test-scan-node");
        scanNode.setSelectedPartitionIds(ImmutableList.of(0L, 1L));
        execFragment.getOrCreateColocatedAssignment(scanNode);

        RuntimeFilterDescription rf = new RuntimeFilterDescription(ctx.sessionVariable);
        rf.setJoinMode(mode);
        fragment.getBuildRuntimeFilters().put(1, rf);
        Assert.assertTrue(rf.getBucketSeqToInstance() == null || rf.getBucketSeqToInstance().isEmpty());
        execFragment.setBucketSeqToInstanceForRuntimeFilters();
        Assert.assertEquals(Arrays.asList(0, 1, 0, 2, 1, 2), rf.getBucketSeqToInstance());
    }

    @Test
    public void testColocateRuntimeFilter() throws IOException {
        testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode.COLOCATE);
    }

    @Test
    public void testBucketShuffleRuntimeFilter() throws IOException {
        testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode.LOCAL_HASH_BUCKET);
    }

    @Test
    public void testBinlogScan() throws Exception {
        PlanFragmentId fragmentId = new PlanFragmentId(0);
        PlanNodeId planNodeId = new PlanNodeId(1);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(2));

        OlapTable olapTable = getOlapTable("t0");
        List<Long> olapTableTabletIds =
                olapTable.getAllPartitions().stream().flatMap(x -> x.getBaseIndex().getTabletIdsInOrder().stream())
                        .collect(Collectors.toList());
        Assert.assertFalse(olapTableTabletIds.isEmpty());
        tupleDesc.setTable(olapTable);

        new MockUp<BinlogScanNode>() {

            @Mock
            TBinlogOffset getBinlogOffset(long tabletId) {
                TBinlogOffset offset = new TBinlogOffset();
                offset.setTablet_id(1);
                offset.setLsn(2);
                offset.setVersion(3);
                return offset;
            }
        };

        BinlogScanNode binlogScan = new BinlogScanNode(planNodeId, tupleDesc);
        binlogScan.setFragmentId(fragmentId);
        binlogScan.finalizeStats(null);

        List<ScanNode> scanNodes = Arrays.asList(binlogScan);
        CoordinatorPreprocessor prepare = new CoordinatorPreprocessor(Lists.newArrayList(), scanNodes);
        prepare.computeScanRangeAssignment();

        FragmentScanRangeAssignment scanRangeMap =
                prepare.getFragmentScanRangeAssignment(fragmentId);
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackends().get(0);
        Assert.assertFalse(scanRangeMap.isEmpty());
        Long expectedWorkerId = backend.getId();
        Assert.assertTrue(scanRangeMap.containsKey(expectedWorkerId));
        Map<Integer, List<TScanRangeParams>> rangesPerNode = scanRangeMap.get(expectedWorkerId);
        Assert.assertTrue(rangesPerNode.containsKey(planNodeId.asInt()));
        List<TScanRangeParams> ranges = rangesPerNode.get(planNodeId.asInt());
        List<Long> tabletIds =
                ranges.stream().map(x -> x.getScan_range().getBinlog_scan_range().getTablet_id())
                        .collect(Collectors.toList());
        Assert.assertEquals(olapTableTabletIds, tabletIds);
    }

    @Test
    public void testStreamAgg() throws Exception {
        new MockUp<BinlogScanNode>() {

            @Mock
            TBinlogOffset getBinlogOffset(long tabletId) {
                TBinlogOffset offset = new TBinlogOffset();
                offset.setTablet_id(1);
                offset.setLsn(2);
                offset.setVersion(3);
                return offset;
            }
        };

        PlanFragmentId fragmentId = new PlanFragmentId(0);
        TupleDescriptor scanTuple = new TupleDescriptor(new TupleId(2));
        scanTuple.setTable(getOlapTable("t0"));
        TupleDescriptor aggTuple = new TupleDescriptor(new TupleId(3));
        SlotDescriptor groupBySlot = new SlotDescriptor(new SlotId(4), "groupBy", Type.INT, false);
        SlotDescriptor aggFuncSlot = new SlotDescriptor(new SlotId(5), "aggFunc", Type.INT, false);
        aggTuple.addSlot(groupBySlot);
        aggTuple.addSlot(aggFuncSlot);

        // Build scan node
        List<PlanFragment> fragments = new ArrayList<>();
        BinlogScanNode binlogScan = new BinlogScanNode(new PlanNodeId(1), scanTuple);
        binlogScan.setFragmentId(fragmentId);
        binlogScan.finalizeStats(null);
        List<ScanNode> scanNodes = Arrays.asList(binlogScan);

        // Build agg node
        AggregateInfo aggInfo = new AggregateInfo(new ArrayList<>(), new ArrayList<>(), AggregateInfo.AggPhase.SECOND);
        aggInfo.setOutputTupleDesc(aggTuple);
        StreamAggNode aggNode = new StreamAggNode(new PlanNodeId(2), binlogScan, aggInfo);

        // Build fragment
        PlanFragment fragment = new PlanFragment(fragmentId, aggNode, DataPartition.RANDOM);
        fragments.add(fragment);

        // Build topology
        CoordinatorPreprocessor prepare = new CoordinatorPreprocessor(fragments, scanNodes);
        prepare.computeScanRangeAssignment();
        prepare.computeFragmentExecParams();

        // Assert
        Map<PlanFragmentId, ExecutionFragment> fragmentParams = prepare.getExecutionDAG().getIdToFragment();
        fragmentParams.forEach((k, v) -> {
            System.err.println("Fragment " + k + " : " + v);
        });
        Assert.assertTrue(fragmentParams.containsKey(fragmentId));
        ExecutionFragment fragmentParam = fragmentParams.get(fragmentId);
        FragmentScanRangeAssignment scanRangeAssignment = fragmentParam.getScanRangeAssignment();
        List<FragmentInstance> instances = fragmentParam.getInstances();
        Assert.assertFalse(fragmentParams.isEmpty());
        Assert.assertEquals(1, scanRangeAssignment.size());
        Assert.assertEquals(1, instances.size());

    }
}
