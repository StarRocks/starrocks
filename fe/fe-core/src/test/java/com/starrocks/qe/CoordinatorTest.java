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
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.planner.BinlogScanNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.stream.StreamAggNode;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CoordinatorTest extends PlanTestBase {
    ConnectContext ctx;
    Coordinator coordinator;
    CoordinatorPreprocessor coordinatorPreprocessor;

    @Before
    public void setUp() throws IOException {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setExecutionId(new TUniqueId(0xdeadbeef, 0xdeadbeef));
        ConnectContext.threadLocalInfo.set(ctx);

        coordinator = new Coordinator(ctx, Lists.newArrayList(), Lists.newArrayList(), new TDescriptorTable());
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
        CoordinatorPreprocessor.FragmentExecParams params = coordinatorPreprocessor.new FragmentExecParams(fragment);
        CoordinatorPreprocessor.FInstanceExecParam instance0 =
                new CoordinatorPreprocessor.FInstanceExecParam(null, null, params);
        CoordinatorPreprocessor.FInstanceExecParam instance1 =
                new CoordinatorPreprocessor.FInstanceExecParam(null, null, params);
        CoordinatorPreprocessor.FInstanceExecParam instance2 =
                new CoordinatorPreprocessor.FInstanceExecParam(null, null, params);
        instance0.bucketSeqToDriverSeq = ImmutableMap.of(2, -1, 0, -1);
        instance1.bucketSeqToDriverSeq = ImmutableMap.of(1, -1, 4, -1);
        instance2.bucketSeqToDriverSeq = ImmutableMap.of(3, -1, 5, -1);
        params.instanceExecParams.add(instance0);
        params.instanceExecParams.add(instance1);
        params.instanceExecParams.add(instance2);
        RuntimeFilterDescription rf = new RuntimeFilterDescription(ctx.sessionVariable);
        rf.setJoinMode(mode);
        fragment.getBuildRuntimeFilters().put(1, rf);
        Assert.assertTrue(rf.getBucketSeqToInstance() == null || rf.getBucketSeqToInstance().isEmpty());
        coordinatorPreprocessor.computeBucketSeq2InstanceOrdinal(params, 6);
        params.setBucketSeqToInstanceForRuntimeFilters();
        Assert.assertEquals(rf.getBucketSeqToInstance(), Arrays.<Integer>asList(0, 1, 0, 2, 1, 2));
    }

    @Test
    public void testColocateRuntimeFilter() throws IOException {
        testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode.COLOCATE);
    }

    @Test
    public void testBucketShuffleRuntimeFilter() throws IOException {
        testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode.LOCAL_HASH_BUCKET);
    }

    private Map<Integer, List<TScanRangeParams>> createScanId2scanRanges(int scanId, int numScanRanges) {
        List<TScanRangeParams> scanRanges = Lists.newArrayList();
        for (int i = 0; i < numScanRanges; ++i) {
            scanRanges.add(new TScanRangeParams());
        }

        return ImmutableMap.of(scanId, scanRanges);
    }

    private void testComputeColocatedJoinInstanceParamHelper(int scanId,
                                                             Map<Integer, Long> bucketSeqToWorkerId,
                                                             int parallelExecInstanceNum,
                                                             int pipelineDop, boolean enablePipeline,
                                                             int expectedInstances,
                                                             List<Long> expectedParamAddresses,
                                                             List<Integer> expectedPipelineDops,
                                                             List<Map<Integer, Integer>> expectedBucketSeqToDriverSeqs,
                                                             List<Integer> expectedNumScanRangesList,
                                                             List<Map<Integer, Integer>> expectedDriverSeq2NumScanRangesList) {
        CoordinatorPreprocessor.FragmentExecParams params =
                coordinatorPreprocessor.new FragmentExecParams(genFragment());
        CoordinatorPreprocessor.BucketSeqToScanRange bucketSeqToScanRange =
                new CoordinatorPreprocessor.BucketSeqToScanRange();
        for (Integer bucketSeq : bucketSeqToWorkerId.keySet()) {
            bucketSeqToScanRange.put(bucketSeq, createScanId2scanRanges(scanId, 1));
        }

        coordinatorPreprocessor.computeColocatedJoinInstanceParam(bucketSeqToWorkerId, bucketSeqToScanRange,
                parallelExecInstanceNum, pipelineDop, enablePipeline, params);
        params.instanceExecParams.sort(Comparator.comparing(CoordinatorPreprocessor.FInstanceExecParam::getWorkerId));

        Assert.assertEquals(expectedInstances, params.instanceExecParams.size());
        for (int i = 0; i < expectedInstances; ++i) {
            CoordinatorPreprocessor.FInstanceExecParam param = params.instanceExecParams.get(i);
            Assert.assertEquals(expectedParamAddresses.get(i), param.getWorkerId());
            Assert.assertEquals(expectedPipelineDops.get(i).intValue(), param.getPipelineDop());

            Map<Integer, Integer> bucketSeqToDriverSeq = param.getBucketSeqToDriverSeq();
            Map<Integer, Integer> expectedBucketSeqToDriverSeq = expectedBucketSeqToDriverSeqs.get(i);

            Assert.assertEquals(expectedBucketSeqToDriverSeq.size(), bucketSeqToDriverSeq.size());
            expectedBucketSeqToDriverSeq.forEach((expectedBucketSeq, expectedDriverSeq) -> {
                Assert.assertEquals(expectedDriverSeq, bucketSeqToDriverSeq.get(expectedBucketSeq));
            });

            if (enablePipeline && expectedPipelineDops.get(i) != -1) {
                Assert.assertTrue(param.getPerNodeScanRanges().isEmpty());

                Map<Integer, Integer> expectedDriverSeq2NumScanRanges = expectedDriverSeq2NumScanRangesList.get(i);
                Map<Integer, List<TScanRangeParams>> perDriverSeqScanRanges =
                        param.getNodeToPerDriverSeqScanRanges().get(scanId);
                Assert.assertEquals(expectedDriverSeq2NumScanRanges.size(), perDriverSeqScanRanges.size());
                expectedDriverSeq2NumScanRanges.forEach((expectedDriverSeq, expectedNumScanRanges) -> {
                    Assert.assertEquals(expectedNumScanRanges.intValue(),
                            perDriverSeqScanRanges.get(expectedDriverSeq).size());
                });

            } else {
                Assert.assertTrue(param.getNodeToPerDriverSeqScanRanges().isEmpty());
                Assert.assertEquals(expectedNumScanRangesList.get(i).intValue(),
                        param.getPerNodeScanRanges().get(scanId).size());
            }
        }
    }

    @Test
    public void testComputeColocatedJoinInstanceParam() {
        int scanId = 0;

        // Bucket distribution:
        // - addr1: 11, 12, 13, 14, 15.
        // - addr2: 21, 22, 23.
        // - addr3: 31, 32
        Long addr1 = 1L;
        Long addr2 = 2L;
        Long addr3 = 3L;
        Map<Integer, Long> bucketSeqToWorkerId = ImmutableMap.<Integer, Long>builder()
                .put(11, addr1).put(12, addr1).put(13, addr1).put(14, addr1).put(15, addr1)
                .put(21, addr2).put(22, addr2).put(23, addr2)
                .put(32, addr3).put(31, addr3)
                .build();

        // Test case 1: numInstance=1, pipelineDop=3, enablePipeline.
        // - addr1
        //      - Instance#0
        //          - DriverSequence#0: 11, 14.
        //          - DriverSequence#1: 12, 15.
        //          - DriverSequence#2: 13.
        // - addr2
        //      - Instance#0
        //          - DriverSequence#0: 21.
        //          - DriverSequence#1: 22.
        //          - DriverSequence#2: 23.
        // - addr3
        //      - Instance#0
        //          - DriverSequence#0: 32.
        //          - DriverSequence#1: 31.
        int expectedInstances = 3;
        List<Long> expectedParamAddresses = ImmutableList.of(addr1, addr2, addr3);
        List<Map<Integer, Integer>> expectedBucketSeqToDriverSeqs = ImmutableList.of(
                ImmutableMap.of(11, 0, 12, 1, 13, 2, 14, 0, 15, 1),
                ImmutableMap.of(21, 0, 22, 1, 23, 2),
                ImmutableMap.of(32, 0, 31, 1)
        );
        List<Integer> expectedPipelineDops = ImmutableList.of(3, 3, 2);
        List<Integer> expectedNumScanRangesList = null;
        List<Map<Integer, Integer>> expectedDriverSeq2NumScanRangesList = ImmutableList.of(
                ImmutableMap.of(0, 2, 1, 2, 2, 1),
                ImmutableMap.of(0, 1, 1, 1, 2, 1),
                ImmutableMap.of(0, 1, 1, 1)
        );
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToWorkerId, 1, 3, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

        // Test case 2: numInstance=3, pipelineDop=2, enablePipeline.
        // - addr1
        //      - Instance#0
        //          - DriverSequence#0: 11.
        //          - DriverSequence#1: 14.
        //      - Instance#1
        //          - DriverSequence#0: 12.
        //          - DriverSequence#1: 15.
        //      - Instance#2
        //          - DriverSequence#0: 13.
        // - addr2
        //      - Instance#0
        //          - DriverSequence#0: 21.
        //      - Instance#1
        //          - DriverSequence#0: 22.
        //      - Instance#2
        //          - DriverSequence#0: 23.
        // - addr3
        //      - Instance#0
        //          - DriverSequence#0: 32.
        //      - Instance#1
        //          - DriverSequence#0: 31.
        expectedInstances = 8;
        expectedParamAddresses = ImmutableList.of(addr1, addr1, addr1, addr2, addr2, addr2, addr3, addr3);
        expectedPipelineDops = ImmutableList.of(2, 2, 1, 1, 1, 1, 1, 1);
        expectedBucketSeqToDriverSeqs = ImmutableList.of(
                ImmutableMap.of(11, 0, 14, 1),
                ImmutableMap.of(12, 0, 15, 1),
                ImmutableMap.of(13, 0),
                ImmutableMap.of(21, 0),
                ImmutableMap.of(22, 0),
                ImmutableMap.of(23, 0),
                ImmutableMap.of(32, 0),
                ImmutableMap.of(31, 0)
        );
        expectedDriverSeq2NumScanRangesList = ImmutableList.of(
                ImmutableMap.of(0, 1, 1, 1),
                ImmutableMap.of(0, 1, 1, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1)
        );
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToWorkerId, 3, 2, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

        // Test case 3: numInstance=3, pipelineDop=1, enablePipeline.
        // - addr1
        //      - Instance#0
        //          - DriverSequence#0: 11, 14.
        //      - Instance#1
        //          - DriverSequence#0: 12, 15.
        //      - Instance#2
        //          - DriverSequence#0: 13.
        // - addr2
        //      - Instance#0
        //          - DriverSequence#0: 21.
        //      - Instance#1
        //          - DriverSequence#0: 22.
        //      - Instance#2
        //          - DriverSequence#0: 23.
        // - addr3
        //      - Instance#0
        //          - DriverSequence#0: 32.
        //      - Instance#1
        //          - DriverSequence#0: 31.
        expectedParamAddresses = ImmutableList.of(addr1, addr1, addr1, addr2, addr2, addr2, addr3, addr3);
        expectedPipelineDops = Collections.nCopies(expectedInstances, 1);
        expectedBucketSeqToDriverSeqs = ImmutableList.of(
                ImmutableMap.of(11, 0, 14, 0),
                ImmutableMap.of(12, 0, 15, 0),
                ImmutableMap.of(13, 0),
                ImmutableMap.of(21, 0),
                ImmutableMap.of(22, 0),
                ImmutableMap.of(23, 0),
                ImmutableMap.of(32, 0),
                ImmutableMap.of(31, 0)
        );
        expectedDriverSeq2NumScanRangesList = ImmutableList.of(
                ImmutableMap.of(0, 2),
                ImmutableMap.of(0, 2),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1),
                ImmutableMap.of(0, 1)
        );
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToWorkerId, 3, 1, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

        // Test case 4: numInstance=3, pipelineDop=1, disablePipeline.
        // - addr1
        //      - Instance#0: 11, 14.
        //      - Instance#1: 12, 15.
        //      - Instance#2: 13.
        // - addr2
        //      - Instance#0: 21.
        //      - Instance#1: 22.
        //      - Instance#2: 23.
        // - addr3
        //      - Instance#0: 32.
        //      - Instance#1: 31.
        expectedNumScanRangesList = ImmutableList.of(2, 2, 1, 1, 1, 1, 1, 1);
        expectedPipelineDops = Collections.nCopies(expectedInstances, -1);
        expectedBucketSeqToDriverSeqs = ImmutableList.of(
                ImmutableMap.of(11, -1, 14, -1),
                ImmutableMap.of(12, -1, 15, -1),
                ImmutableMap.of(13, -1),
                ImmutableMap.of(21, -1),
                ImmutableMap.of(22, -1),
                ImmutableMap.of(23, -1),
                ImmutableMap.of(32, -1),
                ImmutableMap.of(31, -1)
        );
        expectedDriverSeq2NumScanRangesList = null;
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToWorkerId, 3, 1, false,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

        // Test case 5: numInstance=1, pipelineDop=3, enablePipeline, the scan node is replicated scan.
        // - addr1
        //      - Instance#0
        //          - DriverSequence#0: 11.
        //          - DriverSequence#1: empty.
        //          - DriverSequence#2: empty.
        // - addr2
        //      - Instance#0
        //          - DriverSequence#0: 21.
        //          - DriverSequence#1: empty.
        //          - DriverSequence#2: empty.
        // - addr3
        //      - Instance#0
        //          - DriverSequence#0: 31.
        //          - DriverSequence#1: empty.
        coordinator.addReplicateScanId(scanId);
        expectedInstances = 3;
        expectedParamAddresses = ImmutableList.of(addr1, addr2, addr3);
        expectedBucketSeqToDriverSeqs = ImmutableList.of(
                ImmutableMap.of(11, 0, 12, 1, 13, 2, 14, 0, 15, 1),
                ImmutableMap.of(21, 0, 22, 1, 23, 2),
                ImmutableMap.of(32, 0, 31, 1)
        );
        expectedPipelineDops = ImmutableList.of(3, 3, 2);
        expectedNumScanRangesList = null;
        expectedDriverSeq2NumScanRangesList = ImmutableList.of(
                ImmutableMap.of(0, 1, 1, 0, 2, 0),
                ImmutableMap.of(0, 1, 1, 0, 2, 0),
                ImmutableMap.of(0, 1, 1, 0)
        );
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToWorkerId, 1, 3, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

        // Test case 6: numInstance=1, pipelineDop=3, enablePipeline, addr3 is assigned only one bucket.
        // Bucket distribution:
        // - addr1: 11, 12, 13, 14, 15.
        // - addr2: 21, 22, 23.
        // - addr3: 31
        scanId = 1;
        bucketSeqToWorkerId = ImmutableMap.<Integer, Long>builder()
                .put(11, addr1).put(12, addr1).put(13, addr1).put(14, addr1).put(15, addr1)
                .put(21, addr2).put(22, addr2).put(23, addr2)
                .put(31, addr3)
                .build();
        // Test case 1: numInstance=1, pipelineDop=3, enablePipeline.
        // - addr1
        //      - Instance#0: 11, 12, 13, 14, 15.
        // - addr2
        //      - Instance#0: 21, 22, 23.
        // - addr3
        //      - Instance#0: 31
        expectedParamAddresses = ImmutableList.of(addr1, addr2, addr3);
        expectedBucketSeqToDriverSeqs = ImmutableList.of(
                ImmutableMap.of(11, -1, 12, -1, 13, -1, 14, -1, 15, -1),
                ImmutableMap.of(21, -1, 22, -1, 23, -1),
                ImmutableMap.of(31, -1)
        );
        expectedPipelineDops = ImmutableList.of(-1, -1, -1);
        expectedNumScanRangesList = ImmutableList.of(5, 3, 1);
        expectedDriverSeq2NumScanRangesList = null;
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToWorkerId, 1, 3, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

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
        prepare.prepareFragments();
        prepare.computeScanRangeAssignment();
        prepare.computeFragmentExecParams();

        // Assert
        Map<PlanFragmentId, CoordinatorPreprocessor.FragmentExecParams> fragmentParams =
                prepare.getFragmentExecParamsMap();
        fragmentParams.forEach((k, v) -> {
            System.err.println("Fragment " + k + " : " + v);
        });
        Assert.assertTrue(fragmentParams.containsKey(fragmentId));
        CoordinatorPreprocessor.FragmentExecParams fragmentParam = fragmentParams.get(fragmentId);
        FragmentScanRangeAssignment scanRangeAssignment = fragmentParam.scanRangeAssignment;
        List<CoordinatorPreprocessor.FInstanceExecParam> instances = fragmentParam.instanceExecParams;
        Assert.assertFalse(fragmentParams.isEmpty());
        Assert.assertEquals(1, scanRangeAssignment.size());
        Assert.assertEquals(1, instances.size());

    }
}
