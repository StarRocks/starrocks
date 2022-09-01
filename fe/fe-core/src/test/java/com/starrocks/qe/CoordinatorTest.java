// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.TupleId;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
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

public class CoordinatorTest {
    ConnectContext ctx;
    Coordinator coordinator;

    @Before
    public void setUp() throws IOException {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setExecutionId(new TUniqueId(0xdeadbeef, 0xdeadbeef));
        ConnectContext.threadLocalInfo.set(ctx);

        coordinator = new Coordinator(ctx, Lists.newArrayList(), Lists.newArrayList(), new TDescriptorTable());
    }

    private void testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode mode) throws IOException {

        ArrayList<TupleId> tupleIdArrayList = new ArrayList<>();
        tupleIdArrayList.add(new TupleId(1));
        PlanFragment fragment =
                new PlanFragment(new PlanFragmentId(1), new EmptySetNode(new PlanNodeId(1), tupleIdArrayList),
                        new DataPartition(TPartitionType.RANDOM));
        Coordinator.FragmentExecParams params = coordinator.new FragmentExecParams(fragment);
        Coordinator.FInstanceExecParam instance0 = new Coordinator.FInstanceExecParam(null, null, 0, params);
        Coordinator.FInstanceExecParam instance1 = new Coordinator.FInstanceExecParam(null, null, 1, params);
        Coordinator.FInstanceExecParam instance2 = new Coordinator.FInstanceExecParam(null, null, 2, params);
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
        coordinator.computeBucketSeq2InstanceOrdinal(params, 6);
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
                                                             Map<Integer, TNetworkAddress> bucketSeqToAddress,
                                                             int parallelExecInstanceNum,
                                                             int pipelineDop, boolean enablePipeline,
                                                             int expectedInstances,
                                                             List<TNetworkAddress> expectedParamAddresses,
                                                             List<Integer> expectedPipelineDops,
                                                             List<Map<Integer, Integer>> expectedBucketSeqToDriverSeqs,
                                                             List<Integer> expectedNumScanRangesList,
                                                             List<Map<Integer, Integer>> expectedDriverSeq2NumScanRangesList) {
        Coordinator.FragmentExecParams params = coordinator.new FragmentExecParams(null);
        Coordinator.BucketSeqToScanRange bucketSeqToScanRange = new Coordinator.BucketSeqToScanRange();
        for (Integer bucketSeq : bucketSeqToAddress.keySet()) {
            bucketSeqToScanRange.put(bucketSeq, createScanId2scanRanges(scanId, 1));
        }

        coordinator.computeColocatedJoinInstanceParam(bucketSeqToAddress, bucketSeqToScanRange,
                parallelExecInstanceNum, pipelineDop, enablePipeline, params);
        params.instanceExecParams.sort(Comparator.comparing(param -> param.host));

        Assert.assertEquals(expectedInstances, params.instanceExecParams.size());
        for (int i = 0; i < expectedInstances; ++i) {
            Coordinator.FInstanceExecParam param = params.instanceExecParams.get(i);
            Assert.assertEquals(expectedParamAddresses.get(i), param.host);
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
        TNetworkAddress addr1 = new TNetworkAddress("host1", 8000);
        TNetworkAddress addr2 = new TNetworkAddress("host2", 8000);
        TNetworkAddress addr3 = new TNetworkAddress("host3", 8000);
        Map<Integer, TNetworkAddress> bucketSeqToAddress = ImmutableMap.<Integer, TNetworkAddress>builder()
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
        List<TNetworkAddress> expectedParamAddresses = ImmutableList.of(addr1, addr2, addr3);
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
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToAddress, 1, 3, true,
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
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToAddress, 3, 2, true,
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
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToAddress, 3, 1, true,
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
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToAddress, 3, 1, false,
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
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToAddress, 1, 3, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

        // Test case 6: numInstance=1, pipelineDop=3, enablePipeline, addr3 is assigned only one bucket.
        // Bucket distribution:
        // - addr1: 11, 12, 13, 14, 15.
        // - addr2: 21, 22, 23.
        // - addr3: 31
        scanId = 1;
        bucketSeqToAddress = ImmutableMap.<Integer, TNetworkAddress>builder()
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
        testComputeColocatedJoinInstanceParamHelper(scanId, bucketSeqToAddress, 1, 3, true,
                expectedInstances, expectedParamAddresses, expectedPipelineDops, expectedBucketSeqToDriverSeqs,
                expectedNumScanRangesList, expectedDriverSeq2NumScanRangesList);

    }
}
