// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TupleId;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CoordinatorTest {
    private void testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode mode) throws IOException {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setExecutionId(new TUniqueId(0xdeadbeef, 0xdeadbeef));
        ConnectContext.threadLocalInfo.set(ctx);
        Coordinator coordinator = new Coordinator(ctx, Lists.newArrayList(), Lists.newArrayList(), new TDescriptorTable());
        ArrayList<TupleId> tupleIdArrayList = new ArrayList<>();
        tupleIdArrayList.add(new TupleId(1));
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(1), new EmptySetNode(new PlanNodeId(1), tupleIdArrayList),
                new DataPartition(TPartitionType.RANDOM));
        Coordinator.FragmentExecParams params = coordinator.new FragmentExecParams(fragment);
        Coordinator.FInstanceExecParam instance0 = new Coordinator.FInstanceExecParam(null, null, 0, params);
        Coordinator.FInstanceExecParam instance1 = new Coordinator.FInstanceExecParam(null, null, 1, params);
        Coordinator.FInstanceExecParam instance2 = new Coordinator.FInstanceExecParam(null, null, 2, params);
        instance0.bucketSeqSet = Sets.newHashSet(2, 0);
        instance1.bucketSeqSet = Sets.newHashSet(1, 4);
        instance2.bucketSeqSet = Sets.newHashSet(3, 5);
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
}
