// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.planner;

import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TRuntimeFilterBuildJoinMode;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestRuntimeFilterDescription {
    @Test
    public void testPushAcrossExchangeNode() throws IOException {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Object[][] testCases = new Object[][]{
                {HashJoinNode.DistributionMode.BROADCAST, 1, true, TRuntimeFilterBuildJoinMode.BORADCAST},
                {HashJoinNode.DistributionMode.BROADCAST, 2, true, TRuntimeFilterBuildJoinMode.BORADCAST},
                {HashJoinNode.DistributionMode.LOCAL_HASH_BUCKET, 1, true, TRuntimeFilterBuildJoinMode.LOCAL_HASH_BUCKET},
                {HashJoinNode.DistributionMode.LOCAL_HASH_BUCKET, 2, false, TRuntimeFilterBuildJoinMode.LOCAL_HASH_BUCKET},
                {HashJoinNode.DistributionMode.PARTITIONED, 1, true, TRuntimeFilterBuildJoinMode.PARTITIONED},
                {HashJoinNode.DistributionMode.PARTITIONED, 2, false, TRuntimeFilterBuildJoinMode.PARTITIONED},
                {HashJoinNode.DistributionMode.COLOCATE, 1, true, TRuntimeFilterBuildJoinMode.COLOCATE},
                {HashJoinNode.DistributionMode.COLOCATE, 2, false, TRuntimeFilterBuildJoinMode.COLOCATE},
                {HashJoinNode.DistributionMode.REPLICATED, 1, false, TRuntimeFilterBuildJoinMode.REPLICATED},
                {HashJoinNode.DistributionMode.REPLICATED, 2, false, TRuntimeFilterBuildJoinMode.REPLICATED},
                {HashJoinNode.DistributionMode.SHUFFLE_HASH_BUCKET, 1, true, TRuntimeFilterBuildJoinMode.SHUFFLE_HASH_BUCKET},
                {HashJoinNode.DistributionMode.SHUFFLE_HASH_BUCKET, 2, false, TRuntimeFilterBuildJoinMode.SHUFFLE_HASH_BUCKET},
        };
        for (Object[] tc : testCases) {
            HashJoinNode.DistributionMode joinMode = (HashJoinNode.DistributionMode) tc[0];
            Integer eqCount = (Integer) tc[1];
            Boolean canPush = (Boolean) tc[2];
            TRuntimeFilterBuildJoinMode rfJoinMode = (TRuntimeFilterBuildJoinMode)tc[3];
            RuntimeFilterDescription rf = new RuntimeFilterDescription(ctx.getSessionVariable());
            rf.setJoinMode(joinMode);
            rf.setEqualCount(eqCount);
            rf.setOnlyLocal(false);
            Assert.assertEquals(rf.canPushAcrossExchangeNode(), canPush);
            Assert.assertEquals(rf.toThrift().getBuild_join_mode(), rfJoinMode);
        }
    }
}
