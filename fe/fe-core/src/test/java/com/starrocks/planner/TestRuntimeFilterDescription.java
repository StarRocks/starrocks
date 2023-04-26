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
                {JoinNode.DistributionMode.BROADCAST, 1, true, TRuntimeFilterBuildJoinMode.BORADCAST},
                {JoinNode.DistributionMode.BROADCAST, 2, true, TRuntimeFilterBuildJoinMode.BORADCAST},
                {JoinNode.DistributionMode.LOCAL_HASH_BUCKET, 1, true, TRuntimeFilterBuildJoinMode.LOCAL_HASH_BUCKET},
                {JoinNode.DistributionMode.LOCAL_HASH_BUCKET, 2, true, TRuntimeFilterBuildJoinMode.LOCAL_HASH_BUCKET},
                {JoinNode.DistributionMode.PARTITIONED, 1, true, TRuntimeFilterBuildJoinMode.PARTITIONED},
                {JoinNode.DistributionMode.PARTITIONED, 2, true, TRuntimeFilterBuildJoinMode.PARTITIONED},
                {JoinNode.DistributionMode.COLOCATE, 1, true, TRuntimeFilterBuildJoinMode.COLOCATE},
                {JoinNode.DistributionMode.COLOCATE, 2, true, TRuntimeFilterBuildJoinMode.COLOCATE},
                {JoinNode.DistributionMode.REPLICATED, 1, false, TRuntimeFilterBuildJoinMode.REPLICATED},
                {JoinNode.DistributionMode.REPLICATED, 2, false, TRuntimeFilterBuildJoinMode.REPLICATED},
                {JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET, 1, true, TRuntimeFilterBuildJoinMode.SHUFFLE_HASH_BUCKET},
                {JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET, 2, true, TRuntimeFilterBuildJoinMode.SHUFFLE_HASH_BUCKET},
        };
        for (Object[] tc : testCases) {
            JoinNode.DistributionMode joinMode = (JoinNode.DistributionMode) tc[0];
            Integer eqCount = (Integer) tc[1];
            Boolean canPush = (Boolean) tc[2];
            TRuntimeFilterBuildJoinMode rfJoinMode = (TRuntimeFilterBuildJoinMode) tc[3];
            RuntimeFilterDescription rf = new RuntimeFilterDescription(ctx.getSessionVariable());
            rf.setJoinMode(joinMode);
            rf.setEqualCount(eqCount);
            rf.setOnlyLocal(false);
            Assert.assertEquals(rf.canPushAcrossExchangeNode(), canPush);
            Assert.assertEquals(rf.toThrift().getBuild_join_mode(), rfJoinMode);
        }
    }

    @Test
    public void testIsLocalApplicable() throws IOException {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Object[][] testCases = new Object[][]{
                {JoinNode.DistributionMode.BROADCAST, true},
                {JoinNode.DistributionMode.COLOCATE, true},
                {JoinNode.DistributionMode.LOCAL_HASH_BUCKET, true},
                {JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET, true},
                {JoinNode.DistributionMode.PARTITIONED, false},
                {JoinNode.DistributionMode.REPLICATED, true},
        };

        for (Object[] tc : testCases) {
            JoinNode.DistributionMode joinMode = (JoinNode.DistributionMode) tc[0];
            Boolean expect = (Boolean) tc[1];
            RuntimeFilterDescription rf = new RuntimeFilterDescription(ctx.getSessionVariable());
            rf.setJoinMode(joinMode);
            Assert.assertEquals(rf.isLocalApplicable(), expect);
        }
    }
}
