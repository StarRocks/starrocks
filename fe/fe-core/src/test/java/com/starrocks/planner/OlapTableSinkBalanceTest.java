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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.system.ComputeNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OlapTableSinkBalanceTest {

    private static List<ComputeNode> nodes(long... ids) {
        List<ComputeNode> nodes = Lists.newArrayList();
        for (long id : ids) {
            nodes.add(new ComputeNode(id, "127.0.0.1", 9050));
        }
        return nodes;
    }

    private static int maxPerNode(List<Long> assignment) {
        Map<Long, Integer> cnt = new HashMap<>();
        int max = 0;
        for (Long id : assignment) {
            int c = cnt.merge(id, 1, Integer::sum);
            max = Math.max(max, c);
        }
        return max;
    }

    @Test
    public void testSkewedAssignmentIsRebalanced() {
        // All 6 tablets' natural primary is node 10001 (the presplit/reshard skew case).
        List<Long> natural = Lists.newArrayList(10001L, 10001L, 10001L, 10001L, 10001L, 10001L);
        List<Long> balanced = OlapTableSink.balanceCloudNativeLoadPrimaries(natural, nodes(10001L, 10002L, 10003L));
        // 6 tablets over 3 nodes -> exactly 2 per node.
        Assertions.assertEquals(2, maxPerNode(balanced));
        Assertions.assertEquals(6, balanced.size());
    }

    @Test
    public void testBalancedAssignmentIsUntouched() {
        // HASH case: already 2 per node -> returned unchanged (same list instance).
        List<Long> natural = Lists.newArrayList(10001L, 10002L, 10003L, 10001L, 10002L, 10003L);
        List<Long> result = OlapTableSink.balanceCloudNativeLoadPrimaries(natural, nodes(10001L, 10002L, 10003L));
        Assertions.assertSame(natural, result);
    }

    @Test
    public void testPartiallySkewedUsesAllNodes() {
        // 6 tablets on only 2 of 3 alive nodes (3 each) is more skewed than ceil(6/3)=2 -> rebalance to all 3.
        List<Long> natural = Lists.newArrayList(10001L, 10001L, 10001L, 10002L, 10002L, 10002L);
        List<Long> balanced = OlapTableSink.balanceCloudNativeLoadPrimaries(natural, nodes(10001L, 10002L, 10003L));
        Assertions.assertEquals(2, maxPerNode(balanced));
    }

    @Test
    public void testSingleNodeNoOp() {
        List<Long> natural = Lists.newArrayList(10001L, 10001L);
        List<Long> result = OlapTableSink.balanceCloudNativeLoadPrimaries(natural, nodes(10001L));
        Assertions.assertSame(natural, result);
    }

    @Test
    public void testDisabledByConfig() {
        boolean old = Config.lake_balance_load_primary_compute_nodes;
        Config.lake_balance_load_primary_compute_nodes = false;
        try {
            List<Long> natural = Lists.newArrayList(10001L, 10001L, 10001L);
            List<Long> result =
                    OlapTableSink.balanceCloudNativeLoadPrimaries(natural, nodes(10001L, 10002L, 10003L));
            Assertions.assertSame(natural, result);
        } finally {
            Config.lake_balance_load_primary_compute_nodes = old;
        }
    }
}
