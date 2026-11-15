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

package com.starrocks.sql.optimizer.cost;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CostModelTest {
    private ConnectContext previousContext;
    private ConnectContext connectContext;
    private SessionVariable sessionVariable;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        connectContext = mock(ConnectContext.class);
        sessionVariable = new SessionVariable();
        when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        when(connectContext.getAliveBackendNumber()).thenReturn(3);
        ConnectContext.set(connectContext);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.set(previousContext);
    }

    @Test
    public void testDefaultBroadcastThresholdBoundary() {
        long threshold = 2147483648L;
        assertBroadcastCost(threshold - 1, 1);
        assertBroadcastCost(threshold, 1);
        assertBroadcastCost(threshold + 1, 1000);
    }

    @Test
    public void testBroadcastThresholdIndependentOfExecMemLimit() {
        sessionVariable.setBroadcastMemThreshold(4096);
        for (long execMemLimit : new long[] {SessionVariable.MIN_EXEC_MEM_LIMIT, 4294967296L, Long.MAX_VALUE}) {
            sessionVariable.setMaxExecMemByte(execMemLimit);
            assertBroadcastCost(4095, 1);
            assertBroadcastCost(4096, 1);
            assertBroadcastCost(4097, 1000);
        }
        // A low exec_mem_limit must not penalize a broadcast below the dedicated threshold.
        sessionVariable.setBroadcastMemThreshold(4294967296L);
        sessionVariable.setMaxExecMemByte(SessionVariable.MIN_EXEC_MEM_LIMIT);
        assertBroadcastCost(2147483648L, 1);
    }

    @Test
    public void testThresholdComparedWithPerBackendSize() {
        sessionVariable.setBroadcastMemThreshold(4096);
        for (int backendCount : new int[] {0, 1, 3}) {
            when(connectContext.getAliveBackendNumber()).thenReturn(backendCount);
            assertBroadcastCost(4096, 1);
            assertBroadcastCost(4097, 1000);
        }
    }

    @Test
    public void testZeroBroadcastThresholdAndGatherCost() {
        sessionVariable.setBroadcastMemThreshold(0);
        assertBroadcastCost(0, 1);
        assertBroadcastCost(1, 1000);
        CostEstimate gather = distributionCost(DistributionSpec.createGatherDistributionSpec(), 4096);
        assertEquals(4096, gather.getCpuCost());
        assertEquals(0, gather.getMemoryCost());
        assertEquals(4096, gather.getNetworkCost());
    }

    private void assertBroadcastCost(long outputSize, double penalty) {
        CostEstimate cost = distributionCost(DistributionSpec.createReplicatedDistributionSpec(), outputSize);
        int aliveBackendNumber = connectContext.getAliveBackendNumber();
        int beNum = Math.max(1, aliveBackendNumber);
        assertEquals(outputSize * (double) aliveBackendNumber * penalty, cost.getCpuCost());
        assertEquals(outputSize * (double) beNum * penalty, cost.getMemoryCost());
        assertEquals(Math.max(outputSize * (double) beNum, 1) * penalty, cost.getNetworkCost());
    }

    private CostEstimate distributionCost(DistributionSpec spec, long outputSize) {
        ExpressionContext expressionContext = mock(ExpressionContext.class);
        ColumnRefSet outputColumns = new ColumnRefSet();
        Statistics statistics = mock(Statistics.class);
        when(expressionContext.getOp()).thenReturn(new PhysicalDistributionOperator(spec));
        when(expressionContext.getChildOutputColumns(0)).thenReturn(outputColumns);
        when(expressionContext.getChildOperator(0)).thenReturn(mock(Operator.class));
        when(expressionContext.getStatistics()).thenReturn(statistics);
        when(statistics.getOutputSize(outputColumns)).thenReturn((double) outputSize);
        return CostModel.calculateCostEstimate(expressionContext);
    }
}
