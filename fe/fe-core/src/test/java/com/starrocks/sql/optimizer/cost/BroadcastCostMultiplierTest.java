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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BroadcastCostMultiplierTest {
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
    public void testDefaultPenaltyAndMemoryBoundary() {
        long limit = sessionVariable.getMaxExecMemByte();
        assertBroadcastCost(limit - 1, 3.0 * (limit - 1));
        assertBroadcastCost(limit, 3.0 * limit);
        assertBroadcastCost(limit + 1, 3000.0 * (limit + 1));
    }

    @Test
    public void testConfiguredPenaltyOnlyAboveMemoryLimit() {
        sessionVariable.setMaxExecMemByte(4096);
        sessionVariable.setBroadcastMemExceededPenaltyFactor(2.5);
        assertBroadcastCost(4095, 12285);
        assertBroadcastCost(4096, 12288);
        assertBroadcastCost(4097, 30727.5);

        sessionVariable.setBroadcastMemExceededPenaltyFactor(1.0);
        assertBroadcastCost(4097, 12291);
        sessionVariable.setBroadcastMemExceededPenaltyFactor(10.0);
        assertBroadcastCost(4097, 122910);

        sessionVariable.setMaxExecMemByte(8192);
        assertBroadcastCost(4097, 12291);
    }

    @Test
    public void testBroadcastCostRemainsFiniteAtMaximumPenaltyFactor() {
        sessionVariable.setMaxExecMemByte(4096);
        double maxMultiplier = SessionVariable.MAX_BROADCAST_MEM_EXCEEDED_PENALTY_FACTOR;
        sessionVariable.setBroadcastMemExceededPenaltyFactor(maxMultiplier);
        assertBroadcastCost(4096, 12288);
        assertBroadcastCost(4097, 12291 * maxMultiplier);
        CostEstimate cost = distributionCost(DistributionSpec.createReplicatedDistributionSpec(), 4097);

        assertAll(
                () -> assertTrue(Double.isFinite(cost.getCpuCost()), "CPU cost must be finite: " + cost.getCpuCost()),
                () -> assertTrue(Double.isFinite(cost.getMemoryCost()), "Memory cost must be finite: " + cost.getMemoryCost()),
                () -> assertTrue(Double.isFinite(cost.getNetworkCost()), "Network cost must be finite: " + cost.getNetworkCost()),
                () -> assertTrue(Double.isFinite(CostModel.getRealCost(cost)),
                        "Weighted cost must be finite: " + CostModel.getRealCost(cost)));
        assertEquals(49164 * maxMultiplier, CostModel.getRealCost(cost));
    }

    @Test
    public void testMemoryLimitUsesPerBackendSize() {
        sessionVariable.setMaxExecMemByte(4096);
        sessionVariable.setBroadcastMemExceededPenaltyFactor(2.5);
        // Replication to three backends exceeds the limit, but each backend's input does not.
        assertBroadcastCost(2048, 6144);
        when(connectContext.getAliveBackendNumber()).thenReturn(1);
        assertBroadcastCost(4097, 10242.5);
        when(connectContext.getAliveBackendNumber()).thenReturn(0);
        assertCost(distributionCost(DistributionSpec.createReplicatedDistributionSpec(), 4097),
                0, 10242.5, 10242.5);
    }

    @Test
    public void testOtherDistributionsAreUnaffected() {
        sessionVariable.setMaxExecMemByte(1);
        sessionVariable.setBroadcastMemExceededPenaltyFactor(1000.0);
        assertCost(distributionCost(DistributionSpec.createGatherDistributionSpec(), 4096), 4096, 0, 4096);
        assertCost(distributionCost(DistributionSpec.createAnyDistributionSpec(), 4096), 4096, 0, 0);
        assertCost(distributionCost(DistributionSpec.createReplicatedDistributionSpec(), 0), 0, 0, 1);
    }

    private void assertBroadcastCost(long outputSize, double expectedCost) {
        assertCost(distributionCost(DistributionSpec.createReplicatedDistributionSpec(), outputSize),
                expectedCost, expectedCost, expectedCost);
    }

    private void assertCost(CostEstimate cost, double cpu, double memory, double network) {
        assertEquals(cpu, cost.getCpuCost());
        assertEquals(memory, cost.getMemoryCost());
        assertEquals(network, cost.getNetworkCost());
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
