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

package com.starrocks.alter.reshard.presplit;

import com.google.common.collect.Lists;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * A derived-tier boundary source reads no data, so it has no sample to take its input size from. It
 * takes the size the optimizer already estimated for the statement instead; these tests pin how that
 * estimate is read and what happens when it is absent.
 */
public class PreSplitEstimatesTest {

    @Test
    public void testReadsRootStatistics() {
        Statistics statistics = Statistics.builder().setOutputRowCount(1_000_000).build();
        ExecPlan execPlan = planWithRootStatistics(statistics);

        Estimates estimates = PreSplitEstimates.fromExecPlan(execPlan);

        Assertions.assertEquals(1_000_000L, estimates.totalRows());
        // No column statistics means the average row size floors at one byte, so the byte estimate is
        // the row count. The point is that bytes are derived from the SAME statistics object, not
        // invented separately.
        Assertions.assertEquals((long) statistics.getComputeSize(), estimates.totalBytes());
        Assertions.assertTrue(estimates.totalBytes() > 0);
    }

    @Test
    public void testFallsBackToScanNodesWhenRootStatisticsAreAbsent() {
        ExecPlan execPlan = planWithScanNodes(null, scanNode(700_000L, 8.0f), scanNode(300_000L, 4.0f));

        Estimates estimates = PreSplitEstimates.fromExecPlan(execPlan);

        Assertions.assertEquals(1_000_000L, estimates.totalRows());
        Assertions.assertEquals(700_000L * 8 + 300_000L * 4, estimates.totalBytes());
    }

    @Test
    public void testRootStatisticsNeverReportZeroRows() {
        // Statistics.clampOutputRowCount floors the row count at 1, so a root that estimated nothing
        // still reports one row and the scan fallback is NOT used. Pinned because the opposite is the
        // natural assumption, and acting on it would have added an unreachable branch here.
        Statistics clampedToOne = Statistics.builder().setOutputRowCount(0).build();
        ExecPlan execPlan = planWithScanNodes(clampedToOne, scanNode(500L, 2.0f));

        Estimates estimates = PreSplitEstimates.fromExecPlan(execPlan);

        Assertions.assertEquals(1L, estimates.totalRows());
        Assertions.assertNotEquals(500L, estimates.totalRows());
    }

    @Test
    public void testZeroWhenNothingIsAvailable() {
        Assertions.assertEquals(Estimates.ZERO, PreSplitEstimates.fromExecPlan(planWithScanNodes(null)));
        Assertions.assertEquals(Estimates.ZERO, PreSplitEstimates.fromExecPlan(null));
    }

    @Test
    public void testZeroWhenScanCardinalityIsUnknown() {
        // PlanNode.getCardinality() is -1 until the planner sets it; a negative total must not become a
        // bogus positive estimate.
        Estimates estimates = PreSplitEstimates.fromExecPlan(planWithScanNodes(null, scanNode(-1L, 8.0f)));

        Assertions.assertEquals(Estimates.ZERO, estimates);
    }

    private static ExecPlan planWithRootStatistics(Statistics statistics) {
        return planWithScanNodes(statistics);
    }

    private static ExecPlan planWithScanNodes(Statistics rootStatistics, ScanNode... scanNodes) {
        OptExpression root = Mockito.mock(OptExpression.class);
        Mockito.when(root.getStatistics()).thenReturn(rootStatistics);
        ExecPlan execPlan = Mockito.mock(ExecPlan.class);
        Mockito.when(execPlan.getPhysicalPlan()).thenReturn(root);
        Mockito.when(execPlan.getScanNodes()).thenReturn(Lists.<ScanNode>newArrayList(scanNodes));
        return execPlan;
    }

    private static ScanNode scanNode(long cardinality, float avgRowSize) {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getCardinality()).thenReturn(cardinality);
        Mockito.when(scanNode.getAvgRowSize()).thenReturn(avgRowSize);
        return scanNode;
    }
}
