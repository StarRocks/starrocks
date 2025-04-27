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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.BinaryType;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.types.Type;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StatisticsEstimateUtilsTest {

    @Test
    public void testEstimateConjunctiveEqualitySelectivityWithoutMultiColumnStats() {
        // Mock column references and statistics
        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "c1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.INT, "c2", true);

        // Create statistics for these columns
        Statistics.Builder statsBuilder = Statistics.builder();
        statsBuilder.setOutputRowCount(1000);
        statsBuilder.addColumnStatistic(col1,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(100)
                    .setNullsFraction(0.1)
                    .setMinValue(1)
                    .setMaxValue(100)
                    .build());
        statsBuilder.addColumnStatistic(col2,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(50)
                    .setNullsFraction(0.05)
                    .setMinValue(1)
                    .setMaxValue(50)
                    .build());

        Statistics inputStats = statsBuilder.build();

        // Create equality predicates
        Map<ColumnRefOperator, ConstantOperator> equalityPredicates = new HashMap<>();
        equalityPredicates.put(col1, ConstantOperator.createInt(10));
        equalityPredicates.put(col2, ConstantOperator.createInt(20));

        // Mock the getLargestSubsetMCStats method to return null (no multi-column stats)
        new MockUp<Statistics>() {
            @Mock
            public Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> getLargestSubsetMCStats(
                    Set<ColumnRefOperator> targetColumns) {
                return null;
            }
        };

        // Create a compound predicate for testing
        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(BinaryType.EQ, col1, ConstantOperator.createInt(10));
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(BinaryType.EQ, col2, ConstantOperator.createInt(20));
        CompoundPredicateOperator compoundPred = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pred1, pred2);

        // Test through the public method
        Statistics result = StatisticsEstimateUtils.computeCompoundStatsWithMultiColumnOptimize(compoundPred, inputStats);

        // Verify results
        // For columns with no multi-column stats, we expect the individual selectivities to be multiplied
        // c1: 1/100 = 0.01, c2: 1/50 = 0.02, combined: 0.01 * 0.02 = 0.0002
        // rowCount = 1000 * 0.0002 = 0.2, which should be clamped to 1.0
        Assert.assertEquals(1.0, result.getOutputRowCount(), 0.001);

        // Verify column statistics were updated
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMinValue(), 0.001);
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col1).getNullsFraction(), 0.001);

        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMinValue(), 0.001);
        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col2).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateConjunctiveEqualitySelectivityWithMultiColumnStats() {
        // Mock column references and statistics
        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "c1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.INT, "c2", true);

        // Create statistics for these columns
        Statistics.Builder statsBuilder = Statistics.builder();
        statsBuilder.setOutputRowCount(10000);
        statsBuilder.addColumnStatistic(col1,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(100)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(100)
                    .build());
        statsBuilder.addColumnStatistic(col2,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(50)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(50)
                    .build());

        Statistics inputStats = statsBuilder.build();

        // Create equality predicates
        Map<ColumnRefOperator, ConstantOperator> equalityPredicates = new HashMap<>();
        equalityPredicates.put(col1, ConstantOperator.createInt(10));
        equalityPredicates.put(col2, ConstantOperator.createInt(20));

        // Create multi-column stats with NDV = 500 (which is less than col1.ndv * col2.ndv = 5000)
        MultiColumnCombinedStats multiColStats = new MultiColumnCombinedStats(
                ImmutableSet.of(1, 2), 500);

        // Mock the getLargestSubsetMCStats method
        new MockUp<Statistics>() {
            @Mock
            public Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> getLargestSubsetMCStats(
                    Set<ColumnRefOperator> targetColumns) {
                return new Pair<>(ImmutableSet.of(col1, col2), multiColStats);
            }
        };

        // Create a compound predicate for testing
        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(BinaryType.EQ, col1, ConstantOperator.createInt(10));
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(BinaryType.EQ, col2, ConstantOperator.createInt(20));
        CompoundPredicateOperator compoundPred = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pred1, pred2);

        // Test through the public method
        Statistics result = StatisticsEstimateUtils.computeCompoundStatsWithMultiColumnOptimize(compoundPred, inputStats);

        // Verify results
        // With multi-column stats, we expect 1/ndv = 1/500 = 0.002 selectivity
        // rowCount = 10000 * 0.002 = 20
        Assert.assertEquals(20.0, result.getOutputRowCount(), 0.001);

        // Verify column statistics were updated
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMinValue(), 0.001);
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col1).getNullsFraction(), 0.001);

        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMinValue(), 0.001);
        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col2).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateConjunctiveEqualitySelectivityWithPartialMultiColumnStats() {
        // Mock column references and statistics
        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "c1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.INT, "c2", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, Type.INT, "c3", true);

        // Create statistics for these columns
        Statistics.Builder statsBuilder = Statistics.builder();
        statsBuilder.setOutputRowCount(10000);
        statsBuilder.addColumnStatistic(col1,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(100)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(100)
                    .build());
        statsBuilder.addColumnStatistic(col2,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(50)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(50)
                    .build());
        statsBuilder.addColumnStatistic(col3,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(25)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(25)
                    .build());

        Statistics inputStats = statsBuilder.build();

        // Create equality predicates
        Map<ColumnRefOperator, ConstantOperator> equalityPredicates = new HashMap<>();
        equalityPredicates.put(col1, ConstantOperator.createInt(10));
        equalityPredicates.put(col2, ConstantOperator.createInt(20));
        equalityPredicates.put(col3, ConstantOperator.createInt(5));

        // Create multi-column stats for col1+col2 with NDV = 400
        MultiColumnCombinedStats multiColStats = new MultiColumnCombinedStats(
                ImmutableSet.of(1, 2), 400);

        // Mock the getLargestSubsetMCStats method to return stats for col1+col2 only
        new MockUp<Statistics>() {
            @Mock
            public Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> getLargestSubsetMCStats(
                    Set<ColumnRefOperator> targetColumns) {
                return new Pair<>(ImmutableSet.of(col1, col2), multiColStats);
            }
        };

        // Create a compound predicate for testing
        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(BinaryType.EQ, col1, ConstantOperator.createInt(10));
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(BinaryType.EQ, col2, ConstantOperator.createInt(20));
        BinaryPredicateOperator pred3 = new BinaryPredicateOperator(BinaryType.EQ, col3, ConstantOperator.createInt(5));

        CompoundPredicateOperator compoundPred1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pred1, pred2);
        CompoundPredicateOperator compoundPred = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, compoundPred1, pred3);

        // Test through the public method
        Statistics result = StatisticsEstimateUtils.computeCompoundStatsWithMultiColumnOptimize(compoundPred, inputStats);

        // Verify results
        // For col1+col2, we have 1/400 selectivity from multi-column stats
        // For col3, we have 1/25 = 0.04 selectivity
        // Combined: (1/400) * 0.04 = 0.0001
        // rowCount = 10000 * 0.0001 = 1
        Assert.assertEquals(1.0, result.getOutputRowCount(), 0.001);

        // Verify column statistics were updated
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMinValue(), 0.001);
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col1).getNullsFraction(), 0.001);

        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMinValue(), 0.001);
        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col2).getNullsFraction(), 0.001);

        Assert.assertEquals(5.0, result.getColumnStatistic(col3).getMinValue(), 0.001);
        Assert.assertEquals(5.0, result.getColumnStatistic(col3).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col3).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateConjunctiveEqualitySelectivityWithExternalTables() {
        // Mock column references and statistics
        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "c1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.INT, "c2", true);

        // Create statistics for these columns
        Statistics.Builder statsBuilder = Statistics.builder();
        statsBuilder.setOutputRowCount(5000);
        statsBuilder.addColumnStatistic(col1,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(100)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(100)
                    .build());
        statsBuilder.addColumnStatistic(col2,
                ColumnStatistic.builder()
                    .setDistinctValuesCount(50)
                    .setNullsFraction(0.0)
                    .setMinValue(1)
                    .setMaxValue(50)
                    .build());

        Statistics inputStats = statsBuilder.build();

        // Create equality predicates
        Map<ColumnRefOperator, ConstantOperator> equalityPredicates = new HashMap<>();
        equalityPredicates.put(col1, ConstantOperator.createInt(10));
        equalityPredicates.put(col2, ConstantOperator.createInt(20));

        // Create external multi-column stats with NDV = 300
        // This simulates what might happen with data from an external table source
        MultiColumnCombinedStats externalMultiColStats = new MultiColumnCombinedStats(
                ImmutableSet.of(1, 2), 300);

        // Mock the getLargestSubsetMCStats method
        new MockUp<Statistics>() {
            @Mock
            public Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> getLargestSubsetMCStats(
                    Set<ColumnRefOperator> targetColumns) {
                return new Pair<>(ImmutableSet.of(col1, col2), externalMultiColStats);
            }
        };

        // Create a compound predicate for testing
        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(BinaryType.EQ, col1, ConstantOperator.createInt(10));
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(BinaryType.EQ, col2, ConstantOperator.createInt(20));
        CompoundPredicateOperator compoundPred = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, pred1, pred2);

        // Test through the public method
        Statistics result = StatisticsEstimateUtils.computeCompoundStatsWithMultiColumnOptimize(compoundPred, inputStats);

        // Verify results
        // With multi-column stats, we expect 1/ndv = 1/300 = 0.00333 selectivity
        // rowCount = 5000 * 0.00333 = 16.67
        Assert.assertEquals(16.67, result.getOutputRowCount(), 0.01);

        // Verify column statistics were updated
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMinValue(), 0.001);
        Assert.assertEquals(10.0, result.getColumnStatistic(col1).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col1).getNullsFraction(), 0.001);

        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMinValue(), 0.001);
        Assert.assertEquals(20.0, result.getColumnStatistic(col2).getMaxValue(), 0.001);
        Assert.assertEquals(0.0, result.getColumnStatistic(col2).getNullsFraction(), 0.001);
    }
}
