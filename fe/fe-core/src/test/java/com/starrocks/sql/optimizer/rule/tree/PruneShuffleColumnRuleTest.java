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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PruneShuffleColumnRuleTest {
    @Test
    public void testPruneJoinShuffleColumns() {
        JoinShufflePlan plan = createJoinShufflePlan(false);

        new PruneShuffleColumnRule().rewrite(plan.root, createTaskContext(plan.columnRefFactory));

        assertEquals(1, shuffleColumnCount(plan.leftDistribution));
        assertEquals(1, shuffleColumnCount(plan.rightDistribution));
    }

    @Test
    public void testPreserveJoinShuffleColumns() {
        JoinShufflePlan plan = createJoinShufflePlan(true);

        new PruneShuffleColumnRule().rewrite(plan.root, createTaskContext(plan.columnRefFactory));

        assertEquals(2, shuffleColumnCount(plan.leftDistribution));
        assertEquals(2, shuffleColumnCount(plan.rightDistribution));
    }

    private JoinShufflePlan createJoinShufflePlan(boolean preserveShuffleColumns) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator leftHighCardinality = columnRefFactory.create("left_high_cardinality", IntegerType.INT, false);
        ColumnRefOperator leftLowCardinality = columnRefFactory.create("left_low_cardinality", IntegerType.INT, false);
        ColumnRefOperator rightHighCardinality = columnRefFactory.create("right_high_cardinality", IntegerType.INT, false);
        ColumnRefOperator rightLowCardinality = columnRefFactory.create("right_low_cardinality", IntegerType.INT, false);

        OptExpression leftDistribution = createDistribution(
                List.of(leftHighCardinality, leftLowCardinality),
                createStatistics(leftHighCardinality, leftLowCardinality));
        OptExpression rightDistribution = createDistribution(
                List.of(rightHighCardinality, rightLowCardinality),
                createStatistics(rightHighCardinality, rightLowCardinality));

        PhysicalHashJoinOperator joinOperator = new PhysicalHashJoinOperator(
                JoinOperator.INNER_JOIN, null, "", -1, null, null, null, null);
        joinOperator.setPreserveShuffleColumns(preserveShuffleColumns);
        OptExpression root = OptExpression.create(joinOperator, leftDistribution, rightDistribution);
        return new JoinShufflePlan(columnRefFactory, root, leftDistribution, rightDistribution);
    }

    private OptExpression createDistribution(List<ColumnRefOperator> columns, Statistics statistics) {
        List<DistributionCol> distributionCols = columns.stream()
                .map(column -> new DistributionCol(column.getId(), true))
                .toList();
        HashDistributionDesc desc = new HashDistributionDesc(distributionCols,
                HashDistributionDesc.SourceType.SHUFFLE_JOIN);
        OptExpression optExpression = OptExpression.create(new PhysicalDistributionOperator(new HashDistributionSpec(desc)));
        optExpression.setStatistics(statistics);
        return optExpression;
    }

    private Statistics createStatistics(ColumnRefOperator highCardinality, ColumnRefOperator lowCardinality) {
        return Statistics.builder()
                .setOutputRowCount(1_000_000)
                .addColumnStatistic(highCardinality, new ColumnStatistic(0, 1_000_000, 0, 4, 900_000))
                .addColumnStatistic(lowCardinality, new ColumnStatistic(0, 100, 0, 4, 100))
                .build();
    }

    private TaskContext createTaskContext(ColumnRefFactory columnRefFactory) {
        OptimizerContext optimizerContext = OptimizerFactory.mockContext(columnRefFactory);
        return new TaskContext(optimizerContext, PhysicalPropertySet.EMPTY, new ColumnRefSet(), Double.MAX_VALUE);
    }

    private int shuffleColumnCount(OptExpression distribution) {
        PhysicalDistributionOperator distributionOperator = distribution.getOp().cast();
        HashDistributionSpec distributionSpec = (HashDistributionSpec) distributionOperator.getDistributionSpec();
        return distributionSpec.getHashDistributionDesc().getDistributionCols().size();
    }

    private static class JoinShufflePlan {
        private final ColumnRefFactory columnRefFactory;
        private final OptExpression root;
        private final OptExpression leftDistribution;
        private final OptExpression rightDistribution;

        private JoinShufflePlan(ColumnRefFactory columnRefFactory, OptExpression root,
                                OptExpression leftDistribution, OptExpression rightDistribution) {
            this.columnRefFactory = columnRefFactory;
            this.root = root;
            this.leftDistribution = leftDistribution;
            this.rightDistribution = rightDistribution;
        }
    }
}
