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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalcUtils;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SplitScanORToUnionRuleTest {

    @Test
    public void testDirectScanPredicateUpdateRetainsAddedFilterForRewrite(@Mocked OlapTable table) {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator id = factory.create("id", IntegerType.INT, true);
        ColumnRefOperator value = factory.create("v", IntegerType.INT, true);
        Column idColumn = new Column("id", IntegerType.INT);
        Column valueColumn = new Column("v", IntegerType.INT);
        ScalarOperator originalPredicate = new InPredicateOperator(false, id, ConstantOperator.createInt(1));
        ScalarOperator executionPredicate = BinaryPredicateOperator.eq(id, ConstantOperator.createInt(1));
        ScalarOperator addedFilter = BinaryPredicateOperator.eq(value, ConstantOperator.createInt(2));
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        ImmutableMap.of(id, idColumn, value, valueColumn),
                        ImmutableMap.of(idColumn, id, valueColumn, value),
                        null, Operator.DEFAULT_LIMIT, executionPredicate))
                .setPredicateForMvRewrite(originalPredicate)
                .build();

        scan.setPredicate(Utils.compoundAnd(scan.getPredicate(), addedFilter));

        assertEquals(Set.of(executionPredicate, addedFilter),
                MvUtils.getPredicateForRewrite(OptExpression.create(scan)));
    }

    @Test
    public void testSplitBranchesDoNotReuseWholeOrPredicate(@Mocked OlapTable table,
                                                          @Mocked ConnectContext connectContext,
                                                          @Mocked OptimizerContext context,
                                                          @Mocked StatisticsCalcUtils statisticsCalcUtils) {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator firstRef = factory.create("c1", IntegerType.INT, true);
        ColumnRefOperator secondRef = factory.create("c2", IntegerType.INT, true);
        Column firstColumn = new Column("c1", IntegerType.INT);
        Column secondColumn = new Column("c2", IntegerType.INT);

        ScalarOperator firstPredicate = BinaryPredicateOperator.eq(firstRef, ConstantOperator.createInt(1));
        ScalarOperator secondPredicate = BinaryPredicateOperator.eq(secondRef, ConstantOperator.createInt(2));
        ScalarOperator predicateForMvRewrite = Utils.compoundOr(firstPredicate, secondPredicate);
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        ImmutableMap.of(firstRef, firstColumn, secondRef, secondColumn),
                        ImmutableMap.of(firstColumn, firstRef, secondColumn, secondRef),
                        null, Operator.DEFAULT_LIMIT, predicateForMvRewrite))
                .setPredicateForMvRewrite(predicateForMvRewrite)
                .build();

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSelectRatioThreshold(-1);
        sessionVariable.setScanOrToUnionLimit(2);
        Statistics.Builder statistics = Statistics.builder()
                .addColumnStatistic(firstRef, ColumnStatistic.unknown())
                .addColumnStatistic(secondRef, ColumnStatistic.unknown());
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                context.getColumnRefFactory();
                result = factory;
                StatisticsCalcUtils.estimateScanColumns(table, scan.getColRefToColumnMetaMap());
                result = statistics;
            }
        };

        List<OptExpression> result = SplitScanORToUnionRule.getInstance().transform(OptExpression.create(scan), context);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getInputs().size());
        for (OptExpression branch : result.get(0).getInputs()) {
            LogicalOlapScanOperator splitScan = branch.getOp().cast();
            // Each branch is narrower than the whole OR, including duplicate suppression on later branches.
            assertFalse(splitScan.hasPredicateForMvRewrite());
            assertEquals(splitScan.getPredicate(), splitScan.getPredicateForMvRewrite());
            Set<Integer> splitRefIds = splitScan.getColRefToColumnMetaMap().keySet().stream()
                    .map(ColumnRefOperator::getId).collect(Collectors.toSet());
            List<ColumnRefOperator> predicateRefs = splitScan.getPredicateForMvRewrite().getColumnRefs();
            assertTrue(predicateRefs.stream().map(ColumnRefOperator::getId).allMatch(splitRefIds::contains));
            assertFalse(predicateRefs.stream().anyMatch(ref -> ref.getId() == firstRef.getId()
                    || ref.getId() == secondRef.getId()));
        }
    }
}
