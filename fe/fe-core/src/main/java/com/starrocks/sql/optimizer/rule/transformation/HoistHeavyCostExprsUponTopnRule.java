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

import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HoistHeavyCostExprsUponTopnRule extends TransformationRule {
    public HoistHeavyCostExprsUponTopnRule() {
        super(RuleType.TF_HOIST_HEAVY_COST_UPON_TOPN,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.LOGICAL_PROJECT));
    }

    private boolean isHeavyCost(ScalarOperator op) {
        if (op instanceof CallOperator) {
            CallOperator call = op.cast();
            if (call.getFnName().equals(ArithmeticExpr.Operator.DIVIDE.getName()) && (
                    call.getType().getPrimitiveType().equals(PrimitiveType.LARGEINT) ||
                            call.getType().getPrimitiveType().equals(PrimitiveType.DECIMAL128))) {
                return true;
            }
        }
        return op.getChildren().stream().anyMatch(this::isHeavyCost);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topnOp = input.getOp().cast();
        if (topnOp.getPartitionByColumns() != null && !topnOp.getPartitionByColumns().isEmpty()) {
            return false;
        }
        if (topnOp.getPredicate() != null || topnOp.getLimit() < 0) {
            return false;
        }
        OptExpression child = input.inputAt(0);
        Set<ColumnRefOperator> heavyCostColumnRefs = child.getRowOutputInfo().getColumnRefMap().entrySet()
                .stream()
                .filter(e -> isHeavyCost(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (heavyCostColumnRefs.isEmpty()) {
            return false;
        }

        boolean isUsedByPredicate = Optional.ofNullable(child.getOp().getPredicate())
                .map(pred -> pred.getUsedColumns().containsAny(heavyCostColumnRefs))
                .orElse(false);

        if (isUsedByPredicate) {
            return false;
        }

        Set<ColumnRefOperator> orderByColumnRefs = topnOp.getOrderByElements().stream()
                .map(Ordering::getColumnRef)
                .collect(Collectors.toSet());

        ColumnRefSet heavyColumnUsedAsOrderBy = ColumnRefSet.of();
        heavyColumnUsedAsOrderBy.union(orderByColumnRefs);
        ColumnRefSet heavyCostColumnRefSet = ColumnRefSet.of();
        heavyCostColumnRefSet.union(heavyCostColumnRefs);
        heavyColumnUsedAsOrderBy.intersect(heavyCostColumnRefSet);
        return heavyColumnUsedAsOrderBy.isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topnOp = input.getOp().cast();
        OptExpression child = input.inputAt(0);
        Map<Boolean, Map<ColumnRefOperator, ScalarOperator>> columnRefMaps =
                child.getRowOutputInfo().getColumnRefMap().entrySet()
                        .stream()
                        .collect(Collectors.partitioningBy(e -> isHeavyCost(e.getValue()),
                                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        Map<ColumnRefOperator, ScalarOperator> heavyCostColumnRefMap = columnRefMaps.get(true);
        Map<ColumnRefOperator, ScalarOperator> childColumnRefMap = columnRefMaps.get(false);

        Set<ColumnRefOperator> usedColumnRefs = heavyCostColumnRefMap.values().stream()
                .map(ScalarOperator::getColumnRefs)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        if (!childColumnRefMap.keySet().containsAll(usedColumnRefs)) {
            return Collections.emptyList();
        }

        Map<ColumnRefOperator, ScalarOperator> topnColumnRefMap = input.getRowOutputInfo().getColumnRefMap();
        topnColumnRefMap.putAll(columnRefMaps.get(true));
        LogicalProjectOperator projectOp = child.getOp().cast();

        Operator newProjectOp = LogicalProjectOperator.builder().withOperator(projectOp)
                .setColumnRefMap(childColumnRefMap)
                .build();

        OptExpression newChild = OptExpression.builder().setOp(newProjectOp).setInputs(child.getInputs()).build();
        Operator upperProjectOp = LogicalProjectOperator.builder()
                .setColumnRefMap(topnColumnRefMap).build();

        OptExpression newTopn =
                OptExpression.builder().setOp(topnOp).setInputs(Lists.newArrayList(newChild)).build();

        return Collections.singletonList(OptExpression.create(upperProjectOp, newTopn));
    }
}