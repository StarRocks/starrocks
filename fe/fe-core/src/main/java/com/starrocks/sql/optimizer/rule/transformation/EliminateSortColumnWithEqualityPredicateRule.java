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
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EliminateSortColumnWithEqualityPredicateRule extends TransformationRule {
    public EliminateSortColumnWithEqualityPredicateRule() {
        super(RuleType.TF_ELIMINATE_SORT_COLUMN_WITH_EQUALITY_PREDICATE,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scan = (LogicalScanOperator) input.getInputs().get(0).getOp();
        return scan.getPredicate() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator scan = (LogicalScanOperator) input.getInputs().get(0).getOp();
        List<Ordering> reservedOrdering = new ArrayList<>();

        for (Ordering ordering : topn.getOrderByElements()) {
            ColumnRefOperator column = ordering.getColumnRef();
            if (!allPredicatesAreEquality(scan, column)) {
                reservedOrdering.add(ordering);
            }
        }

        if (reservedOrdering.isEmpty()) {
            if (topn.hasLimit()) {
                scan.setLimit(topn.getLimit());
            }

            return input.getInputs();
        } else {
            LogicalTopNOperator newTopn = LogicalTopNOperator.builder().withOperator(topn)
                    .setOrderByElements(reservedOrdering).build();
            return Lists.newArrayList(OptExpression.create(newTopn, input.getInputs()));
        }
    }

    private boolean allPredicatesAreEquality(LogicalScanOperator scanOperator, ColumnRefOperator column) {
        return scanOperator.getPredicate().accept(new Visitor(column), null);
    }

    private static class Visitor extends ScalarOperatorVisitor<Boolean, Void> {
        private final ColumnRefOperator column;

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            return false;
        }

        public Visitor(ColumnRefOperator column) {
            this.column = column;
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            return predicate.getBinaryType() == BinaryType.EQ && predicate.getChildren().get(0).equals(column) &&
                    predicate.getChildren().get(1).isConstant();
        }

        @Override
        public Boolean visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (predicate.isAnd()) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate).stream()
                        .filter(x -> x.getUsedColumns().contains(column.getId()))
                        .collect(Collectors.toList());

                if (conjuncts.size() == 1) {
                    return conjuncts.get(0).accept(this, null);
                }
            }
            return false;
        }
    }
}
