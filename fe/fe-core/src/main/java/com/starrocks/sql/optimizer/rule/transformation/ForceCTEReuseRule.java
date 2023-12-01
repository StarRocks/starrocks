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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * If the opt expression contains non-deterministic function, force cte reuse to avoid producing wrong result.
 */
public class ForceCTEReuseRule extends TransformationRule {
    public ForceCTEReuseRule() {
        super(RuleType.TF_FORCE_CTE_REUSE,
                Pattern.create(OperatorType.LOGICAL_CTE_PRODUCE, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (NonDeterministicVisitor.hasNonDeterministicFunction(input)) {
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) input.getOp();
            CTEContext cteContext = context.getCteContext();
            int cteId = produce.getCteId();
            cteContext.addForceCTE(cteId);
        }

        return Collections.emptyList();
    }

    private static class NonDeterministicVisitor extends OptExpressionVisitor<Boolean, Void> {
        public static boolean hasNonDeterministicFunction(OptExpression root) {
            return new NonDeterministicVisitor().visit(root, null);
        }

        boolean checkColumnRefMap(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            if (columnRefMap == null) {
                return false;
            }
            for (ScalarOperator ref : columnRefMap.values()) {
                if (hasNonDeterministicFunc(ref)) {
                    return true;
                }
            }
            return false;
        }

        private boolean hasNonDeterministicFunc(ScalarOperator scalarOperator) {
            if (scalarOperator instanceof CallOperator) {
                String fnName = ((CallOperator) scalarOperator).getFnName();
                if (FunctionSet.nonDeterministicFunctions.contains(fnName)) {
                    return true;
                }
            } else if (scalarOperator instanceof LambdaFunctionOperator) {
                LambdaFunctionOperator lambdaOp = (LambdaFunctionOperator) scalarOperator;
                Map<ColumnRefOperator, ScalarOperator> columnRefMap = lambdaOp.getColumnRefMap();
                if (checkColumnRefMap(columnRefMap)) {
                    return true;
                }
            }
            for (ScalarOperator child : scalarOperator.getChildren()) {
                if (hasNonDeterministicFunc(child)) {
                    return true;
                }
            }
            return false;
        }

        private boolean checkAggCall(Map<ColumnRefOperator, CallOperator> aggregations) {
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
                CallOperator aggCall = entry.getValue();
                for (ScalarOperator arg : aggCall.getArguments()) {
                    if (hasNonDeterministicFunc(arg)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean checkProject(Projection projection) {
            if (projection == null) {
                return false;
            }
            Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                    projection.getColumnRefMap();
            if (columnRefMap == null) {
                return false;
            }
            for (ScalarOperator scalarOperator : columnRefMap.values()) {
                if (hasNonDeterministicFunc(scalarOperator)) {
                    return true;
                }
            }
            return false;
        }

        private boolean checkOptExpression(OptExpression optExpression) {
            Operator operator = optExpression.getOp();
            // projections
            if (operator.getProjection() != null && checkProject(operator.getProjection())) {
                return true;
            }
            // predicates
            if (operator.getPredicate() != null &&
                    hasNonDeterministicFunc(operator.getPredicate())) {
                return true;
            }
            return optExpression.getOp().accept(this, optExpression, null);
        }

        private Boolean visitChildren(OptExpression optExpression) {
            for (OptExpression input : optExpression.getInputs()) {
                if (checkOptExpression(input)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            return visitChildren(optExpression);
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) optExpression.getOp();
            if (scanOperator.getPredicate() != null && hasNonDeterministicFunc(scanOperator.getPredicate())) {
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitLogicalJoin(OptExpression optExpression, Void context) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            if (joinOperator.getOnPredicate() != null &&
                    hasNonDeterministicFunc(joinOperator.getOnPredicate())) {
                return true;
            }
            return visitChildren(optExpression);
        }

        @Override
        public Boolean visitLogicalAggregate(OptExpression optExpression, Void context) {
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            if (checkAggCall(aggregationOperator.getAggregations())) {
                return true;
            }
            return visitChildren(optExpression);
        }

        @Override
        public Boolean visitLogicalWindow(OptExpression optExpression, Void context) {
            LogicalWindowOperator operator = (LogicalWindowOperator) optExpression.getOp();
            if (checkAggCall(operator.getWindowCall())) {
                return true;
            }
            return visitChildren(optExpression);
        }

        @Override
        public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
            Map<ColumnRefOperator, ScalarOperator> map = ((LogicalProjectOperator) optExpression.getOp())
                    .getColumnRefMap();
            for (ScalarOperator scalarOperator : map.values()) {
                if (hasNonDeterministicFunc(scalarOperator)) {
                    return true;
                }
            }
            return visitChildren(optExpression);
        }

        @Override
        public Boolean visitLogicalFilter(OptExpression optExpression, Void context) {
            LogicalFilterOperator filter = (LogicalFilterOperator) optExpression.getOp();
            if (filter.getPredicate() != null && hasNonDeterministicFunc(filter.getPredicate()))  {
                return true;
            }
            return visitChildren(optExpression);
        }
    }
}
