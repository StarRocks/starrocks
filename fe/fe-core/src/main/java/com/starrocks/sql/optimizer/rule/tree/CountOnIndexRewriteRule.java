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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

/**
 * Marks a MATCH scan below a local/one-phase {@code COUNT(*)} so the storage layer can avoid
 * materializing columns whose predicates have been completely evaluated by the inverted index.
 *
 * <p>The aggregate remains in the plan and still counts the rows emitted by the scan. This keeps
 * delete-vector, residual-predicate and distributed aggregation semantics unchanged while removing
 * the expensive base-column read for the indexed text column.
 */
public class CountOnIndexRewriteRule implements TreeRewriteRule {
    private static final CountOnIndexVisitor VISITOR = new CountOnIndexVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(VISITOR, root, null);
        return root;
    }

    private static class CountOnIndexVisitor extends OptExpressionVisitor<Void, Void> {
        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator aggregate = optExpression.getOp().cast();
            if (isCountStarAggregate(aggregate) && optExpression.getInputs().size() == 1) {
                PhysicalOlapScanOperator scan = findTransparentScan(optExpression.inputAt(0));
                if (scan != null && containsMatch(scan.getPredicate())) {
                    scan.setCountOnIndex(true);
                }
            }
            return visit(optExpression, context);
        }

        private boolean isCountStarAggregate(PhysicalHashAggregateOperator aggregate) {
            if (!(aggregate.getType().isLocal() || aggregate.isOnePhaseAgg()) || !aggregate.getGroupBys().isEmpty() ||
                    aggregate.getAggregations().isEmpty()) {
                return false;
            }
            for (CallOperator call : aggregate.getAggregations().values()) {
                if (!call.isCountStar() || call.isDistinct() || call.isRemovedDistinct()) {
                    return false;
                }
            }
            return true;
        }

        private PhysicalOlapScanOperator findTransparentScan(OptExpression expression) {
            if (expression.getOp() instanceof PhysicalOlapScanOperator) {
                return expression.getOp().cast();
            }
            OperatorType type = expression.getOp().getOpType();
            if (expression.getInputs().size() != 1 ||
                    (type != OperatorType.PHYSICAL_PROJECT && type != OperatorType.PHYSICAL_DISTRIBUTION &&
                            type != OperatorType.PHYSICAL_DECODE)) {
                return null;
            }
            return findTransparentScan(expression.inputAt(0));
        }

        private boolean containsMatch(ScalarOperator predicate) {
            if (predicate == null) {
                return false;
            }
            if (predicate instanceof MatchExprOperator) {
                return true;
            }
            for (ScalarOperator child : predicate.getChildren()) {
                if (containsMatch(child)) {
                    return true;
                }
            }
            return false;
        }
    }
}
