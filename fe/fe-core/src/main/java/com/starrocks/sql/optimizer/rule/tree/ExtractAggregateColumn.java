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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Map;
import java.util.Set;

public class ExtractAggregateColumn implements TreeRewriteRule {
    ColumnRefFactory columnRefFactory = null;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        columnRefFactory = taskContext.getOptimizerContext().getColumnRefFactory();
        root.getOp().accept(new ExtractAggregateVisitor(), root, null);
        return root;
    }

    public class ExtractAggregateVisitor extends OptExpressionVisitor<Void, Void> {
        // TODO: remove this if BE aggregate node support dict mapping expr
        private boolean hasDictMappingOperator(ScalarOperator operator) {
            if (operator instanceof DictMappingOperator) {
                return true;
            }
            for (ScalarOperator child : operator.getChildren()) {
                if (hasDictMappingOperator(child)) {
                    return true;
                }
            }

            return false;
        }

        private boolean hasNonColumnRefParameter(PhysicalHashAggregateOperator aggregateOperator) {
            for (CallOperator value : aggregateOperator.getAggregations().values()) {
                for (ScalarOperator child : value.getChildren()) {
                    if (!child.isColumnRef()) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void rewriteAggregateOperator(PhysicalHashAggregateOperator aggregateOperator, Projection projection) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
            // record the column has been extracted
            Set<ColumnRefOperator> extractedColumns = Sets.newHashSet();

            // sum(func(col1)), max(func(col1)) won't be extract to aggregate
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregateOperator.getAggregations()
                    .entrySet()) {
                for (ScalarOperator child : entry.getValue().getChildren()) {
                    if (!child.isColumnRef()) {
                        return;
                    }
                    ColumnRefOperator childRef = (ColumnRefOperator) child;
                    if (extractedColumns.contains(childRef)) {
                        rewriteMap.remove(childRef);
                    } else {
                        ScalarOperator scalarOperator = columnRefMap.get(childRef);
                        // TODO: remove this if fix meta scan bug
                        if (scalarOperator == null) {
                            return;
                        }
                        if (!scalarOperator.isColumnRef() && !hasDictMappingOperator(scalarOperator) &&
                                !(scalarOperator.getOpType() == OperatorType.SUBFIELD)) {
                            rewriteMap.put(childRef, scalarOperator);
                            extractedColumns.add(childRef);
                        }
                    }
                }
            }

            // replace aggregate column
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
            aggregateOperator.getAggregations().forEach((k, v) -> {
                for (int i = 0; i < v.getChildren().size(); i++) {
                    v.setChild(i, rewriter.rewrite(v.getChild(i)));
                }
            });

            // get all used column
            ColumnRefSet usedColumns = new ColumnRefSet();
            for (ScalarOperator value : rewriteMap.values()) {
                usedColumns.union(value.getUsedColumns());
            }

            // remove rewrite expression from projection
            for (ColumnRefOperator columnRefOperator : rewriteMap.keySet()) {
                columnRefMap.remove(columnRefOperator);
            }

            // common expression
            if (!projection.getCommonSubOperatorMap().isEmpty()) {
                final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap =
                        projection.getCommonSubOperatorMap();
                for (ColumnRefOperator columnRefOperator : commonSubOperatorMap.keySet()) {
                    if (usedColumns.contains(columnRefOperator)) {
                        columnRefMap.put(columnRefOperator, columnRefOperator);
                    }
                }
            }

            // append used column
            for (int columnId : usedColumns.getColumnIds()) {
                final ColumnRefOperator columnRef = columnRefFactory.getColumnRef(columnId);
                if (!columnRefMap.containsKey(columnRef)) {
                    columnRefMap.put(columnRef, columnRef);
                }
            }
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
            // child has projection
            Projection projection = optExpression.getInputs().get(0).getOp().getProjection();
            if (projection != null && aggOperator.getGroupBys().isEmpty()) {

                boolean unSupported = hasNonColumnRefParameter(aggOperator);

                if (!unSupported) {
                    rewriteAggregateOperator(aggOperator, projection);
                }

                if (projection.getColumnRefMap().isEmpty()) {
                    optExpression.getInputs().get(0).getOp().setProjection(null);
                }
            }

            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }
    }

}
