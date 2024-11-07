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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ComplexTypeAccessGroup;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;

public class PruneSubfieldsForComplexType implements TreeRewriteRule {

    private static final MarkSubfieldsOptVisitor MARK_SUBFIELDS_OPT_VISITOR = new MarkSubfieldsOptVisitor();

    private static final PruneSubfieldsOptVisitor PRUNE_SUBFIELDS_OPT_VISITOR =
            new PruneSubfieldsOptVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        boolean canPrune = taskContext.getOptimizerContext().getSessionVariable().getEnablePruneComplexTypes();
        if (!canPrune) {
            return root;
        }
        // Store all operator's context, used for PRUNE_SUBFIELDS_OPT_VISITOR.
        // globalContext contains all physical operators' context
        PruneComplexTypeUtil.Context context = new PruneComplexTypeUtil.Context(
                taskContext.getOptimizerContext().getSessionVariable().getEnablePruneComplexTypesInUnnest());
        root.getOp().accept(MARK_SUBFIELDS_OPT_VISITOR, root, context);
        if (context.getEnablePruneComplexTypes()) {
            // Still do prune
            root.getOp().accept(PRUNE_SUBFIELDS_OPT_VISITOR, root, context);
        }
        return root;
    }

    private static class MarkSubfieldsOptVisitor extends OptExpressionVisitor<Void, PruneComplexTypeUtil.Context> {

        @Override
        public Void visit(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            // collect output ColumnRefOperator
            RowOutputInfo rowOutputInfo = optExpression.getOp().getRowOutputInfo(optExpression.getInputs());
            List<ColumnOutputInfo> columnOutputInfos = rowOutputInfo.getColumnOutputInfo();
            for (ColumnOutputInfo columnOutputInfo : columnOutputInfos) {
                context.add(columnOutputInfo.getColumnRef(), columnOutputInfo.getScalarOp());
            }

            ScalarOperator predicate = optExpression.getOp().getPredicate();
            if (predicate != null) {
                context.add(null, predicate);
            }

            for (OptExpression opt : optExpression.getInputs()) {
                opt.getOp().accept(this, opt, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalTableFunction(OptExpression optExpression,
                                               PruneComplexTypeUtil.Context context) {
            PhysicalTableFunctionOperator tableFunctionOperator = (PhysicalTableFunctionOperator) optExpression.getOp();
            // only prune type in unnest()
            if (context.isEnablePruneComplexTypesInUnnest() &&
                    tableFunctionOperator.getFn().functionName().equalsIgnoreCase("unnest") &&
                    tableFunctionOperator.getFnParamColumnRefs().size() == tableFunctionOperator.getFnResultColRefs().size()) {
                context.setUnnest(tableFunctionOperator);
            }
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalAnalytic(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            PhysicalWindowOperator physicalWindowOperator = (PhysicalWindowOperator) optExpression.getOp();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : physicalWindowOperator.getAnalyticCall()
                    .entrySet()) {
                context.add(entry.getKey(), entry.getValue());
            }
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression,
                                               PruneComplexTypeUtil.Context context) {
            PhysicalHashAggregateOperator physicalHashAggregateOperator =
                    (PhysicalHashAggregateOperator) optExpression.getOp();
            if (physicalHashAggregateOperator.getAggregations() != null) {
                for (Map.Entry<ColumnRefOperator, CallOperator> entry : physicalHashAggregateOperator.getAggregations()
                        .entrySet()) {
                    context.add(entry.getKey(), entry.getValue());
                }
            }
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalJoin(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            PhysicalJoinOperator physicalJoinOperator = (PhysicalJoinOperator) optExpression.getOp();
            ScalarOperator predicate = physicalJoinOperator.getOnPredicate();
            if (predicate != null) {
                context.add(null, predicate);
            }
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            PhysicalScanOperator physicalScanOperator = (PhysicalScanOperator) optExpression.getOp();
            Projection projection = optExpression.getOp().getProjection();
            if (projection == null) {
                for (ColumnRefOperator columnRefOperator : physicalScanOperator.getOutputColumns()) {
                    if ((columnRefOperator.getType().isMapType() || columnRefOperator.getType().isStructType()) &&
                            !context.hasUnnestColRefMapValue(columnRefOperator)) {
                        context.add(columnRefOperator, columnRefOperator);
                    }
                }
            }
            return visit(optExpression, context);
        }
    }

    private static class PruneSubfieldsOptVisitor extends OptExpressionVisitor<Void, PruneComplexTypeUtil.Context> {
        @Override
        public Void visit(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            Projection projection = optExpression.getOp().getProjection();

            if (projection != null) {
                pruneForColumnRefMap(projection.getColumnRefMap(), context);
                pruneForColumnRefMap(projection.getCommonSubOperatorMap(), context);
            }

            for (OptExpression opt : optExpression.getInputs()) {
                opt.getOp().accept(this, opt, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            PhysicalScanOperator physicalScanOperator = (PhysicalScanOperator) optExpression.getOp();

            if (OperatorType.PHYSICAL_OLAP_SCAN.equals(physicalScanOperator.getOpType()) && !FeConstants.runningUnitTest) {
                // olap scan operator prune column not in this rule
                return null;
            }

            for (Map.Entry<ColumnRefOperator, Column> entry : physicalScanOperator.getColRefToColumnMetaMap()
                    .entrySet()) {
                if (entry.getKey().getType().isComplexType()) {
                    pruneForComplexType(entry.getKey(), context);
                }
            }
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalTableFunction(OptExpression optExpression, PruneComplexTypeUtil.Context context) {
            PhysicalTableFunctionOperator operator = (PhysicalTableFunctionOperator) optExpression.getOp();
            for (int i = 0; i < operator.getFnResultColRefs().size(); i++) {
                ColumnRefOperator output = operator.getFnResultColRefs().get(i);
                if (output.getType().isComplexType() && context.hasUnnestColRefMapKey(output)) {
                    pruneForComplexType(output, context);
                    operator.getFn().getTableFnReturnTypes().set(i, output.getType());
                }
            }
            return visit(optExpression, context);
        }

        private static void pruneForColumnRefMap(Map<ColumnRefOperator, ScalarOperator> map,
                                                 PruneComplexTypeUtil.Context context) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : map.entrySet()) {
                if (entry.getKey().getType().isComplexType()) {
                    pruneForComplexType(entry.getKey(), context);
                    entry.getValue().setType(entry.getKey().getType());
                }
            }
        }

        private static void pruneForComplexType(ColumnRefOperator columnRefOperator,
                                                PruneComplexTypeUtil.Context context) {
            ComplexTypeAccessGroup accessGroup = context.getVisitedAccessGroup(columnRefOperator);
            if (accessGroup == null) {
                return;
            }
            // Clone a new type for prune
            Type cloneType = columnRefOperator.getType().clone();
            PruneComplexTypeUtil.setAccessGroup(cloneType, accessGroup);
            cloneType.pruneUnusedSubfields();
            columnRefOperator.setType(cloneType);
        }
    }
}
