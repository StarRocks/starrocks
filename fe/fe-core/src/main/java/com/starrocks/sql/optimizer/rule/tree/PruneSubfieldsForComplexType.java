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
import com.starrocks.catalog.ComplexTypeAccessPaths;
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
            physicalScanOperator.getColRefToColumnMetaMap().keySet().forEach(context::addScan);            
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
            List<ColumnRefOperator> paramColRefs = operator.getFnParamColumnRefs();
            for (int i = 0; i < operator.getFnResultColRefs().size() && i < paramColRefs.size(); i++) {
                ColumnRefOperator output = operator.getFnResultColRefs().get(i);
                ColumnRefOperator input = paramColRefs.get(i);
                if (output.getType().isComplexType() && context.hasUnnestColRefMapKey(output)) {
                    // Only prune the UNNEST output when its input array is a real prune boundary that
                    // will actually be pruned to matching subfields: a scan column that is not also
                    // fully materialized elsewhere. For a derived input (e.g. a struct subfield kept
                    // full via SELECT t.*) or a fully-materialized scan column, the input array keeps
                    // every subfield, so the output must stay full too. Pruning it would make FE
                    // declare a narrower struct than BE materializes for that input, producing
                    // "encode size does not equal when decoding" on shuffle deserialization.
                    if (!canSafelyPruneUnnestOutput(input, context, optExpression)) {
                        continue;
                    }
                    // Prune the output to the INPUT array column's access group (not the output's own
                    // group), so the output struct equals exactly the element type the input/scan
                    // column is pruned to. The input's group is the union of every access to that array
                    // (multiple UNNESTs of the same array, or the array also read directly), which is
                    // what BE materializes - the output's own group can be a strict subset of it.
                    pruneUnnestOutputToInputGroup(output, input, context);
                    operator.getFn().getTableFnReturnTypes().set(i, output.getType());
                }
            }
            return visit(optExpression, context);
        }

        // The UNNEST output may be pruned only when its input array element is itself pruned to a
        // narrower type. That holds when the input has a non-empty, non-"select all" access group AND
        // is a real prune boundary:
        //   - a scan column, or
        //   - another UNNEST output whose input is (recursively) prunable (stacked UNNEST), or
        //   - a derived struct-subfield column (unnest(c3.c3_sub1)) whose BASE scan column is
        //     (recursively) prunable - following the parameter expression back to its source instead
        //     of blanket-treating every non-scan input as unsafe.
        // If the input (or a derived input's base column) is fully materialized (empty / "select all"
        // access path, e.g. via SELECT t.*), the element stays full, so the output must stay full to
        // match what BE materializes.
        private static boolean canSafelyPruneUnnestOutput(ColumnRefOperator input,
                                                          PruneComplexTypeUtil.Context context,
                                                          OptExpression optExpression) {
            ComplexTypeAccessGroup inputGroup = context.getVisitedAccessGroup(input);
            if (inputGroup == null) {
                return false;
            }
            for (ComplexTypeAccessPaths paths : inputGroup.getAccessGroup()) {
                if (paths.isEmpty()) {
                    return false;
                }
            }
            if (context.getScanRefs().contains(input)) {
                return true;
            }
            ColumnRefOperator innerInput = context.getUnnestInput(input);
            if (innerInput != null) {
                // Stacked UNNEST: the input is itself an UNNEST output, pruned here iff its own input
                // is safely prunable. Recurse so we only prune when the whole chain ends at a pruned
                // scan column, and keep the output full otherwise.
                return canSafelyPruneUnnestOutput(innerInput, context, optExpression);
            }
            // Derived input (e.g. unnest(c3.c3_sub1)): a synthesized column from a child Project, not a
            // scan ref. Follow it to its base column and gate on THAT column, so a base kept full via
            // SELECT t.* keeps the output full while a base pruned narrow prunes the output in lockstep.
            ScalarOperator definition = findDefiningExpr(input, optExpression);
            if (definition != null) {
                List<ColumnRefOperator> baseColumns = definition.getColumnRefs();
                if (baseColumns.size() == 1 && !baseColumns.get(0).equals(input)) {
                    return canSafelyPruneUnnestOutput(baseColumns.get(0), context, optExpression);
                }
            }
            return false;
        }

        // Find the scalar expression that defines a (derived) column by searching projections in the
        // table function's input subtree; null if the column is not produced by a projection.
        private static ScalarOperator findDefiningExpr(ColumnRefOperator col, OptExpression optExpression) {
            for (OptExpression child : optExpression.getInputs()) {
                Projection projection = child.getOp().getProjection();
                if (projection != null && projection.getColumnRefMap().get(col) != null) {
                    return projection.getColumnRefMap().get(col);
                }
                ScalarOperator definition = findDefiningExpr(col, child);
                if (definition != null) {
                    return definition;
                }
            }
            return null;
        }

        private static void pruneUnnestOutputToInputGroup(ColumnRefOperator output, ColumnRefOperator input,
                                                          PruneComplexTypeUtil.Context context) {
            ComplexTypeAccessGroup inputGroup = context.getVisitedAccessGroup(input);
            if (inputGroup == null) {
                return;
            }
            // The input array's access paths are relative to its element, so applying them to the
            // output struct selects the same subfields the input/scan column retains.
            Type cloneType = output.getType().clone();
            PruneComplexTypeUtil.setAccessGroup(cloneType, inputGroup);
            cloneType.pruneUnusedSubfields();
            output.setType(cloneType);
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
            if (accessGroup == null || !context.getScanRefs().contains(columnRefOperator)) {
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
