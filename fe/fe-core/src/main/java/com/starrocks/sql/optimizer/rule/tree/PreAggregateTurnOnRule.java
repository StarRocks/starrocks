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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This rule will turn on PreAggregation if conditions are met,
 * and turning on PreAggregation will help optimize the storage layer merge on read.
 * This rule traverses the query plan from the top down, and the Olap Scan node determines
 * whether preAggregation can be turned on or not based on the information recorded by the PreAggregationContext.
 * <p>
 * A cross join cannot turn on PreAggregation, and other types of joins can only be turn on on one side.
 * If both sides are opened, many-to-many join results will appear, leading to errors in the upper aggregation results
 */
public class PreAggregateTurnOnRule implements TreeRewriteRule {
    private static final PreAggregateVisitor VISITOR = new PreAggregateVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        boolean hasAggregation = false;
        List<PhysicalOlapScanOperator> scans = Lists.newArrayList();
        Utils.extractOperator(root, scans, o -> (o instanceof PhysicalOlapScanOperator));
        for (PhysicalOlapScanOperator scan : scans) {
            // default false
            scan.setPreAggregation(false);
            long selectedIndex = scan.getSelectedIndexId();
            MaterializedIndexMeta meta = ((OlapTable) scan.getTable()).getIndexMetaByIndexId(selectedIndex);
            if (!meta.getKeysType().isAggregationFamily()) {
                scan.setPreAggregation(true);
                scan.setTurnOffReason("");
            } else {
                hasAggregation = true;
            }
        }

        if (hasAggregation) {
            root.getOp().accept(VISITOR, root, new PreAggregationContext());
        }
        return root;
    }

    private static class PreAggregateVisitor extends OptExpressionVisitor<Void, PreAggregationContext> {
        private static final List<String> AGGREGATE_ONLY_KEY = ImmutableList.<String>builder()
                .add(FunctionSet.NDV)
                .add(FunctionSet.MULTI_DISTINCT_COUNT)
                .add(FunctionSet.APPROX_COUNT_DISTINCT)
                .add(FunctionSet.DS_HLL_COUNT_DISTINCT)
                .add(FunctionSet.DS_THETA_COUNT_DISTINCT)
                .add(FunctionSet.BITMAP_UNION_INT.toUpperCase()).build();

        @Override
        public Void visit(OptExpression opt, PreAggregationContext context) {
            opt.getInputs().forEach(o -> process(o, context));
            return null;
        }

        public Void process(OptExpression opt, PreAggregationContext context) {
            if (opt.getOp().getProjection() != null) {
                rewriteProject(opt, context);
            }

            opt.getOp().accept(this, opt, context);
            return null;
        }

        void rewriteProject(OptExpression opt, PreAggregationContext context) {
            Projection projection = opt.getOp().getProjection();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projection.getColumnRefMap());

            context.aggregations = context.aggregations.stream()
                    .map(rewriter::rewriteWithoutClone)
                    .collect(Collectors.toList());

            context.groupings = context.groupings.stream()
                    .map(rewriter::rewriteWithoutClone)
                    .collect(Collectors.toList());

            context.joinPredicates = context.joinPredicates.stream().filter(Objects::nonNull)
                    .map(rewriter::rewriteWithoutClone)
                    .collect(Collectors.toList());
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, PreAggregationContext context) {
            PhysicalHashAggregateOperator aggregate = (PhysicalHashAggregateOperator) optExpression.getOp();
            // Only save the recently aggregate
            context.aggregations =
                    aggregate.getAggregations().values().stream().map(CallOperator::clone).collect(Collectors.toList());
            context.groupings =
                    aggregate.getGroupBys().stream().map(ScalarOperator::clone).collect(Collectors.toList());
            context.notPreAggregationJoin = false;

            return process(optExpression.inputAt(0), context);
        }

        @Override
        public Void visitPhysicalOlapScan(OptExpression optExpression, PreAggregationContext context) {
            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();

            // default false
            scan.setPreAggregation(false);

            long selectedIndex = scan.getSelectedIndexId();
            OlapTable olapTable = ((OlapTable) scan.getTable());
            MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexMetaByIndexId(selectedIndex);
            if (!materializedIndexMeta.getKeysType().isAggregationFamily()) {
                scan.setPreAggregation(true);
                scan.setTurnOffReason("");
                return null;
            }

            if (context.notPreAggregationJoin) {
                scan.setTurnOffReason("Has can not pre-aggregation Join");
                return null;
            }

            if (context.aggregations.isEmpty() && context.groupings.isEmpty()) {
                scan.setTurnOffReason("None aggregate function");
                return null;
            }
            // check has value conjunct
            List<ColumnRefOperator> predicateColRefs =
                    Utils.extractColumnRef(Utils.compoundAnd(scan.getPredicate(), Utils.compoundAnd(context.joinPredicates)));
            boolean allKeyConjunct = predicateColRefs.stream()
                    .map(ref -> scan.getColRefToColumnMetaMap().get(ref)).filter(Objects::nonNull)
                    .allMatch(Column::isKey);
            if (!allKeyConjunct) {
                scan.setTurnOffReason("Predicates include the value column");
                return null;
            }

            // check grouping
            if (checkGroupings(context, scan)) {
                return null;
            }

            // check aggregation function
            if (checkAggregations(context, scan)) {
                return null;
            }

            scan.setPreAggregation(true);
            scan.setTurnOffReason("");
            return null;
        }

        private boolean checkGroupings(PreAggregationContext context, PhysicalOlapScanOperator scan) {
            Map<ColumnRefOperator, Column> refColumnMap = scan.getColRefToColumnMetaMap();

            List<ColumnRefOperator> groups = Lists.newArrayList();
            context.groupings.stream().map(Utils::extractColumnRef).forEach(groups::addAll);

            if (groups.stream().anyMatch(d -> !refColumnMap.containsKey(d))) {
                scan.setTurnOffReason("Group columns isn't bound table " + scan.getTable().getName());
                return true;
            }

            if (groups.stream().anyMatch(d -> !refColumnMap.get(d).isKey())) {
                scan.setTurnOffReason("Group columns isn't Key column");
                return true;
            }
            return false;
        }

        private boolean checkAggregations(PreAggregationContext context, PhysicalOlapScanOperator scan) {
            Map<ColumnRefOperator, Column> refColumnMap = scan.getColRefToColumnMetaMap();

            for (final ScalarOperator so : context.aggregations) {
                Preconditions.checkState(OperatorType.CALL.equals(so.getOpType()));

                CallOperator call = (CallOperator) so;
                if (call.getChildren().size() != 1) {
                    scan.setTurnOffReason("Aggregate function has more than one parameter");
                    return true;
                }

                ScalarOperator child = call.getChild(0);

                if (child instanceof CallOperator &&
                        FunctionSet.IF.equalsIgnoreCase(((CallOperator) child).getFnName())) {
                    child = new CaseWhenOperator(child.getType(), null, child.getChild(2),
                            Lists.newArrayList(child.getChild(0), child.getChild(1)));
                }

                List<ColumnRefOperator> returns = Lists.newArrayList();
                List<ColumnRefOperator> conditions = Lists.newArrayList();

                if (OperatorType.VARIABLE.equals(child.getOpType())) {
                    returns.add(child.cast());
                } else if (child instanceof CastOperator
                        && OperatorType.VARIABLE.equals(child.getChild(0).getOpType())) {
                    if (child.getType().isNumericType() && child.getChild(0).getType().isNumericType()) {
                        returns.add((ColumnRefOperator) child.getChild(0));
                    } else {
                        scan.setTurnOffReason("The parameter of aggregate function isn't numeric type");
                        return true;
                    }
                } else if (child instanceof CaseWhenOperator) {
                    CaseWhenOperator cwo = (CaseWhenOperator) child;

                    for (int i = 0; i < cwo.getWhenClauseSize(); i++) {
                        if (cwo.getThenClause(i).isColumnRef()) {
                            conditions.addAll(Utils.extractColumnRef(cwo.getWhenClause(i)));
                            returns.add((ColumnRefOperator) cwo.getThenClause(i));
                        } else if (cwo.getThenClause(i).isConstantNull()
                                || cwo.getThenClause(i).isConstantZero()) {
                            // If then expr is NULL or Zero, open the preaggregation
                        } else {
                            scan.setTurnOffReason("The result of THEN isn't value column");
                            return true;
                        }
                    }

                    if (cwo.hasCase()) {
                        conditions.addAll(Utils.extractColumnRef(cwo.getCaseClause()));
                    }

                    if (cwo.hasElse()) {
                        if (cwo.getElseClause().isColumnRef()) {
                            returns.add((ColumnRefOperator) cwo.getElseClause());
                        } else if (cwo.getElseClause().isConstantNull()
                                || cwo.getElseClause().isConstantZero()) {
                            // If else expr is NULL or Zero, open the preaggregation
                        } else {
                            scan.setTurnOffReason("The result of ELSE isn't value column");
                            return true;
                        }
                    }
                } else {
                    scan.setTurnOffReason(
                            "The parameter of aggregate function isn't value column or CAST/CASE-WHEN expression");
                    return true;
                }

                // check conditions
                if (conditions.stream().anyMatch(d -> !refColumnMap.containsKey(d))) {
                    scan.setTurnOffReason("The column of aggregate function isn't bound " + scan.getTable().getName());
                    return true;
                }

                if (conditions.stream().anyMatch(d -> !refColumnMap.get(d).isKey())) {
                    scan.setTurnOffReason("The column of aggregate function isn't key");
                    return true;
                }

                // check returns
                for (ColumnRefOperator ref : returns) {
                    if (!refColumnMap.containsKey(ref)) {
                        scan.setTurnOffReason(
                                "The column of aggregate function isn't bound " + scan.getTable().getName());
                        return true;
                    }

                    Column column = refColumnMap.get(ref);
                    // key column
                    if (column.isKey()) {
                        if (!FunctionSet.MAX.equalsIgnoreCase(call.getFnName()) &&
                                !FunctionSet.MIN.equalsIgnoreCase(call.getFnName()) &&
                                !AGGREGATE_ONLY_KEY.contains(call.getFnName().toLowerCase())) {
                            scan.setTurnOffReason("The key column don't support aggregate function: "
                                    + call.getFnName().toUpperCase());
                            return true;
                        }
                        continue;
                    }

                    // value column
                    if (FunctionSet.HLL_UNION_AGG.equalsIgnoreCase(call.getFnName()) ||
                            FunctionSet.HLL_RAW_AGG.equalsIgnoreCase(call.getFnName())) {
                        // skip
                    } else if (AGGREGATE_ONLY_KEY.contains(call.getFnName().toLowerCase())) {
                        scan.setTurnOffReason(
                                "Aggregation function " + call.getFnName().toUpperCase() + " just work on key column");
                        return true;
                    } else if ((FunctionSet.BITMAP_UNION.equalsIgnoreCase(call.getFnName())
                            || FunctionSet.BITMAP_UNION_COUNT.equalsIgnoreCase(call.getFnName()))) {
                        if (!AggregateType.BITMAP_UNION.equals(column.getAggregationType())) {
                            scan.setTurnOffReason(
                                    "Aggregate Operator not match: BITMAP_UNION <--> " + column.getAggregationType());
                            return true;
                        }
                    } else if (!call.getFnName().equalsIgnoreCase(column.getAggregationType().name())) {
                        scan.setTurnOffReason(
                                "Aggregate Operator not match: " + call.getFnName().toUpperCase() + " <--> " + column
                                        .getAggregationType().name().toUpperCase());
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, PreAggregationContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public Void visitPhysicalMergeJoin(OptExpression optExpression, PreAggregationContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public Void visitPhysicalNestLoopJoin(OptExpression optExpr, PreAggregationContext context) {
            return visitPhysicalJoin(optExpr, context);
        }

        public Void visitPhysicalJoin(OptExpression optExpression, PreAggregationContext context) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) optExpression.getOp();
            OptExpression leftChild = optExpression.getInputs().get(0);
            OptExpression rightChild = optExpression.getInputs().get(1);

            ColumnRefSet leftOutputColumns = optExpression.getInputs().get(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = optExpression.getInputs().get(1).getOutputColumns();

            List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(leftOutputColumns,
                    rightOutputColumns, Utils.extractConjuncts(joinOperator.getOnPredicate()));
            // cross join can not do pre-aggregation
            if (joinOperator.getJoinType().isCrossJoin() || eqOnPredicates.isEmpty()) {
                context.notPreAggregationJoin = true;
                context.groupings.clear();
                context.aggregations.clear();
                process(optExpression.inputAt(0), context);
                // Avoid left child modify context will effect right child
                process(optExpression.inputAt(1), context);
                return null;
            }

            // For other types of joins, only one side can turn on pre aggregation which side has aggregation.
            // The columns used by the aggregate functions can only be the columns of one of the children,
            // the olap scan node will turn off pre aggregation if aggregation function used both sides columns,
            // this can be guaranteed by checkAggregations in visitPhysicalOlapScan.
            ColumnRefSet aggregationColumns = new ColumnRefSet();
            List<ScalarOperator> leftGroupOperator = Lists.newArrayList();
            List<ScalarOperator> rightGroupOperator = Lists.newArrayList();

            context.groupings.forEach(g -> {
                if (g.getUsedColumns().isIntersect(leftOutputColumns)) {
                    leftGroupOperator.add(g);
                }
            });
            context.groupings.forEach(g -> {
                if (g.getUsedColumns().isIntersect(rightOutputColumns)) {
                    rightGroupOperator.add(g);
                }
            });
            context.aggregations.forEach(a -> aggregationColumns.union(a.getUsedColumns()));
            boolean checkLeft = leftOutputColumns.containsAll(aggregationColumns);
            boolean checkRight = rightOutputColumns.containsAll(aggregationColumns);
            // Add join on predicate and predicate to context
            if (joinOperator.getOnPredicate() != null) {
                context.joinPredicates.add(joinOperator.getOnPredicate().clone());
            }
            if (joinOperator.getPredicate() != null) {
                context.joinPredicates.add(joinOperator.getPredicate().clone());
            }

            PreAggregationContext disableContext = new PreAggregationContext();
            disableContext.notPreAggregationJoin = true;

            if (checkLeft) {
                context.groupings = leftGroupOperator;
                process(leftChild, context);
                process(rightChild, disableContext);
            } else if (checkRight) {
                context.groupings = rightGroupOperator;
                process(rightChild, context);
                process(leftChild, disableContext);
            } else {
                process(leftChild, disableContext);
                process(rightChild, disableContext);
            }
            return null;
        }
    }

    public static class PreAggregationContext {
        // Indicates that a pre-aggregation can not be turned on below the join
        public boolean notPreAggregationJoin = false;
        public List<ScalarOperator> aggregations = Lists.newArrayList();
        public List<ScalarOperator> groupings = Lists.newArrayList();
        public List<ScalarOperator> joinPredicates = Lists.newArrayList();
    }
}
