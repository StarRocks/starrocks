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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
 * Push down subfield expression to scan node
 */
public class PushDownSubfieldRule implements TreeRewriteRule {
    private static final ColumnRefSet EMPTY_COLUMN_SET = new ColumnRefSet();

    private ColumnRefFactory factory = null;

    private boolean hasRewrite = false;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        factory = taskContext.getOptimizerContext().getColumnRefFactory();
        return root.getOp().accept(new PushDowner(), root, new Context());
    }

    public boolean hasRewrite() {
        return hasRewrite;
    }

    private static class Context {
        private final Map<ScalarOperator, ColumnRefSet> pushDownExprUseColumns = Maps.newHashMap();
        private final Map<ScalarOperator, ColumnRefOperator> pushDownExprRefsIndex = Maps.newHashMap();
        private final Map<ColumnRefOperator, ScalarOperator> pushDownExprRefs = Maps.newHashMap();

        private void put(ColumnRefOperator index, ScalarOperator subfieldExpr) {
            pushDownExprRefs.put(index, subfieldExpr);
            pushDownExprRefsIndex.put(subfieldExpr, index);
            pushDownExprUseColumns.put(subfieldExpr, subfieldExpr.getUsedColumns());
        }

        private Context copy() {
            Context other = new Context();
            other.pushDownExprRefs.putAll(pushDownExprRefs);
            other.pushDownExprRefsIndex.putAll(pushDownExprRefsIndex);
            other.pushDownExprUseColumns.putAll(pushDownExprUseColumns);
            return other;
        }
    }

    private class PushDowner extends OptExpressionVisitor<OptExpression, Context> {
        private final Map<Integer, Context> cteContextMap = Maps.newHashMap();

        private Optional<ScalarOperator> pushDownPredicate(OptExpression optExpression, Context context,
                                                           ColumnRefSet checkColumns) {
            LogicalOperator operator = optExpression.getOp().cast();
            ScalarOperator predicate = operator.getPredicate();
            if (predicate == null) {
                return Optional.empty();
            }
            boolean needRewritePredicate = false;
            SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
            predicate.accept(collector, null);

            for (ScalarOperator expr : collector.getComplexExpressions()) {
                if (expr.getUsedColumns().isIntersect(checkColumns)) {
                    // predicate use columns in on-predicate, so we can't rewrite it
                    continue;
                }
                needRewritePredicate = true;
                if (context.pushDownExprRefsIndex.containsKey(expr)) {
                    // duplicate subfield expression
                    continue;
                }

                ColumnRefOperator index = factory.create(expr, expr.getType(), expr.isNullable());
                context.put(index, expr);
            }

            // rewrite join predicate
            if (needRewritePredicate) {
                ExpressionReplacer replacer = new ExpressionReplacer(context.pushDownExprRefsIndex);
                ScalarOperator newPredicate = predicate.accept(replacer, null);
                return Optional.of(newPredicate);
            }
            return Optional.empty();
        }

        private OptExpression visitChildren(OptExpression optExpression, Context context) {
            for (int i = optExpression.getInputs().size() - 1; i >= 0; i--) {
                OptExpression child = optExpression.inputAt(i);
                optExpression.setChild(i, child.getOp().accept(this, child, context));
            }
            return optExpression;
        }

        private OptExpression visitChild(OptExpression optExpression, int index, Context context) {
            OptExpression child = optExpression.inputAt(index);
            optExpression.setChild(index, child.getOp().accept(this, child, context));
            return optExpression;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Context context) {
            optExpression = generatePushDownProject(optExpression, EMPTY_COLUMN_SET, context);
            return visitChildren(optExpression, new Context());
        }

        private OptExpression generatePushDownProject(OptExpression optExpression, ColumnRefSet subfieldRefs,
                                                      Context context) {
            if (context.pushDownExprRefs.isEmpty()) {
                return optExpression;
            }

            hasRewrite = true;
            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
            ColumnRefSet output = optExpression.getOutputColumns();
            output.getStream().map(o -> factory.getColumnRef(o)).forEach(k -> newProjectMap.put(k, k));
            subfieldRefs.getStream().map(o -> factory.getColumnRef(o)).forEach(k -> newProjectMap.put(k, k));
            newProjectMap.putAll(context.pushDownExprRefs);

            return OptExpression.create(new LogicalProjectOperator(newProjectMap, optExpression.getOp().getLimit()),
                    optExpression);
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpression, Context context) {
            if (context.pushDownExprRefs.isEmpty() && optExpression.inputAt(0).getInputs().isEmpty()) {
                return visitChildren(optExpression, context);
            }

            LogicalProjectOperator lpo = optExpression.getOp().cast();

            Map<ColumnRefOperator, ScalarOperator> projectMap = lpo.getColumnRefMap();
            // rewrite push down expressions
            if (!context.pushDownExprRefs.isEmpty()) {
                context.pushDownExprRefsIndex.clear();
                context.pushDownExprUseColumns.clear();
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectMap);
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.pushDownExprRefs.entrySet()) {
                    context.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
                }
            }

            // collect & push down expressions
            ColumnRefSet allUsedColumns = new ColumnRefSet();
            context.pushDownExprUseColumns.values().forEach(allUsedColumns::union);

            SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
            for (ScalarOperator value : lpo.getColumnRefMap().values()) {
                // check repeat put complex column, like that
                //      project( columnB: structA.b.c.d )
                //         |
                //      project( structA: structA ) -- structA use for `structA.b.c.d`, so we don't need put it again
                //         |
                //       .....
                if (value.isColumnRef() && allUsedColumns.contains((ColumnRefOperator) value)) {
                    continue;
                }
                value.accept(collector, null);
            }

            for (ScalarOperator expr : collector.getComplexExpressions()) {
                if (context.pushDownExprRefsIndex.containsKey(expr)) {
                    continue;
                }

                ColumnRefOperator index = factory.create(expr, expr.getType(), expr.isNullable());
                context.put(index, expr);
            }

            if (context.pushDownExprRefs.isEmpty()) {
                return visitChildren(optExpression, context);
            }

            // rewrite project node
            ExpressionReplacer replacer = new ExpressionReplacer(context.pushDownExprRefsIndex);
            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
            lpo.getColumnRefMap().forEach((k, v) -> newProjectMap.put(k, v.accept(replacer, null)));
            context.pushDownExprRefs.forEach((k, v) -> newProjectMap.put(k, k));

            optExpression = OptExpression.create(LogicalProjectOperator.builder().withOperator(lpo)
                    .setColumnRefMap(newProjectMap)
                    .build(), optExpression.getInputs());

            return visitChildren(optExpression, context);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Context context) {
            LogicalJoinOperator join = optExpression.getOp().cast();
            ColumnRefSet checkColumns = new ColumnRefSet();

            // check on-predicate used columns
            if (join.getOnPredicate() != null) {
                SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
                join.getOnPredicate().accept(collector, null);
                for (ScalarOperator expr : collector.getComplexExpressions()) {
                    // the expression in on-predicate must was push down to children
                    Preconditions.checkState(expr.isColumnRef());
                    checkColumns.union(expr.getUsedColumns());
                }
            }

            // handle predicate
            Optional<ScalarOperator> predicate = pushDownPredicate(optExpression, context, checkColumns);
            if (predicate.isPresent()) {
                join = LogicalJoinOperator.builder().withOperator(join)
                        .setOnPredicate(predicate.get())
                        .build();
                optExpression = OptExpression.create(join, optExpression.getInputs());
            }

            // split push down expressions to left and right child accord by child's output columns
            Context leftContext = new Context();
            Context rightContext = new Context();
            Context localContext = new Context();

            ColumnRefSet leftOutput = optExpression.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutput = optExpression.inputAt(1).getOutputColumns();
            ColumnRefSet childSubfieldOutputs = new ColumnRefSet();

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.pushDownExprRefs.entrySet()) {
                ColumnRefOperator index = entry.getKey();
                ScalarOperator subfieldExpr = entry.getValue();
                ColumnRefSet subfieldUseColumns = context.pushDownExprUseColumns.get(subfieldExpr);

                if (leftOutput.isIntersect(subfieldUseColumns)) {
                    leftContext.put(index, subfieldExpr);
                    childSubfieldOutputs.union(index);
                } else if (rightOutput.isIntersect(subfieldUseColumns)) {
                    rightContext.put(index, subfieldExpr);
                    childSubfieldOutputs.union(index);
                } else {
                    localContext.put(index, subfieldExpr);
                }
            }

            if (!leftContext.pushDownExprRefs.isEmpty()) {
                visitChild(optExpression, 0, leftContext);
            }
            if (!rightContext.pushDownExprRefs.isEmpty()) {
                visitChild(optExpression, 1, rightContext);
            }
            if (!localContext.pushDownExprRefs.isEmpty()) {
                optExpression = generatePushDownProject(optExpression, childSubfieldOutputs, localContext);
            }
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalUnion(OptExpression optExpression, Context context) {
            Optional<ScalarOperator> predicate = pushDownPredicate(optExpression, context, EMPTY_COLUMN_SET);

            if (context.pushDownExprRefs.isEmpty()) {
                return optExpression;
            }

            // rewrite union node, put all push down column
            LogicalUnionOperator union = optExpression.getOp().cast();
            List<ColumnRefOperator> newOutputColumns = Lists.newArrayList(union.getOutputColumnRefOp());
            List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.pushDownExprRefs.entrySet()) {
                ColumnRefOperator key = entry.getKey();
                newOutputColumns.add(key);
            }

            List<Context> childContexts = Lists.newArrayList();
            for (List<ColumnRefOperator> child : union.getChildOutputColumns()) {
                List<ColumnRefOperator> newChild = Lists.newArrayList(child);

                // rewrite push down expression
                Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
                for (int i = 0; i < newChild.size(); i++) {
                    columnRefMap.put(newOutputColumns.get(i), newChild.get(i));
                }

                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(columnRefMap);

                Context childContext = new Context();
                // add child's output expression column
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.pushDownExprRefs.entrySet()) {
                    ColumnRefOperator key = entry.getKey();
                    ColumnRefOperator newChildOutputRef = factory.create(key, key.getType(), key.isNullable());
                    newChild.add(newChildOutputRef);
                    childContext.put(newChildOutputRef, rewriter.rewrite(entry.getValue()));
                }

                childContexts.add(childContext);
                childOutputColumns.add(newChild);
            }

            union = LogicalUnionOperator.builder().withOperator(union)
                    .setOutputColumnRefOp(newOutputColumns)
                    .setChildOutputColumns(childOutputColumns)
                    .setPredicate(predicate.orElse(union.getPredicate())).build();
            optExpression = OptExpression.create(union, optExpression.getInputs());
            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                visitChild(optExpression, i, childContexts.get(i));
            }
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalWindow(OptExpression optExpression, Context context) {
            if (context.pushDownExprRefs.isEmpty()) {
                return visit(optExpression, context);
            }

            LogicalWindowOperator window = optExpression.getOp().cast();

            ColumnRefSet windowUseColumns = new ColumnRefSet();
            window.getOrderByElements().stream().map(Ordering::getColumnRef).forEach(windowUseColumns::union);
            window.getPartitionExpressions().forEach(p -> windowUseColumns.union(p.getUsedColumns()));
            window.getWindowCall().keySet().forEach(windowUseColumns::union);
            window.getWindowCall().values().forEach(p -> windowUseColumns.union(p.getUsedColumns()));

            Context localContext = new Context();
            Context childContext = new Context();
            ColumnRefSet childSubfieldOutputs = new ColumnRefSet();

            for (Map.Entry<ScalarOperator, ColumnRefSet> entry : context.pushDownExprUseColumns.entrySet()) {
                ScalarOperator expr = entry.getKey();
                ColumnRefSet useColumns = entry.getValue();

                if (windowUseColumns.isIntersect(useColumns)) {
                    localContext.put(context.pushDownExprRefsIndex.get(expr), expr);
                } else {
                    childContext.put(context.pushDownExprRefsIndex.get(expr), expr);
                    childSubfieldOutputs.union(context.pushDownExprRefsIndex.get(expr));
                }
            }

            if (!localContext.pushDownExprRefs.isEmpty()) {
                optExpression = generatePushDownProject(optExpression, childSubfieldOutputs, localContext);
            }

            Optional<ScalarOperator> predicate = pushDownPredicate(optExpression, context, windowUseColumns);

            if (predicate.isPresent()) {
                window = LogicalWindowOperator.builder().withOperator(window)
                        .setPredicate(predicate.get())
                        .build();
                optExpression = OptExpression.create(window, optExpression.getInputs());
            }

            return visitChildren(optExpression, childContext);
        }

        @Override
        public OptExpression visitLogicalCTEAnchor(OptExpression optExpression, Context context) {
            visitChild(optExpression, 1, context);

            LogicalCTEAnchorOperator anchor = optExpression.getOp().cast();
            visitChild(optExpression, 0, cteContextMap.getOrDefault(anchor.getCteId(), new Context()));
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalCTEConsume(OptExpression optExpression, Context context) {
            Optional<ScalarOperator> predicate = pushDownPredicate(optExpression, context, EMPTY_COLUMN_SET);

            if (context.pushDownExprRefs.isEmpty()) {
                return visitChildren(optExpression, context);
            }

            LogicalCTEConsumeOperator consume = optExpression.getOp().cast();

            Map<ColumnRefOperator, ColumnRefOperator> newCteRefMap = Maps.newHashMap();

            // cte context
            Context cteContext = cteContextMap.getOrDefault(consume.getCteId(), new Context());
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(consume.getCteOutputColumnRefMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.pushDownExprRefs.entrySet()) {
                ColumnRefOperator key = entry.getKey();
                ScalarOperator value = entry.getValue();
                ScalarOperator cteExpr = rewriter.rewrite(value);

                if (cteContext.pushDownExprRefsIndex.containsKey(cteExpr)) {
                    // exists cte output expr in producer
                    newCteRefMap.put(key, cteContext.pushDownExprRefsIndex.get(cteExpr));
                } else {
                    ColumnRefOperator cteRef = factory.create(key, key.getType(), key.isNullable());
                    newCteRefMap.put(key, cteRef);
                    cteContext.put(cteRef, cteExpr);
                }
            }
            cteContextMap.put(consume.getCteId(), cteContext);

            newCteRefMap.putAll(consume.getCteOutputColumnRefMap());
            consume = LogicalCTEConsumeOperator.builder().withOperator(consume)
                    .setCteOutputColumnRefMap(newCteRefMap)
                    .setPredicate(predicate.orElse(consume.getPredicate()))
                    .build();

            optExpression = OptExpression.create(consume, optExpression.getInputs());

            // inline plan
            if (!optExpression.getInputs().isEmpty()) {
                Context inlineContext = context.copy();
                optExpression = visitChild(optExpression, 0, inlineContext);
            }

            return optExpression;
        }

        /* push down direct */
        @Override
        public OptExpression visitLogicalFilter(OptExpression optExpression, Context context) {
            Optional<ScalarOperator> predicate = pushDownPredicate(optExpression, context, EMPTY_COLUMN_SET);
            if (predicate.isPresent()) {
                optExpression =
                        OptExpression.create(new LogicalFilterOperator(predicate.get()), optExpression.getInputs());
            }
            return visitChildren(optExpression, context);
        }

        @Override
        public OptExpression visitLogicalCTEProduce(OptExpression optExpression, Context context) {
            return visitChildren(optExpression, context);
        }

        @Override
        public OptExpression visitLogicalLimit(OptExpression optExpression, Context context) {
            return visitChildren(optExpression, context);
        }

        @Override
        public OptExpression visitLogicalAssertOneRow(OptExpression optExpression, Context context) {
            return visitChildren(optExpression, context);
        }
    }

    private static class ExpressionReplacer extends BaseScalarOperatorShuttle {
        private final Map<ScalarOperator, ColumnRefOperator> subfieldExprRefs;

        public ExpressionReplacer(Map<ScalarOperator, ColumnRefOperator> subfieldExprRefs) {
            this.subfieldExprRefs = subfieldExprRefs;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (subfieldExprRefs.containsKey(call)) {
                return subfieldExprRefs.get(call);
            }
            return super.visitCall(call, context);
        }

        @Override
        public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
            if (subfieldExprRefs.containsKey(collectionElementOp)) {
                return subfieldExprRefs.get(collectionElementOp);
            }
            return super.visitCollectionElement(collectionElementOp, context);
        }

        @Override
        public ScalarOperator visitSubfield(SubfieldOperator operator, Void context) {
            if (subfieldExprRefs.containsKey(operator)) {
                return subfieldExprRefs.get(operator);
            }
            return super.visitSubfield(operator, context);
        }
    }

}
