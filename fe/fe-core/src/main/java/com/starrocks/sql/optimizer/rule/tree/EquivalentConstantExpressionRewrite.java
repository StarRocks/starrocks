// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EquivalentConstantExpressionRewrite implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(new Visitor(), root, new RewriteContext());
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<OptExpression, RewriteContext> {

        @Override
        public OptExpression visit(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);
            end(optExpression, context);
            return optExpression;
        }

        public void start(OptExpression optExpression, RewriteContext context) {
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OptExpression child = optExpression.inputAt(childIdx);
                OptExpression rewriteChild = child.getOp().accept(this, child, context);
                optExpression.setChild(childIdx, rewriteChild);
            }
        }

        public void end(OptExpression optExpression, RewriteContext context) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();
            if (predicate != null) {
                context.predicate.addAll(Utils.extractConjuncts(predicate));
            }

            for (ScalarOperator ss : Utils.extractConjuncts(predicate)) {
                if (ss instanceof BinaryPredicateOperator
                        && ((BinaryPredicateOperator) ss).getBinaryType().isEqual()
                        && ss.getChild(0) instanceof ColumnRefOperator
                        && ss.getChild(1) instanceof ConstantOperator) {
                    context.columnRefToConstant.put((ColumnRefOperator) ss.getChild(0), ss.getChild(1));
                }
            }

            //LogicalAnchor
            if (optExpression.getOutputColumns() != null) {
                Iterator<ScalarOperator> iter = context.predicate.iterator();
                while (iter.hasNext()) {
                    ScalarOperator entry = iter.next();
                    if (!optExpression.getOutputColumns().containsAll(entry.getUsedColumns())) {
                        iter.remove();
                    }
                }
            }
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);

            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(context.columnRefToConstant);

            Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();

            List<ColumnRefOperator> groupingKeys = new ArrayList<>();
            for (ColumnRefOperator columnRefOperator : aggregationOperator.getGroupingKeys()) {
                if (context.columnRefToConstant.containsKey(columnRefOperator)) {
                    projection.put(columnRefOperator, context.columnRefToConstant.get(columnRefOperator));
                    continue;
                }
                groupingKeys.add(columnRefOperator);
            }

            if (groupingKeys.isEmpty() && !aggregationOperator.getGroupingKeys().isEmpty()) {
                groupingKeys.add(aggregationOperator.getGroupingKeys().get(0));
            }

            Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
                aggregations.put(entry.getKey(), (CallOperator) rewriter.rewrite(entry.getValue()));
            }

            for (ColumnRefOperator columnRefOperator : groupingKeys) {
                projection.put(columnRefOperator, columnRefOperator);
            }
            for (ColumnRefOperator columnRefOperator : aggregations.keySet()) {
                projection.put(columnRefOperator, columnRefOperator);
            }

            end(optExpression, context);

            OptExpression aggOpt = OptExpression.create(new LogicalAggregationOperator.Builder()
                    .withOperator(aggregationOperator)
                    .setGroupingKeys(groupingKeys)
                    .setAggregations(aggregations)
                    .build(), optExpression.getInputs());
            return OptExpression.create(new LogicalProjectOperator(projection), Lists.newArrayList(aggOpt));
        }

        @Override
        public OptExpression visitLogicalTopN(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);

            LogicalTopNOperator logicalTopNOperator = (LogicalTopNOperator) optExpression.getOp();

            Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();

            List<Ordering> orderingList = new ArrayList<>();
            for (Ordering ordering : logicalTopNOperator.getOrderByElements()) {
                if (context.columnRefToConstant.containsKey(ordering.getColumnRef())) {
                    projection.put(ordering.getColumnRef(), context.columnRefToConstant.get(ordering.getColumnRef()));
                    continue;
                }
                orderingList.add(ordering);
                projection.put(ordering.getColumnRef(), ordering.getColumnRef());
            }

            end(optExpression, context);

            if (orderingList.isEmpty()) {
                return optExpression.inputAt(0);
            }

            if (orderingList.size() == logicalTopNOperator.getOrderByElements().size()) {
                return optExpression;
            }

            OptExpression aggOpt = OptExpression.create(new LogicalTopNOperator.Builder()
                    .withOperator(logicalTopNOperator)
                    .setOrderByElements(orderingList)
                    .build(), optExpression.getInputs());
            return OptExpression.create(new LogicalProjectOperator(projection), Lists.newArrayList(aggOpt));
        }
    }

    static class RewriteContext {
        public List<ScalarOperator> predicate = new ArrayList<>();
        public Map<ColumnRefOperator, ScalarOperator> columnRefToConstant = new HashMap<>();
    }
}