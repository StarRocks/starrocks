// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarEquivalenceExtractor;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BottomUp implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        ColumnRefFactory columnRefFactory = taskContext.getOptimizerContext().getColumnRefFactory();
        root.getOp().accept(new Visitor(columnRefFactory), root, new RewriteContext());
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<Void, RewriteContext> {
        private final ColumnRefFactory columnRefFactory;

        public Visitor(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        @Override
        public Void visit(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);
            end(optExpression, context);
            return null;
        }

        public void start(OptExpression optExpression, RewriteContext context) {
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OptExpression child = optExpression.inputAt(childIdx);
                child.getOp().accept(this, child, context);
            }
        }

        public void end(OptExpression optExpression, RewriteContext context) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();

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
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iterator =
                        context.columnRefToConstant.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iterator.next();
                    if (!optExpression.getOutputColumns().contains(entry.getKey())) {
                        iterator.remove();
                    }
                }
            }
        }

        @Override
        public Void visitLogicalFilter(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);
            LogicalFilterOperator filterOperator = (LogicalFilterOperator) optExpression.getOp();

            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(filterOperator.getPredicate());
            scalarEquivalenceExtractor.union(inputPredicates);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.columnRefToConstant.entrySet()) {
                scalarEquivalenceExtractor.union(Lists.newArrayList(
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, entry.getKey(), entry.getValue())));
            }

            ScalarOperator scalarOperator = filterOperator.getPredicate();
            for (int columnId : optExpression.inputAt(0).getOutputColumns().getColumnIds()) {
                ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);
                Set<ScalarOperator> equalPredicate = scalarEquivalenceExtractor.getEquivalentScalar(columnRefOperator);

                scalarOperator = Utils.compoundAnd(scalarOperator, Utils.compoundAnd(new ArrayList<>(equalPredicate)));
            }
            filterOperator.setPredicate(scalarOperator);
            end(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();

            if (joinOperator.getJoinType().isLeftOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = context.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();
                    if (optExpression.inputAt(1).getOutputColumns().contains(entry.getKey())) {
                        context.columnRefToConstant.remove(entry.getKey());
                    }
                }
            } else if (joinOperator.getJoinType().isRightOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = context.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();
                    if (optExpression.inputAt(0).getOutputColumns().contains(entry.getKey())) {
                        context.columnRefToConstant.remove(entry.getKey());
                    }
                }
            }

            end(optExpression, context);
            return null;
        }
    }

    static class RewriteContext {
        public Map<ColumnRefOperator, ScalarOperator> columnRefToConstant = new HashMap<>();
    }
}