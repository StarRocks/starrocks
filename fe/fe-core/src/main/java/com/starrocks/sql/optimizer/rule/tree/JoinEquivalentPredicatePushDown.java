// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarEquivalenceExtractor;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinPredicateBase;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class JoinEquivalentPredicatePushDown implements TreeRewriteRule {

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

        public Void visit(OptExpression optExpression, RewriteContext context) {
            //优先遍历子孩子
            for (OptExpression o : optExpression.getInputs()) {
                o.getOp().accept(this, o, context);
            }

            ScalarOperator predicate = optExpression.getOp().getPredicate();
            if (predicate != null) {
                context.columnRefToConstant.addAll(Utils.extractConjuncts(predicate));
            }

            //LogicalAnchor
            if (optExpression.getOutputColumns() != null) {
                Iterator<ScalarOperator> iter = context.columnRefToConstant.iterator();
                while (iter.hasNext()) {
                    ScalarOperator entry = iter.next();
                    if (!optExpression.getOutputColumns().containsAll(entry.getUsedColumns())) {
                        iter.remove();
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, RewriteContext context) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();

            List<ScalarOperator> pushDownToLeft = new ArrayList<>();
            List<ScalarOperator> pushDownToRight = new ArrayList<>();

            if (joinOperator.getJoinType().isFullOuterJoin()) {
                return null;
            }

            if (!joinOperator.getJoinType().isRightOuterJoin() && !joinOperator.getJoinType().isRightAntiJoin()) {
                getPredicatePushDownToRight(optExpression, joinOperator, pushDownToRight);
            }

            if (!joinOperator.getJoinType().isLeftOuterJoin() && !joinOperator.getJoinType().isLeftAntiJoin()) {
                getPredicatePushDownToLeft(optExpression, joinOperator, pushDownToLeft);
            }

            PushDownJoinPredicateBase.pushDownPredicate(optExpression, pushDownToLeft, pushDownToRight);
            return null;
        }

        void getPredicatePushDownToRight(OptExpression optExpression, LogicalJoinOperator joinOperator,
                                         List<ScalarOperator> pushDownToRight) {
            RewriteContext rleft = new RewriteContext();
            optExpression.inputAt(0).getOp().accept(this, optExpression.inputAt(0), rleft);

            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());
            scalarEquivalenceExtractor.union(inputPredicates);
            for (ScalarOperator entry : rleft.columnRefToConstant) {
                scalarEquivalenceExtractor.union(Utils.extractConjuncts(entry));
            }

            for (int columnId : optExpression.getInputs().get(1).getOutputColumns().getColumnIds()) {
                ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);
                Set<ScalarOperator> equalPredicate = scalarEquivalenceExtractor.getEquivalentScalar(columnRefOperator);

                for (ScalarOperator scalarOperator : equalPredicate) {
                    if (optExpression.getInputs().get(1).getOutputColumns().containsAll(scalarOperator.getUsedColumns())) {
                        pushDownToRight.add(scalarOperator);
                    }
                }
            }
        }

        void getPredicatePushDownToLeft(OptExpression optExpression, LogicalJoinOperator joinOperator,
                                        List<ScalarOperator> pushDownToLeft) {
            RewriteContext rright = new RewriteContext();
            optExpression.inputAt(1).getOp().accept(this, optExpression.inputAt(1), rright);

            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());
            scalarEquivalenceExtractor.union(inputPredicates);
            for (ScalarOperator entry : rright.columnRefToConstant) {
                scalarEquivalenceExtractor.union(Utils.extractConjuncts(entry));
            }

            for (int columnId : optExpression.getInputs().get(0).getOutputColumns().getColumnIds()) {
                ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);
                Set<ScalarOperator> equalPredicate = scalarEquivalenceExtractor.getEquivalentScalar(columnRefOperator);

                for (ScalarOperator scalarOperator : equalPredicate) {
                    if (optExpression.getInputs().get(0).getOutputColumns().containsAll(scalarOperator.getUsedColumns())) {
                        pushDownToLeft.add(scalarOperator);
                    }
                }
            }
        }
    }

    static class RewriteContext {
        public List<ScalarOperator> columnRefToConstant = new ArrayList<>();
    }
}