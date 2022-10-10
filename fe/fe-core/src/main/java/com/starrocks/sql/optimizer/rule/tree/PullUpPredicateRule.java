// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PullUpPredicateRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        RewriteContext rewriteContext = new RewriteContext();
        root.getOp().accept(new Visitor(), root, rewriteContext);

        //Convert all remaining predicates to LogicalFilter
        convertPredicateToLogicalFilter(root, rewriteContext);
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<Void, RewriteContext> {

        public Visitor() {
        }

        @Override
        public Void visit(OptExpression optExpression, RewriteContext context) {
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OptExpression child = optExpression.inputAt(childIdx);

                RewriteContext rewriteContext = new RewriteContext();
                child.getOp().accept(this, child, rewriteContext);
                OptExpression c = handleLegacyPredicate(optExpression.getOutputColumns(), child, rewriteContext);
                optExpression.setChild(childIdx, c);

                context.columnRefToConstant.putAll(rewriteContext.columnRefToConstant);
            }

            return null;
        }

        public OptExpression handleLegacyPredicate(ColumnRefSet parentInputColumns, OptExpression optExpression,
                                                   RewriteContext context) {
            OptExpression root = optExpression;
            if (parentInputColumns != null) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iterator =
                        context.columnRefToConstant.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iterator.next();
                    if (!parentInputColumns.contains(entry.getKey())) {

                        // If the output columns does not contain this predicate,
                        // leave the predicate under this node to provide more possibilities for subsequent predicate push down
                        root = OptExpression.create(new LogicalFilterOperator(
                                BinaryPredicateOperator.eq(entry.getKey(), entry.getValue())), root);
                        iterator.remove();
                    }
                }
            }
            return root;
        }

        @Override
        public Void visitLogicalFilter(OptExpression optExpression, RewriteContext context) {
            OptExpression child = optExpression.inputAt(0);
            child.getOp().accept(this, child, context);

            LogicalFilterOperator filterOperator = (LogicalFilterOperator) optExpression.getOp();
            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(filterOperator.getPredicate());
            for (ScalarOperator scalar : inputPredicates) {
                if (Utils.isConstantEqualPredicate(scalar)) {
                    context.columnRefToConstant.put((ColumnRefOperator) scalar.getChild(0), scalar.getChild(1));
                }
            }

            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, RewriteContext context) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();

            RewriteContext rleft = new RewriteContext();
            optExpression.inputAt(0).getOp().accept(this, optExpression.inputAt(0), rleft);
            RewriteContext rright = new RewriteContext();
            optExpression.inputAt(1).getOp().accept(this, optExpression.inputAt(1), rright);

            List<ScalarOperator> predicates = new ArrayList<>();
            predicates.add(joinOperator.getOnPredicate());

            if (!joinOperator.getJoinType().isLeftOuterJoin() && !joinOperator.getJoinType().isLeftAntiJoin()) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rright.columnRefToConstant.entrySet()) {
                    predicates.add(BinaryPredicateOperator.eq(entry.getKey(), entry.getValue()));
                }
            }

            if (!joinOperator.getJoinType().isRightOuterJoin() && !joinOperator.getJoinType().isRightAntiJoin()) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rleft.columnRefToConstant.entrySet()) {
                    predicates.add(BinaryPredicateOperator.eq(entry.getKey(), entry.getValue()));
                }
            }

            ScalarOperator scalarOperator = Utils.compoundAnd(predicates);
            joinOperator.setOnPredicate(scalarOperator);

            if (joinOperator.getJoinType().isLeftOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = rright.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();
                    if (optExpression.inputAt(1).getOutputColumns().contains(entry.getKey())) {
                        rright.columnRefToConstant.remove(entry.getKey());

                        OptExpression o = OptExpression.create(new LogicalFilterOperator(
                                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                                entry.getKey(), entry.getValue())),
                                optExpression.inputAt(1));
                        optExpression.setChild(1, o);
                    }
                }
            } else if (joinOperator.getJoinType().isRightOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = rleft.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();
                    if (optExpression.inputAt(0).getOutputColumns().contains(entry.getKey())) {
                        rleft.columnRefToConstant.remove(entry.getKey());
                    }

                    OptExpression o = OptExpression.create(new LogicalFilterOperator(
                                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                            entry.getKey(), entry.getValue())),
                            optExpression.inputAt(0));
                    optExpression.setChild(0, o);
                }
            }

            context.columnRefToConstant.putAll(rleft.columnRefToConstant);
            context.columnRefToConstant.putAll(rright.columnRefToConstant);

            return null;
        }

        // At present, the Filter under the Window/Repeat/Table Function node is no longer pulled upwards.
        // If there is such a demand case in the future, it can be continuously optimized

        @Override
        public Void visitLogicalWindow(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalRepeat(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }

        public Void visitLogicalTableFunction(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }
    }

    static class RewriteContext {
        public Map<ColumnRefOperator, ScalarOperator> columnRefToConstant = new HashMap<>();
    }

    static void convertPredicateToLogicalFilter(OptExpression root, RewriteContext context) {
        //Convert all remaining predicates to LogicalFilter
        OptExpression child = root.inputAt(0);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.columnRefToConstant.entrySet()) {
            child = OptExpression.create(
                    new LogicalFilterOperator(BinaryPredicateOperator.eq(entry.getKey(), entry.getValue())), child);
            root.setChild(0, child);
        }
    }
}