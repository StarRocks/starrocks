// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pulls the predicate up to a position where it cannot go any further, and generates a LogicalFilter there.
 * This filter will be pushed down again in the subsequent predicate pushdown rule.
 * A series of equivalence derivations and constant simplifications can be applied during pushdown.
 */
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

                for (BinaryPredicateOperator scalarOperator : rewriteContext.operatorSet) {
                    context.put(scalarOperator);
                }
            }

            return null;
        }

        public OptExpression handleLegacyPredicate(ColumnRefSet parentInputColumns, OptExpression optExpression,
                                                   RewriteContext context) {
            OptExpression root = optExpression;
            if (parentInputColumns != null) {

                Iterator<BinaryPredicateOperator> iterator = context.operatorSet.iterator();

                while (iterator.hasNext()) {
                    BinaryPredicateOperator scalarOperator = iterator.next();
                    ColumnRefOperator columnRefOperator = (ColumnRefOperator) scalarOperator.getChild(0);
                    if (!parentInputColumns.contains(columnRefOperator.getId())) {
                        // If the output columns does not contain this predicate,
                        // leave the predicate under this node to provide more possibilities for subsequent predicate push down
                        root = OptExpression.create(new LogicalFilterOperator(scalarOperator), root);
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
                if (scalar instanceof BinaryPredicateOperator) {
                    BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) scalar;
                    if (binaryPredicateOperator.getChild(0) instanceof ColumnRefOperator &&
                            binaryPredicateOperator.getChild(1) instanceof ConstantOperator) {
                        context.put((BinaryPredicateOperator) scalar);
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitLogicalProject(OptExpression optExpression, RewriteContext context) {
            OptExpression child = optExpression.inputAt(0);
            child.getOp().accept(this, child, context);

            LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOperator.getColumnRefMap().entrySet()) {
                if (entry.getValue() instanceof ConstantOperator && !((ConstantOperator) entry.getValue()).isNull()) {
                    context.put(BinaryPredicateOperator.eq(entry.getKey(), entry.getValue()));
                }
            }

            child = handleLegacyPredicate(optExpression.getOutputColumns(), child, context);
            optExpression.setChild(0, child);

            return null;
        }

        @Override
        public Void visitLogicalTopN(OptExpression optExpression, RewriteContext context) {
            OptExpression child = optExpression.inputAt(0);
            child.getOp().accept(this, child, context);

            LogicalTopNOperator logicalTopNOperator = (LogicalTopNOperator) optExpression.getOp();
            List<Ordering> orderingList = new ArrayList<>();
            for (Ordering ordering : logicalTopNOperator.getOrderByElements()) {
                if (context.columnRefToConstant.containsKey(ordering.getColumnRef())) {
                    orderingList.add(ordering);
                }
            }

            logicalTopNOperator.getOrderByElements().removeAll(orderingList);
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, RewriteContext context) {
            LogicalJoinOperator logicalJoinOperator = (LogicalJoinOperator) optExpression.getOp();

            OptExpression leftChild = optExpression.inputAt(0);
            RewriteContext leftChildRewriteContext = new RewriteContext();
            leftChild.getOp().accept(this, leftChild, leftChildRewriteContext);
            if (logicalJoinOperator.getJoinType().isRightOuterJoin()) {
                for (BinaryPredicateOperator binaryOp : leftChildRewriteContext.operatorSet) {
                    leftChild = OptExpression.create(new LogicalFilterOperator(binaryOp), leftChild);
                    optExpression.setChild(0, leftChild);
                }
                leftChildRewriteContext.clear();
            } else {
                OptExpression c = handleLegacyPredicate(optExpression.getOutputColumns(), leftChild, leftChildRewriteContext);
                optExpression.setChild(0, c);
            }

            OptExpression rightChild = optExpression.inputAt(1);
            RewriteContext rightChildRewriteContext = new RewriteContext();
            rightChild.getOp().accept(this, rightChild, rightChildRewriteContext);
            if (logicalJoinOperator.getJoinType().isLeftOuterJoin()) {
                for (BinaryPredicateOperator binaryOp : rightChildRewriteContext.operatorSet) {
                    rightChild = OptExpression.create(new LogicalFilterOperator(binaryOp), rightChild);
                    optExpression.setChild(1, rightChild);
                }
                rightChildRewriteContext.clear();
            } else {
                OptExpression c = handleLegacyPredicate(optExpression.getOutputColumns(), rightChild, rightChildRewriteContext);
                optExpression.setChild(1, c);
            }

            for (BinaryPredicateOperator binaryPredicateOperator : leftChildRewriteContext.operatorSet) {
                context.put(binaryPredicateOperator);
            }

            for (BinaryPredicateOperator binaryPredicateOperator : rightChildRewriteContext.operatorSet) {
                context.put(binaryPredicateOperator);
            }

            //context.columnRefToConstant.putAll(leftChildRewriteContext.columnRefToConstant);
            //context.columnRefToConstant.putAll(rightChildRewriteContext.columnRefToConstant);

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

        @Override
        public Void visitLogicalTableFunction(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalCTEAnchor(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalCTEProduce(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalCTEConsume(OptExpression optExpression, RewriteContext context) {
            convertPredicateToLogicalFilter(optExpression, context);
            return null;
        }
    }

    private static class RewriteContext {
        private final Map<ColumnRefOperator, ConstantOperator> columnRefToConstant = new HashMap<>();
        public Set<BinaryPredicateOperator> operatorSet = new HashSet<>();


        public Map<ColumnRefOperator, ConstantOperator> getColumnRefToConstant() {
            return columnRefToConstant;
        }

        void put(BinaryPredicateOperator scalarOperator) {
            operatorSet.add(scalarOperator);
            if (!(scalarOperator.getChild(1) instanceof ConstantOperator)) {
                return;
            }

            if (scalarOperator.getBinaryType().isEqual()) {
                columnRefToConstant.put((ColumnRefOperator) scalarOperator.getChild(0),
                        (ConstantOperator) scalarOperator.getChild(1));
            }
        }

        void clear() {
            columnRefToConstant.clear();
            operatorSet.clear();
        }
    }

    static void convertPredicateToLogicalFilter(OptExpression root, RewriteContext context) {
        //Convert all remaining predicates to LogicalFilter
        OptExpression child = root.inputAt(0);
        for (ScalarOperator scalarOperator : context.operatorSet) {
            child = OptExpression.create(new LogicalFilterOperator(scalarOperator), child);
            root.setChild(0, child);
        }
        context.clear();
    }
}