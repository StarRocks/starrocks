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
        RewriteContext rewriteContext = new RewriteContext();
        root.getOp().accept(new Visitor(columnRefFactory), root, rewriteContext);

        //最终将所有filter下推
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rewriteContext.columnRefToConstant.entrySet()) {
            OptExpression o = OptExpression.create(
                    new LogicalFilterOperator(
                            new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, entry.getKey(), entry.getValue())),
                    root.inputAt(0));
            root.setChild(0, o);
        }
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

                        OptExpression o = OptExpression.create(new LogicalFilterOperator(
                                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                                entry.getKey(), entry.getValue())),
                                optExpression.inputAt(0));
                        optExpression.setChild(0, o);

                        //删除这个谓词，并将此谓词留在此处
                        iterator.remove();
                    }
                }
            }
        }

        @Override
        public Void visitLogicalFilter(OptExpression optExpression, RewriteContext context) {
            start(optExpression, context);
            LogicalFilterOperator filterOperator = (LogicalFilterOperator) optExpression.getOp();

            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(filterOperator.getPredicate());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.columnRefToConstant.entrySet()) {
                inputPredicates.add(BinaryPredicateOperator.eq(entry.getKey(), entry.getValue()));
            }

            filterOperator.setPredicate(Utils.compoundAnd(inputPredicates));
            end(optExpression, context);
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, RewriteContext context) {
            //start(optExpression, context);
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();

            //计算等值判断，On Predicate 左右互推
            List<ScalarOperator> pushDownToLeft = new ArrayList<>();
            List<ScalarOperator> pushDownToRight = new ArrayList<>();

            if (joinOperator.getJoinType().isFullOuterJoin()) {
                //full outer 不互推
            }

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

                /*
                ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
                List<ScalarOperator> inputPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());
                scalarEquivalenceExtractor.union(inputPredicates);
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rright.columnRefToConstant.entrySet()) {
                    scalarEquivalenceExtractor.union(Lists.newArrayList(
                            new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, entry.getKey(),
                            entry.getValue())));
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

                 */
            }


            if (!joinOperator.getJoinType().isRightOuterJoin() && !joinOperator.getJoinType().isRightAntiJoin()) {
                //getPredicatePushDownToLeft(context, optExpression, joinOperator, pushDownToLeft);


                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rleft.columnRefToConstant.entrySet()) {
                    predicates.add(BinaryPredicateOperator.eq(entry.getKey(), entry.getValue()));
                }


                /*
                ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
                List<ScalarOperator> inputPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());
                scalarEquivalenceExtractor.union(inputPredicates);
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rleft.columnRefToConstant.entrySet()) {
                    scalarEquivalenceExtractor.union(Lists.newArrayList(
                            new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, entry.getKey(),
                            entry.getValue())));
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
                */

            }

            //ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
            ScalarOperator scalarOperator = Utils.compoundAnd(predicates);
            //if (scalarOperator != null) {
            //     scalarOperator = scalarRewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            // }

            joinOperator.setOnPredicate(scalarOperator);


            // PushDownJoinPredicateBase.pushDownPredicate(optExpression, pushDownToLeft, pushDownToRight);

            if (joinOperator.getJoinType().isLeftOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = rright.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();

                    //Left outer join 右孩子的常量等值不再向上传递
                    if (optExpression.inputAt(1).getOutputColumns().contains(entry.getKey())) {
                        rright.columnRefToConstant.remove(entry.getKey());

                        OptExpression o = OptExpression.create(new LogicalFilterOperator(
                                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                                entry.getKey(), entry.getValue())),
                                optExpression.inputAt(1));
                        optExpression.setChild(1, o);
                    }
                }
            }

            if (joinOperator.getJoinType().isRightOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = rleft.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();

                    //Right outer join 左孩子的常量等值不再向上传递
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

            /*
            if (joinOperator.getJoinType().isLeftOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = context.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();

                    //Left outer join 右孩子的常量等值不再向上传递
                    if (optExpression.inputAt(1).getOutputColumns().contains(entry.getKey())) {
                        context.columnRefToConstant.remove(entry.getKey());

                        OptExpression o = OptExpression.create(new LogicalFilterOperator(
                                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                                entry.getKey(), entry.getValue())),
                                optExpression.inputAt(1));
                        optExpression.setChild(1, o);

                    }
                }
            } else if (joinOperator.getJoinType().isRightOuterJoin()) {
                Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> iter = context.columnRefToConstant.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<ColumnRefOperator, ScalarOperator> entry = iter.next();

                    //Right outer join 左孩子的常量等值不再向上传递
                    if (optExpression.inputAt(0).getOutputColumns().contains(entry.getKey())) {
                        context.columnRefToConstant.remove(entry.getKey());
                    }

                    OptExpression o = OptExpression.create(new LogicalFilterOperator(
                                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                                            entry.getKey(), entry.getValue())),
                            optExpression.inputAt(0));
                    optExpression.setChild(0, o);

                }
            }

            end(optExpression, context);

             */
            return null;
        }

        @Override
        public Void visitLogicalUnion(OptExpression optExpression, RewriteContext context) {
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OptExpression child = optExpression.inputAt(childIdx);

                RewriteContext rewriteContext = new RewriteContext();
                child.getOp().accept(this, child, rewriteContext);
                end(child, rewriteContext);
            }

            return null;
        }

        @Override
        public Void visitLogicalExcept(OptExpression optExpression, RewriteContext context) {
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OptExpression child = optExpression.inputAt(childIdx);

                RewriteContext rewriteContext = new RewriteContext();
                child.getOp().accept(this, child, rewriteContext);
                end(child, rewriteContext);
            }

            return null;
        }

        @Override
        public Void visitLogicalIntersect(OptExpression optExpression, RewriteContext context) {
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OptExpression child = optExpression.inputAt(childIdx);

                RewriteContext rewriteContext = new RewriteContext();
                child.getOp().accept(this, child, rewriteContext);
                end(child, rewriteContext);
            }

            return null;
        }

        void getPredicatePushDownToRight(RewriteContext context,
                                         OptExpression optExpression,
                                         LogicalJoinOperator joinOperator,
                                         List<ScalarOperator> pushDownToRight) {
            //JoinEquivalentPredicatePushDown.RewriteContext rleft = new JoinEquivalentPredicatePushDown.RewriteContext();
            //optExpression.inputAt(0).getOp().accept(this, optExpression.inputAt(0), rleft);

            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());
            scalarEquivalenceExtractor.union(inputPredicates);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.columnRefToConstant.entrySet()) {
                BinaryPredicateOperator binaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, entry.getKey(), entry.getValue());

                scalarEquivalenceExtractor.union(Lists.newArrayList(binaryPredicateOperator));
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

        void getPredicatePushDownToLeft(RewriteContext context,
                                        OptExpression optExpression,
                                        LogicalJoinOperator joinOperator,
                                        List<ScalarOperator> pushDownToLeft) {
            //JoinEquivalentPredicatePushDown.RewriteContext rright = new JoinEquivalentPredicatePushDown.RewriteContext();
            //optExpression.inputAt(1).getOp().accept(this, optExpression.inputAt(1), rright);

            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            List<ScalarOperator> inputPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());
            scalarEquivalenceExtractor.union(inputPredicates);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.columnRefToConstant.entrySet()) {
                BinaryPredicateOperator binaryPredicateOperator =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, entry.getKey(), entry.getValue());

                scalarEquivalenceExtractor.union(Lists.newArrayList(binaryPredicateOperator));
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
        public Map<ColumnRefOperator, ScalarOperator> columnRefToConstant = new HashMap<>();
    }
}