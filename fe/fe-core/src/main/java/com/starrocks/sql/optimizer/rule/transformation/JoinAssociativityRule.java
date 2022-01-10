// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 *        Join          Join
 *       /    \        /    \
 *    Join     C  =>  A     Join
 *   /    \                /    \
 *  A     B               B      C
 * */
public class JoinAssociativityRule extends TransformationRule {
    private JoinAssociativityRule() {
        super(RuleType.TF_JOIN_ASSOCIATIVITY,
                Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                        Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(
                                Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                                        Pattern.create(OperatorType.PATTERN_MULTI_LEAF))),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final JoinAssociativityRule instance = new JoinAssociativityRule();

    public static JoinAssociativityRule getInstance() {
        return instance;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        OptExpression topJoinOpt = input;
        OptExpression bottomJoinOpt = topJoinOpt.inputAt(0).inputAt(0);
        OptExpression optA = bottomJoinOpt.inputAt(0);
        OptExpression optB = bottomJoinOpt.inputAt(1);

        LogicalJoinOperator topJoin = (LogicalJoinOperator) input.getOp();
        if (!topJoin.getJoinHint().isEmpty() || bottomJoinOpt.getOp().hasLimit() ||
                !((LogicalJoinOperator) bottomJoinOpt.getOp()).getJoinHint().isEmpty()) {
            return false;
        }

        if (!topJoin.getJoinType().isInnerJoin()) {
            return false;
        }

        LogicalJoinOperator bottomJoin = (LogicalJoinOperator) bottomJoinOpt.getOp();
        if (!bottomJoin.getJoinType().isInnerJoin() && !bottomJoin.getJoinType().isCrossJoin()) {
            return false;
        }

        // 1. Forbidden expression column on join-reorder
        // 2. Forbidden on-predicate use columns from two children at same time
        LogicalProjectOperator bottomJoinProject = (LogicalProjectOperator) input.inputAt(0).getOp();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : bottomJoinProject.getColumnRefMap().entrySet()) {
            if (!entry.getValue().isColumnRef() &&
                    entry.getValue().getUsedColumns().isIntersect(optA.getOutputColumns()) &&
                    entry.getValue().getUsedColumns().isIntersect(optB.getOutputColumns())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression topJoinOpt = input;
        OptExpression bottomJoinOpt = topJoinOpt.inputAt(0).inputAt(0);
        OptExpression optA = bottomJoinOpt.inputAt(0);
        OptExpression optB = bottomJoinOpt.inputAt(1);
        OptExpression optC = topJoinOpt.inputAt(1);

        LogicalJoinOperator topJoin = (LogicalJoinOperator) topJoinOpt.getOp();
        LogicalJoinOperator bottomJoin = (LogicalJoinOperator) bottomJoinOpt.getOp();

        //re-allocate on-predicate of two join operator
        List<ScalarOperator> allOnPredicate = Lists.newArrayList();
        allOnPredicate.addAll(Utils.extractConjuncts(topJoin.getOnPredicate()));
        allOnPredicate.addAll(Utils.extractConjuncts(bottomJoin.getOnPredicate()));

        // todo
        //          join (b+c=a)                    join (b+c=a)
        //        /      \                          /   \
        //      join      C           ->           A    join
        //      /  \                                    /   \
        //     A    B                                  B     C
        // cross join on predicate b+c=a transform to inner join predicate, and it's equals on predicate, but it need to
        // generate projection xx = b+c on the new right join which we could not do now. so we just forbidden this
        // transform easily.
        for (ScalarOperator parentConjunct : Utils.extractConjuncts(topJoin.getOnPredicate())) {
            if (parentConjunct.getUsedColumns().isIntersect(optA.getOutputColumns()) &&
                    parentConjunct.getUsedColumns().isIntersect(optB.getOutputColumns()) &&
                    parentConjunct.getUsedColumns().isIntersect(optC.getOutputColumns())) {
                return Collections.emptyList();
            }
        }

        ColumnRefSet newBottomJoinInputColumns = new ColumnRefSet();
        newBottomJoinInputColumns.union(optB.getOutputColumns());
        newBottomJoinInputColumns.union(optC.getOutputColumns());

        List<ScalarOperator> newBottomJoinOnPredicate = Lists.newArrayList();
        List<ScalarOperator> newTopJoinOnPredicate = Lists.newArrayList();
        for (ScalarOperator conjunct : allOnPredicate) {
            if (newBottomJoinInputColumns.containsAll(conjunct.getUsedColumns())) {
                newBottomJoinOnPredicate.add(conjunct);
            } else {
                newTopJoinOnPredicate.add(conjunct);
            }
        }

        // Eliminate cross join
        if (newBottomJoinOnPredicate.isEmpty() || newTopJoinOnPredicate.isEmpty()) {
            return Collections.emptyList();
        }

        // If left child join contains predicate, it's means the predicate must can't push down to child, it's
        // will use columns which from all children, so we should add the predicate to new top join
        ScalarOperator newTopJoinPredicate = (topJoin.getPredicate() == null ? null : topJoin.getPredicate().clone());
        if (bottomJoin.getPredicate() != null) {
            newTopJoinPredicate = Utils.compoundAnd(newTopJoinPredicate, bottomJoin.getPredicate());
        }

        LogicalJoinOperator newTopJoin = new LogicalJoinOperator.Builder()
                .setOutputColumns(topJoin.getOutputColumns())
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(newTopJoinOnPredicate))
                .setPredicate(newTopJoinPredicate)
                .build();

        //ColumnRefSet newTopJoinRequiredColumns = topJoin.getOutputColumns(new ExpressionContext(input));
        ColumnRefSet newTopJoinRequiredColumns = new ColumnRefSet();
        newTopJoinRequiredColumns.union(newTopJoin.getRequiredChildInputColumns());
        newTopJoinRequiredColumns.union(topJoinOpt.getOutputColumns());
        List<ColumnRefOperator> newBottomJoinOutputColumns = newBottomJoinInputColumns.getStream()
                .filter(newTopJoinRequiredColumns::contains)
                .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

        //re-allocate project of bottom join
        LogicalProjectOperator bottomJoinProject = (LogicalProjectOperator) input.inputAt(0).getOp();
        HashMap<ColumnRefOperator, ScalarOperator> rightExpression = new HashMap<>();
        HashMap<ColumnRefOperator, ScalarOperator> leftExpression = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : bottomJoinProject.getColumnRefMap().entrySet()) {
            if (!entry.getValue().isColumnRef() &&
                    newBottomJoinInputColumns.containsAll(entry.getValue().getUsedColumns())) {
                rightExpression.put(entry.getKey(), entry.getValue());
            } else if (!entry.getValue().isColumnRef() &&
                    optA.getOutputColumns().containsAll(entry.getValue().getUsedColumns())) {
                leftExpression.put(entry.getKey(), entry.getValue());
            }
        }

        //build new left child join
        OptExpression newTopJoinLeftChildOpt = buildNewTopLeftChild(context, optA, leftExpression);
        //build new right child join
        OptExpression newBottomJoinOpt =
                buildNewBottomJoin(newBottomJoinOutputColumns, newBottomJoinOnPredicate, optB, optC, rightExpression);

        OptExpression newTopJoinOpt = OptExpression.create(newTopJoin, newTopJoinLeftChildOpt, newBottomJoinOpt);

        ColumnRefSet left = new ColumnRefSet();
        if (!leftExpression.isEmpty()) {
            left.union(new ArrayList<>(leftExpression.keySet()));
            left.union(optA.getOutputColumns());
        } else {
            left.union(optA.getOutputColumns());
        }

        ColumnRefSet right = new ColumnRefSet();
        right.union(newBottomJoinOutputColumns);
        right.union(new ArrayList<>(rightExpression.keySet()));

        ColumnRefSet all = new ColumnRefSet();
        all.union(left);
        all.union(right);
        ColumnRefSet a = all;
        ColumnRefSet b = topJoinOpt.getOutputColumns();

        return Lists.newArrayList(newTopJoinOpt);
    }

    private OptExpression buildNewTopLeftChild(OptimizerContext context,
                                               OptExpression bottomJoinLeftChild,
                                               HashMap<ColumnRefOperator, ScalarOperator> leftExpression) {
        OptExpression newLeftOpt;
        if (!leftExpression.isEmpty()) {
            leftExpression.putAll(bottomJoinLeftChild.getOutputColumns().getStream()
                    .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id))
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));
            newLeftOpt = OptExpression.create(new LogicalProjectOperator(leftExpression),
                    Lists.newArrayList(bottomJoinLeftChild));
        } else {
            newLeftOpt = bottomJoinLeftChild;
        }
        return newLeftOpt;
    }

    private OptExpression buildNewBottomJoin(
            List<ColumnRefOperator> newBottomJoinOutputColumns,
            List<ScalarOperator> newBottomJoinOnPredicate,
            OptExpression optB, OptExpression optC,
            HashMap<ColumnRefOperator, ScalarOperator> rightExpression) {
        rightExpression.putAll(newBottomJoinOutputColumns.stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity())));

        ColumnRefSet outputColumns = new ColumnRefSet();
        outputColumns.union(newBottomJoinOutputColumns);
        for (ScalarOperator expr : rightExpression.values()) {
            outputColumns.union(expr.getUsedColumns());
        }

        LogicalJoinOperator newBottomJoin = new LogicalJoinOperator.Builder()
                .setOutputColumns(outputColumns)
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(newBottomJoinOnPredicate))
                .build();
        LogicalProjectOperator newBottomJoinProject = new LogicalProjectOperator(rightExpression);
        OptExpression newBottomJoinOpt = OptExpression.create(
                newBottomJoinProject,
                Lists.newArrayList(OptExpression.create(newBottomJoin, Lists.newArrayList(optB, optC))));
        return newBottomJoinOpt;
    }
}
