// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        super(RuleType.TF_JOIN_ASSOCIATIVITY, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF))
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final JoinAssociativityRule INSTANCE = new JoinAssociativityRule();

    public static JoinAssociativityRule getInstance() {
        return INSTANCE;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        if (!joinOperator.getJoinHint().isEmpty() ||
                !((LogicalJoinOperator) input.inputAt(0).getOp()).getJoinHint().isEmpty()) {
            return false;
        }

        if (!joinOperator.getJoinType().isInnerJoin()) {
            return false;
        }

        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) input.inputAt(0).getOp();
        if (leftChildJoin.getProjection() != null) {
            Projection projection = leftChildJoin.getProjection();
            // 1. Forbidden expression column on join-reorder
            // 2. Forbidden on-predicate use columns from two children at same time
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                if (!entry.getValue().isColumnRef() &&
                        entry.getValue().getUsedColumns().isIntersect(input.inputAt(0).inputAt(0).getOutputColumns()) &&
                        entry.getValue().getUsedColumns().isIntersect(input.inputAt(0).inputAt(1).getOutputColumns())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression leftChild = input.inputAt(0);
        OptExpression rightChild = input.inputAt(1);

        LogicalJoinOperator parentJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) leftChild.getOp();
        // We do this check here not in check method, because check here is very simple
        if (!leftChildJoin.getJoinType().isInnerJoin() && !leftChildJoin.getJoinType().isCrossJoin()) {
            return Collections.emptyList();
        }

        List<ScalarOperator> parentConjuncts = Utils.extractConjuncts(parentJoin.getOnPredicate());
        List<ScalarOperator> childConjuncts = Utils.extractConjuncts(leftChildJoin.getOnPredicate());

        List<ScalarOperator> allConjuncts = Lists.newArrayList();
        allConjuncts.addAll(parentConjuncts);
        allConjuncts.addAll(childConjuncts);

        OptExpression leftChild1 = leftChild.inputAt(0);
        OptExpression leftChild2 = leftChild.inputAt(1);
        // todo
        //          join (b+c=a)                    join (b+c=a)
        //        /      \                          /   \
        //      join      C           ->           A    join
        //      /  \                                    /   \
        //     A    B                                  B     C
        // cross join on predicate b+c=a transform to inner join predicate, and it's equals on predicate, but it need to
        // generate projection xx = b+c on the new right join which we could not do now. so we just forbidden this
        // transform easily.
        for (ScalarOperator parentConjunct : parentConjuncts) {
            if (parentConjunct.getUsedColumns().isIntersect(leftChild1.getOutputColumns()) &&
                    parentConjunct.getUsedColumns().isIntersect(leftChild2.getOutputColumns()) &&
                    parentConjunct.getUsedColumns().isIntersect(rightChild.getOutputColumns())) {
                return Collections.emptyList();
            }
        }

        ColumnRefSet newRightChildColumns = new ColumnRefSet();
        newRightChildColumns.union(rightChild.getOutputColumns());
        newRightChildColumns.union(leftChild2.getOutputColumns());

        List<ScalarOperator> newChildConjuncts = Lists.newArrayList();
        List<ScalarOperator> newParentConjuncts = Lists.newArrayList();
        for (ScalarOperator conjunct : allConjuncts) {
            if (newRightChildColumns.containsAll(conjunct.getUsedColumns())) {
                newChildConjuncts.add(conjunct);
            } else {
                newParentConjuncts.add(conjunct);
            }
        }

        // Eliminate cross join
        if (newChildConjuncts.isEmpty() || newParentConjuncts.isEmpty()) {
            return Collections.emptyList();
        }

        LogicalJoinOperator.Builder topJoinBuilder = new LogicalJoinOperator.Builder();

        // If left child join contains predicate, it's means the predicate must can't push down to child, it's
        // will use columns which from all children, so we should add the predicate to new top join
        ScalarOperator topJoinPredicate = parentJoin.getPredicate();
        if (leftChildJoin.getPredicate() != null) {
            topJoinPredicate = Utils.compoundAnd(topJoinPredicate, leftChildJoin.getPredicate());
        }

        LogicalJoinOperator topJoinOperator = topJoinBuilder.withOperator(parentJoin)
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(newParentConjuncts))
                .setPredicate(topJoinPredicate)
                .build();

        ColumnRefSet outputCols = parentJoin.getOutputColumns(new ExpressionContext(input));
        ColumnRefSet parentJoinRequiredColumns = outputCols.clone();
        parentJoinRequiredColumns.union(topJoinOperator.getRequiredChildInputColumns());
        List<ColumnRefOperator> newRightOutputColumns = newRightChildColumns.getStream()
                .filter(parentJoinRequiredColumns::contains)
                .map(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

        Projection leftChildJoinProjection = leftChildJoin.getProjection();
        HashMap<ColumnRefOperator, ScalarOperator> rightExpression = new HashMap<>();
        HashMap<ColumnRefOperator, ScalarOperator> leftExpression = new HashMap<>();
        if (leftChildJoinProjection != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : leftChildJoinProjection.getColumnRefMap()
                    .entrySet()) {
                // To handle mappings of expressions in projection, special processing is needed like
                // ColumnRefOperator -> ColumnRefOperator mappings with name ("1: expr" -> "2: column_name", or "1: sum" -> "2: sum"),
                // it needs to be handled like expression mapping.
                String keyIdentifier = entry.getKey().toString();
                String valueIdentifier = entry.getValue().toString();
                boolean isProjectToColumnRef = Objects.equals(keyIdentifier, valueIdentifier);
                if (!isProjectToColumnRef &&
                        newRightChildColumns.containsAll(entry.getValue().getUsedColumns())) {
                    rightExpression.put(entry.getKey(), entry.getValue());
                } else if (!isProjectToColumnRef &&
                        leftChild1.getOutputColumns().containsAll(entry.getValue().getUsedColumns())) {
                    leftExpression.put(entry.getKey(), entry.getValue());
                }
            }
        }

        //build new right child join
        OptExpression newRightChildJoin;
        if (rightExpression.isEmpty()) {
            LogicalJoinOperator.Builder rightChildJoinOperatorBuilder = new LogicalJoinOperator.Builder();
            LogicalJoinOperator rightChildJoinOperator = rightChildJoinOperatorBuilder
                    .setJoinType(JoinOperator.INNER_JOIN)
                    .setOnPredicate(Utils.compoundAnd(newChildConjuncts))
                    .setProjection(new Projection(newRightOutputColumns.stream()
                            .collect(Collectors.toMap(Function.identity(), Function.identity())), new HashMap<>()))
                    .build();
            newRightChildJoin = OptExpression.create(rightChildJoinOperator, leftChild2, rightChild);
        } else {
            rightExpression.putAll(newRightOutputColumns.stream()
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));
            LogicalJoinOperator.Builder rightChildJoinOperatorBuilder = new LogicalJoinOperator.Builder();
            LogicalJoinOperator rightChildJoinOperator = rightChildJoinOperatorBuilder
                    .setJoinType(JoinOperator.INNER_JOIN)
                    .setOnPredicate(Utils.compoundAnd(newChildConjuncts))
                    .setProjection(new Projection(rightExpression))
                    .build();
            newRightChildJoin = OptExpression.create(rightChildJoinOperator, leftChild2, rightChild);

            newRightOutputColumns = new ArrayList<>(rightExpression.keySet());
        }

        //build left
        if (!leftExpression.isEmpty()) {
            OptExpression left;
            Map<ColumnRefOperator, ScalarOperator> expressionProject;
            if (leftChild1.getOp().getProjection() == null) {
                expressionProject = leftChild1.getOutputColumns().getStream()
                        .map(id -> context.getColumnRefFactory().getColumnRef(id))
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
            } else {
                expressionProject = Maps.newHashMap(leftChild1.getOp().getProjection().getColumnRefMap());
            }
            // Use leftChild1 projection to rewrite the leftExpression, it's like two project node merge.
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionProject);
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap(expressionProject);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : leftExpression.entrySet()) {
                rewriteMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }

            Operator.Builder builder = OperatorBuilderFactory.build(leftChild1.getOp());
            Operator newOp = builder.withOperator(leftChild1.getOp())
                    .setProjection(new Projection(rewriteMap)).build();
            left = OptExpression.create(newOp, leftChild1.getInputs());

            //If all the columns in onPredicate come from one side, it means that it is CrossJoin, and give up this Plan
            if (new ColumnRefSet(new ArrayList<>(expressionProject.keySet())).containsAll(
                    topJoinOperator.getOnPredicate().getUsedColumns())
                    || new ColumnRefSet(newRightOutputColumns).containsAll(
                    topJoinOperator.getOnPredicate().getUsedColumns())) {
                return Collections.emptyList();
            }
            if (topJoinOperator.getProjection() == null) {
                topJoinOperator.setProjection(buildTopJoinProjection(outputCols, newOp.getProjection(),
                        newRightChildJoin.getOp().getProjection()));
            }


            OptExpression topJoin = OptExpression.create(topJoinOperator, left, newRightChildJoin);
            return Lists.newArrayList(topJoin);
        } else {

            //If all the columns in onPredicate come from one side, it means that it is CrossJoin, and give up this Plan
            if (leftChild1.getOutputColumns().containsAll(topJoinOperator.getOnPredicate().getUsedColumns())
                    || new ColumnRefSet(newRightOutputColumns).containsAll(
                    topJoinOperator.getOnPredicate().getUsedColumns())) {
                return Collections.emptyList();
            }
            Projection leftProjection;
            if (leftChild1.getOp().getProjection() != null) {
                leftProjection = leftChild1.getOp().getProjection();
            } else {
                leftProjection = new Projection(leftChild1.getOutputColumns().getStream()
                        .map(id -> context.getColumnRefFactory().getColumnRef(id))
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));
            }
            if (topJoinOperator.getProjection() == null) {
                topJoinOperator.setProjection(buildTopJoinProjection(outputCols, leftProjection,
                        newRightChildJoin.getOp().getProjection()));
            }

            OptExpression topJoin = OptExpression.create(topJoinOperator, leftChild1, newRightChildJoin);
            return Lists.newArrayList(topJoin);
        }
    }


    private Projection buildTopJoinProjection(ColumnRefSet oldOutputCols, Projection leftProjection, Projection rightProjection) {
        ColumnRefSet newOutputCols = new ColumnRefSet();
        newOutputCols.union(new ColumnRefSet(leftProjection.getOutputColumns()));
        newOutputCols.union(new ColumnRefSet(rightProjection.getOutputColumns()));

        if (oldOutputCols.equals(newOutputCols)) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : leftProjection.getColumnRefMap().entrySet()) {
            if (oldOutputCols.contains(entry.getKey())) {
                columnRefMap.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rightProjection.getColumnRefMap().entrySet()) {
            if (oldOutputCols.contains(entry.getKey())) {
                columnRefMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new Projection(columnRefMap);
    }
}
