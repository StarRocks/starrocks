// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * If InnerJoin(LeftJoin(a, b), c) doesn't have any conjunction between b and c
 *      inner join                  left join
 *      /        \                  /       \
 *   left join     c   <=>      inner join    b
 *  /       \                  /        \
 * a         b                a          c
 *
 */
public class DimJoinAssociativityRule extends JoinAssociativityRule {
    private DimJoinAssociativityRule() {
        super(RuleType.TF_DIM_JOIN_ASSOCIATIVITY, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(
                        Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                                Pattern.create(OperatorType.PATTERN_LEAF)
                                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)),
                                Pattern.create(OperatorType.PATTERN_LEAF)),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final DimJoinAssociativityRule INSTANCE = new DimJoinAssociativityRule();

    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isCboEnableDimJoinReorder()) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression leftChild = input.inputAt(0);
        OptExpression rightChild = input.inputAt(1);

        LogicalJoinOperator parentJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) leftChild.getOp();
        // We do this check here not in check method, because check here is very simple
        if (!(leftChildJoin.getJoinType().isLeftOuterJoin() || leftChildJoin.getJoinType().isInnerJoin())
                && !(parentJoin.getJoinType().isInnerJoin() || parentJoin.getJoinType().isLeftOuterJoin())) {
            return Collections.emptyList();
        }
        List<ScalarOperator> parentConjuncts = Utils.extractConjuncts(parentJoin.getOnPredicate());
        List<ScalarOperator> childConjuncts = Utils.extractConjuncts(leftChildJoin.getOnPredicate());

        OptExpression leftChild1 = leftChild.inputAt(0);
        OptExpression leftChild2 = leftChild.inputAt(1);
        //parent conjunct can't contains any leftchild2 columns
        for (ScalarOperator parentConjunct : parentConjuncts) {
            if (parentConjunct.getUsedColumns().isIntersect(leftChild2.getOutputColumns())) {
                return Collections.emptyList();
            }
        }
        ColumnRefSet newLeftChildColumns = new ColumnRefSet();
        newLeftChildColumns.union(leftChild1.getOutputColumns());
        newLeftChildColumns.union(rightChild.getOutputColumns());

        List<ScalarOperator> newChildConjuncts = Lists.newArrayList();
        List<ScalarOperator> newParentConjuncts = Lists.newArrayList();

        List<ScalarOperator> allConjuncts = Lists.newArrayList();
        allConjuncts.addAll(parentConjuncts);
        allConjuncts.addAll(childConjuncts);

        for (ScalarOperator conjunct : allConjuncts) {
            if (newLeftChildColumns.containsAll(conjunct.getUsedColumns())) {
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

        // same to JoinAssociativityRule
        // If left child join contains predicate, it's means the predicate must can't push down to child, it's
        // will use columns which from all children, so we should add the predicate to new top join
        ScalarOperator topJoinPredicate = parentJoin.getPredicate();
        if (leftChildJoin.getPredicate() != null) {
            topJoinPredicate = Utils.compoundAnd(topJoinPredicate, leftChildJoin.getPredicate());
        }

        LogicalJoinOperator topJoinOperator = topJoinBuilder.withOperator(parentJoin)
                .setJoinType(leftChildJoin.getJoinType())
                .setOnPredicate(Utils.compoundAnd(newParentConjuncts))
                .setPredicate(topJoinPredicate)
                .build();

        ColumnRefSet parentJoinRequiredColumns = parentJoin.getOutputColumns(new ExpressionContext(input));
        parentJoinRequiredColumns.union(topJoinOperator.getRequiredChildInputColumns());
        List<ColumnRefOperator> newLeftOutputColumns = newLeftChildColumns.getStream()
                .filter(parentJoinRequiredColumns::contains)
                .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

        Projection leftChildJoinProjection = leftChildJoin.getProjection();
        HashMap<ColumnRefOperator, ScalarOperator> rightExpression = new HashMap<>();
        HashMap<ColumnRefOperator, ScalarOperator> leftExpression = new HashMap<>();
        if (leftChildJoinProjection != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : leftChildJoinProjection.getColumnRefMap()
                    .entrySet()) {
                if (!entry.getValue().isColumnRef() &&
                        newLeftChildColumns.containsAll(entry.getValue().getUsedColumns())) {
                    leftExpression.put(entry.getKey(), entry.getValue());
                } else if (!entry.getValue().isColumnRef() &&
                        leftChild2.getOutputColumns().containsAll(entry.getValue().getUsedColumns())) {
                    rightExpression.put(entry.getKey(), entry.getValue());
                }
            }
        }

        //build new left child join
        OptExpression newLeftChildJoin;
        if (leftExpression.isEmpty()) {
            LogicalJoinOperator.Builder leftChildJoinOperatorBuilder = new LogicalJoinOperator.Builder();
            LogicalJoinOperator leftChildJoinOperator = leftChildJoinOperatorBuilder
                    .setJoinType(parentJoin.getJoinType())
                    .setOnPredicate(Utils.compoundAnd(newChildConjuncts))
                    .setProjection(new Projection(newLeftOutputColumns.stream()
                            .collect(Collectors.toMap(Function.identity(), Function.identity())), new HashMap<>()))
                    .build();
            newLeftChildJoin = OptExpression.create(leftChildJoinOperator, leftChild1, rightChild);
        } else {
            rightExpression.putAll(newLeftOutputColumns.stream()
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));
            LogicalJoinOperator.Builder rightChildJoinOperatorBuilder = new LogicalJoinOperator.Builder();
            LogicalJoinOperator rightChildJoinOperator = rightChildJoinOperatorBuilder
                    .setJoinType(parentJoin.getJoinType())
                    .setOnPredicate(Utils.compoundAnd(newChildConjuncts))
                    .setProjection(new Projection(rightExpression))
                    .build();
            newLeftChildJoin = OptExpression.create(rightChildJoinOperator, leftChild1, rightChild);
            newLeftOutputColumns = new ArrayList<>(rightExpression.keySet());
        }

        //build right
        if (!rightExpression.isEmpty()) {
            OptExpression right;
            Map<ColumnRefOperator, ScalarOperator> expressionProject;
            if (leftChild2.getOp().getProjection() == null) {
                expressionProject = leftChild2.getOutputColumns().getStream()
                        .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id))
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
            } else {
                expressionProject = Maps.newHashMap(leftChild2.getOp().getProjection().getColumnRefMap());
            }
            // Use leftChild1 projection to rewrite the leftExpression, it's like two project node merge.
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionProject);
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap(expressionProject);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rightExpression.entrySet()) {
                rewriteMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }

            Operator.Builder builder = OperatorBuilderFactory.build(leftChild2.getOp());
            Operator newOp = builder.withOperator(leftChild2.getOp())
                    .setProjection(new Projection(rewriteMap)).build();
            right = OptExpression.create(newOp, leftChild2.getInputs());

            //If all the columns in onPredicate come from one side, it means that it is CrossJoin, and give up this Plan
            if (new ColumnRefSet(new ArrayList<>(expressionProject.keySet())).containsAll(
                    topJoinOperator.getOnPredicate().getUsedColumns())
                    || new ColumnRefSet(newLeftOutputColumns).containsAll(
                    topJoinOperator.getOnPredicate().getUsedColumns())) {
                return Collections.emptyList();
            }

            OptExpression topJoin = OptExpression.create(topJoinOperator, newLeftChildJoin, right);
            return Lists.newArrayList(topJoin);
        } else {

            //If all the columns in onPredicate come from one side, it means that it is CrossJoin, and give up this Plan
            if (leftChild2.getOutputColumns().containsAll(topJoinOperator.getOnPredicate().getUsedColumns())
                    || new ColumnRefSet(newLeftOutputColumns).containsAll(
                    topJoinOperator.getOnPredicate().getUsedColumns())) {
                return Collections.emptyList();
            }

            OptExpression topJoin = OptExpression.create(topJoinOperator, newLeftChildJoin, leftChild2);
            return Lists.newArrayList(topJoin);
        }
    }

    public static DimJoinAssociativityRule getInstance() {
        return INSTANCE;
    }
}
