// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ScalarOperatorsReuse {
    /**
     * Rewrite the operators, if they have common sub operators.
     * For [a+b, a+b+c, a+b+d], we will rewrite them to
     * [x, x+c, x+d], the x is the result of a + b
     */
    public static List<ScalarOperator> rewriteOperators(List<ScalarOperator> operators,
                                                        ColumnRefFactory factory) {
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>>
                commonSubOperatorsByDepth = collectCommonSubScalarOperators(operators,
                factory);

        Map<ScalarOperator, ColumnRefOperator> commonSubOperators =
                commonSubOperatorsByDepth.values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!commonSubOperators.isEmpty()) {
            return operators.stream()
                    .map(p -> rewriteOperatorWithCommonOperator(p, commonSubOperators))
                    .collect(toImmutableList());
        }
        return operators;
    }

    public static ScalarOperator rewriteOperatorWithCommonOperator(ScalarOperator operator,
                                                                   Map<ScalarOperator, ColumnRefOperator> rewriteWith) {
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter(rewriteWith);
        return operator.accept(rewriter, null);
    }

    /**
     * Collect the common sub operators for the input operators
     * For [a+b, a+b+c, a+b+d], the common sub operators is [a + b]
     */
    public static Map<Integer, Map<ScalarOperator, ColumnRefOperator>> collectCommonSubScalarOperators(
            List<ScalarOperator> scalarOperators,
            ColumnRefFactory columnRefFactory) {
        // 1. Recursively collect common sub operators for the input operators
        CommonSubScalarOperatorCollector operatorCollector = new CommonSubScalarOperatorCollector();
        scalarOperators.forEach(operator -> operator.accept(operatorCollector, null));
        if (operatorCollector.commonOperatorsByDepth.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubOperators =
                ImmutableMap.builder();
        Map<ScalarOperator, ColumnRefOperator> rewriteWith = new HashMap<>();
        Map<Integer, Set<ScalarOperator>> sortedCommonOperatorsByDepth =
                operatorCollector.commonOperatorsByDepth.entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        // 2. Rewrite high depth common operators with low depth common operators
        // 3. Create the result column ref for each common operators
        for (Map.Entry<Integer, Set<ScalarOperator>> kv : sortedCommonOperatorsByDepth.entrySet()) {
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter(rewriteWith);
            ImmutableMap.Builder<ScalarOperator, ColumnRefOperator> operatorColumnMapBuilder = ImmutableMap.builder();
            for (ScalarOperator operator : kv.getValue()) {
                ScalarOperator rewrittenOperator = operator.accept(rewriter, null);
                operatorColumnMapBuilder.put(rewrittenOperator,
                        columnRefFactory.create(operator, rewrittenOperator.getType(), rewrittenOperator.isNullable()));
            }
            Map<ScalarOperator, ColumnRefOperator> operatorColumnMap = operatorColumnMapBuilder.build();
            commonSubOperators.put(kv.getKey(), operatorColumnMap);
            rewriteWith.putAll(operatorColumnMap.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        return commonSubOperators.build();
    }

    private static class ScalarOperatorRewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {
        private final Map<ScalarOperator, ColumnRefOperator> commonOperatorsMap;

        public ScalarOperatorRewriter(Map<ScalarOperator, ColumnRefOperator> commonOperatorsMap) {
            this.commonOperatorsMap = ImmutableMap.copyOf(commonOperatorsMap);
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            return scalarOperator;
        }

        private ScalarOperator tryRewrite(ScalarOperator operator) {
            if (commonOperatorsMap.containsKey(operator)) {
                return commonOperatorsMap.get(operator);
            }
            return operator;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            ScalarOperator operator = new CallOperator(call.getFnName(),
                    call.getType(),
                    call.getChildren().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()),
                    call.getFunction(),
                    call.isDistinct());
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            List<ScalarOperator> newChildren =
                    operator.getChildren().stream().map(argument -> argument.accept(this, null)).
                            collect(Collectors.toList());
            CaseWhenOperator newOperator = new CaseWhenOperator(operator, newChildren);
            return tryRewrite(newOperator);
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
            CastOperator newOperator = new CastOperator(operator.getType(),
                    operator.getChild(0).accept(this, null),
                    operator.isImplicit());
            return tryRewrite(newOperator);
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            ScalarOperator operator = new BinaryPredicateOperator(predicate.getBinaryType(),
                    predicate.getChildren().stream().map(argument -> argument.accept(this, null))
                            .collect(Collectors.toList()));
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            ScalarOperator operator = new CompoundPredicateOperator(predicate.getCompoundType(),
                    predicate.getChildren().stream().map(argument -> argument.accept(this, null))
                            .toArray(ScalarOperator[]::new));
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
            ScalarOperator operator = new ExistsPredicateOperator(predicate.isNotExists(),
                    predicate.getChildren().stream().map(argument -> argument.accept(this, null))
                            .toArray(ScalarOperator[]::new));
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
            ScalarOperator operator = new InPredicateOperator(predicate.isNotIn(),
                    predicate.getChildren().stream().map(argument -> argument.accept(this, null))
                            .toArray(ScalarOperator[]::new));
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            ScalarOperator operator = new IsNullPredicateOperator(predicate.isNotNull(),
                    predicate.getChild(0).accept(this, null));
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            ScalarOperator operator = new LikePredicateOperator(predicate.getLikeType(),
                    predicate.getChildren().stream().map(argument -> argument.accept(this, null))
                            .toArray(ScalarOperator[]::new));
            return tryRewrite(operator);
        }

        @Override
        public ScalarOperator visitDictMappingOperator(DictMappingOperator operator, Void context) {
            return tryRewrite(operator.clone());
        }

        @Override
        public ScalarOperator visitCloneOperator(CloneOperator operator, Void context) {
            ScalarOperator clone = new CloneOperator(operator.getChild(0).accept(this, null));
            return tryRewrite(clone);
        }
    }

    private static class CommonSubScalarOperatorCollector extends ScalarOperatorVisitor<Integer, Void> {
        // The key is operator tree depth, the value is operator set with same tree depth.
        // For operator list [a + b, a + b + c, a + d]
        // The operatorsByDepth is
        // {[1] -> [a + b, a + d], [2] -> [a + b + c]}
        // The commonOperatorsByDepth is
        // {[1] -> [a + b]}
        private final Map<Integer, Set<ScalarOperator>> operatorsByDepth = new HashMap<>();
        private final Map<Integer, Set<ScalarOperator>> commonOperatorsByDepth = new HashMap<>();

        private int collectCommonOperatorsByDepth(int depth, ScalarOperator operator) {
            Set<ScalarOperator> operators = getOperatorsByDepth(depth, operatorsByDepth);
            if (operators.contains(operator)) {
                Set<ScalarOperator> commonOperators = getOperatorsByDepth(depth, commonOperatorsByDepth);
                commonOperators.add(operator);
            }
            operators.add(operator);
            return depth;
        }

        private static Set<ScalarOperator> getOperatorsByDepth(int depth,
                                                               Map<Integer, Set<ScalarOperator>> operatorsByDepth) {
            operatorsByDepth.putIfAbsent(depth, new LinkedHashSet<>());
            return operatorsByDepth.get(depth);
        }

        @Override
        public Integer visit(ScalarOperator scalarOperator, Void context) {
            if (scalarOperator.getChildren().isEmpty()) {
                return 0;
            }

            return collectCommonOperatorsByDepth(scalarOperator.getChildren().stream().map(
                    argument -> argument.accept(this, context)).reduce(Math::max).get() + 1, scalarOperator);
        }

        @Override
        public Integer visitDictMappingOperator(DictMappingOperator scalarOperator, Void context) {
            return collectCommonOperatorsByDepth(1, scalarOperator);
        }

    }
}
