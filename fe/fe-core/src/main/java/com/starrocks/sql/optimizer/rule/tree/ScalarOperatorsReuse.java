// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
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
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;

import java.util.Collections;
import java.util.Comparator;
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
                factory, false);

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
            ColumnRefFactory columnRefFactory, boolean forLambda) {
        // 1. Recursively collect common sub operators for the input operators
        CommonSubScalarOperatorCollector operatorCollector = new CommonSubScalarOperatorCollector(forLambda);
        scalarOperators.forEach(operator -> operator.accept(operatorCollector, new CollectorContext(false)));
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
        public ScalarOperator visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void context) {
            ScalarOperator newOperator = new LambdaFunctionOperator(operator.getRefColumns(),
                    operator.getLambdaExpr().accept(this, null), operator.getType()
            );
            return tryRewrite(newOperator);
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

    private static class CollectorContext {
        public boolean isWithinLambda() {
            return withinLambda;
        }

        private final boolean withinLambda;

        CollectorContext(boolean withinLambda) {
            this.withinLambda = withinLambda;
        }

    }

    private static class CommonSubScalarOperatorCollector extends ScalarOperatorVisitor<Integer, CollectorContext> {
        // The key is operator tree depth, the value is operator set with same tree depth.
        // For operator list [a + b, a + b + c, a + d]
        // The operatorsByDepth is
        // {[1] -> [a + b, a + d], [2] -> [a + b + c]}
        // The commonOperatorsByDepth is
        // {[1] -> [a + b]}
        private final Map<Integer, Set<ScalarOperator>> operatorsByDepth = new HashMap<>();
        private final Map<Integer, Set<ScalarOperator>> commonOperatorsByDepth = new HashMap<>();

        private final boolean forLambda;

        private CommonSubScalarOperatorCollector(boolean forLambda) {
            this.forLambda = forLambda;
        }


        private int collectCommonOperatorsByDepth(int depth, ScalarOperator operator, boolean lambda) {
            Set<ScalarOperator> operators = getOperatorsByDepth(depth, operatorsByDepth);
            if (!isNonDeterministicFuncOrLambdaArgumentExist(operator, lambda) && operators.contains(operator)) {
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
        public Integer visit(ScalarOperator scalarOperator, CollectorContext context) {
            if (scalarOperator.getChildren().isEmpty()) {
                return 0;
            }

            return collectCommonOperatorsByDepth(scalarOperator.getChildren().stream().map(argument ->
                    argument.accept(this, context)).reduce(Math::max).get() + 1, scalarOperator, context.isWithinLambda());
        }

        // get rid of the expressions with lambda arguments, for example, select a+b, array_map(x-> 2*x+2*x > a+b, [1])
        // the a+b is reused, but 2*x is not reused as it contains the lambda argument x.
        // TODO(fzh) support reusing lambda argument related expressions.
        @Override
        public Integer visitLambdaFunctionOperator(LambdaFunctionOperator scalarOperator, CollectorContext context) {
            return collectCommonOperatorsByDepth(scalarOperator.getLambdaExpr().accept(this, new CollectorContext(true)),
                    scalarOperator, true);
        }

        @Override
        public Integer visitDictMappingOperator(DictMappingOperator scalarOperator, CollectorContext context) {
            return collectCommonOperatorsByDepth(1, scalarOperator, context.isWithinLambda());
        }

        // If a scalarOperator contains any non-deterministic function, it cannot be reused
        // because the non-deterministic function results returned each time are inconsistent.
        private boolean isNonDeterministicFuncOrLambdaArgumentExist(ScalarOperator scalarOperator, boolean lambda) {
            if (!forLambda && lambda && scalarOperator.getOpType().equals(OperatorType.LAMBDA_ARGUMENT)) {
                return true;
            }

            if (scalarOperator instanceof CallOperator) {
                String fnName = ((CallOperator) scalarOperator).getFnName();
                if (FunctionSet.nonDeterministicFunctions.contains(fnName)) {
                    return true;
                }
            }

            for (ScalarOperator child : scalarOperator.getChildren()) {
                if (isNonDeterministicFuncOrLambdaArgumentExist(child, lambda)) {
                    return true;
                }
            }
            return false;
        }

    }


    public static Projection getNewProjection(Projection projection, ColumnRefFactory columnRefFactory, boolean forLambda) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
        List<ScalarOperator> scalarOperators = Lists.newArrayList(columnRefMap.values());
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubOperatorsByDepth = ScalarOperatorsReuse
                .collectCommonSubScalarOperators(scalarOperators,
                        columnRefFactory, forLambda);

        Map<ScalarOperator, ColumnRefOperator> commonSubOperators =
                commonSubOperatorsByDepth.values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (commonSubOperators.isEmpty()) {
            // no rewrite
            return projection;
        }

        boolean hasRewritten = false;
        for (ScalarOperator operator : columnRefMap.values()) {
            ScalarOperator rewriteOperator =
                    ScalarOperatorsReuse.rewriteOperatorWithCommonOperator(operator, commonSubOperators);
            if (!rewriteOperator.equals(operator)) {
                hasRewritten = true;
                break;
            }
        }

        /*
         * 1. Rewrite the operator with the common sub operators
         * 2. Put the common sub operators to projection, we need to compute
         * common sub operators firstly in BE
         */
        if (hasRewritten) {
            Map<ColumnRefOperator, ScalarOperator> newMap =
                    Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
            // Apply to normalize rule to eliminate invalid ColumnRef usage for in-predicate
            com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter rewriter =
                    new com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter();
            List<ScalarOperatorRewriteRule> rules = Collections.singletonList(new NormalizePredicateRule());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
                ScalarOperator rewriteOperator =
                        ScalarOperatorsReuse.rewriteOperatorWithCommonOperator(kv.getValue(), commonSubOperators);
                rewriteOperator = rewriter.rewrite(rewriteOperator, rules);

                if (rewriteOperator.isColumnRef() && newMap.containsValue(rewriteOperator)) {
                    // must avoid multi columnRef: columnRef
                    //@TODO(hechenfeng): remove it if BE support COW column
                    newMap.put(kv.getKey(), kv.getValue());
                } else {
                    newMap.put(kv.getKey(), rewriteOperator);
                }
            }

            Map<ColumnRefOperator, ScalarOperator> newCommonMap =
                    Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
            for (Map.Entry<ScalarOperator, ColumnRefOperator> kv : commonSubOperators.entrySet()) {
                Preconditions.checkState(!newMap.containsKey(kv.getValue()));
                ScalarOperator rewrittenOperator = rewriter.rewrite(kv.getKey(), rules);
                newCommonMap.put(kv.getValue(), rewrittenOperator);
            }

            return new Projection(newMap, newCommonMap);
        }
        return projection;
    }

    public static class LambdaOperatorRewriter extends ScalarOperatorVisitor<Void, Void> {

        private final ColumnRefFactory columnRefFactory;

        public LambdaOperatorRewriter(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }


        @Override
        public Void visit(ScalarOperator operator, Void context) {
            if (!operator.getChildren().isEmpty()) {
                operator.getChildren().forEach(argument -> argument.accept(this, context));
            }
            return null;
        }

        @Override
        public Void visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void context) {
            operator.getLambdaExpr().accept(this, context);
            Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                    Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
            ColumnRefOperator keyCol = columnRefFactory.create("lambda", operator.getType(),
                    operator.isNullable(), false);
            columnRefMap.put(keyCol, operator.getLambdaExpr());
            Projection projection = getNewProjection(new Projection(columnRefMap), columnRefFactory, true);
            columnRefMap = projection.getCommonSubOperatorMap();
            if (!columnRefMap.isEmpty()) {
                operator.addColumnToExpr(columnRefMap);
                operator.setChild(0, projection.getColumnRefMap().get(keyCol));
            }
            return null;
        }
    }
}
