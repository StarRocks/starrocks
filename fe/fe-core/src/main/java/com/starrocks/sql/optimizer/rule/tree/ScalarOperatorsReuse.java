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
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictionaryGetOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
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
                commonSubOperatorsByDepth = collectCommonSubScalarOperators(null, operators,
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
            Projection projection,
            List<ScalarOperator> scalarOperators,
            ColumnRefFactory columnRefFactory, boolean reuseLambdaDependentExpr) {
        // 1. Recursively collect common sub operators for the input operators
        CommonSubScalarOperatorCollector operatorCollector = new CommonSubScalarOperatorCollector(reuseLambdaDependentExpr);
        scalarOperators.forEach(operator -> operator.accept(operatorCollector, null));
        if (projection != null) {
            projection.setNeedReuseLambdaDependentExpr(operatorCollector.hasLambdaFunction());
        }
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

        @Override
        public ScalarOperator visitCollectionElement(CollectionElementOperator elemOp, Void context) {
            ScalarOperator operator = new CollectionElementOperator(elemOp.getType(),
                    elemOp.getChild(0).accept(this, null),
                    elemOp.getChild(1).accept(this, null),
                    elemOp.isCheckOutOfBounds());
            return tryRewrite(operator);
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
        public ScalarOperator visitSubfield(SubfieldOperator predicate, Void context) {
            // only rewrite subfield operator if and only if child is DictionaryGetOperator
            if (predicate.getChild(0) instanceof DictionaryGetOperator) {
                ScalarOperator operator = new SubfieldOperator(predicate.getChild(0).accept(this, null),
                                predicate.getType(), predicate.getFieldNames(), predicate.getCopyFlag());
                return tryRewrite(operator);
            }
            return predicate;
        }

        @Override
        public ScalarOperator visitDictionaryGetOperator(DictionaryGetOperator predicate, Void context) {
            ScalarOperator operator = new DictionaryGetOperator(
                    predicate.getChildren().stream().map(
                        argument -> argument.accept(this, null)).collect(Collectors.toList()),
                            predicate.getType(), predicate.getDictionaryId(),
                                predicate.getDictionaryTxnId(), predicate.getKeySize());
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

        // isLambdaDependent means whether an expression is dependent on outer lambda arguments,
        // it works when not reuse lambda-dependent sub expressions. For example, select array_length(a), array_sum(a)
        // from (select array_map(x->2*x+1+2*x, array) as a from t)A; when reuseLambdaDependentExpr is
        // false, the 2*x is lambda dependent, but array_map(x->2*x+1+2*x, array) isn't,
        // so 2*x can't be reused, array_map(x->2*x+1+2*x, array) can be reused.
        // 2*x can be reused within the array_map when reuseLambdaDependentExpr is true.
        private boolean isLambdaDependent;

        private final boolean reuseLambdaDependentExpr;

        public boolean hasLambdaFunction() {
            return hasLambdaFunction;
        }

        // enable some special logic codes only for lambda functions.
        private boolean hasLambdaFunction;
        private Set<ColumnRefOperator> lambdaArguments = Sets.newHashSet();

        private CommonSubScalarOperatorCollector(boolean reuseLambdaDependentExpr) {
            this.reuseLambdaDependentExpr = reuseLambdaDependentExpr;
            this.isLambdaDependent = false;
            this.hasLambdaFunction = reuseLambdaDependentExpr;
        }


        private int collectCommonOperatorsByDepth(int depth, ScalarOperator operator) {
            Set<ScalarOperator> operators = getOperatorsByDepth(depth, operatorsByDepth);
            isLambdaDependent = false;
            lambdaArguments.clear();
            if (!isNonDeterministicFuncOrLambdaDependent(operator) && operators.contains(operator)) {
                Set<ScalarOperator> commonOperators = getOperatorsByDepth(depth, commonOperatorsByDepth);
                commonOperators.add(operator);
            }
            // lambda-dependent expressions should not be put into operators.
            if (!isLambdaDependent) {
                operators.add(operator);
            }
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

            return collectCommonOperatorsByDepth(scalarOperator.getChildren().stream().map(argument ->
                    argument.accept(this, context)).reduce(Math::max).map(m -> m + 1).orElse(1), scalarOperator);
        }

        @Override
        public Integer visitLambdaFunctionOperator(LambdaFunctionOperator scalarOperator, Void context) {
            // a lambda function like  x->x+1 can't be reused anymore, so directly visit its lambda expression.
            hasLambdaFunction = true;
            return visit(scalarOperator.getLambdaExpr(), context);
        }

        @Override
        public Integer visitDictMappingOperator(DictMappingOperator scalarOperator, Void context) {
            return collectCommonOperatorsByDepth(1, scalarOperator);
        }

        // If a scalarOperator contains any non-deterministic function, it cannot be reused
        // because the non-deterministic function results returned each time are inconsistent.
        // a lambda-dependent expressions also can't be reused if reuseLambdaDependentExpr is false.
        // For example, array_map(x->2x+1+2x,array),2x is lambda-dependent, so it can't be reused if
        // reuseLambdaDependentExpr is false, but array_map(x->2x+1+2x,array) can be reused if needed.
        private boolean isNonDeterministicFuncOrLambdaDependent(ScalarOperator scalarOperator) {

            if (hasLambdaFunction && !reuseLambdaDependentExpr) {
                if (scalarOperator instanceof LambdaFunctionOperator) {
                    LambdaFunctionOperator lambdaOp = (LambdaFunctionOperator) scalarOperator;
                    lambdaArguments.addAll(lambdaOp.getRefColumns());
                }

                if (scalarOperator.getOpType().equals(OperatorType.LAMBDA_ARGUMENT)) {
                    isLambdaDependent = !lambdaArguments.contains(scalarOperator);
                    return isLambdaDependent;
                }
            }

            if (scalarOperator instanceof CallOperator) {
                String fnName = ((CallOperator) scalarOperator).getFnName();
                if (FunctionSet.nonDeterministicFunctions.contains(fnName)) {
                    return true;
                }
            }
            for (ScalarOperator child : scalarOperator.getChildren()) {
                if (isNonDeterministicFuncOrLambdaDependent(child)) {
                    return true;
                }
            }
            return false;
        }
    }


    public static Projection rewriteProjectionOrLambdaExpr(Projection projection, ColumnRefFactory columnRefFactory,
                                                           boolean reuseLambdaDependentExpr) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
        List<ScalarOperator> scalarOperators = Lists.newArrayList(columnRefMap.values());
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubOperatorsByDepth = ScalarOperatorsReuse
                .collectCommonSubScalarOperators(projection, scalarOperators,
                        columnRefFactory, reuseLambdaDependentExpr);

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

            return new Projection(newMap, newCommonMap, projection.needReuseLambdaDependentExpr());
        }
        return projection;
    }

    public static class LambdaFunctionOperatorRewriter extends ScalarOperatorVisitor<Void, Void> {

        private final ColumnRefFactory columnRefFactory;

        public LambdaFunctionOperatorRewriter(ColumnRefFactory columnRefFactory) {
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
            Projection fakeProjection =
                    rewriteProjectionOrLambdaExpr(new Projection(columnRefMap), columnRefFactory, true);
            columnRefMap = fakeProjection.getCommonSubOperatorMap();
            if (!columnRefMap.isEmpty()) {
                operator.addColumnToExpr(columnRefMap);
                // replace the original lambda expression.
                operator.setChild(0, fakeProjection.getColumnRefMap().get(keyCol));
            }
            return null;
        }
    }
}
