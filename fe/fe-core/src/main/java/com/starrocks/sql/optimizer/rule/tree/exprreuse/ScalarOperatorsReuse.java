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

package com.starrocks.sql.optimizer.rule.tree.exprreuse;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
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
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
            Projection projection,
            List<ScalarOperator> scalarOperators,
            ColumnRefFactory columnRefFactory) {
        // 1. Recursively collect common sub operators for the input operators
        CommonSubScalarOperatorCollector operatorCollector = new CommonSubScalarOperatorCollector();
        for (ScalarOperator operator : scalarOperators) {
            // To avoid stack overflow if the operator tree is too deep(eg: contains too many `or` functions), set a
            // limit to the depth of the operator tree.
            if (operator.getDepth() > Config.max_scalar_operator_optimize_depth) {
                continue;
            }
            operator.accept(operatorCollector, new CommonOperatorContext(false));
        }
        if (projection != null) {
            projection.setNeedReuseLambdaDependentExpr(operatorCollector.hasLambdaFunction());
        }
        if (operatorCollector.commonOperatorsByDepth.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Integer, List<ScalarOperator>> sortedCommonOperatorsByDepth = new LinkedHashMap<>();
        operatorCollector.commonOperatorsByDepth.entrySet().stream()
                .filter(e -> e.getKey() > 0).forEach(
                        e -> sortedCommonOperatorsByDepth.put(e.getKey(),
                                e.getValue().stream().map(CommonSubScalarOperatorCollector.OperatorId::key).toList()));

        // 2. Rewrite high depth common operators with low depth common operators
        // 3. Create the result column ref for each common operators
        Map<ScalarOperator, ColumnRefOperator> rewriteWith = new HashMap<>();
        ImmutableMap.Builder<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubOperators =
                ImmutableMap.builder();
        for (Map.Entry<Integer, List<ScalarOperator>> kv : sortedCommonOperatorsByDepth.entrySet()) {
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter(rewriteWith);
            ImmutableMap.Builder<ScalarOperator, ColumnRefOperator> operatorColumnMapBuilder = ImmutableMap.builder();
            for (ScalarOperator operator : kv.getValue()) {
                ScalarOperator rewrittenOperator = operator.accept(rewriter, null);
                ColumnRefOperator op =
                        columnRefFactory.create(operator, rewrittenOperator.getType(), rewrittenOperator.isNullable());
                operatorColumnMapBuilder.put(rewrittenOperator, op);
            }
            Map<ScalarOperator, ColumnRefOperator> operatorColumnMap = operatorColumnMapBuilder.build();
            commonSubOperators.put(kv.getKey(), operatorColumnMap);
            rewriteWith.putAll(operatorColumnMap);
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
            CallOperator operator = new CallOperator(call.getFnName(),
                    call.getType(),
                    call.getChildren().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()),
                    call.getFunction(),
                    call.isDistinct(), call.isRemovedDistinct());
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
        public ScalarOperator visitMap(MapOperator operator, Void context) {
            ScalarOperator newOperator = new MapOperator(operator.getType(),
                    operator.getChildren().stream().map(argument -> argument.accept(this, null)).
                            collect(Collectors.toList()));
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
                                predicate.getDictionaryTxnId(), predicate.getKeySize(), predicate.getNullIfNotExist());
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

    private static class CommonOperatorContext {
        public boolean isPartOfLambdaExpr;
        // used to record the lambda arguments during visiting operator.

        // take map_apply((k,v)->(k, array_sum(array_map(arg -> arg * v, array_column), map_column))) as an example,
        // when visiting `array_sum(array_map(arg -> arg * v, array_column))`,
        // currentLambdaArguments will contain k and v,
        // outerLambdaArguments will be empty since there are no higher-order lambda functions nested outside.
        // when visiting `arg * v`,
        // currentLambdaArguments will contain arg,
        // outerLambdaArguments will contain k and v since there is a map_apply's lambda expr outside.

        // this information will help us determine whether an operator can be reused.
        public Set<ColumnRefOperator> currentLambdaArguments;
        public Set<ColumnRefOperator> outerLambdaArguments;
        public ColumnRefSet usedColumns;

        public CommonOperatorContext(boolean isPartOfLambdaExpr) {
            this.isPartOfLambdaExpr = isPartOfLambdaExpr;
            this.currentLambdaArguments = Sets.newHashSet();
            this.outerLambdaArguments = Sets.newHashSet();
            this.usedColumns = new ColumnRefSet();
        }

        public CommonOperatorContext(boolean isPartOfLambdaExpr, Set<ColumnRefOperator> currentLambdaArguments,
                                     Set<ColumnRefOperator> outerLambdaArguments) {
            this.isPartOfLambdaExpr = isPartOfLambdaExpr;
            this.currentLambdaArguments = currentLambdaArguments;
            this.outerLambdaArguments = outerLambdaArguments;
            this.usedColumns = new ColumnRefSet();
        }
    }

    record CommonResult(int depth, List<Integer> childrenGroup) {}

    private static class CommonSubScalarOperatorCollector
            extends ScalarOperatorVisitor<CommonResult, CommonOperatorContext> {
        record OperatorId(ScalarOperator key, List<Integer> childrenGroup) {
            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                OperatorId that = (OperatorId) o;
                return key.equalsSelf(that.key) && childrenGroup.equals(that.childrenGroup);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key.hashCodeSelf(), childrenGroup);
            }
        }

        // The key is operator tree depth, the value is operator set with same tree depth.
        // For operator list [a + b, a + b + c, a + d]
        // The operatorsByDepth is
        // {[1] -> [a + b, a + d], [2] -> [a + b + c]}
        // The commonOperatorsByDepth is
        // {[1] -> [a + b]}
        private final Map<Integer, Map<OperatorId, Integer>> operatorsByDepth = new HashMap<>();
        private final Map<Integer, Set<OperatorId>> commonOperatorsByDepth = new HashMap<>();
        private int currentId = 0;

        // enable some special logic codes only for lambda functions.
        private boolean hasLambdaFunction;

        public boolean hasLambdaFunction() {
            return hasLambdaFunction;
        }

        private CommonResult collectCommonOperatorsByDepth(int depth, ScalarOperator operator, List<Integer> groups,
                                                           CommonOperatorContext context) {
            OperatorId id = new OperatorId(operator, groups);
            Map<OperatorId, Integer> level = operatorsByDepth.computeIfAbsent(depth, c -> Maps.newHashMap());

            boolean isDuplicated = level.containsKey(id);
            int group = level.computeIfAbsent(id, k -> currentId++);
            CommonResult result = new CommonResult(depth, List.of(group));

            boolean isDependentOnOuterLambda = context.usedColumns.containsAny(context.outerLambdaArguments);
            if (isDependentOnOuterLambda) {
                return result;
            }

            boolean isDependentOnCurrentLambdaArguments =
                    context.usedColumns.containsAny(context.currentLambdaArguments);
            if (isDuplicated && !isDependentOnCurrentLambdaArguments) {
                Set<OperatorId> commonGroup =
                        commonOperatorsByDepth.computeIfAbsent(depth, c -> Sets.newLinkedHashSet());
                commonGroup.add(id);
            } else if (!isDependentOnCurrentLambdaArguments) {
                // if this operator has appeared before,
                // ot it is within a lambda function but does not depend on current lambda function's arguments,
                // we treat it as a common operator.
                if (context.isPartOfLambdaExpr) {
                    Set<OperatorId> commonGroup =
                            commonOperatorsByDepth.computeIfAbsent(depth, c -> Sets.newLinkedHashSet());
                    commonGroup.add(id);
                }
            }
            return result;
        }

        @Override
        public CommonResult visit(ScalarOperator scalarOperator, CommonOperatorContext context) {
            if (scalarOperator.isConstant() || scalarOperator.getChildren().isEmpty()) {
                // leaf node
                OperatorId id = new OperatorId(scalarOperator, List.of());
                Map<OperatorId, Integer> level = operatorsByDepth.computeIfAbsent(0, c -> Maps.newLinkedHashMap());
                int group = level.computeIfAbsent(id, k -> currentId++);
                return new CommonResult(0, List.of(group));
            }

            if (scalarOperator instanceof LambdaFunctionOperator) {
                context.currentLambdaArguments.addAll(((LambdaFunctionOperator) scalarOperator).getRefColumns());
            }

            CommonResult result = visitChildren(scalarOperator, context);
            return collectCommonOperatorsByDepth(result.depth + 1, scalarOperator, result.childrenGroup, context);
        }

        private CommonResult visitChildren(ScalarOperator scalarOperator, CommonOperatorContext context) {
            int depth = 0;
            List<Integer> groups = Lists.newArrayList();
            if (!scalarOperator.getChildren().isEmpty()) {
                CommonResult res = scalarOperator.getChild(0).accept(this, context);
                depth = Math.max(depth, res.depth);
                groups.addAll(res.childrenGroup);
            }
            for (int i = 1; i < scalarOperator.getChildren().size(); i++) {
                CommonOperatorContext childContext = new CommonOperatorContext(context.isPartOfLambdaExpr,
                        context.currentLambdaArguments, context.outerLambdaArguments);
                CommonResult res = scalarOperator.getChild(i).accept(this, childContext);
                depth = Math.max(depth, res.depth);
                groups.addAll(res.childrenGroup);
                context.usedColumns.union(childContext.usedColumns);
            }

            return new CommonResult(depth, groups);
        }

        @Override
        public CommonResult visitVariableReference(ColumnRefOperator variable, CommonOperatorContext context) {
            if (variable.getOpType() == OperatorType.LAMBDA_ARGUMENT) {
                context.usedColumns.union(variable);
            }
            return super.visitVariableReference(variable, context);
        }

        @Override
        public CommonResult visitLambdaFunctionOperator(LambdaFunctionOperator scalarOperator,
                                                        CommonOperatorContext context) {
            // a lambda function like  x->x+1 can't be reused anymore, so directly visit its lambda expression.
            hasLambdaFunction = true;
            CommonOperatorContext newContext = new CommonOperatorContext(true);
            newContext.outerLambdaArguments.addAll(context.outerLambdaArguments);
            newContext.outerLambdaArguments.addAll(context.currentLambdaArguments);
            newContext.currentLambdaArguments.addAll(scalarOperator.getRefColumns());
            CommonResult result = visit(scalarOperator.getLambdaExpr(), newContext);
            context.usedColumns.union(newContext.usedColumns);
            return result;
        }

        @Override
        public CommonResult visitCall(CallOperator scalarOperator, CommonOperatorContext context) {
            CallOperator callOperator = scalarOperator.cast();
            if (FunctionSet.nonDeterministicFunctions.contains(callOperator.getFnName())) {
                // try to reuse non deterministic function
                // for example:
                // select (rnd + 1) as rnd1, (rnd + 2) as rnd2 from (select rand() as rnd) sub
                CommonResult result = visitChildren(scalarOperator, context);
                return collectCommonOperatorsByDepth(1, scalarOperator, result.childrenGroup, context);
            } else {
                return visit(scalarOperator, context);
            }
        }

        @Override
        public CommonResult visitDictMappingOperator(DictMappingOperator scalarOperator,
                                                     CommonOperatorContext context) {
            return collectCommonOperatorsByDepth(1, scalarOperator, List.of(), context);
        }
    }

    public static Projection rewriteProjectionOrLambdaExpr(Projection projection, ColumnRefFactory columnRefFactory) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
        List<ScalarOperator> scalarOperators = Lists.newArrayList(columnRefMap.values());
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubOperatorsByDepth = ScalarOperatorsReuse
                .collectCommonSubScalarOperators(projection, scalarOperators,
                        columnRefFactory);

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

            // array_sort_lambda/array_map contains non-deterministic functions like rand(), should not
            // be extracted CSE from.
            boolean shouldNotReplace = ScalarOperatorUtil.getStream(operator)
                    .filter(child -> (child instanceof CallOperator))
                    .map(child -> (CallOperator) child)
                    .filter(child -> child.getFnName().equals(FunctionSet.ARRAY_SORT_LAMBDA) ||
                            child.getFnName().equals(FunctionSet.ARRAY_MAP))
                    .anyMatch(Utils::hasNonDeterministicFunc);
            if (shouldNotReplace) {
                continue;
            }
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
            List<ScalarOperatorRewriteRule> rules =
                    ImmutableList.of(new NormalizePredicateRule(), new ReduceCastRule());
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
                    rewriteProjectionOrLambdaExpr(new Projection(columnRefMap), columnRefFactory);
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
