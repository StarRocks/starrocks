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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.structure.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/*
 *
 * Rewrite count(distinct xx) group by x when distinct columns satisfy scan distribution
 *
 * e.g.
 * select count(distinct xx) from t group by x
 * ->
 * select count(xx) from (select x, xx from t group by x, xx) tt group by x;
 *
 */
public class GroupByCountDistinctRewriteRule extends TransformationRule {
    // multi-stage distinct function mapping
    // origin aggregate -> 2nd stage
    private static final Map<String, String> DISTINCT_FUNCTION_TRANS = ImmutableMap.<String, String>builder()
            .put(FunctionSet.COUNT, FunctionSet.COUNT)
            .put(FunctionSet.SUM, FunctionSet.SUM)
            .put(FunctionSet.MULTI_DISTINCT_SUM, FunctionSet.SUM)
            .put(FunctionSet.MULTI_DISTINCT_COUNT, FunctionSet.COUNT)
            .build();

    // multi-stage other function mapping
    // origin aggregate -> Pair<1st stage, 2nd stage>
    // these aggregate function can be support multi-stage
    private static final Map<String, Pair<String, String>> OTHER_FUNCTION_TRANS =
            ImmutableMap.<String, Pair<String, String>>builder()
                    .put(FunctionSet.COUNT, Pair.create(FunctionSet.COUNT, FunctionSet.SUM))
                    .put(FunctionSet.MAX, Pair.create(FunctionSet.MAX, FunctionSet.MAX))
                    .put(FunctionSet.MIN, Pair.create(FunctionSet.MIN, FunctionSet.MIN))
                    .put(FunctionSet.SUM, Pair.create(FunctionSet.SUM, FunctionSet.SUM))
                    .put(FunctionSet.HLL_UNION_AGG, Pair.create(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION_AGG))
                    .put(FunctionSet.BITMAP_UNION_COUNT,
                            Pair.create(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION_COUNT))
                    .put(FunctionSet.HLL_UNION, Pair.create(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION))
                    .put(FunctionSet.BITMAP_UNION, Pair.create(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION))
                    .put(FunctionSet.PERCENTILE_UNION,
                            Pair.create(FunctionSet.PERCENTILE_UNION, FunctionSet.PERCENTILE_UNION))
                    .build();

    public GroupByCountDistinctRewriteRule() {
        super(RuleType.TF_REWRITE_GROUP_BY_COUNT_DISTINCT,
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();

        Collection<CallOperator> calls = aggregate.getAggregations().values();

        List<CallOperator> distinctList = calls.stream().filter(this::isDistinct).collect(Collectors.toList());
        List<CallOperator> otherList = calls.stream().filter(c -> !isDistinct(c)).collect(Collectors.toList());

        // check other function
        if (!otherList.isEmpty() && !otherList.stream().map(c -> c.getFunction().getFunctionName().getFunction())
                .allMatch(OTHER_FUNCTION_TRANS::containsKey)) {
            return false;
        }

        // check distinct function
        if (distinctList.isEmpty() || !distinctList.stream().map(f -> f.getFunction().getFunctionName().getFunction())
                .allMatch(DISTINCT_FUNCTION_TRANS::containsKey)) {
            return false;
        }

        // check distinct column only one
        if (distinctList.stream().anyMatch(f -> f.getChildren().size() > 1)) {
            return false;
        }

        if (!distinctList.stream().map(f -> f.getChild(0)).allMatch(ScalarOperator::isColumnRef)) {
            return false;
        }

        List<ColumnRefOperator> distinctColumns = distinctList.stream().map(f -> f.getChild(0))
                .map(c -> (ColumnRefOperator) c).distinct().collect(Collectors.toList());
        if (distinctColumns.size() != 1) {
            return false;
        }

        if (!(scan.getDistributionSpec() instanceof HashDistributionSpec)) {
            return false;
        }

        // check distribution satisfy scan node
        List<Integer> groupBy = aggregate.getGroupingKeys().stream().map(ColumnRefOperator::getId)
                .collect(Collectors.toList());

        List<Integer> distributionCols = ((HashDistributionSpec) scan.getDistributionSpec()).getShuffleColumns().stream().map(
                DistributionCol::getColId).collect(Collectors.toList());

        if (groupBy.isEmpty() || groupBy.containsAll(distributionCols)) {
            return false;
        }

        // check limit
        if (aggregate.hasLimit()) {
            return false;
        }

        groupBy.add(distinctColumns.get(0).getId());
        return groupBy.containsAll(distributionCols);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();

        Map<ColumnRefOperator, CallOperator> distinctMap = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> otherMap = Maps.newHashMap();

        aggregate.getAggregations().forEach((k, v) -> {
            if (isDistinct(v)) {
                distinctMap.put(k, v);
            } else {
                otherMap.put(k, v);
            }
        });

        LogicalAggregationOperator.Builder second = new LogicalAggregationOperator.Builder();

        Optional<ColumnRefOperator> distinctColumn = distinctMap.values().stream().map(f -> f.getChild(0))
                .map(c -> (ColumnRefOperator) c).distinct().findFirst();

        List<ColumnRefOperator> firstGroupBy = Lists.newArrayList(aggregate.getGroupingKeys());
        Preconditions.checkState(distinctColumn.isPresent());
        firstGroupBy.add(distinctColumn.get());

        Map<ColumnRefOperator, CallOperator> firstAggregations = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> secondAggregations = Maps.newHashMap();

        ColumnRefFactory factory = context.getColumnRefFactory();
        otherMap.forEach((k, v) -> {
            CallOperator firstAgg = transformOtherAgg(v, v.getArguments(), true);
            ColumnRefOperator firstOutput = factory.create(firstAgg, firstAgg.getType(), firstAgg.isNullable());
            firstAggregations.put(firstOutput, firstAgg);

            CallOperator secondAgg = transformOtherAgg(v, Lists.newArrayList(firstOutput), false);
            secondAggregations.put(k, secondAgg);
        });

        distinctMap.forEach((k, v) -> {
            CallOperator secondAgg = transformDistinctAgg(v);
            secondAggregations.put(k, secondAgg);
        });

        LogicalAggregationOperator first =
                new LogicalAggregationOperator(AggType.GLOBAL, firstGroupBy, firstAggregations);
        second.withOperator(aggregate).setAggregations(secondAggregations);

        return Lists.newArrayList(
                OptExpression.create(second.build(), OptExpression.create(first, input.getInputs())));
    }

    private CallOperator transformOtherAgg(CallOperator origin, List<ScalarOperator> args, boolean isFirst) {
        String originFuncName = origin.getFunction().functionName();
        if (isFirst) {
            String firstFuncName = OTHER_FUNCTION_TRANS.get(originFuncName).first;
            if (StringUtils.equals(originFuncName, firstFuncName)) {
                return origin;
            } else {
                Function newFunc = Expr.getBuiltinFunction(firstFuncName, origin.getFunction().getArgs(),
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                return new CallOperator(firstFuncName, newFunc.getReturnType(), args, newFunc);
            }
        } else {
            String secondFuncName = OTHER_FUNCTION_TRANS.get(originFuncName).second;
            Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
            Function newFunc = Expr.getBuiltinFunction(secondFuncName, argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Preconditions.checkNotNull(newFunc);
            newFunc = newFunc.copy();
            // for decimal
            newFunc = newFunc.updateArgType(argTypes);
            newFunc.setRetType(origin.getFunction().getReturnType());
            return new CallOperator(secondFuncName, newFunc.getReturnType(), args, newFunc);
        }
    }


    private CallOperator transformDistinctAgg(CallOperator origin) {
        String originFuncName = origin.getFunction().functionName();
        String newFuncName = DISTINCT_FUNCTION_TRANS.get(originFuncName);
        if (StringUtils.equals(originFuncName, newFuncName)) {
            return new CallOperator(originFuncName, origin.getType(), origin.getChildren(), origin.getFunction());
        } else {
            Function newFunc = Expr.getBuiltinFunction(newFuncName, origin.getFunction().getArgs(),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            return new CallOperator(newFuncName, newFunc.getReturnType(), origin.getChildren(), newFunc);
        }
    }

    private boolean isDistinct(CallOperator call) {
        return call.isDistinct() ||
                FunctionSet.MULTI_DISTINCT_SUM.equals(call.getFunction().functionName()) ||
                FunctionSet.MULTI_DISTINCT_COUNT.equals(call.getFunction().functionName()) ||
                FunctionSet.ARRAY_AGG_DISTINCT.equals(call.getFunction().functionName());
    }
}
