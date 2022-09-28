// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.jetbrains.annotations.NotNull;
import org.spark_project.guava.collect.ImmutableMap;

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
                    .put(FunctionSet.AVG, Pair.create(FunctionSet.AVG, FunctionSet.AVG))
                    .put(FunctionSet.HLL_UNION, Pair.create(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION))
                    .put(FunctionSet.NDV, Pair.create(FunctionSet.HLL_UNION, FunctionSet.NDV))
                    .put(FunctionSet.BITMAP_UNION, Pair.create(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION))
                    .put(FunctionSet.BITMAP_UNION_COUNT,
                            Pair.create(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION_COUNT))
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

        // check distribution satisfy scan node
        List<Integer> groupBy = aggregate.getGroupingKeys().stream().map(ColumnRefOperator::getId)
                .collect(Collectors.toList());

        if (groupBy.isEmpty() || groupBy.containsAll(scan.getDistributionSpec().getShuffleColumns())) {
            return false;
        }

        groupBy.add(distinctColumns.get(0).getId());
        return groupBy.containsAll(scan.getDistributionSpec().getShuffleColumns());
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
            Function origin = v.getFunction();
            String firstFn = OTHER_FUNCTION_TRANS.get(origin.getFunctionName().getFunction()).first;
            String secondFn = OTHER_FUNCTION_TRANS.get(origin.getFunctionName().getFunction()).second;

            CallOperator firstAgg = genAggregation(firstFn, origin.getArgs(), v.getChildren(), v);
            ColumnRefOperator firstOutput = factory.create(firstAgg, firstAgg.getType(), firstAgg.isNullable());

            firstAggregations.put(firstOutput, firstAgg);

            CallOperator secondAgg =
                    genAggregation(secondFn, new Type[] {firstAgg.getType()}, Lists.newArrayList(firstOutput), v);
            secondAggregations.put(k, secondAgg);
        });

        distinctMap.forEach((k, v) -> {
            Function origin = v.getFunction();
            String secondFn = DISTINCT_FUNCTION_TRANS.get(origin.getFunctionName().getFunction());
            CallOperator secondAgg = genAggregation(secondFn, origin.getArgs(), v.getChildren(), v);
            secondAggregations.put(k, secondAgg);
        });

        LogicalAggregationOperator first =
                new LogicalAggregationOperator(AggType.GLOBAL, firstGroupBy, firstAggregations);
        second.withOperator(aggregate).setAggregations(secondAggregations);

        return Lists.newArrayList(
                OptExpression.create(second.build(), OptExpression.create(first, input.getInputs())));
    }

    @NotNull
    private CallOperator genAggregation(String name, Type[] argTypes, List<ScalarOperator> args, CallOperator origin) {
        if (FunctionSet.SUM.equals(name) || FunctionSet.MAX.equals(name) || FunctionSet.MIN.equals(name)) {
            // for decimal
            return new CallOperator(name, origin.getType(), args, origin.getFunction());
        }
        Function fn = Expr.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, fn.getReturnType(), args, fn);
    }

    private boolean isDistinct(CallOperator call) {
        return call.isDistinct() ||
                FunctionSet.MULTI_DISTINCT_SUM.equals(call.getFunction().getFunctionName().getFunction()) ||
                FunctionSet.MULTI_DISTINCT_COUNT.equals(call.getFunction().getFunctionName().getFunction());
    }
}
