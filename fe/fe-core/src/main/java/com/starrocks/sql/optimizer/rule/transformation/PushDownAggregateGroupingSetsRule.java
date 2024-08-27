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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * `PushDownAggregateGroupingSetsRule` will rewrite grouping sets. eg:
 *      select a, b, c, d, e sum(f) from t group by rollup(a, b, c, d, e);
 * rewrite to:
 *    with cte1 as (select a, b, c, d, e, sum(f) x from t group by rollup(a, b, c, d, e))
 *      selec * fom cte1
 *      union all
 *      select a, b, c, d, null, sum(x) x from t group by rollup(a, b, c, d)
 */
public class PushDownAggregateGroupingSetsRule extends TransformationRule {
    private static final List<String> SUPPORT_AGGREGATE_FUNCTIONS = Lists.newArrayList(FunctionSet.MAX,
            FunctionSet.MIN, FunctionSet.SUM);

    public PushDownAggregateGroupingSetsRule() {
        super(RuleType.TF_PUSHDOWN_AGG_GROUPING_SET,
                Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_REPEAT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        LogicalRepeatOperator repeatOperator = (LogicalRepeatOperator) input.inputAt(0).getOp();
        if (aggregate.getType() != AggType.GLOBAL || repeatOperator.getRepeatColumnRef().size() <= 3
                || repeatOperator.hasPushDown()) {
            return false;
        }

        if (!aggregate.getAggregations().values().stream()
                .allMatch(agg -> SUPPORT_AGGREGATE_FUNCTIONS.contains(agg.getFnName()) &&
                        !agg.isDistinct() && agg.getUsedColumns().cardinality() <= 1)) {
            return false;
        }

        List<ColumnRefOperator> allRepeatRefs = repeatOperator.getRepeatColumnRef()
                .get(repeatOperator.getRepeatColumnRef().size() - 1);
        // check grouping sets, rollup/cube, last group must contain all keys
        Set<ColumnRefOperator> checkRefs = new HashSet<>(allRepeatRefs);
        for (int i = 0; i < repeatOperator.getRepeatColumnRef().size() - 1; i++) {
            List<ColumnRefOperator> refs = repeatOperator.getRepeatColumnRef().get(i);
            if (refs.stream().anyMatch(ref -> !allRepeatRefs.contains(ref))) {
                return false;
            }
            refs.forEach(checkRefs::remove);
        }

        checkRefs.addAll(repeatOperator.getOutputGrouping());
        return !checkRefs.containsAll(aggregate.getGroupingKeys());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        LogicalRepeatOperator repeat = (LogicalRepeatOperator) input.inputAt(0).getOp();

        ColumnRefFactory factory = context.getColumnRefFactory();
        int cteId = context.getCteContext().getNextCteId();

        // cte produce and push down aggregate
        context.getCteContext().addForceCTE(cteId);
        OptExpression cteProduce = buildCTEProduce(context, input, cteId);

        // new grouping sets consume
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputs1 = Maps.newHashMap();
        OptExpression subRepeatConsume = buildSubRepeatConsume(factory, consumeOutputs1, aggregate, repeat, cteId);

        // select consume
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputs2 = Maps.newHashMap();
        OptExpression selectConsume = buildSelectConsume(factory, consumeOutputs2, aggregate, repeat, cteId);

        // union all
        OptExpression union =
                buildUnionAll(aggregate, consumeOutputs1, subRepeatConsume, consumeOutputs2, selectConsume);

        return Lists.newArrayList(OptExpression.create(new LogicalCTEAnchorOperator(cteId), cteProduce, union));
    }

    private OptExpression buildUnionAll(LogicalAggregationOperator aggregate,
                                        Map<ColumnRefOperator, ColumnRefOperator> inputs1, OptExpression repeatConsume,
                                        Map<ColumnRefOperator, ColumnRefOperator> inputs2,
                                        OptExpression selectConsume) {

        List<ColumnRefOperator> outputs = Lists.newArrayList();
        outputs.addAll(aggregate.getGroupingKeys());
        outputs.addAll(aggregate.getAggregations().keySet());

        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList();
        childOutputs.add(outputs.stream().map(inputs1::get).collect(Collectors.toList()));
        childOutputs.add(outputs.stream().map(inputs2::get).collect(Collectors.toList()));
        LogicalUnionOperator union = LogicalUnionOperator.builder()
                .setOutputColumnRefOp(outputs)
                .setChildOutputColumns(childOutputs)
                .setLimit(aggregate.getLimit())
                .setPredicate(aggregate.getPredicate())
                .build();
        return OptExpression.create(union, repeatConsume, selectConsume);
    }

    private OptExpression buildCTEProduce(OptimizerContext context, OptExpression input, int cteId) {
        OptExpression repeatInput = input.inputAt(0);
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();
        LogicalRepeatOperator repeat = (LogicalRepeatOperator) repeatInput.getOp();

        List<ColumnRefOperator> allGroupByRefs = repeat.getRepeatColumnRef()
                .get(repeat.getRepeatColumnRef().size() - 1);
        allGroupByRefs.retainAll(aggregate.getGroupingKeys());

        List<ColumnRefOperator> partitionRefs = Collections.emptyList();
        if (null == repeatInput.getStatistics()) {
            Utils.calculateStatistics(input, context);
        }
        if (null != repeatInput.getStatistics()) {
            // use one column to shuffle
            Statistics statistics = repeatInput.getStatistics();
            partitionRefs = allGroupByRefs.stream()
                    .filter(ref -> !statistics.getColumnStatistic(ref).isUnknown())
                    .sorted((o1, o2) -> Double.compare(statistics.getColumnStatistic(o2).getDistinctValuesCount(),
                            statistics.getColumnStatistic(o1).getDistinctValuesCount()))
                    .limit(1)
                    .collect(Collectors.toList());
        }
        if (!context.getSessionVariable().isCboPushDownGroupingSetReshuffle() || partitionRefs.isEmpty()) {
            partitionRefs = allGroupByRefs;
        }

        // replace output columns
        LogicalAggregationOperator.Builder builder = LogicalAggregationOperator.builder();
        builder.setType(AggType.GLOBAL)
                .setGroupingKeys(allGroupByRefs)
                .setAggregations(aggregate.getAggregations())
                .setPredicate(aggregate.getPredicate())
                .setPartitionByColumns(partitionRefs);
        LogicalAggregationOperator allColumnRefsAggregate = builder.build();
        // cte produce
        LogicalCTEProduceOperator produce = new LogicalCTEProduceOperator(cteId);

        return OptExpression.create(produce,
                OptExpression.create(allColumnRefsAggregate, input.inputAt(0).getInputs()));
    }

    /*
     * selec *, (grouping_id, grouping_set) fom cte1
     */
    private OptExpression buildSelectConsume(ColumnRefFactory factory,
                                             Map<ColumnRefOperator, ColumnRefOperator> outputs,
                                             LogicalAggregationOperator aggregate, LogicalRepeatOperator repeat,
                                             int cteId) {

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        // consume
        Map<ColumnRefOperator, ColumnRefOperator> cteColumnRefs = Maps.newHashMap();
        for (ColumnRefOperator input : aggregate.getAggregations().keySet()) {
            ColumnRefOperator cteOutput = factory.create(input, input.getType(), input.isNullable());
            cteColumnRefs.put(cteOutput, input);
            outputs.put(input, cteOutput);
            projectMap.put(cteOutput, cteOutput);
        }
        for (ColumnRefOperator input : aggregate.getGroupingKeys()) {
            if (!repeat.getOutputGrouping().contains(input)) {
                ColumnRefOperator cteOutput = factory.create(input, input.getType(), input.isNullable());
                cteColumnRefs.put(cteOutput, input);
                outputs.put(input, cteOutput);
                projectMap.put(cteOutput, cteOutput);
            }
        }

        LogicalCTEConsumeOperator consume = new LogicalCTEConsumeOperator(cteId, cteColumnRefs);

        // project
        int lastGroups = repeat.getRepeatColumnRef().size() - 1;
        for (int i = 0; i < repeat.getOutputGrouping().size(); i++) {
            ColumnRefOperator input = repeat.getOutputGrouping().get(i);
            ColumnRefOperator output = factory.create(input, input.getType(), input.isNullable());
            outputs.put(input, output);
            projectMap.put(output, ConstantOperator.createBigint(repeat.getGroupingIds().get(i).get(lastGroups)));
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        OptExpression result = OptExpression.create(projectOperator, OptExpression.create(consume));

        if (null != repeat.getPredicate()) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(outputs);
            ScalarOperator predicate = rewriter.rewrite(repeat.getPredicate());
            return OptExpression.create(new LogicalFilterOperator(predicate), result);
        }
        return result;
    }

    /*
     * select a, b, c, d, null, sum(x) x from t group by rollup(a, b, c, d)
     */
    private OptExpression buildSubRepeatConsume(ColumnRefFactory factory,
                                                Map<ColumnRefOperator, ColumnRefOperator> outputs,
                                                LogicalAggregationOperator aggregate, LogicalRepeatOperator repeat,
                                                int cteId) {
        int subGroups = repeat.getRepeatColumnRef().size() - 1;
        List<ColumnRefOperator> nullRefs = Lists.newArrayList(repeat.getRepeatColumnRef().get(subGroups));
        repeat.getRepeatColumnRef().stream().limit(subGroups).forEach(nullRefs::removeAll);

        // consume
        Map<ColumnRefOperator, ColumnRefOperator> cteColumnRefs = Maps.newHashMap();
        for (ColumnRefOperator input : aggregate.getAggregations().keySet()) {
            ColumnRefOperator cteOutput = factory.create(input, input.getType(), input.isNullable());
            cteColumnRefs.put(cteOutput, input);
            outputs.put(input, cteOutput);
        }
        for (ColumnRefOperator input : aggregate.getGroupingKeys()) {
            if (!repeat.getOutputGrouping().contains(input) && !nullRefs.contains(input)) {
                ColumnRefOperator cteOutput = factory.create(input, input.getType(), input.isNullable());
                cteColumnRefs.put(cteOutput, input);
                outputs.put(input, cteOutput);
            }
        }

        LogicalCTEConsumeOperator consume = new LogicalCTEConsumeOperator(cteId, cteColumnRefs);

        // repeat
        List<ColumnRefOperator> outputGrouping = Lists.newArrayList();
        repeat.getOutputGrouping().forEach(k -> {
            ColumnRefOperator x = factory.create(k, k.getType(), k.isNullable());
            outputs.put(k, x);
            outputGrouping.add(x);
        });

        List<List<ColumnRefOperator>> repeatRefs = repeat.getRepeatColumnRef().stream().limit(subGroups)
                .map(l -> l.stream().map(outputs::get).filter(Objects::nonNull).collect(Collectors.toList()))
                .collect(Collectors.toList());

        List<List<Long>> groupingIds = repeat.getGroupingIds().stream()
                .map(s -> s.subList(0, subGroups)).collect(Collectors.toList());

        ScalarOperator predicate = null;
        if (null != repeat.getPredicate()) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(outputs);
            predicate = rewriter.rewrite(repeat.getPredicate());
        }

        LogicalRepeatOperator newRepeat = LogicalRepeatOperator.builder()
                .setOutputGrouping(outputGrouping)
                .setRepeatColumnRefList(repeatRefs)
                .setGroupingIds(groupingIds)
                .setHasPushDown(true)
                .setPredicate(predicate)
                .build();

        // aggregate
        Map<ColumnRefOperator, CallOperator> aggregations = Maps.newHashMap();
        aggregate.getAggregations().forEach((k, v) -> {
            ColumnRefOperator x = factory.create(k, k.getType(), k.isNullable());
            Function aggFunc = Expr.getBuiltinFunction(v.getFnName(), new Type[] {k.getType()},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            Preconditions.checkState(aggFunc instanceof AggregateFunction);
            if (k.getType().isDecimalOfAnyVersion()) {
                aggFunc = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) aggFunc, k.getType(),
                        v.getType());
            }

            aggregations.put(x,
                    new CallOperator(v.getFnName(), k.getType(), Lists.newArrayList(outputs.get(k)), aggFunc));
            outputs.put(k, x);
        });

        List<ColumnRefOperator> groupings = aggregate.getGroupingKeys().stream()
                .filter(c -> !nullRefs.contains(c)).map(outputs::get).collect(Collectors.toList());
        LogicalAggregationOperator newAggregate = LogicalAggregationOperator.builder()
                .setAggregations(aggregations)
                .setGroupingKeys(groupings)
                .setType(AggType.GLOBAL)
                .setPartitionByColumns(groupings)
                .build();

        // project
        Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
        aggregations.keySet().forEach(k -> projection.put(k, k));
        groupings.forEach(k -> projection.put(k, k));

        for (ColumnRefOperator nullRef : nullRefs) {
            ColumnRefOperator m = factory.create(nullRef, nullRef.getType(), true);
            projection.put(m, ConstantOperator.createNull(nullRef.getType()));
            outputs.put(nullRef, m);
        }
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projection);

        return OptExpression.create(projectOperator,
                OptExpression.create(newAggregate, OptExpression.create(newRepeat, OptExpression.create(consume))));
    }
}
