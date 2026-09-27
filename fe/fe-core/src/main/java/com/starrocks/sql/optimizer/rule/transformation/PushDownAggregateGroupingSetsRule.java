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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.ast.expression.ExprUtils;
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
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.FloatType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

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
            FunctionSet.MIN, FunctionSet.SUM, FunctionSet.AVG, FunctionSet.COUNT);

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

        if (aggregate.getPredicate() != null) {
            // AVG is decomposed into sum/count and only recombined into a final avg value by a Project
            // above the re-aggregation (see buildSubRepeatConsume); a HAVING predicate directly on the
            // aggregation output can't be safely re-evaluated against the pre-divide sum/count columns,
            // so skip push down whenever the predicate touches an AVG output column.
            List<ColumnRefOperator> avgOutputRefs = aggregate.getAggregations().entrySet().stream()
                    .filter(e -> FunctionSet.AVG.equals(e.getValue().getFnName()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            if (aggregate.getPredicate().getUsedColumns().containsAny(avgOutputRefs)) {
                return false;
            }
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

        // AVG can't be re-invoked on an already-aggregated value (unlike sum/min/max), so decompose
        // each avg(x) into sum(x)/count(x) once up-front; every builder below consults this map to
        // know which aggregations need the sum+count treatment instead of a plain pass-through/re-agg.
        Map<ColumnRefOperator, AvgDecomposition> avgDecompositions = buildAvgDecompositions(factory, aggregate);

        // cte produce and push down aggregate
        context.getCteContext().addForceCTE(cteId);
        OptExpression cteProduce = buildCTEProduce(context, input, cteId, avgDecompositions);

        // new grouping sets consume
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputs1 = Maps.newHashMap();
        OptExpression subRepeatConsume =
                buildSubRepeatConsume(factory, consumeOutputs1, aggregate, repeat, cteId, avgDecompositions);

        // select consume
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputs2 = Maps.newHashMap();
        OptExpression selectConsume =
                buildSelectConsume(factory, consumeOutputs2, aggregate, repeat, cteId, avgDecompositions);

        // union all
        OptExpression union =
                buildUnionAll(aggregate, consumeOutputs1, subRepeatConsume, consumeOutputs2, selectConsume);

        return Lists.newArrayList(OptExpression.create(new LogicalCTEAnchorOperator(cteId), cteProduce, union));
    }

    /**
     * avg(x) can't be recombined across rollup levels by re-invoking avg() on already-averaged values
     * (unlike sum/min/max, which are self-recombining) - weighting would be wrong whenever finer groups
     * have different sizes. So every avg(x) is computed as sum(x)/count(x) instead: the finest-grain CTE
     * produces sum(x) and count(x), coarser rollup levels re-aggregate with sum(sum(x))/sum(count(x)),
     * and the final avg value is recovered via a division Project wherever it's consumed.
     */
    public static final class AvgDecomposition {
        final ColumnRefOperator sumRef;
        final CallOperator sumCall;
        final ColumnRefOperator countRef;
        final CallOperator countCall;

        private AvgDecomposition(ColumnRefOperator sumRef, CallOperator sumCall,
                                 ColumnRefOperator countRef, CallOperator countCall) {
            this.sumRef = sumRef;
            this.sumCall = sumCall;
            this.countRef = countRef;
            this.countCall = countCall;
        }
    }

    private Map<ColumnRefOperator, AvgDecomposition> buildAvgDecompositions(ColumnRefFactory factory,
                                                                            LogicalAggregationOperator aggregate) {
        Map<ColumnRefOperator, AvgDecomposition> result = Maps.newHashMap();

        // An avg(x) decomposition frequently duplicates an aggregation the query already asks for:
        // `avg(x), count(x)` needs a single count(x), and two avg() over the same argument need a single
        // sum/count pair. Index what is already available by call, so a synthesized part can bind to an
        // existing output column instead of adding a redundant one to the CTE. Reuse is driven purely by
        // CallOperator equality, so it never fires across genuinely different expressions - notably when
        // the analyzer has wrapped avg's argument in a scale-capping CAST, avg's arg is not equal to the
        // bare column a sibling count(x) reads, and the two correctly stay separate.
        Map<CallOperator, ColumnRefOperator> reusable = Maps.newHashMap();
        aggregate.getAggregations().forEach((colRef, call) -> {
            if (!FunctionSet.AVG.equals(call.getFnName())) {
                reusable.putIfAbsent(call, colRef);
            }
        });

        aggregate.getAggregations().forEach((colRef, call) -> {
            if (!FunctionSet.AVG.equals(call.getFnName())) {
                return;
            }
            ScalarOperator arg = call.getChild(0);
            Type argType = arg.getType();

            // AVG accumulates integer inputs as a double internally (see AvgDoubleLTGuard in the BE), not
            // as the argument's native type; decomposing via the native-typed SUM (e.g. plain BIGINT sum)
            // would overflow in cases the original avg wouldn't, so sum over a double-cast argument
            // instead to keep the same accumulator width. Float/decimal args already have a SUM overload
            // with matching (or wider, for decimal) accumulation, so they're summed as-is.
            //
            // Note isFixedPointType() is TINYINT..LARGEINT and excludes BOOLEAN, so avg(bool) is summed
            // as-is through sum(BOOLEAN) -> BIGINT. That is deliberate: 0/1 inputs cannot overflow a
            // BIGINT accumulator, so the cast would only add plan noise.
            ScalarOperator sumArg = argType.isFixedPointType() ? new CastOperator(FloatType.DOUBLE, arg, true) : arg;

            Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM, new Type[] {sumArg.getType()},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Function countFn = ExprUtils.getBuiltinFunction(FunctionSet.COUNT, new Type[] {argType},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Preconditions.checkState(sumFn instanceof AggregateFunction);
            Preconditions.checkState(countFn instanceof AggregateFunction);

            if (sumArg.getType().isDecimalV3()) {
                // DecimalV3 SUM always widens its accumulator - to DECIMAL128(38, scale) for
                // decimal32/64/128 inputs, or DECIMAL256(76, scale) for decimal256 inputs, since the BE
                // registers a dedicated decimal256->decimal256 decimal_sum mapping (see
                // aggregate_resolver_sumcount.cpp's SumDispatcher). Narrowing to the argument's own
                // precision - or always widening to DECIMAL128 regardless of input width, as
                // ScalarOperatorUtil.findSumFn does - would both mismatch the BE's registered accumulator.
                //
                // This must test isDecimalV3(), NOT isDecimalOfAnyVersion(): legacy DECIMALV2 is a
                // separate BE implementation registered as sum(DECIMALV2) -> DECIMALV2, so handing it a
                // DecimalV3 return type makes the BE reject the plan outright with
                // "Invalid agg function plan: sum with (arg type DECIMALV2, ..., result type DECIMAL128)".
                // DECIMALV2 keeps the natively resolved SUM signature instead.
                ScalarType decimalArgType = (ScalarType) sumArg.getType();
                int widePrecision = decimalArgType.isDecimal256() ? 76 : 38;
                AggregateFunction decimalSumFn = (AggregateFunction) sumFn.copy();
                decimalSumFn.setArgsType(new Type[] {decimalArgType});
                decimalSumFn.setRetType(TypeFactory.createDecimalV3NarrowestType(widePrecision,
                        decimalArgType.getScalarScale()));
                sumFn = decimalSumFn;
            }

            CallOperator sumCall = new CallOperator(FunctionSet.SUM, sumFn.getReturnType(),
                    Lists.newArrayList(sumArg), sumFn);
            CallOperator countCall = new CallOperator(FunctionSet.COUNT, countFn.getReturnType(),
                    Lists.newArrayList(arg), countFn);
            ColumnRefOperator sumRef =
                    reusable.computeIfAbsent(sumCall, c -> factory.create(c, c.getType(), c.isNullable()));
            ColumnRefOperator countRef =
                    reusable.computeIfAbsent(countCall, c -> factory.create(c, c.getType(), c.isNullable()));
            result.put(colRef, new AvgDecomposition(sumRef, sumCall, countRef, countCall));
        });
        return result;
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
                .build();
        return OptExpression.create(union, repeatConsume, selectConsume);
    }

    private OptExpression buildCTEProduce(OptimizerContext context, OptExpression input, int cteId,
                                          Map<ColumnRefOperator, AvgDecomposition> avgDecompositions) {
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

        // replace output columns; avg(x) is computed as sum(x)/count(x) here so the finest grain still
        // produces a correct avg (nothing rewritten yet), but the underlying sum/count are what coarser
        // rollup levels actually need to re-aggregate correctly (see buildSubRepeatConsume).
        Map<ColumnRefOperator, CallOperator> cteAggregations = Maps.newHashMap();
        aggregate.getAggregations().forEach((colRef, call) -> {
            AvgDecomposition decomposition = avgDecompositions.get(colRef);
            if (decomposition != null) {
                cteAggregations.put(decomposition.sumRef, decomposition.sumCall);
                cteAggregations.put(decomposition.countRef, decomposition.countCall);
            } else {
                cteAggregations.put(colRef, call);
            }
        });

        LogicalAggregationOperator.Builder builder = LogicalAggregationOperator.builder();
        builder.setType(AggType.GLOBAL)
                .setGroupingKeys(allGroupByRefs)
                .setAggregations(cteAggregations)
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
    /**
     * Create (or reuse) the consume-side column that reads one CTE output column.
     *
     * A single CTE output can now be wanted by more than one aggregation - an avg(x) decomposition and a
     * sibling count(x) bind to the same column once buildAvgDecompositions dedupes them. Reading it twice
     * would still be correct but would add a redundant consume column, so requests are cached by CTE
     * output ref. The cache is what makes this order-independent: aggregations are iterated from a
     * HashMap, so either the avg branch or the plain branch may run first, and both must end up with the
     * same consume ref rather than whichever one happened to be created first.
     */
    private ColumnRefOperator consumeOf(ColumnRefFactory factory,
                                        Map<ColumnRefOperator, ColumnRefOperator> cteColumnRefs,
                                        Map<ColumnRefOperator, ColumnRefOperator> consumed,
                                        ColumnRefOperator cteOutput) {
        return consumed.computeIfAbsent(cteOutput, output -> {
            ColumnRefOperator consume = factory.create(output, output.getType(), output.isNullable());
            cteColumnRefs.put(consume, output);
            return consume;
        });
    }

    private OptExpression buildSelectConsume(ColumnRefFactory factory,
                                             Map<ColumnRefOperator, ColumnRefOperator> outputs,
                                             LogicalAggregationOperator aggregate, LogicalRepeatOperator repeat,
                                             int cteId, Map<ColumnRefOperator, AvgDecomposition> avgDecompositions) {

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        // consume
        Map<ColumnRefOperator, ColumnRefOperator> cteColumnRefs = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> consumed = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregate.getAggregations().entrySet()) {
            ColumnRefOperator input = entry.getKey();
            AvgDecomposition decomposition = avgDecompositions.get(input);
            if (decomposition != null) {
                // finest grain already has a correct avg (sum/count are exact, no rollup happened yet),
                // so just recompute avg = sum/count from the CTE's sum/count columns.
                ColumnRefOperator sumConsume = consumeOf(factory, cteColumnRefs, consumed, decomposition.sumRef);
                ColumnRefOperator countConsume = consumeOf(factory, cteColumnRefs, consumed, decomposition.countRef);

                ColumnRefOperator avgOutput = factory.create(input, input.getType(), input.isNullable());
                outputs.put(input, avgOutput);
                projectMap.put(avgOutput,
                        AggregatePushDownUtils.createAvgBySumCount(entry.getValue(), sumConsume, countConsume));
            } else {
                ColumnRefOperator cteOutput = consumeOf(factory, cteColumnRefs, consumed, input);
                outputs.put(input, cteOutput);
                projectMap.put(cteOutput, cteOutput);
            }
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

        ScalarOperator predicate = Utils.compoundAnd(repeat.getPredicate(), aggregate.getPredicate());
        if (null != predicate) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(outputs);
            predicate = rewriter.rewrite(predicate);
            return OptExpression.create(new LogicalFilterOperator(predicate), result);
        }
        return result;
    }

    /*
     * select a, b, c, d, null, sum(x) x from t group by rollup(a, b, c, d)
     */
    public OptExpression buildSubRepeatConsume(ColumnRefFactory factory,
                                                Map<ColumnRefOperator, ColumnRefOperator> outputs,
                                                LogicalAggregationOperator aggregate, LogicalRepeatOperator repeat,
                                                int cteId, Map<ColumnRefOperator, AvgDecomposition> avgDecompositions) {
        int subGroups = repeat.getRepeatColumnRef().size() - 1;
        List<ColumnRefOperator> nullRefs = Lists.newArrayList(repeat.getRepeatColumnRef().get(subGroups));
        repeat.getRepeatColumnRef().stream().limit(subGroups).forEach(nullRefs::removeAll);

        // consume; for avg(x), consume its sum(x)/count(x) columns instead of the (non-existent as a
        // CTE output) avg column itself - tracked in avgSumConsume/avgCountConsume, kept out of `outputs`
        // since `outputs` must end up holding the final recombined avg value, not an intermediate one.
        Map<ColumnRefOperator, ColumnRefOperator> avgSumConsume = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> avgCountConsume = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> cteColumnRefs = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> consumed = Maps.newHashMap();
        for (ColumnRefOperator input : aggregate.getAggregations().keySet()) {
            AvgDecomposition decomposition = avgDecompositions.get(input);
            if (decomposition != null) {
                avgSumConsume.put(input, consumeOf(factory, cteColumnRefs, consumed, decomposition.sumRef));
                avgCountConsume.put(input, consumeOf(factory, cteColumnRefs, consumed, decomposition.countRef));
            } else {
                outputs.put(input, consumeOf(factory, cteColumnRefs, consumed, input));
            }
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

        // NOTE: keep the repeat predicate and the aggregate (HAVING) predicate in separate variables.
        // Sharing one variable makes the repeat predicate leak onto the aggregate whenever the aggregate
        // has no HAVING of its own; that happens to be idempotent today only because the leaked predicate
        // references grouping columns whose values the re-aggregation does not change.
        ScalarOperator repeatPredicate = null;
        if (null != repeat.getPredicate()) {
            Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap(outputs);
            nullRefs.forEach(c -> replaceMap.put(c, ConstantOperator.createNull(c.getType())));

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMap);
            repeatPredicate = rewriter.rewrite(repeat.getPredicate());

            ScalarOperatorRewriter r = new ScalarOperatorRewriter();
            repeatPredicate =
                    r.rewrite(repeatPredicate, List.of(new FoldConstantsRule(), new SimplifiedPredicateRule()));
        }

        LogicalRepeatOperator newRepeat = LogicalRepeatOperator.builder()
                .setOutputGrouping(outputGrouping)
                .setRepeatColumnRefList(repeatRefs)
                .setGroupingIds(groupingIds)
                .setHasPushDown(true)
                .setPredicate(repeatPredicate)
                .build();

        // aggregate; avg(x)'s sum/count must be re-summed (never re-averaged) to stay correct across
        // rollup levels of unequal weight, so both are tracked here and only recombined into avg by the
        // final Project below.
        Map<ColumnRefOperator, CallOperator> aggregations = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> avgSumRolledUp = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> avgCountRolledUp = Maps.newHashMap();
        // Consume columns shared by several aggregations (see consumeOf) must be re-aggregated once, not
        // once per consumer. Keyed by consume ref, and safe to key on that alone: sharing only happens
        // when the underlying CTE column is the very same call, and every rollup over such a column is a
        // SUM either way - avg's partials are summed, and count rolls up to sum too. Like consumeOf this
        // has to be a cache rather than an ordering assumption, since the two branches run in HashMap
        // order.
        Map<ColumnRefOperator, ColumnRefOperator> rolledUp = Maps.newHashMap();
        aggregate.getAggregations().forEach((k, v) -> {
            AvgDecomposition decomposition = avgDecompositions.get(k);
            if (decomposition != null) {
                ColumnRefOperator sumConsumeRef = avgSumConsume.get(k);
                ColumnRefOperator countConsumeRef = avgCountConsume.get(k);

                ColumnRefOperator rolledUpSum = rolledUp.computeIfAbsent(sumConsumeRef, ref -> {
                    Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                            new Type[] {ref.getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    Preconditions.checkState(sumFn instanceof AggregateFunction);
                    if (ref.getType().isDecimalOfAnyVersion()) {
                        sumFn = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) sumFn,
                                ref.getType(), ref.getType());
                    }
                    ColumnRefOperator out = factory.create(ref, ref.getType(), ref.isNullable());
                    aggregations.put(out, new CallOperator(FunctionSet.SUM, ref.getType(),
                            Lists.newArrayList(ref), sumFn));
                    return out;
                });

                ColumnRefOperator rolledUpCount = rolledUp.computeIfAbsent(countConsumeRef, ref -> {
                    Function countFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                            new Type[] {ref.getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    Preconditions.checkState(countFn instanceof AggregateFunction);
                    ColumnRefOperator out = factory.create(ref, ref.getType(), ref.isNullable());
                    aggregations.put(out, new CallOperator(FunctionSet.SUM, ref.getType(),
                            Lists.newArrayList(ref), countFn));
                    return out;
                });

                avgSumRolledUp.put(k, rolledUpSum);
                avgCountRolledUp.put(k, rolledUpCount);
            } else {
                // sum/min/max are self-recombining (same function name); count is not - the coarser
                // level must re-SUM the finer levels' partial counts, not re-COUNT them (which would
                // count the number of partial-count rows instead of summing their values). Reuse the
                // same rollup-function mapping already used by the MV-rewrite path and by pdagg.
                //
                // That mapping lives in UNSAFE_REWRITE_ROLLUP_FUNCTION_MAP, whose contract is
                // `count(col) = coalesce(sum(col), 0)` - the MV path needs the coalesce because it may
                // roll up over an empty match. Here the coalesce is provably unnecessary: the value
                // being re-summed is a count produced by the CTE, so it is never NULL, and an
                // aggregation group always holds at least one row - therefore this sum() can never
                // return NULL. Do not drop that reasoning when touching this branch.
                String rollupFnName = AggregateFunctionRollupUtils.getRollupFunctionName(v, false);
                String fnName = rollupFnName != null ? rollupFnName : v.getFnName();

                ColumnRefOperator x = rolledUp.computeIfAbsent(outputs.get(k), ref -> {
                    ColumnRefOperator out = factory.create(k, k.getType(), k.isNullable());
                    Function aggFunc = ExprUtils.getBuiltinFunction(fnName, new Type[] {k.getType()},
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

                    Preconditions.checkState(aggFunc instanceof AggregateFunction);
                    if (k.getType().isDecimalOfAnyVersion()) {
                        aggFunc = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) aggFunc,
                                k.getType(), v.getType());
                    }

                    // The rolled-up call keeps the original aggregation's output type, which only holds
                    // while every rollup mapping is type-preserving over its own output (count->sum is:
                    // sum(BIGINT) is BIGINT). Other entries in the mapping are not - bitmap_agg->
                    // bitmap_union and array_agg_distinct->array_unique_agg both change the type - so
                    // fail loudly here rather than silently emitting a CallOperator whose declared type
                    // contradicts its function.
                    Preconditions.checkState(aggFunc.getReturnType().matchesType(k.getType()),
                            "rollup function %s returns %s but the aggregation output column is %s",
                            fnName, aggFunc.getReturnType(), k.getType());

                    aggregations.put(out, new CallOperator(fnName, k.getType(), Lists.newArrayList(ref), aggFunc));
                    return out;
                });
                outputs.put(k, x);
            }
        });

        List<ColumnRefOperator> groupings = aggregate.getGroupingKeys().stream()
                .filter(c -> !nullRefs.contains(c)).map(outputs::get).collect(Collectors.toList());

        ScalarOperator havingPredicate = null;
        if (null != aggregate.getPredicate()) {
            Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap(outputs);
            nullRefs.forEach(c -> replaceMap.put(c, ConstantOperator.createNull(c.getType())));
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMap);
            havingPredicate = rewriter.rewrite(aggregate.getPredicate());
        }
        LogicalAggregationOperator newAggregate = LogicalAggregationOperator.builder()
                .setAggregations(aggregations)
                .setGroupingKeys(groupings)
                .setType(AggType.GLOBAL)
                .setPredicate(havingPredicate)
                .setPartitionByColumns(groupings)
                .build();

        // project
        Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
        aggregations.keySet().forEach(k -> projection.put(k, k));
        groupings.forEach(k -> projection.put(k, k));

        avgSumRolledUp.forEach((origAvgRef, rolledUpSum) -> {
            ColumnRefOperator rolledUpCount = avgCountRolledUp.get(origAvgRef);
            CallOperator origAvgCall = aggregate.getAggregations().get(origAvgRef);
            ColumnRefOperator avgOutput = factory.create(origAvgRef, origAvgRef.getType(), origAvgRef.isNullable());
            projection.put(avgOutput,
                    AggregatePushDownUtils.createAvgBySumCount(origAvgCall, rolledUpSum, rolledUpCount));
            outputs.put(origAvgRef, avgOutput);
        });

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