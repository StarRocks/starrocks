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

package com.starrocks.sql.automv.policies;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

public class AggregatePolicies {
    public static final ImmutableSet<String> ROLLUP_UNABLE_AGGREGATIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.CORR)
            .add(FunctionSet.COVAR_POP)
            .add(FunctionSet.COVAR_SAMP)
            .add(FunctionSet.DICT_MERGE)
            .add(FunctionSet.EXCHANGE_BYTES)
            .add(FunctionSet.EXCHANGE_SPEED)
            .add(FunctionSet.HISTOGRAM)
            .add(FunctionSet.INTERSECT_COUNT)
            .add(FunctionSet.MAX_BY)
            .add(FunctionSet.MIN_BY)
            .add(FunctionSet.PERCENTILE_CONT)
            .add(FunctionSet.PERCENTILE_DISC)
            .add(FunctionSet.RETENTION)
            .add(FunctionSet.STD)
            .add(FunctionSet.STDDEV)
            .add(FunctionSet.STDDEV_POP)
            .add(FunctionSet.STDDEV_SAMP)
            .add(FunctionSet.VARIANCE)
            .add(FunctionSet.VARIANCE_POP)
            .add(FunctionSet.VARIANCE_SAMP)
            .add(FunctionSet.VAR_POP)
            .add(FunctionSet.VAR_SAMP)
            .add(FunctionSet.MULTI_DISTINCT_COUNT)
            .add(FunctionSet.MULTI_DISTINCT_SUM)
            .add(FunctionSet.WINDOW_FUNNEL)
            .build();

    public static final ImmutableSet<String> ROLLUP_CONVERTIBLE_AGGREGATIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.APPROX_COUNT_DISTINCT)
            .add(FunctionSet.NDV)
            .add(FunctionSet.AVG)
            .add(FunctionSet.BITMAP_UNION_COUNT)
            .add(FunctionSet.BITMAP_UNION_INT)
            .add(FunctionSet.HLL_UNION_AGG)
            .add(FunctionSet.MULTI_DISTINCT_COUNT)
            .add(FunctionSet.MULTI_DISTINCT_SUM)
            .add(FunctionSet.PERCENTILE_APPROX)
            .add(FunctionSet.SUM)
            .add(FunctionSet.COUNT)
            .build();
    public static final ImmutableSet<String> ROLLUP_ABLE_AGGREGATIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.ARRAY_AGG)
            .add(FunctionSet.ARRAY_AGG_DISTINCT)
            .add(FunctionSet.BITMAP_AGG)
            .add(FunctionSet.BITMAP_INTERSECT)
            .add(FunctionSet.BITMAP_UNION)
            .add(FunctionSet.COUNT)
            .add(FunctionSet.COUNT_IF)
            .add(FunctionSet.HLL_RAW)
            .add(FunctionSet.HLL_RAW_AGG)
            .add(FunctionSet.HLL_UNION)
            .add(FunctionSet.MAX)
            .add(FunctionSet.MIN)
            .add(FunctionSet.PERCENTILE_UNION)
            .add(FunctionSet.SUM)
            .add(FunctionSet.ANY_VALUE)
            .add(FunctionSet.GROUP_CONCAT)
            .build();

    public static boolean isRollupAble(GenericColumn metric) {
        return ROLLUP_ABLE_AGGREGATIONS.contains(OpUtil.mustGetFnName(metric));
    }

    public static boolean isRollupConvertible(GenericColumn metric) {
        return ROLLUP_CONVERTIBLE_AGGREGATIONS.contains(OpUtil.mustGetFnName(metric));
    }

    public static boolean isRollupUnable(GenericColumn metric) {
        return ROLLUP_UNABLE_AGGREGATIONS.contains(OpUtil.mustGetFnName(metric)) ||
                (!isRollupAble(metric) && !isRollupConvertible(metric));
    }

    public static boolean hasRollupUnable(Collection<GenericColumn> metrics) {
        return metrics.stream().anyMatch(
                metric -> AggregatePolicies.isRollupUnable(metric) || metric.getOp().isDistinctAgg());
    }

    public static AggregatePolicy.AbstractAggregatePolicy distinctRollupPolicy(AutoMVOptions options) {
        return AggregatePolicy.and(
                ConditionalPolicy.EXISTS_DISTINCT_METRICS,
                AggregatePolicy.seq(
                        MergeDistinctMetricsIntoMetricsPolicy.INSTANCE,
                        AggregatePolicy.and(
                                ConditionalPolicy.EXISTS_DISTINCT_AVG_METRICS,
                                AvgPolicy.INSTANCE),
                        options.isUseBitmapCountDistinct() ? BitmapBasedCountDistinctPolicy.INSTANCE :
                                AggregatePolicy.IDENTITY_POLICY,
                        options.isUseArrayAggCountDistinct() ? ArrayAggBasedCountDistinctPolicy.INSTANCE :
                                AggregatePolicy.IDENTITY_POLICY,
                        options.isUseHllCountDistinct() ? HllBasedCountDistinctPolicy.INSTANCE :
                                AggregatePolicy.IDENTITY_POLICY),
                SplitDistinctMetricsFromMetricsPolicy.INSTANCE);
    }

    public static AggregatePiece applyRollupOrPerfectMatch(AutoMVOptions options, AggregatePiece aggPiece) {
        AggregatePolicy policy = AggregatePolicy.seq(
                distinctRollupPolicy(options),
                AggregatePolicy.or(
                        RollupAblePolicy.INSTANCE,
                        RollupUnablePolicy.INSTANCE));
        return policy.convert(aggPiece).orElse(aggPiece);
    }

    public static AggregatePolicy.AbstractAggregatePolicy partitionByPolicies(AutoMVOptions options) {
        return AggregatePolicy.and(
                ConditionalPolicy.EXISTS_ONLY_ROLLUP_ABLE_METRICS,
                AggregatePolicy.seq(
                        TimeGranuleExtractPolicy.INSTANCE,
                        TimeGranulePartitionPolicy.resolvePolicy(options.getPartitionExtractor(),
                                options.getDefaultPartitionByTimeGranule())
                )
        );
    }

    public static AggregatePolicy.AbstractAggregatePolicy defaultPolicies(AutoMVOptions options,
                                                                          PrettyPrinter traceLog) {

        AggregatePolicy.AbstractAggregatePolicy distinctPolicy =
                distinctRollupPolicy(options);

        AggregatePolicy.AbstractAggregatePolicy basicPolicies = AggregatePolicy.seq(
                EliminateDerivedVarPolicy.INSTANCE,
                AvgPolicy.INSTANCE,
                ExpandAggregateMetricsPolicy.INSTANCE,
                SumExprAddConstantPolicy.INSTANCE,
                distinctPolicy,
                BitmapPolicy.INSTANCE,
                HllPolicy.INSTANCE,
                PercentilePolicy.INSTANCE);

        AggregatePolicy.AbstractAggregatePolicy policy =
                AggregatePolicy.seq(
                        ReprogramAggregatePolicy.INSTANCE,
                        options.isPushDownAggBelowSemiAntiJoin() ? EliminateSemiAntiJoinPolicy.INSTANCE :
                                AggregatePolicy.IDENTITY_POLICY,
                        AggregatePolicy.and(
                                ConditionalPolicy.EXISTS_ROLLUP_REWRITABLE_BUT_ROLLUP_UNABLE_METRICS,
                                basicPolicies
                        ),
                        partitionByPolicies(options)
                );

        return Optional.ofNullable(traceLog).map(log -> AggregatePolicy.trace(policy, traceLog, 1)).orElse(policy);
    }

    public static AggregatePolicy defaultPolicies(AutoMVOptions options) {
        return defaultPolicies(options, null);
    }

    public static PlanPiece perfectMatch(PlanPiece piece) {
        Optional<AggregatePiece> optAggPiece = piece.cast(AggregatePiece.class).
                flatMap(RollupUnablePolicy.INSTANCE::convert);
        if (optAggPiece.isPresent()) {
            return optAggPiece.get();
        } else {
            return piece;
        }
    }

    public static final class RollupUnablePolicy extends AggregatePolicy.SimplePolicy {

        public static final RollupUnablePolicy INSTANCE = new RollupUnablePolicy();

        private RollupUnablePolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece.toPerfect());
        }
    }

    public static final class RollupAblePolicy extends AggregatePolicy.SimplePolicy {
        public static final RollupAblePolicy INSTANCE = new RollupAblePolicy();

        private RollupAblePolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            boolean allRollupAble = aggPiece.getMetrics()
                    .values().stream()
                    .allMatch(AggregatePolicies::isRollupAble);
            if (allRollupAble && aggPiece.getDistinctMetrics().isEmpty()) {
                return Optional.of(aggPiece.toRollup());
            } else {
                return Optional.empty();
            }
        }
    }

    public static final class BitmapPolicy extends AggregatePolicy.SimplePolicy {

        public static final BitmapPolicy INSTANCE = new BitmapPolicy();

        private BitmapPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.BITMAP_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class PercentilePolicy extends AggregatePolicy.SimplePolicy {
        public static final PercentilePolicy INSTANCE = new PercentilePolicy();

        private PercentilePolicy() {

        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.PERCENTILE_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class AvgPolicy extends AggregatePolicy.SimplePolicy {

        public static final AvgPolicy INSTANCE = new AvgPolicy();

        private AvgPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.AVG_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class HllPolicy extends AggregatePolicy.SimplePolicy {
        public static final HllPolicy INSTANCE = new HllPolicy();

        private HllPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.HLL_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class ArrayAggBasedCountDistinctPolicy extends AggregatePolicy.SimplePolicy {

        public static final ArrayAggBasedCountDistinctPolicy INSTANCE = new ArrayAggBasedCountDistinctPolicy();

        private ArrayAggBasedCountDistinctPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.ARRAY_AGG_BASED_DISTINCT_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class HllBasedCountDistinctPolicy extends AggregatePolicy.SimplePolicy {
        public static final HllBasedCountDistinctPolicy INSTANCE = new HllBasedCountDistinctPolicy();

        private HllBasedCountDistinctPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.HLL_AGG_BASED_DISTINCT_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class BitmapBasedCountDistinctPolicy extends AggregatePolicy.SimplePolicy {

        public static final BitmapBasedCountDistinctPolicy INSTANCE = new BitmapBasedCountDistinctPolicy();

        private BitmapBasedCountDistinctPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.BITMAP_AGG_BASED_DISTINCT_ROLLUP_REWRITER.rewrite(aggPiece);
        }
    }

    public static final class SumExprAddConstantPolicy extends AggregatePolicy.SimplePolicy {

        public static final SumExprAddConstantPolicy INSTANCE = new SumExprAddConstantPolicy();

        private SumExprAddConstantPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return AggregateRewriter.SUM_EXPR_ADD_CONSTANT_REWRITER.rewrite(aggPiece);
        }
    }

    private static final class MergeDistinctMetricsIntoMetricsPolicy extends AggregatePolicy.SimplePolicy {
        public static final MergeDistinctMetricsIntoMetricsPolicy INSTANCE =
                new MergeDistinctMetricsIntoMetricsPolicy();

        private MergeDistinctMetricsIntoMetricsPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece.mergeDistinctMetricsIntoMetrics());
        }
    }

    private static final class SplitDistinctMetricsFromMetricsPolicy extends AggregatePolicy.SimplePolicy {
        public static final SplitDistinctMetricsFromMetricsPolicy INSTANCE =
                new SplitDistinctMetricsFromMetricsPolicy();

        private SplitDistinctMetricsFromMetricsPolicy() {
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            return Optional.of(aggPiece.splitDistinctMetricsFromMetrics());
        }
    }

    public static final class ConditionalPolicy extends AggregatePolicy.SimplePolicy {
        public static final ConditionalPolicy EXISTS_DISTINCT_METRICS =
                ConditionalPolicy.of("Exist DistinctMetrics",
                        aggPiece -> !aggPiece.getDistinctMetrics().isEmpty());
        public static final ConditionalPolicy EXISTS_DISTINCT_AVG_METRICS =
                ConditionalPolicy.of("Exist DistinctMetrics",
                        aggPiece -> aggPiece.getMetrics().values().stream().noneMatch(OpUtil::isAvgDistinct));
        public static final ConditionalPolicy EXISTS_ROLLUP_REWRITABLE_BUT_ROLLUP_UNABLE_METRICS =
                ConditionalPolicy.of("Exist rollup-rewritable but rollup-unable metrics",
                        aggPiece -> {
                            Collection<GenericColumn> columns =
                                    aggPiece.getMetrics().merge(aggPiece.getDistinctMetrics()).values();
                            boolean existsRollupRewritable = columns.stream()
                                    .anyMatch(AggregatePolicies::isRollupConvertible);
                            boolean existsNoRollupUnable = columns.stream()
                                    .noneMatch(AggregatePolicies::isRollupUnable);
                            return existsRollupRewritable && existsNoRollupUnable;
                        });

        public static final ConditionalPolicy EXISTS_ONLY_ROLLUP_ABLE_METRICS =
                ConditionalPolicy.of("Exist only rollup-able metrics", aggPiece -> {
                    boolean hasNoDistinctMetrics = aggPiece.getDistinctMetrics().isEmpty();
                    boolean hasOnlyRollupAbleMetrics = aggPiece.getMetrics().values()
                            .stream()
                            .allMatch(AggregatePolicies::isRollupAble);
                    return hasNoDistinctMetrics && hasOnlyRollupAbleMetrics;
                });
        private final String description;
        private final Predicate<AggregatePiece> predicate;

        private ConditionalPolicy(String description, Predicate<AggregatePiece> predicate) {
            this.description = Objects.requireNonNull(description);
            this.predicate = Objects.requireNonNull(predicate);
        }

        public static ConditionalPolicy of(String description, Predicate<AggregatePiece> predicate) {
            return new ConditionalPolicy(description, predicate);
        }

        @Override
        public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
            if (predicate.test(aggPiece)) {
                return Optional.of(aggPiece);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public PrettyPrinter toPrettyString() {
            PrettyPrinter printer = new PrettyPrinter();
            printer.add(this.getClass().getSimpleName()).add('[').add(description).add(']');
            return printer;
        }
    }
}