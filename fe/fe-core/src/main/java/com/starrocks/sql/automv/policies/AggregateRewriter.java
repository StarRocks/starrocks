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

import com.google.api.client.util.Lists;
import com.google.api.client.util.Preconditions;
import com.starrocks.sql.automv.column.DerivedColumn;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpPlus;
import com.starrocks.sql.automv.pn.OpPlus2;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.StrictOp;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toMap;

public class AggregateRewriter {
    public static final AggregateRewriter AVG_ROLLUP_REWRITER;
    public static final AggregateRewriter HLL_ROLLUP_REWRITER;
    public static final AggregateRewriter BITMAP_ROLLUP_REWRITER;
    public static final AggregateRewriter PERCENTILE_ROLLUP_REWRITER;
    public static final AggregateRewriter ARRAY_AGG_BASED_DISTINCT_ROLLUP_REWRITER;
    public static final AggregateRewriter BITMAP_AGG_BASED_DISTINCT_ROLLUP_REWRITER;
    public static final AggregateRewriter HLL_AGG_BASED_DISTINCT_ROLLUP_REWRITER;

    public static final AggregateRewriter SUM_EXPR_ADD_CONSTANT_REWRITER;

    static {
        AVG_ROLLUP_REWRITER = of(OpUtil::isAvg, OpUtil::isSumOrCount, OpUtil::rewriteAvg);
        HLL_ROLLUP_REWRITER = of(OpUtil::isHllFinal, OpUtil::isHllMerge, OpUtil::rewriteHll);
        BITMAP_ROLLUP_REWRITER = of(OpUtil::isBitmapFinal, OpUtil::isBitmapMerge, OpUtil::rewriteBitmap);
        PERCENTILE_ROLLUP_REWRITER =
                of(OpUtil::isPercentileApprox, OpUtil::isPercentileUnion, OpUtil::rewritePercentile);
        ARRAY_AGG_BASED_DISTINCT_ROLLUP_REWRITER =
                of(OpUtil::isCountOrSumDistinct, OpUtil::isArrayAggDistinct, OpUtil::rewriteDistinctByArrayAggDistinct);
        BITMAP_AGG_BASED_DISTINCT_ROLLUP_REWRITER =
                of(OpUtil::isCountOrSumDistinct, OpUtil::isBitmapAgg, OpUtil::rewriteDistinctByBitmapAgg);
        HLL_AGG_BASED_DISTINCT_ROLLUP_REWRITER =
                of(OpUtil::isCountOrSumDistinct, OpUtil::isHllRawAgg, OpUtil::rewriteDistinctByHllAgg);
        SUM_EXPR_ADD_CONSTANT_REWRITER =
                of(OpUtil::isSumExprAddConstant, OpUtil::isSumOrCount, OpUtil::rewriteSumExprAddConstant);
    }

    private final Predicate<GenericColumn> isWantedAgg;
    private final Predicate<GenericColumn> isAggUsedByWantedAgg;
    private final AggRewriteFunction aggRewriter;

    private AggregateRewriter(
            Predicate<GenericColumn> isWantedAgg,
            Predicate<GenericColumn> isAggUsedByWantedAgg,
            AggRewriteFunction aggRewriter) {
        this.isWantedAgg = Objects.requireNonNull(isWantedAgg);
        this.isAggUsedByWantedAgg = Objects.requireNonNull(isAggUsedByWantedAgg);
        this.aggRewriter = Objects.requireNonNull(aggRewriter);
    }

    public static AggregateRewriter of(
            Predicate<GenericColumn> isWantedAgg,
            Predicate<GenericColumn> isAggUsedByWantedAgg,
            AggRewriteFunction aggWriter) {
        return new AggregateRewriter(isWantedAgg, isAggUsedByWantedAgg, aggWriter);
    }

    public static AggregateRewriter union(AggregateRewriter lhs, AggregateRewriter rhs) {
        Predicate<GenericColumn> isWantedAgg = lhs.isWantedAgg.or(rhs.isWantedAgg);
        Predicate<GenericColumn> isAggUsedByWantedAgg = lhs.isAggUsedByWantedAgg.or(rhs.isAggUsedByWantedAgg);
        AggRewriteFunction newAggWriter = (op, idGen, alreadyExists) -> {
            Optional<OpPlus2> result = lhs.aggRewriter.rewrite(op, idGen, alreadyExists);
            return result.isPresent() ? result : rhs.aggRewriter.rewrite(op, idGen, alreadyExists);
        };
        return of(isWantedAgg, isAggUsedByWantedAgg, newAggWriter);
    }

    public static AggregateRewriter union(List<AggregateRewriter> rewriters) {
        Preconditions.checkArgument(!rewriters.isEmpty());
        return rewriters.stream().reduce(AggregateRewriter::union).get();
    }

    Optional<AggregatePiece> rewrite(AggregatePiece aggPiece) {
        if (aggPiece.getMetrics().isEmpty()) {
            return Optional.empty();
        }

        Supplier<Integer> idGen = aggPiece.getCommonState().getIdConverter()::nextId;

        Map<Boolean, TieredMap<Integer, GenericColumn>> aggGroups = aggPiece.getMetrics().entrySet().stream()
                .collect(partitioningBy(e -> isWantedAgg.test(e.getValue()), TieredMap.toMap()));

        TieredMap<Integer, GenericColumn> wantedMetrics = aggGroups.get(true);
        TieredMap<Integer, GenericColumn> notWantedMetrics = aggGroups.get(false);

        if (wantedMetrics.isEmpty()) {
            return Optional.empty();
        }

        Map<StrictOp, Integer> opUsedByNewWantedAggToIds = notWantedMetrics.entrySet().stream()
                .filter(p -> isAggUsedByWantedAgg.test(p.getValue()))
                .collect(toMap(e -> OpUtil.mustGetExpr(e.getValue()), Map.Entry::getKey));

        // Aggregate functions that succeed in rewriting
        List<OpPlus2> rewritten = Lists.newArrayList();
        // Aggregate functions that fail to be rewritten.
        List<OpPlus> unRewritten = Lists.newArrayList();
        for (Map.Entry<Integer, GenericColumn> e : wantedMetrics.entrySet()) {
            OpPlus opPlus = OpPlus.of(OpUtil.mustGetOp(e.getValue()), e.getKey());
            Optional<OpPlus2> newOp = aggRewriter.rewrite(opPlus, idGen, opUsedByNewWantedAggToIds);
            if (newOp.isPresent()) {
                rewritten.add(newOp.get());
                // keep new-generated metrics to avoid generate duplicate metrics.
                newOp.get().getNewArgs().forEach(newArg ->
                        opUsedByNewWantedAggToIds.put(newArg.getOp().strict(), newArg.getId()));
            } else {
                unRewritten.add(opPlus);
            }
        }
        // none is rewritten, so return immediately.
        if (rewritten.isEmpty()) {
            return Optional.empty();
        }

        TieredMap<Integer, GenericColumn> metricsUsedByNewWantedAgg = rewritten.stream()
                .flatMap(OpPlus2::getNewArgs)
                .collect(TieredMap.toMap(OpPlus::getId, opp -> GenericColumn.derived(opp.getOp())));

        TieredMap<Integer, Op> newWantedAggOps = rewritten.stream()
                .map(OpPlus2::getOp)
                .collect(TieredMap.toMap(OpPlus::getId, OpPlus::getOp));

        TieredMap<Integer, GenericColumn> newWantedMetrics = OpUtil.columnize(newWantedAggOps);
        TieredMap<Integer, GenericColumn> unWrittenMetrics = unRewritten.stream()
                .collect(TieredMap.toMap(OpPlus::getId, opp -> GenericColumn.derived(opp.getOp())));

        TieredMap<Integer, GenericColumn> newMetrics =
                notWantedMetrics.merge(metricsUsedByNewWantedAgg).merge(unWrittenMetrics);

        TieredMap<Integer, GenericColumn> newColumns = aggPiece.getDimensions()
                .merge(aggPiece.getRollupDimensions())
                .merge(newMetrics)
                .merge(aggPiece.getDistinctMetrics())
                .merge(newWantedMetrics);

        ColumnRefSet columnIds = ColumnRefSet.createByIds(newColumns.keySet());
        Function<Op, Op> substitute = OpUtil.toSubstitute(newWantedAggOps);
        Function<GenericColumn, GenericColumn> columnSubst =
                column -> column.cast(DerivedColumn.class)
                        .map(c -> GenericColumn.derived(substitute.apply(c.getExpr().getOp()))).orElse(column);

        TieredMap<Integer, GenericColumn> otherColumns = aggPiece.getColumns().entrySet()
                .stream().filter(e -> !columnIds.contains(e.getKey()))
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> columnSubst.apply(e.getValue())));

        TieredList<Op> newConjuncts = aggPiece.getConjuncts().stream().map(substitute).collect(TieredList.toList());

        AggregatePiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(aggPiece.getFlatTable())
                .setDimensions(aggPiece.getDimensions())
                .setRollupDimensions(aggPiece.getRollupDimensions())
                .setMetrics(newMetrics)
                .setDistinctMetrics(aggPiece.getDistinctMetrics())
                .setColumns(otherColumns.merge(newColumns))
                .setConjuncts(newConjuncts)
                .build().cast();

        newAggPiece.setColumns(otherColumns.merge(newColumns));
        newAggPiece.setConjuncts(newConjuncts);
        return Optional.of(newAggPiece);
    }
}
