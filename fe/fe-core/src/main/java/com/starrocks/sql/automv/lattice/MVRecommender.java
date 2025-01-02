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

package com.starrocks.sql.automv.lattice;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.generator.AggregateMVGenerator;
import com.starrocks.sql.automv.generator.MVGenerateContext;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.generator.QueryGenerateResult;
import com.starrocks.sql.automv.lifecycle.MVRecommendationSelectOptions;
import com.starrocks.sql.automv.lifecycle.MVRecommendationSelector;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.pieces.StarJoinPiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.Var;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.PartitionPlus;
import com.starrocks.sql.automv.util.Box;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MVRecommender {
    private final ConnectContext ctx;
    private final AutoMVOptions options;

    public MVRecommender(ConnectContext ctx, AutoMVOptions options) {
        this.ctx = Objects.requireNonNull(ctx);
        this.options = Objects.requireNonNull(options);
    }

    private static boolean hasComplexDerivedMetrics(AggregatePiece aggPiece) {
        return aggPiece.getMetrics().merge(aggPiece.getDistinctMetrics()).values()
                .stream()
                .anyMatch(op -> OpUtil.hasComplexOp(op.getNorm()));
    }

    private static boolean hasComplexDerivedDimensions(AggregatePiece aggPiece) {
        return aggPiece.getDimensions().merge(aggPiece.getRollupDimensions()).values()
                .stream()
                .anyMatch(op -> OpUtil.hasComplexOp(op.getNorm()));
    }

    private static boolean hasJoinOfTypes(PlanPiece rootPiece, Predicate<JoinOperator> joinTypePredicate) {
        return rootPiece.stream()
                .filter(PlanPiece::isStarJoin)
                .flatMap(piece -> piece.mustCast(StarJoinPiece.class).getCorners().stream())
                .map(StarJoinPiece.StarCorner::getJoinType)
                .anyMatch(joinTypePredicate);
    }

    private static boolean hasSemiAntiJoin(PlanPiece piece) {
        return hasJoinOfTypes(piece, JoinOperator::isSemiAntiJoin);
    }

    private static boolean hasCrossJoin(PlanPiece piece) {
        return hasJoinOfTypes(piece, JoinOperator::isCrossJoin);
    }

    public List<MVRecommendation> recommend(List<PlanPiece> planPieces, int startIdx, int endIdx) {

        planPieces.forEach(PlanPiece::assignPieceIds);
        List<PlanPiece> normalizedPieces = planPieces.stream()
                .map(PlanPieceNormalizer::normalize)
                .collect(Collectors.toList());

        normalizedPieces = normalizedPieces
                .stream()
                .filter(piece -> options.isEnableComplexDerivedDimensions() ||
                        !hasComplexDerivedDimensions(piece.mustCast(AggregatePiece.class)))
                .filter(piece -> options.isEnableComplexDerivedMetrics() ||
                        !hasComplexDerivedMetrics(piece.mustCast(AggregatePiece.class)))
                .filter(piece -> options.isEnableSemiAntiJoin() ||
                        !hasSemiAntiJoin(piece))
                .collect(Collectors.toList());

        Collection<List<PlanPiece>> pieceGroups = normalizedPieces.stream()
                .collect(Collectors.groupingBy(PlanPiece::getFlatTableNormHash))
                .values();

        List<List<PlanPiece>> newPieceGroups = pieceGroups.stream()
                .flatMap(pieces -> splitConjuncts(pieces, options.getPartialRollupMinAggPieces()).stream())
                .filter(pieces -> !pieces.isEmpty())
                .map(LatticeNode::mergeDerivedColumnsOfFlatTables)
                .collect(Collectors.toList());
        if (options.isUseCardinalityEstimation()) {
            return recommendUsingCardinalityEstimation(newPieceGroups, endIdx);
        } else {
            return recommendSimply(newPieceGroups, endIdx);
        }
    }

    private Optional<List<PlanPiece>> toPartialRollup(
            List<Pair<AggregatePiece, Integer>> identicalColumnGroup) {

        int n = identicalColumnGroup.size();
        List<TieredList<Op>> hoistingConjunctsList = Lists.newArrayListWithCapacity(n);
        List<TieredList<Op>> reservedConjunctsList = Lists.newArrayListWithCapacity(n);
        List<Var> varList = Lists.newArrayListWithCapacity(n);
        for (Pair<AggregatePiece, Integer> pieceAndColumn : identicalColumnGroup) {
            AggregatePiece aggPiece = pieceAndColumn.first;
            ColumnRefSet columnId = new ColumnRefSet(pieceAndColumn.second);

            Map<Boolean, TieredList<Op>> conjunctsGroup = aggPiece.getFlatTable().getFlexibleConjuncts()
                    .stream()
                    .collect(Collectors.partitioningBy(op -> op.getIds().equals(columnId), TieredList.<Op>toList()));

            hoistingConjunctsList.add(conjunctsGroup.get(false));
            TieredList<Op> reservedConjuncts = conjunctsGroup.get(true);
            reservedConjunctsList.add(reservedConjuncts);
            Preconditions.checkArgument(!reservedConjuncts.isEmpty());
            Var var = reservedConjuncts.get(0).collect(op -> op.getClass().equals(Var.class)).get(0).cast();
            varList.add(var);
        }
        Optional<List<TieredList<Op>>> optRangeConjunctsList = OpUtil.getRangeConjuncts(varList, reservedConjunctsList);
        if (optRangeConjunctsList.isPresent()) {
            List<TieredList<Op>> rangeConjunctsList = optRangeConjunctsList.get();
            List<PlanPiece> aggPieces = IntStream.range(0, n).mapToObj(i -> {
                AggregatePiece aggPiece = identicalColumnGroup.get(i).first;
                TieredList<Op> hoistingConjuncts = hoistingConjunctsList.get(i);
                TieredList<Op> rangeConjuncts = rangeConjunctsList.get(i);
                return aggPiece.toPartialRollup(hoistingConjuncts, rangeConjuncts);
            }).collect(Collectors.toList());
            return Optional.of(aggPieces);
        } else {
            return Optional.empty();
        }
    }

    private List<List<PlanPiece>> splitConjuncts(List<PlanPiece> pieces, int minPartialRollup) {
        return splitConjunctsOfRollupAbleAggPieces(pieces, minPartialRollup)
                .orElseGet(() -> Collections.singletonList(pieces.stream()
                        .map(piece -> piece.mustCast(AggregatePiece.class))
                        .map(piece -> AggregatePolicies.applyRollupOrPerfectMatch(options, piece))
                        .collect(Collectors.toList())));
    }

    private Optional<List<List<PlanPiece>>> splitConjunctsOfRollupAbleAggPieces(List<PlanPiece> pieces,
                                                                                int minPartialRollup) {
        String normHash = pieces.get(0).getFlatTableNormHash();
        Preconditions.checkArgument(pieces.stream().map(PlanPiece::getFlatTableNormHash).allMatch(normHash::equals));

        if (pieces.size() < minPartialRollup) {
            return Optional.empty();
        }

        // only agg pieces carrying rollup dimensions and has no distinct metrics can be split conjuncts.
        // 1. agg pieces has distinct metrics only support perfect-match, so all conjuncts can be hoisted.
        // 2. agg pieces has no rollup dimensions has no conjuncts to split.
        Predicate<PlanPiece> hasRollupDimensions = piece -> piece.cast(AggregatePiece.class)
                .map(aggPiece -> aggPiece.getDistinctMetrics().isEmpty() &&
                        !aggPiece.getRollupDimensions().isEmpty()).orElse(false);

        Map<Boolean, TieredList<PlanPiece>> pieceGroups = pieces.stream()
                .collect(Collectors.partitioningBy(hasRollupDimensions, TieredList.<PlanPiece>toList()));

        List<PlanPiece> piecesHasNoRollupDim = pieceGroups.get(false);
        List<PlanPiece> piecesHasRollupDim = pieceGroups.get(true);

        if (piecesHasRollupDim.size() < minPartialRollup) {
            return Optional.empty();
        }

        Map<String, List<Pair<AggregatePiece, Integer>>> normToPieces = Maps.newHashMap();
        for (PlanPiece piece : piecesHasRollupDim) {
            AggregatePiece aggPiece = piece.cast();
            aggPiece.getRollupDimensions().forEach((k, v) -> {
                normToPieces
                        .computeIfAbsent(v.getNorm().toString(), a -> Lists.newArrayList())
                        .add(Pair.create(aggPiece, k));
            });
        }

        List<List<Pair<AggregatePiece, Integer>>> identicalNormColumnGroups =
                normToPieces.entrySet().stream()
                        .filter(e -> e.getValue().size() >= minPartialRollup)
                        .map(e -> Pair.create(e, e.getValue().size()))
                        .sorted(Collections.reverseOrder(Comparator.comparingInt(p -> p.second)))
                        .map(p -> p.first.getValue())
                        .collect(Collectors.toList());

        if (identicalNormColumnGroups.isEmpty()) {
            return Optional.empty();
        }

        List<List<PlanPiece>> newAggPieceGroups = Lists.newArrayList();
        Set<Box<AggregatePiece>> nonSplitPieceSet = Sets.newHashSet();
        Set<Box<AggregatePiece>> spiltPieceSet = Sets.newHashSet();

        PartitionExtractor extractor = options.getPartitionExtractor();
        for (List<Pair<AggregatePiece, Integer>> columnGroup : identicalNormColumnGroups) {
            Pair<AggregatePiece, Integer> firstAggAndColumn = columnGroup.get(0);
            AggregatePiece firstAggPiece = firstAggAndColumn.first;
            Integer columnId = firstAggAndColumn.second;
            List<PartitionPlus> partitions = firstAggPiece.getPartitionColumns(extractor);
            ColumnRefSet partitionColumnIds = ColumnRefSet.of();
            partitions.stream()
                    .map(pp -> pp.getPartitionColumns().stream().map(p -> p.first).collect(Collectors.toList()))
                    .map(ColumnRefSet::createByIds)
                    .forEach(partitionColumnIds::union);

            List<Box<AggregatePiece>> aggPieceGroup = columnGroup.stream()
                    .map(p -> p.first)
                    .map(Box::of).collect(Collectors.toList());

            if (!partitionColumnIds.contains(columnId)) {
                nonSplitPieceSet.addAll(aggPieceGroup);
                continue;
            }
            Optional<List<PlanPiece>> optNewAggPieces = toPartialRollup(columnGroup);
            if (optNewAggPieces.isPresent()) {
                spiltPieceSet.addAll(aggPieceGroup);
                newAggPieceGroups.add(optNewAggPieces.get());
            } else {
                nonSplitPieceSet.addAll(aggPieceGroup);
            }
        }

        TieredList<PlanPiece> nonSplitAggPieces = Sets.difference(nonSplitPieceSet, spiltPieceSet)
                .stream()
                .map(Box::unboxed)
                .map(piece -> piece.mustCast(AggregatePiece.class).toRollup())
                .collect(TieredList.toList());

        nonSplitAggPieces = nonSplitAggPieces.concat(piecesHasNoRollupDim).stream()
                .map(piece -> piece.mustCast(AggregatePiece.class))
                .map(piece -> AggregatePolicies.applyRollupOrPerfectMatch(options, piece))
                .collect(TieredList.toList());
        newAggPieceGroups.add(nonSplitAggPieces);

        return Optional.of(newAggPieceGroups);
    }

    private List<MVRecommendation> recommendSimply(Collection<List<PlanPiece>> pieceGroups, int limit) {
        List<MVRecommendation> resultList = Lists.newArrayList();
        Preconditions.checkArgument(limit >= 0);
        for (List<PlanPiece> pieceGroup : pieceGroups) {
            resultList.addAll(recommendMVBasedLattice(pieceGroup));
            if (resultList.size() >= limit) {
                break;
            }
        }
        limit = Math.min(limit, resultList.size());
        return resultList.subList(0, limit);
    }

    private Optional<QueryGenerateResult> recommendOneMv(PlanPiece piece) {
        AggregatePiece aggPiece = piece.cast();
        ColumnRefToIdConverter idConverter = aggPiece.getFlatTable().getPiece().getCommonState().getIdConverter();
        MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                .setOptions(options)
                .enableGenerateTraceLog()
                .enablePolicyTraceLog()
                .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                .setNextId(idConverter::nextId)
                .build();
        return AggregateMVGenerator.generate(aggPiece, mvGenerateContext);
    }

    private List<MVRecommendation> recommendMVBasedLattice(List<PlanPiece> pieces) {
        Lattice lattice = Lattice.createLattice(pieces, false);
        lattice.getNodes().forEach(LatticeNode::hoist);
        lattice.consolidateFully(options.isPruneRollupAbleWithConjuncts());
        lattice.rearrange();
        Predicate<LatticeNode> partitioner = node -> node.getFinalAggPiece().getDimensions().size()
                >= GlobalVariable.getAutoMVColocateMVDimensionsLimit();
        Map<Boolean, List<LatticeNode>> nodeGroups = lattice.getNodes().stream()
                .collect(Collectors.partitioningBy(partitioner));

        TieredList<MVRecommendation> colocatedMVs =
                lattice.pickupCollocateMVRecommendations(nodeGroups.get(true), null);
        TieredList<MVRecommendation> nonColocatedMVs = nodeGroups.get(false)
                .stream()
                .map(MVRecommendation::new)
                .collect(TieredList.<MVRecommendation>toList());

        return colocatedMVs.concat(nonColocatedMVs)
                .stream()
                .peek(rec -> rec.setMvResult(recommendOneMv(rec.getLatticeNode().getFinalAggPiece()).orElse(null)))
                .filter(rec -> rec.getMvResult() != null)
                .collect(Collectors.toList());
    }

    // Only AutoMV L3 shall turn on MV Selector
    private boolean enableMVSelector() {
        return GlobalVariable.isEnableAutoMVLifecycleKeeper() &&
                (GlobalVariable.getAutoMVPerLatticeMVLimit() > 0 ||
                        GlobalVariable.getAutoMVPerLatticeMVSelectivityRatio() > 0);
    }

    private TieredList<MVRecommendation> pickupRecommendations(List<PlanPiece> pieces,
                                                               CardEstimationPolicy cardEstimationPolicy) {

        Lattice lattice = Lattice.createLattice(pieces, options.isDecayAcceleratedQueries());
        TieredList<MVRecommendation> recommendations = cardEstimationPolicy.estimate(lattice);
        if (enableMVSelector()) {
            MVRecommendationSelectOptions selectOptions = new MVRecommendationSelectOptions();
            MVRecommendationSelector selector = new MVRecommendationSelector(selectOptions);
            return selector.select(recommendations);
        }
        return recommendations;
    }

    public List<MVRecommendation> recommendUsingCardinalityEstimation(Collection<List<PlanPiece>> pieceGroups,
                                                                      int endIdx) {
        CardEstimationPolicy policy = new CardEstimationPolicy(options, ctx);

        pieceGroups = pieceGroups.stream()
                .filter(pieces -> !pieces.isEmpty() && !hasCrossJoin(pieces.get(0)))
                .collect(Collectors.toList());

        TieredList<MVRecommendation> recommendationList = pieceGroups.stream()
                .map(pieces -> pickupRecommendations(pieces, policy))
                .reduce(TieredList.genesis(), TieredList::concat);

        List<MVRecommendation> recommendations = Lists.newArrayList(recommendationList).stream()
                .sorted(Collections.reverseOrder(
                        Comparator.comparingDouble(MVRecommendation::getEffectiveTotalBenefit)))
                .collect(Collectors.toList());

        Preconditions.checkArgument(endIdx >= 0);
        List<MVRecommendation> selectedRecommendations =
                recommendations.subList(0, Math.min(endIdx, recommendations.size()));

        return selectedRecommendations.stream()
                .peek(rec -> rec.setMvResult(recommendOneMv(rec.getLatticeNode().getFinalAggPiece()).orElse(null)))
                .filter(rec -> rec.getMvResult() != null)
                .collect(Collectors.toList());
    }
}
