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

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PieceCommonState;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.Box;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Lattice is a standard algebraic system - semi-lattice. A family of AggregatePieces
// over the identical flat table(exactly speaking, flat tables that are semantic-equivalent
// with each other and have lexical differences are defined identical) forms a semi-lattice.
// Partial set (A, subset(<=)) is defined as follows:
// 1. A is a set formed by AggregatePieces with the identical flat table.
// 2. subset(<=) is partial order relation, a<=b holds iff a's dimensions is subset of b's;
//
// Partial set (A, subset(<=)) is a semi-lattice two, because we can deduce a binary operation
// -- merge operation(U) from this partial set. that is to say (A, subset(<=), merge(U)) is
// semi-lattice:
// 1. for each pair of a and b belong A;  a U b is a new AggregatePiece that its metrics are
// union of both a's metrics and b's metrics, and its dimensions are union of both a's dimensions
// and b's dimensions.
// 2. a <= (a U b) and b <= (a U b) always holds.
//
// we use Lattice represents this family of AggregatePieces, Lattice consists of LatticeNode,
// each LatticeNode represents a subset a AggregatePieces that with the same dimensions. so,
// We can apply two operations to a Lattice:
// 1. Consolidate a LatticeNode: we can merge a bundle of AggregatePieces with the same
//  dimensions into one AggregatePiece;
// 2. Cover two LatticeNode: we can merge two LatticeNode into one LatticeNode whose dimensions
//  is union of dimensions of the two.
// So Lattice is used to merge AggregatePieces.
public class Lattice {
    private final Map<String, Integer> columnToOrdinalMap;
    private final List<Integer> columnIds;
    private final List<Integer> dimensionColumnIds;
    private final List<String> columnNorms;
    private final PlanPiece flatTable;
    private final Map<String, Op> flatTableNormToOpMap;

    private final List<LatticeNodeId> nodeIds = Lists.newArrayList();
    private final Map<LatticeNodeId, Integer> dimensionsFreqs = Maps.newHashMap();
    private final TieredMap<Integer, GenericColumn> flatTableNorms;
    private final LatticeNode root;

    private final List<LatticeNode> nodes = Lists.newArrayList();

    public Lattice(Map<String, Integer> columnToOrdinalMap,
                   List<Integer> columnIds,
                   List<Integer> dimensionColumnIds,
                   List<String> columnNorms,
                   PlanPiece flatTable,
                   LatticeNodeId rootId,
                   AggregatePiece rootAggPiece) {
        this.columnToOrdinalMap = Objects.requireNonNull(columnToOrdinalMap);
        this.columnIds = Objects.requireNonNull(columnIds);
        this.dimensionColumnIds = Objects.requireNonNull(dimensionColumnIds);
        this.columnNorms = Objects.requireNonNull(columnNorms);
        this.flatTable = Objects.requireNonNull(flatTable);
        this.flatTableNormToOpMap = flatTable.getColumns().entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> e.getValue().getNorm().toString(),
                        e -> OpUtil.columnToOp(e.getKey(), e.getValue())));
        this.flatTableNorms = flatTable.getColumns().entrySet().stream()
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> e.getValue().getNorm()));
        this.root = new LatticeNode(this, rootId, rootAggPiece);
    }

    private static AggregatePiece createRootPiece(PlanPiece piece) {
        return AggregatePiece.newBuilder()
                .setFlatTable(piece)
                .setDimensions(piece.getColumns())
                .setRollupDimensions(TieredMap.genesis())
                .setMetrics(TieredMap.genesis())
                .setDistinctMetrics(TieredMap.genesis())
                .setHoistConjuncts(TieredList.genesis())
                .setNonHoistConjuncts(TieredList.genesis())
                .setConjuncts(TieredList.genesis())
                .setCommonState(piece.getCommonState())
                .build().cast();
    }

    public static Lattice createLattice(List<PlanPiece> pieces) {
        AggregatePiece firstAggPiece = pieces.get(0).cast();
        Set<String> dimensionNorms = pieces.stream()
                .map(piece -> piece.mustCast(AggregatePiece.class))
                .flatMap(aggPiece -> aggPiece.getDimensions().values().stream())
                .map(GenericColumn::getNorm)
                .map(GenericColumn::toString)
                .collect(Collectors.toSet());

        Lattice lattice = Lattice.createLattice(firstAggPiece, dimensionNorms);
        pieces.forEach(piece -> lattice.insert(piece.cast()));
        lattice.updateDimensionsFrequencies();
        return lattice;
    }

    private static Lattice createLattice(PlanPiece seed, Set<String> dimensionNorms) {
        PlanPiece flatTable = seed.cast(AggregatePiece.class).map(AggregatePiece::getFlatTable).orElse(seed);

        Set<String> uniqueColumnNorms = Sets.newHashSet();
        List<Pair<Integer, GenericColumn>> uniqueColumns =
                Lists.newArrayListWithCapacity(flatTable.getColumns().size());
        for (Map.Entry<Integer, GenericColumn> e : flatTable.getColumns().entrySet()) {
            int columnId = e.getKey();
            GenericColumn column = e.getValue();
            if (!uniqueColumnNorms.contains(column.getNorm().toString())) {
                uniqueColumnNorms.add(column.getNorm().toString());
                uniqueColumns.add(Pair.create(columnId, column));
            }
        }

        Map<Boolean, List<Pair<Integer, GenericColumn>>> columnGroups =
                uniqueColumns.stream()
                        .collect(Collectors.partitioningBy(p ->
                                dimensionNorms.contains(p.second.getNorm().toString())));

        // sorted column norms and keep dimension norms first
        List<Pair<Integer, String>> orderedColumnIdsAndNorms =
                Stream.of(columnGroups.get(true), columnGroups.get(false))
                        .flatMap(columns -> columns.stream()
                                .map(e -> Pair.create(e.first, e.second.getNorm().toString()))
                                .sorted(Pair.comparingBySecond()))
                        .collect(Collectors.toList());

        List<Integer> columnIds = orderedColumnIdsAndNorms.stream().map(p -> p.first).collect(Collectors.toList());
        List<Integer> dimensionColumnIds = columnIds.subList(0, columnGroups.get(true).size());
        List<String> columnNorms = orderedColumnIdsAndNorms.stream().map(p -> p.second).collect(Collectors.toList());

        Supplier<Integer> idGen = Util.nextIdGenerator();
        Map<String, Integer> columnToOrdinalMap = columnNorms.stream()
                .collect(ImmutableMap.toImmutableMap(Function.identity(), (k) -> idGen.get()));

        AggregatePiece rootPiece = createRootPiece(flatTable);
        LatticeNodeId rootId = LatticeNodeId.calcRootId(columnNorms.size());
        return new Lattice(columnToOrdinalMap, columnIds, dimensionColumnIds, columnNorms, flatTable, rootId,
                rootPiece);
    }

    public List<LatticeNode> getNodes() {
        return nodes;
    }

    public List<String> getColumnNorms() {
        return columnNorms;
    }

    public List<Integer> getDimensionColumnIds() {
        return dimensionColumnIds;
    }

    public PlanPiece getFlatTable() {
        return flatTable;
    }

    public void addAllMinimalCoveringNodes() {
        Set<Pair<LatticeNodeId, LatticeNodeId>> processed = Sets.newHashSet();
        while (addAllMinimalCoveringNodesOnePass(processed)) {
            consolidate();
        }
    }

    public void addMaximalNode() {
        if (root.getChildren().size() < 2) {
            return;
        }
        insert(cover(root.getChildren()));
    }

    private boolean addAllMinimalCoveringNodesOnePass(Set<Pair<LatticeNodeId, LatticeNodeId>> processed) {
        int numNodes = nodes.size();
        List<LatticeNode> bfsNodes = bfsIncludingRoot();
        List<Pair<LatticeNode, LatticeNode>> partialOverlappingPairs = Lists.newArrayList();
        for (int n = numNodes - 1; n >= 0; --n) {
            LatticeNode node = bfsNodes.get(n);
            int numChildren = node.getChildren().size();
            if (numChildren < 2) {
                continue;
            }
            for (int c0 = 0; c0 < numChildren; ++c0) {
                for (int c1 = c0 + 1; c1 < numChildren; ++c1) {
                    LatticeNode nodeA = node.getChildren().get(c0);
                    LatticeNode nodeB = node.getChildren().get(c1);
                    Pair<LatticeNodeId, LatticeNodeId> idPair = Pair.create(nodeA.getId(), nodeB.getId());

                    if (processed.contains(idPair) || processed.contains(idPair.inverse())) {
                        continue;
                    }

                    if (!nodeA.getId().isOverlappingPartially(nodeB.getId())) {
                        continue;
                    }
                    processed.add(idPair);
                    partialOverlappingPairs.add(Pair.create(nodeA, nodeB));
                }
            }
        }
        for (Pair<LatticeNode, LatticeNode> pair : partialOverlappingPairs) {
            insert(cover(pair.first, pair.second));
        }
        return !partialOverlappingPairs.isEmpty();
    }

    private AggregatePiece cover(List<LatticeNode> nodes) {
        Optional<LatticeNodeId> optId =
                nodes.stream().map(LatticeNode::getId).reduce(LatticeNodeId::merge);

        Preconditions.checkArgument(optId.isPresent());
        LatticeNodeId id = optId.get();
        List<AggregatePiece> aggPieces = nodes.stream()
                .map(LatticeNode::getCoverablePieces).flatMap(Collection::stream)
                .collect(Collectors.toList());

        TieredMap<Integer, GenericColumn> dimensions = id.getColumnOrdinals().stream()
                .map(columnIds::get)
                .collect(TieredMap.toMap(Function.identity(), flatTable.getColumns()::get));

        ColumnRefToIdConverter idConverter = flatTable.getCommonState().getIdConverter().duplicate();
        TieredMap<Integer, GenericColumn> metrics =
                LatticeNode.mergeMetrics(idConverter, flatTableNormToOpMap, aggPieces);
        TieredList<Op> hoistConjuncts = LatticeNode.mergeHoistConjuncts(flatTableNormToOpMap, aggPieces);
        TieredList<Op> nonHoistConjuncts = LatticeNode.mergeNonHoistConjuncts(flatTableNormToOpMap, aggPieces);
        PieceCommonState commonState = new PieceCommonState(idConverter, flatTable.getCommonState().getFqTableMap());
        AggregatePiece aggPiece = AggregatePiece.newBuilder()
                .setFlatTable(flatTable)
                .setDimensions(dimensions)
                .setRollupDimensions(TieredMap.genesis())
                .setMetrics(metrics)
                .setDistinctMetrics(TieredMap.genesis())
                .setHoistConjuncts(hoistConjuncts)
                .setNonHoistConjuncts(nonHoistConjuncts)
                .setCommonState(commonState)
                .setConjuncts(TieredList.genesis())
                .build()
                .cast();

        aggPiece = aggPiece.revise(piece -> piece.builder().setCommonState(commonState).build()).cast();
        aggPiece.assignPieceIds();
        return PlanPieceNormalizer.normalizeTopPiece(aggPiece, flatTableNorms).cast();
    }

    private AggregatePiece cover(LatticeNode... nodes) {
        return cover(Arrays.asList(nodes));
    }

    public void insert(AggregatePiece aggPiece) {
        Preconditions.checkArgument(aggPiece.getFlatTable().getAuxState().getNormHash()
                .equals(flatTable.getAuxState().getNormHash()));
        LatticeNodeId id = LatticeNodeId.calcId(aggPiece.getDimensions().values(), columnToOrdinalMap);
        insertWithId(root, aggPiece, id);
    }

    public List<LatticeNode> bfs() {
        LinkedList<LatticeNode> nodes = bfsIncludingRoot();
        Preconditions.checkArgument(nodes.get(0) == root);
        nodes.removeFirst();
        return nodes;
    }

    private LinkedList<LatticeNode> bfsIncludingRoot() {
        Queue<LatticeNode> q0 = new LinkedList<>();
        Queue<LatticeNode> q1 = new LinkedList<>();
        Set<Box<LatticeNode>> visited = Sets.newHashSet();
        LinkedList<LatticeNode> result = new LinkedList<>();
        q0.add(root);
        while (!q0.isEmpty()) {
            while (!q0.isEmpty()) {
                LatticeNode node = q0.remove();
                if (visited.contains(Box.of(node))) {
                    continue;
                }
                visited.add(Box.of(node));
                result.add(node);
                node.getChildren()
                        .stream()
                        .filter(child -> !visited.contains(Box.of(child)))
                        .forEach(q1::add);
            }
            Queue<LatticeNode> qTemp = q0;
            q0 = q1;
            q1 = qTemp;
        }
        return result;
    }

    private void updateDimensionsFrequencies() {
        this.nodeIds.forEach(id -> dimensionsFreqs.merge(id, 1, Integer::sum));
    }

    private void insertWithId(LatticeNode node, AggregatePiece aggPiece, LatticeNodeId id) {
        this.nodeIds.add(id);
        TieredList<LatticeNode> commonPath = TieredList.genesis();
        List<TieredList<LatticeNode>> ancestorPathList = seekFor(node, commonPath, id);
        Preconditions.checkArgument(!ancestorPathList.isEmpty());

        List<TieredList<LatticeNode>> pathListEndWithTargetId = ancestorPathList.stream()
                .filter(ancestorPath -> {
                    LatticeNode lastNode = ancestorPath.get(-1);
                    return lastNode.getId().equals(id) && lastNode != root;
                })
                .collect(Collectors.toList());

        if (!pathListEndWithTargetId.isEmpty()) {
            LatticeNode existingNode = pathListEndWithTargetId.get(0).get(-1);
            Preconditions.checkArgument(
                    pathListEndWithTargetId.stream().allMatch(path -> path.get(-1) == existingNode));
            existingNode.addPiece(aggPiece);
        } else {
            LatticeNode newChild = new LatticeNode(this, id, aggPiece);
            nodes.add(newChild);
            Set<LatticeNode> parents =
                    ancestorPathList.stream().map(path -> path.get(-1)).collect(Collectors.toSet());
            // step1: Set newChild's parents.
            newChild.setParent(Lists.newArrayList(parents));

            Set<LatticeNode> grandChildren = Sets.newHashSet();

            // step2: Set parents' children.
            // Some children of parents are newChild's siblings, the others are demoted to be grandchildren
            // of the parents, in another word, they are children of the newChild, so we gather them together.
            for (LatticeNode parent : parents) {
                Map<Boolean, List<LatticeNode>> childGroups = parent.getChildren().stream()
                        .collect(Collectors.partitioningBy(child -> child.getId().isCoveredStrictlyBy(id)));
                grandChildren.addAll(childGroups.get(true));
                List<LatticeNode> siblings = childGroups.get(false);
                siblings.add(newChild);
                parent.setChildren(siblings);
            }
            // deduplicate grandchildren
            // step3: Set newChild's children to be grandChildren of parents.
            newChild.getChildren().addAll(grandChildren);

            // step4: Set grandChildren's parents.
            // grandChildren's current parents should be preserved if it does not cover
            // newChild. the newChild should be added to grandchildren's parents.
            for (LatticeNode grandChild : grandChildren) {
                List<LatticeNode> siblings = grandChild.getParents().stream()
                        .filter(parent -> !parent.getId().isCovering(id))
                        .collect(Collectors.toList());
                siblings.add(newChild);
                grandChild.setParent(siblings);
            }
        }
    }

    private List<TieredList<LatticeNode>> seekFor(LatticeNode node, TieredList<LatticeNode> commonPath,
                                                  LatticeNodeId id) {
        Preconditions.checkArgument(node.getId().isCovering(id));
        TieredList<LatticeNode> newCommonPath = commonPath.concatOne(node);
        List<LatticeNode> superSetNodes = node.getChildren().stream()
                .filter(child -> child.getId().isCovering(id))
                .collect(Collectors.toList());

        if (superSetNodes.isEmpty()) {
            return Collections.singletonList(newCommonPath);
        }

        return superSetNodes.stream().map(n -> seekFor(n, newCommonPath, id))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void consolidate() {
        nodes.forEach(node -> {
            node.consolidateCoverable();
            node.consolidateUncoverable();
        });
    }

    public void consolidateFully(boolean pruneRollupUnableWithConjuncts) {
        nodes.forEach(node -> node.consolidateFully(pruneRollupUnableWithConjuncts));
    }

    public List<AggregatePiece> getAllPieces() {
        return nodes.stream().flatMap(node -> Stream.of(node.getCoverablePieces(), node.getUncoverablePieces()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public TieredList<MVRecommendation> pickupRecommendations(AutoMVOptions options) {
        Function<LatticeNode, Integer> classifier = node -> {
            double ratio = node.getCard().getCardRowCountRatio();
            if (ratio > options.getCardRowCountRatioHWM()) {
                return 0;
            } else if (ratio < options.getCardRowCountRatioLWM()) {
                return 1;
            } else {
                return 2;
            }
        };

        List<LatticeNode> nodes = bfs();
        Map<Integer, List<LatticeNode>> nodeGroups = nodes.stream()
                .collect(Collectors.groupingBy(classifier));

        List<LatticeNode> nodesWithHighCard = nodeGroups.getOrDefault(0, Collections.emptyList());
        List<LatticeNode> nodesWithLowCard = nodeGroups.getOrDefault(1, Collections.emptyList());
        List<LatticeNode> nodesWithModerateCard = nodeGroups.getOrDefault(2, Collections.emptyList());

        // high-cardinality MV is pruned here
        // TODO(by satanson): in future, a bundle of high-cardinality MVs should merge into
        //  a flat table MV that based on cardinality-preserving join.
        Set<LatticeNodeId> prunedMVs = Sets.newHashSet();
        nodesWithHighCard.forEach(node -> prunedMVs.add(node.getId()));

        List<MVRecommendation> candidateMVs = nodesWithModerateCard
                .stream().map(MVRecommendation::new).collect(Collectors.toList());

        Function<LatticeNode, MVRecommendation> lowCardNodeBenefitGetter = node -> {
            MVRecommendation mvRec = new MVRecommendation(node);
            mvRec.setProcessed(true);
            long numQueriesAccelerated = dimensionsFreqs.entrySet().stream()
                    .filter(e -> node.getId().isCovering(e.getKey()))
                    .map(Map.Entry::getValue)
                    .reduce(0, Integer::sum);
            mvRec.setNumQueriesAccelerated((int) numQueriesAccelerated);
            mvRec.setTotalBenefit(node.getCard().getBenefit() * numQueriesAccelerated);
            return mvRec;
        };

        TieredList<MVRecommendation> nodeAndBenefitWithLowCardList = nodesWithLowCard
                .stream().map(lowCardNodeBenefitGetter)
                .collect(TieredList.toList());

        double rowCount = nodes.iterator().next().getCard().getRowCount();
        List<QueryBenefit> queryBenefits = this.dimensionsFreqs.entrySet()
                .stream().map(e -> new QueryBenefit(e.getKey(), e.getValue().doubleValue(), rowCount))
                .collect(Collectors.toList());

        BenefitTable benefitTable = new BenefitTable(candidateMVs, queryBenefits);
        TieredList<MVRecommendation> selectedNodeAndBenefitList =
                benefitTable.calculate(options.getMaxCalculateSteps());

        return selectedNodeAndBenefitList.concat(nodeAndBenefitWithLowCardList);
    }

}
