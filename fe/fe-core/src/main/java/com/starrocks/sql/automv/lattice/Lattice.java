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
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.qe.GlobalVariable;
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
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
    private final AggregatePiece.FlatTable flatTable;
    private final Map<String, Op> flatTableNormToOpMap;

    private final List<LatticeNodeId> coverableNodeIds = Lists.newArrayList();
    private final List<LatticeNodeId> uncoverableINodeIds = Lists.newArrayList();
    private final Map<LatticeNodeId, List<String>> uncoverableIINodeIds = Maps.newHashMap();
    private final TieredMap<Integer, GenericColumn> flatTableNorms;
    private final LatticeNode root;
    private final List<LatticeNode> nodes = Lists.newArrayList();
    private transient BiMap<Box<LatticeNode>, Integer> nodeOrdinal;

    private Map<LatticeNodeId, Long> coverableNumAcceleratedQueries = null;
    private Map<LatticeNodeId, Long> accCoverableNumAcceleratedQueries = null;
    private Map<LatticeNodeId, Long> uncoverableINumAcceleratedQueries = null;
    private Map<LatticeNodeId, Map<String, Long>> uncoverableIINumAcceleratedQueries = null;
    private BenefitTable benefitTable;

    public Lattice(Map<String, Integer> columnToOrdinalMap,
                   List<Integer> columnIds,
                   List<Integer> dimensionColumnIds,
                   List<String> columnNorms,
                   AggregatePiece.FlatTable flatTable,
                   LatticeNodeId rootId,
                   AggregatePiece rootAggPiece) {
        this.columnToOrdinalMap = Objects.requireNonNull(columnToOrdinalMap);
        this.columnIds = Objects.requireNonNull(columnIds);
        this.dimensionColumnIds = Objects.requireNonNull(dimensionColumnIds);
        this.columnNorms = Objects.requireNonNull(columnNorms);
        this.flatTable = Objects.requireNonNull(flatTable);
        this.flatTableNormToOpMap = flatTable.getPiece().getColumns().entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> e.getValue().getNorm().toString(),
                        e -> OpUtil.columnToOp(e.getKey(), e.getValue())));
        this.flatTableNorms = flatTable.getPiece().getColumns().entrySet().stream()
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> e.getValue().getNorm()));
        this.root = new LatticeNode(this, rootId, rootAggPiece);
    }

    private static AggregatePiece createRootPiece(AggregatePiece.FlatTable flatTable) {
        return AggregatePiece.newBuilder()
                .setFlatTable(flatTable)
                .setDimensions(flatTable.getPiece().getColumns())
                .setRollupDimensions(TieredMap.genesis())
                .setMetrics(TieredMap.genesis())
                .setDistinctMetrics(TieredMap.genesis())
                .setHoistConjuncts(TieredList.genesis())
                .setNonHoistConjuncts(TieredList.genesis())
                .setConjuncts(TieredList.genesis())
                .setCommonState(flatTable.getPiece().getCommonState())
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
        return lattice;
    }

    private static Lattice createLattice(PlanPiece seed, Set<String> dimensionNorms) {
        AggregatePiece aggPiece = seed.mustCast(AggregatePiece.class);
        AggregatePiece.FlatTable flatTable = aggPiece.getFlatTable();
        PlanPiece flatTablePiece = flatTable.getPiece();
        Set<String> uniqueColumnNorms = Sets.newHashSet();
        List<Pair<Integer, GenericColumn>> uniqueColumns =
                Lists.newArrayListWithCapacity(flatTablePiece.getColumns().size());
        for (Map.Entry<Integer, GenericColumn> e : flatTablePiece.getColumns().entrySet()) {
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
        return new Lattice(columnToOrdinalMap, columnIds, dimensionColumnIds, columnNorms, rootPiece.getFlatTable(),
                rootId, rootPiece);
    }

    public void updateNodeIds(LatticeNodeId id, LatticeNode.Category category, String conjunctsNorm) {
        switch (category) {
            case COVERABLE: {
                coverableNodeIds.add(id);
                break;
            }
            case UNCOVERABLE_I: {
                uncoverableINodeIds.add(id);
                break;
            }
            case UNCOVERABLE_II: {
                uncoverableIINodeIds.computeIfAbsent(id, key -> Lists.newArrayList()).add(conjunctsNorm);
            }
        }
    }

    public List<LatticeNode> getNodes() {
        return nodes;
    }

    public int getNodeOrdinal(LatticeNode node) {
        if (nodeOrdinal == null) {
            Supplier<Integer> idGen = Util.nextIdGenerator();
            nodeOrdinal = Stream.concat(Stream.of(root), nodes.stream())
                    .sorted(LatticeNode.getComparator()).map(n -> Pair.create(n, idGen.get()))
                    .collect(ImmutableBiMap.toImmutableBiMap(p -> Box.of(p.first), p -> p.second));
        }
        return nodeOrdinal.get(Box.of(node));
    }

    public List<String> getColumnNorms() {
        return columnNorms;
    }

    public List<Integer> getDimensionColumnIds() {
        return dimensionColumnIds;
    }

    public AggregatePiece.FlatTable getFlatTable() {
        return flatTable;
    }

    public void addAllMinimalCoveringNodes() {
        Set<Pair<LatticeNodeId, LatticeNodeId>> processed = Sets.newHashSet();
        while (addAllMinimalCoveringNodesOnePass(processed)) {
            consolidateCoverable();
        }
    }

    public void addMaximalNode() {
        if (root.getChildren().size() < 2) {
            return;
        }
        insert(cover(root.getChildren()));
    }

    private boolean addAllMinimalCoveringNodesOnePass(Set<Pair<LatticeNodeId, LatticeNodeId>> processed) {
        int nodeLimit = GlobalVariable.getAutoMVPerLatticeNodeLimit();
        int numNodes = nodes.size();
        if (numNodes >= nodeLimit) {
            return false;
        }
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
                    // number of dimensions is too large so that later card estimation query would be
                    // time-consuming, and these nodes' cardinality are approaching to row count of the
                    // underlying table, so the corresponding MVs would BE poor-performance.
                    if (idPair.first.merge(idPair.second).size() >= 10) {
                        continue;
                    }
                    processed.add(idPair);
                    partialOverlappingPairs.add(Pair.create(nodeA, nodeB));
                }
            }
        }
        int n = Math.min(nodeLimit - numNodes, partialOverlappingPairs.size());
        partialOverlappingPairs = partialOverlappingPairs.subList(0, n);
        for (Pair<LatticeNode, LatticeNode> pair : partialOverlappingPairs) {
            insert(cover(pair.first, pair.second));
        }
        return !partialOverlappingPairs.isEmpty() && nodes.size() < nodeLimit;
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
                .collect(TieredMap.toMap(Function.identity(), flatTable.getPiece().getColumns()::get));

        ColumnRefToIdConverter idConverter = flatTable.getPiece().getCommonState().getIdConverter().duplicate();
        TieredMap<Integer, GenericColumn> metrics =
                LatticeNode.mergeMetrics(idConverter, flatTableNormToOpMap, aggPieces);
        TieredList<Op> hoistConjuncts = LatticeNode.mergeHoistConjuncts(flatTableNormToOpMap, aggPieces);
        TieredList<Op> nonHoistConjuncts = LatticeNode.mergeNonHoistConjuncts(flatTableNormToOpMap, aggPieces);
        Set<String> mergedCoveredQueries = aggPieces
                .stream()
                .flatMap(piece -> piece.getCommonState().getCoveredQueries().stream())
                .collect(ImmutableSet.toImmutableSet());
        PieceCommonState commonState =
                new PieceCommonState(idConverter, mergedCoveredQueries,
                        flatTable.getPiece().getCommonState().getFqTableMap());
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
        Preconditions.checkArgument(aggPiece.getFlatTable().getPiece().getAuxState().getNormHash()
                .equals(flatTable.getPiece().getAuxState().getNormHash()));
        LatticeNodeId id = LatticeNodeId.calcId(aggPiece.getDimensions().values(), columnToOrdinalMap);
        insertWithId(aggPiece, id);
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

    private void linkNodeToItsParents(Collection<LatticeNode> parents, LatticeNode node) {
        // step1: Set newChild's parents.
        LatticeNodeId id = node.getId();
        node.setParent(Lists.newArrayList(parents));

        Set<LatticeNode> grandChildren = Sets.newHashSet();

        // step2: Set parents' children.
        // Some children of parents are newChild's siblings, the others are demoted to be grandchildren
        // of the parents, in another word, they are children of the newChild, so we gather them together.
        for (LatticeNode parent : parents) {
            Map<Boolean, List<LatticeNode>> childGroups = parent.getChildren().stream()
                    .collect(Collectors.partitioningBy(child -> child.getId().isCoveredStrictlyBy(id)));
            grandChildren.addAll(childGroups.get(true));
            List<LatticeNode> siblings = childGroups.get(false);
            siblings.add(node);
            parent.setChildren(siblings);
        }
        // deduplicate grandchildren
        // step3: Set newChild's children to be grandChildren of parents.
        node.getChildren().addAll(grandChildren);

        // step4: Set grandChildren's parents.
        // grandChildren's current parents should be preserved if it does not cover
        // newChild. the newChild should be added to grandchildren's parents.
        for (LatticeNode grandChild : grandChildren) {
            List<LatticeNode> siblings = grandChild.getParents().stream()
                    .filter(parent -> !parent.getId().isCovering(id))
                    .collect(Collectors.toList());
            siblings.add(node);
            grandChild.setParent(siblings);
        }
    }

    private void addNode(Collection<LatticeNode> parents, LatticeNode node) {
        linkNodeToItsParents(parents, node);
        linkNodeToItsChildren(node);
    }

    private void insertNode(LatticeNode node) {
        TieredList<LatticeNode> commonPath = TieredList.genesis();
        LatticeNodeId id = node.getId();
        List<TieredList<LatticeNode>> ancestorPathList = seekFor(this.root, commonPath, id);
        Preconditions.checkArgument(!ancestorPathList.isEmpty());

        List<TieredList<LatticeNode>> pathListEndWithTargetId = ancestorPathList.stream()
                .filter(ancestorPath -> {
                    LatticeNode lastNode = ancestorPath.get(-1);
                    return lastNode.getId().equals(id) && lastNode != root;
                })
                .collect(Collectors.toList());

        Preconditions.checkState(pathListEndWithTargetId.isEmpty());
        this.nodes.add(node);
        Set<LatticeNode> parents =
                ancestorPathList.stream().map(path -> path.get(-1)).collect(Collectors.toSet());
        addNode(parents, node);
    }

    private void insertWithId(AggregatePiece aggPiece, LatticeNodeId id) {
        TieredList<LatticeNode> commonPath = TieredList.genesis();
        List<TieredList<LatticeNode>> ancestorPathList = seekFor(root, commonPath, id);
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
            addNode(parents, newChild);
        }
    }

    private Set<Box<LatticeNode>> gatherChildren(LatticeNode node) {
        Set<Box<LatticeNode>> legacyChildren = node.getChildren()
                .stream()
                .map(Box::of)
                .collect(Collectors.toSet());

        Set<Box<LatticeNode>> newChildren = Sets.newHashSet();

        Set<Box<LatticeNode>> closerOffsprings = nodes
                .stream()
                .filter(n -> node.getId().isCoveringStrictly(n.getId()))
                .map(Box::of)
                .filter(n -> legacyChildren.stream()
                        .noneMatch(ch -> ch.unboxed().getId().isCovering(n.unboxed().getId())))
                .collect(Collectors.toSet());

        while (!closerOffsprings.isEmpty()) {
            Set<Box<LatticeNode>> currCloserOffsprings = Sets.newHashSet();
            for (Box<LatticeNode> currNode : closerOffsprings) {
                List<Box<LatticeNode>> nodeCloserOffsprings = currNode.unboxed().getParents().stream()
                        .filter(np -> node.getId().isCoveringStrictly(np.getId()))
                        .map(Box::of)
                        .collect(Collectors.toList());
                if (nodeCloserOffsprings.isEmpty()) {
                    newChildren.add(currNode);
                } else {
                    currCloserOffsprings.addAll(nodeCloserOffsprings);
                }
            }
            closerOffsprings = currCloserOffsprings;
        }
        return newChildren;
    }

    private void linkNodeToItsChildren(LatticeNode node) {
        List<LatticeNode> children = gatherChildren(node)
                .stream()
                .map(Box::unboxed)
                .collect(Collectors.toList());

        node.getChildren().addAll(children);
        children.forEach(child -> child.getParents().add(node));
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

    public void consolidateCoverable() {
        nodes.forEach(LatticeNode::consolidateCoverable);
    }

    public void consolidate() {
        nodes.forEach(node -> {
            node.consolidateCoverable();
            node.consolidateConsolidatable();
            node.consolidateUnconsolidatable();
        });
    }

    public void consolidateFully(boolean pruneRollupUnableWithConjuncts) {
        nodes.forEach(node -> node.consolidateFully(pruneRollupUnableWithConjuncts));
    }

    public List<AggregatePiece> getAllPieces() {
        return nodes.stream().flatMap(node -> Stream.of(node.getCoverablePieces(), node.getUncoverableIPieces()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void rearrange() {
        List<LatticeNode> uncoverableIINodes = nodes.stream()
                .map(LatticeNode::split)
                .reduce(TieredList.<LatticeNode>genesis(), TieredList::concat);

        Map<Boolean, List<LatticeNode>> nodeGroups = nodes.stream()
                .collect(Collectors.partitioningBy(node -> node.getUncoverableIPieces().isEmpty()));
        List<LatticeNode> choppedNodes = nodeGroups.get(true);
        List<LatticeNode> retainedNodes = nodeGroups.get(false);

        if (!choppedNodes.isEmpty()) {
            reformat(retainedNodes);
        }
        nodes.addAll(uncoverableIINodes);
    }

    private void reformat(List<LatticeNode> nodes) {
        this.nodes.clear();
        this.root.getParents().clear();
        this.root.getChildren().clear();
        nodes.forEach(node -> {
            node.getParents().clear();
            node.getChildren().clear();
        });
        nodes.forEach(this::insertNode);
    }

    public LatticeNode getFarthestAncestor(LatticeNode node, Set<Box<LatticeNode>> nodeSet) {
        return node.getParents()
                .stream()
                .filter(parent -> nodeSet.contains(Box.of(parent)))
                .map(parent -> getFarthestAncestor(parent, nodeSet))
                .max(Comparator.comparingDouble(ancestor -> ancestor.getCard().getCardRowCountRatio()))
                .orElse(node);
    }

    public Set<Box<LatticeNode>> getSubtree(LatticeNode node, Set<Box<LatticeNode>> nodeSet) {
        Set<Box<LatticeNode>> subtreeNodes = node.getChildren()
                .stream()
                .filter(child -> nodeSet.contains(Box.of(child)))
                .flatMap(child -> getSubtree(child, nodeSet).stream())
                .collect(Collectors.toSet());
        subtreeNodes.add(Box.of(node));
        return subtreeNodes;
    }

    public TieredList<MVRecommendation> pickupCollocateMVRecommendations(List<LatticeNode> nodes,
                                                                         List<QueryBenefit> queryBenefits) {
        Set<Box<LatticeNode>> nodeSet = nodes.stream().map(Box::of).collect(Collectors.toSet());
        Set<Box<LatticeNode>> coveredNodes = Sets.newHashSet();
        TieredList.Builder<MVRecommendation> recBuilder = TieredList.<MVRecommendation>newGenesisTier();
        for (LatticeNode node : nodes) {
            if (coveredNodes.contains(Box.of(node))) {
                continue;
            }

            LatticeNode farthestAncestor = getFarthestAncestor(node, nodeSet);
            Set<Box<LatticeNode>> offsprings = getSubtree(farthestAncestor, nodeSet);
            List<LatticeNodeId> ids = offsprings.stream()
                    .map(Box::unboxed)
                    .map(LatticeNode::getId)
                    .collect(Collectors.toList());
            LatticeNodeId greatestCommonIds = LatticeNodeId.intersectAll(ids);

            if (greatestCommonIds.size() == 0) {
                farthestAncestor = node;
                offsprings = Collections.emptySet();
                greatestCommonIds = node.getId();
            }

            coveredNodes.add(Box.of(farthestAncestor));
            coveredNodes.addAll(offsprings);
            Set<Integer> collocateBucketKey = greatestCommonIds.getColumnOrdinals()
                    .stream()
                    .map(this.columnIds::get)
                    .collect(Collectors.toSet());
            if (collocateBucketKey.isEmpty()) {
                continue;
            }
            farthestAncestor.getFinalAggPiece().getAuxState().setColocateBucketKey(collocateBucketKey);
            MVRecommendation mvRec = new MVRecommendation(farthestAncestor);
            recBuilder.add(mvRec);
        }
        TieredList<MVRecommendation> collocateMVs = recBuilder.build();
        collocateMVs.forEach(candiMV -> BenefitTable.computeTentativeBenefit(candiMV, queryBenefits));
        collocateMVs.forEach(candiMV -> {
            double mvCost = candiMV.getLatticeNode().getCard().getCardinality();
            double totalBenefit = 0;
            int numQueriesAccelerated = 0;
            for (TentativeQueryBenefit tBenefit : candiMV.getTentativeBenefits()) {
                QueryBenefit qBenefit = queryBenefits.get(tBenefit.getIndex());
                double prevCost = qBenefit.getCost();
                if (mvCost < prevCost) {
                    totalBenefit += (prevCost - mvCost) * qBenefit.getWeight();
                }
                numQueriesAccelerated += Double.valueOf(qBenefit.getWeight()).intValue();
            }
            candiMV.setTotalBenefit(totalBenefit);
            candiMV.setNumQueriesAccelerated(numQueriesAccelerated);
        });
        return collocateMVs;
    }

    public TieredList<MVRecommendation> pickupRecommendations(AutoMVOptions options) {
        if (nodes.isEmpty()) {
            return TieredList.<MVRecommendation>genesis();
        }
        int collocateDimensionsLimit = GlobalVariable.getAutoMVColocateMVDimensionsLimit();
        Function<LatticeNode, Integer> classifier = node -> {
            double ratio = node.getCard().getCardRowCountRatio();
            if (ratio <= options.getCardRowCountRatioLWM()) {
                return 0;
            } else if (ratio <= options.getCardRowCountRatioHWM() && node.getId().size() <= collocateDimensionsLimit) {
                return 1;
            } else {
                return 2;
            }
        };

        computeNumAcceleratedQueries();

        Map<Integer, List<LatticeNode>> nodeGroups = nodes.stream()
                .collect(Collectors.groupingBy(classifier));

        List<LatticeNode> nodesWithLowCard = nodeGroups.getOrDefault(0, Collections.emptyList());
        List<LatticeNode> nodesWithModerateCard = nodeGroups.getOrDefault(1, Collections.emptyList());

        // high-cardinality MV is pruned here
        // TODO(by satanson): in future, a bundle of high-cardinality MVs should merge into
        //  a flat table MV that based on cardinality-preserving join.
        List<LatticeNode> nodesWithHighCard = nodeGroups.getOrDefault(2, Collections.emptyList());

        List<MVRecommendation> candidateMVs =
                nodesWithLowCard.stream().map(MVRecommendation::new).collect(Collectors.toList());

        double rowCount = nodes.iterator().next().getCard().getRowCount();

        List<QueryBenefit> queryBenefits = collectQueryBenefits(rowCount);

        this.benefitTable = new BenefitTable(candidateMVs, queryBenefits);
        TieredList<MVRecommendation> selectedNodeAndBenefitList = this.benefitTable.calculate();
        TieredList<MVRecommendation> collocateMVs =
                pickupCollocateMVRecommendations(nodesWithModerateCard, queryBenefits);

        return selectedNodeAndBenefitList.concat(collocateMVs);
    }

    public BenefitTable getBenefitTable() {
        return benefitTable;
    }

    private List<QueryBenefit> collectQueryBenefits(double initialCost) {
        List<QueryBenefit> queryBenefits = Lists.newArrayList();
        coverableNumAcceleratedQueries.forEach((id, weight) ->
                queryBenefits.add(new QueryBenefit(id, LatticeNode.Category.COVERABLE, null, weight, initialCost)));
        uncoverableINumAcceleratedQueries.forEach((id, weight) ->
                queryBenefits.add(new QueryBenefit(id, LatticeNode.Category.UNCOVERABLE_I, null, weight, initialCost)));
        uncoverableIINumAcceleratedQueries.forEach(
                (id, normRepetitions) -> normRepetitions.forEach((norm, repetitions) -> queryBenefits.add(
                        new QueryBenefit(id, LatticeNode.Category.UNCOVERABLE_II, norm, repetitions, initialCost))));
        return queryBenefits;
    }

    private long getNumAcceleratedQueries(LatticeNode node) {
        Preconditions.checkState(node.getCoverablePieces().isEmpty());
        AggregatePiece aggPiece = node.getFinalAggPiece();
        switch (LatticeNode.Category.getCategory(aggPiece)) {
            case COVERABLE:
            case UNCOVERABLE_I:
                return getNumAcceleratedQueriesForCoverableAndUncoverableI(node);
            case UNCOVERABLE_II:
                return getNumAcceleratedQueriesForUncoverableII(node);
        }
        return 0L;
    }

    private void computeNumAcceleratedQueries() {
        computeCoverableNumAcceleratedQueries();
        computeUncoverableINumAcceleratedQueries();
        computeUncoverableIINumAcceleratedQueries();
    }

    private void computeCoverableNumAcceleratedQueries() {
        Preconditions.checkState(coverableNumAcceleratedQueries == null);
        coverableNumAcceleratedQueries = coverableNodeIds
                .stream()
                .collect(Collectors.groupingBy(id -> id, Collectors.counting()));

        Function<LatticeNode, Long> accNumAccelerated = node ->
                Stream.concat(Stream.of(node.getId()), node.getOffsprings().keySet().stream())
                        .map(id -> coverableNumAcceleratedQueries.getOrDefault(id, 0L))
                        .reduce(0L, Long::sum);

        accCoverableNumAcceleratedQueries = nodes
                .stream()
                .filter(node -> node.getUncoverableIIPieces().isEmpty())
                .collect(Collectors.toMap(LatticeNode::getId, accNumAccelerated));

    }

    private void computeUncoverableINumAcceleratedQueries() {
        Preconditions.checkState(uncoverableINumAcceleratedQueries == null);
        this.uncoverableINumAcceleratedQueries = uncoverableINodeIds
                .stream()
                .collect(Collectors.groupingBy(id -> id, Collectors.counting()));
    }

    private void computeUncoverableIINumAcceleratedQueries() {
        Preconditions.checkState(uncoverableIINumAcceleratedQueries == null);
        uncoverableIINumAcceleratedQueries = uncoverableIINodeIds.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue()
                                .stream()
                                .collect(Collectors.groupingBy(norm -> norm, Collectors.counting()))));
    }

    private long getNumAcceleratedQueriesForCoverableAndUncoverableI(LatticeNode node) {
        Preconditions.checkNotNull(this.coverableNumAcceleratedQueries);
        Preconditions.checkNotNull(this.uncoverableINumAcceleratedQueries);
        return accCoverableNumAcceleratedQueries.getOrDefault(node.getId(), 0L) +
                uncoverableINumAcceleratedQueries.getOrDefault(node.getId(), 0L);

    }

    private long getNumAcceleratedQueriesForUncoverableII(LatticeNode node) {
        Preconditions.checkNotNull(this.uncoverableIINumAcceleratedQueries);
        String conjunctsNormHash = node.getFinalAggPiece().getFlatTable().getFlexibleConjunctsNormHash();
        Long repetitionCount = Optional.ofNullable(uncoverableIINumAcceleratedQueries.get(node.getId()))
                .map(repetitionCounts -> repetitionCounts.get(conjunctsNormHash))
                .orElse(null);
        return Objects.requireNonNull(repetitionCount);
    }

    public PrettyPrinter dump(int rootIdx) {
        Map<String, String> idMap = nodeOrdinal.entrySet().stream()
                .map(e -> Pair.create(e.getKey().unboxed().getId().toString(), e.getValue()))
                .map(p -> Pair.create(p.first, "#" + p.second + ":" + p.first))
                .collect(Collectors.toMap(p -> p.first, p -> p.second));

        List<LatticeNode> bfsNodes = this.bfsIncludingRoot();
        LatticeNode rootNode = nodeOrdinal.inverse().get(rootIdx).unboxed();
        TieredMap<LatticeNodeId, Integer> offsprings = rootNode.getOffsprings();
        Set<LatticeNodeId> nodeIds = Sets.newHashSet();
        nodeIds.addAll(offsprings.keySet());
        nodeIds.add(rootNode.getId());

        PrettyPrinter printer = new PrettyPrinter();
        printer.add("Summary: ").newLine();
        printer.indentEnclose(() -> {
            printer.add("TotalNodes: ").add(bfsNodes.size()).newLine();
            printer.add("NumNodes: ").add(nodeIds.size()).newLine();
            printer.add("NumDimensions: ").add(dimensionColumnIds.size()).newLine();
            printer.add("NumMetrics: ").add(columnIds.size() - dimensionColumnIds.size()).newLine();
        });
        printer.newLine().newLine();
        printer.add("LatticeNodes:").newLine();
        List<PrettyPrinter> nodeList = bfsNodes.stream()
                .filter(node -> nodeIds.contains(node.getId()))
                .sorted(LatticeNode.getComparator())
                .map(node -> node.dump(nodeIds, idMap))
                .collect(Collectors.toList());
        printer.indentEnclose(() -> printer.addSuperStepsWithDelNl("\n", nodeList));
        return printer;
    }

}
