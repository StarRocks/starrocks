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

package com.starrocks.sql.optimizer.rule.transformation.pruner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.structure.Pair;
import com.starrocks.common.util.UnionFind;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// CPJoinGardener is used to analysis cardinality-preserving joins and prune unused
// tables from cardinality-preserving joins.
// Cardinality-preserving join means that joins' equality predicates match foreign key constraints bridging
// its left child and right child.
// Assume that table A has foreign key(A.fk) references to B.pk, so queries as follows are cardinality-preserving
// joins:
// 1. A inner join B on A.fk = B.pk: each row in A matches B exactly once;
// 2. A left join B on A.fk = B.pk: each row in A matches B at most once;
// So the number of this join's output rows is equivalent to A's. if B's columns is unused or it can be substituted
// by A's column that equivalent to B's column, then B can be pruned from this join.
public class CPJoinGardener extends OptExpressionVisitor<Boolean, Void> {
    private final ColumnRefFactory columnRefFactory;
    private final Map<ColumnRefOperator, OptExpression> columnToScans = Maps.newHashMap();
    private final UnionFind<ColumnRefOperator> columnRefEquivClasses = new UnionFind<>();
    private final Map<OptExpression, RoaringBitmap> cpScanOps = Maps.newHashMap();
    private final Map<OptExpression, RoaringBitmap> scanOps = Maps.newHashMap();
    private final Map<OptExpression, Pair<OptExpression, Integer>> cpFrontiers = Maps.newHashMap();
    private final Set<CPEdge> cpEdges = Sets.newHashSet();
    private final Map<OptExpression, CPNode> optToGraphNode = Maps.newHashMap();
    private final Set<CPNode> hubNodes = Sets.newHashSet();
    private final Map<Integer, ColumnRefSet> columnOrigins = Maps.newHashMap();
    private final Map<OptExpression, Set<ColumnRefOperator>> uniqueKeyColRefs = Maps.newHashMap();
    private final Map<ColumnRefOperator, Map<OptExpression, ColumnRefOperator>> foreignKeyColRefs = Maps.newHashMap();
    private final Map<OptExpression, Integer> scanNodeOrdinals = Maps.newHashMap();
    private final List<OptExpression> ordinalToScanNodes = Lists.newArrayList();

    public CPJoinGardener(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
    }

    void computeOriginsOfColumnRefs(OptExpression optExpression) {
        Map<ColumnRefOperator, ScalarOperator> colRefMap = Collections.emptyMap();
        if (optExpression.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator projectOp = optExpression.getOp().cast();
            colRefMap = projectOp.getColumnRefMap();
        } else if (optExpression.getOp().getProjection() != null) {
            colRefMap = optExpression.getOp().getProjection().getColumnRefMap();
        }

        colRefMap.forEach((col, scalarOp) -> {
            if (!columnOrigins.containsKey(col.getId())) {
                ColumnRefSet columnRefSet = new ColumnRefSet();
                scalarOp.getUsedColumns().getStream()
                        .forEach(id -> columnRefSet.union(columnOrigins.getOrDefault(id, new ColumnRefSet())));
                columnOrigins.put(col.getId(), columnRefSet);
            }
        });
    }

    public List<CPBiRel> getCardinalityPreserving(OptExpression optExpression,
                                                  Set<OptExpression> candidateLhsScanOpSet,
                                                  Set<OptExpression> candidateRhsScanOpSet) {
        OptExpression lhs = optExpression.inputAt(0);
        OptExpression rhs = optExpression.inputAt(1);
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        List<CPBiRel> biRels = Lists.newArrayList();
        if ((joinOp.getJoinType().isInnerJoin() || joinOp.getJoinType().isLeftOuterJoin()) &&
                scanOps.containsKey(lhs) && cpScanOps.containsKey(rhs)) {
            List<CPBiRel> leftToRightBiRels =
                    Utils.getIntStream(scanOps.get(lhs)).map(ordinalToScanNodes::get)
                            .filter(candidateLhsScanOpSet::contains)
                            .flatMap(lhsScan -> Utils.getIntStream(cpScanOps.get(rhs)).map(
                                            ordinalToScanNodes::get)
                                    .filter(candidateRhsScanOpSet::contains)
                                    .flatMap(rhsScan -> CPBiRel.extractCPBiRels(lhsScan, rhsScan, true).stream()
                                    )).collect(Collectors.toList());
            biRels.addAll(leftToRightBiRels);
        }
        if ((joinOp.getJoinType().isInnerJoin() || joinOp.getJoinType().isRightOuterJoin()) &&
                scanOps.containsKey(rhs) && cpScanOps.containsKey(lhs)) {
            List<CPBiRel> rightToLeftBiRels =
                    Utils.getIntStream(scanOps.get(rhs)).map(ordinalToScanNodes::get)
                            .filter(candidateRhsScanOpSet::contains)
                            .flatMap(rhsScan -> Utils.getIntStream(cpScanOps.get(lhs)).map(
                                            ordinalToScanNodes::get)
                                    .filter(candidateLhsScanOpSet::contains)
                                    .flatMap(lhsScan -> CPBiRel.extractCPBiRels(rhsScan, lhsScan, false).stream()
                                    )).collect(Collectors.toList());
            biRels.addAll(rightToLeftBiRels);
        }
        if (joinOp.getJoinType().isInnerJoin()) {
            biRels = biRels.stream()
                    .filter(biRel -> biRel.getPairs().stream()
                            .allMatch(p -> !p.first.isNullable() && !p.second.isNullable()))
                    .collect(Collectors.toList());
        }
        return biRels;
    }

    private Pair<Set<OptExpression>, Set<OptExpression>> getCandidateScanOpSet(
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> eqColumnRefPairs) {
        Set<Pair<Integer, Integer>> joinColumnEqClasses =
                eqColumnRefPairs.stream().map(biCol ->
                        Pair.create(
                                columnRefEquivClasses.getGroupIdOrAdd(biCol.first),
                                columnRefEquivClasses.getGroupIdOrAdd(biCol.second))
                ).filter(p -> !Objects.equals(p.first, p.second)).collect(Collectors.toSet());

        Set<OptExpression> candidateLhsScanOpSet =
                joinColumnEqClasses.stream()
                        .flatMap(p ->
                                columnRefEquivClasses.getGroup(p.first).stream().map(columnToScans::get))
                        .collect(Collectors.toSet());

        Set<OptExpression> candidateRhsScanOpSet =
                joinColumnEqClasses.stream()
                        .flatMap(p ->
                                columnRefEquivClasses.getGroup(p.second).stream().map(columnToScans::get))
                        .collect(Collectors.toSet());
        return Pair.create(candidateLhsScanOpSet, candidateRhsScanOpSet);
    }

    private void synthesizeCPScanOpsOfJoin(OptExpression joinOpt, CPBiRel biRel,
                                           Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs) {
        OptExpression lhsOpt = joinOpt.inputAt(0);
        OptExpression rhsOpt = joinOpt.inputAt(1);
        RoaringBitmap lhsCardPreservingScanOps = cpScanOps.get(lhsOpt);
        RoaringBitmap rhsCardPreservingScanOps = cpScanOps.get(rhsOpt);

        RoaringBitmap lhsScanOps = scanOps.get(lhsOpt);
        RoaringBitmap rhsScanOps = scanOps.get(rhsOpt);

        scanOps.put(joinOpt, RoaringBitmap.or(lhsScanOps, rhsScanOps));
        LogicalJoinOperator joinOp = joinOpt.getOp().cast();
        JoinOperator joinType = joinOp.getJoinType();
        Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs = biRel.getPairs();
        RoaringBitmap currCPScanOps;
        if (!biRel.isFromForeignKey() && joinType.isInnerJoin()) {
            cpEdges.add(new CPEdge(biRel.getLhs(), biRel.getRhs(), false, eqColumnRefs));
            currCPScanOps = RoaringBitmap.or(lhsCardPreservingScanOps, rhsCardPreservingScanOps);
            uniqueKeyColRefs.computeIfAbsent(biRel.getLhs(), k -> Sets.newHashSet())
                    .addAll(pairs.stream().map(p -> p.first).collect(Collectors.toList()));
            uniqueKeyColRefs.computeIfAbsent(biRel.getRhs(), k -> Sets.newHashSet())
                    .addAll(pairs.stream().map(p -> p.second).collect(Collectors.toList()));
            pairs.forEach(p -> {
                foreignKeyColRefs.computeIfAbsent(p.first, k -> Maps.newHashMap()).put(biRel.getRhs(), p.second);
                foreignKeyColRefs.computeIfAbsent(p.second, k -> Maps.newHashMap()).put(biRel.getLhs(), p.first);
            });
        } else {
            CPEdge edge = new CPEdge(biRel.getLhs(), biRel.getRhs(), true, eqColumnRefs);
            currCPScanOps = biRel.isLeftToRight() ? lhsCardPreservingScanOps : rhsCardPreservingScanOps;
            uniqueKeyColRefs.computeIfAbsent(biRel.getRhs(), k -> Sets.newHashSet())
                    .addAll(pairs.stream().map(p -> p.second).collect(Collectors.toList()));
            pairs.forEach(p -> foreignKeyColRefs.computeIfAbsent(p.second, k -> Maps.newHashMap())
                    .put(biRel.getLhs(), p.first));
            cpEdges.add(edge);
        }
        cpScanOps.merge(joinOpt, currCPScanOps, (a, b) -> RoaringBitmap.or(a, b));
    }

    @Override
    public Boolean visitLogicalJoin(OptExpression optExpression, Void context) {
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        JoinOperator joinType = joinOp.getJoinType();
        if (!joinType.isInnerJoin() && !joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin()) {
            return visit(optExpression, context);
        }
        if (joinOp.getPredicate() != null) {
            return visit(optExpression, context);
        }

        Set<Pair<ColumnRefOperator, ColumnRefOperator>> eqColumnRefPairs = Utils.getJoinEqualColRefPairs(optExpression);
        Pair<Set<OptExpression>, Set<OptExpression>> candidateScanOpSets = getCandidateScanOpSet(eqColumnRefPairs);
        Set<OptExpression> lhsCandidateScanOpSet = candidateScanOpSets.first;
        Set<OptExpression> rhsCandidateScanOpSet = candidateScanOpSets.second;

        List<CPBiRel> biRels =
                getCardinalityPreserving(optExpression, lhsCandidateScanOpSet, rhsCandidateScanOpSet);

        for (CPBiRel biRel : biRels) {
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs = biRel.getPairs();
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> targetPairs = pairs;
            if (!biRel.isLeftToRight()) {
                targetPairs = targetPairs.stream().map(Pair::inverse).collect(Collectors.toSet());
            }
            if (!eqColRefPairsMatch(columnRefEquivClasses, eqColumnRefPairs, targetPairs)) {
                continue;
            }
            LogicalScanOperator lhsScanOp = biRel.getLhs().getOp().cast();
            LogicalScanOperator rhsScanOp = biRel.getRhs().getOp().cast();
            Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs = Maps.newHashMap();
            // two ScanOperators access the same tables, which are joined together on PK/UK
            if (!biRel.isFromForeignKey() && joinType.isInnerJoin()) {
                eqColumnRefs = Utils.makeEqColumRefMapFromSameTables(lhsScanOp, rhsScanOp);
            } else if (joinType.isInnerJoin()) {
                eqColumnRefs = pairs.stream().collect(Collectors.toMap(p -> p.first, p -> p.second));
            }
            eqColumnRefs.forEach(columnRefEquivClasses::union);
            synthesizeCPScanOpsOfJoin(optExpression, biRel, eqColumnRefs);
        }
        if (!joinOp.hasLimit() && cpScanOps.containsKey(optExpression)) {
            return true;
        } else {
            return visit(optExpression, context);
        }
    }

    @Override
    public Boolean visit(OptExpression optExpression, Void context) {
        for (int i = 0; i < optExpression.getInputs().size(); ++i) {
            OptExpression child = optExpression.inputAt(i);
            if (cpScanOps.containsKey(child)) {
                cpFrontiers.put(child, Pair.create(optExpression, i));
            }
        }
        scanOps.put(optExpression, RoaringBitmap.bitmapOf());
        cpScanOps.put(optExpression, RoaringBitmap.bitmapOf());
        computeOriginsOfColumnRefs(optExpression);
        return false;
    }

    @Override
    public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
        LogicalScanOperator scanOp = optExpression.getOp().cast();
        if (scanOp.hasLimit()) {
            return visit(optExpression, context);
        }
        //TODO(by satanson): non-OlapTable will be supported in future
        if (!(scanOp.getTable() instanceof OlapTable)) {
            return visit(optExpression, context);
        }
        OlapTable table = ((OlapTable) scanOp.getTable());
        // A Table that has no PK/UK/FK can not associate with other tables via
        // cardinality-preserving relations.
        if (!table.hasUniqueConstraints() && !table.hasForeignKeyConstraints()) {
            return visit(optExpression, context);
        }

        scanOp.getColRefToColumnMetaMap().keySet().forEach(col -> {
            columnOrigins.put(col.getId(), new ColumnRefSet(col.getId()));
            columnToScans.put(col, optExpression);
        });

        computeOriginsOfColumnRefs(optExpression);
        int ordinal = scanNodeOrdinals.getOrDefault(optExpression, -1);
        Preconditions.checkArgument(ordinal >= 0);
        RoaringBitmap scanSet = RoaringBitmap.bitmapOf(ordinal);
        scanOps.put(optExpression, scanSet);
        cpScanOps.put(optExpression, scanSet);
        return true;
    }

    Boolean propagateBottomUp(OptExpression optExpression, int fromChildIdx, Void context) {
        if (optExpression.getOp().hasLimit()) {
            return visit(optExpression, context);
        }

        OptExpression child = optExpression.inputAt(fromChildIdx);
        scanOps.put(optExpression, scanOps.get(child));
        cpScanOps.put(optExpression, cpScanOps.get(child));
        computeOriginsOfColumnRefs(optExpression);
        return true;
    }

    @Override
    public Boolean visitLogicalCTEConsume(OptExpression optExpression, Void context) {
        // In some cases(TPC-DS q45), we have high certainty that a query can benefit from CTE optimization;
        // then LogicalCTEConsumeOperators in its plan has no children to avoid CTE inlining or other
        // respective optimization.
        if (optExpression.getInputs().isEmpty()) {
            return false;
        }
        return propagateBottomUp(optExpression, 0, context);
    }

    @Override
    public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
        return propagateBottomUp(optExpression, 0, context);
    }

    @Override
    public Boolean visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
        return propagateBottomUp(optExpression, 1, context);
    }

    boolean process(OptExpression root) {
        // Assign ordinal in ascending order in pre-order travel, so the larger the ordinal,
        // the higher the Scan operator appears in plan, we prefer to prune the higher scan
        // Operator if there are many Scan operator access the same tables and join on PK to
        // prevent the operator who sits amid the path from the common ancestor of the
        // pruned and the retained Scan operator to the pruned Scan operator from missing the
        // referenced ColumnRef originates from the pruned Scan operator.
        for (OptExpression child : root.getInputs()) {
            if (child.getOp() instanceof LogicalOlapScanOperator) {
                int ordinal = scanNodeOrdinals.size();
                scanNodeOrdinals.put(child, ordinal);
                ordinalToScanNodes.add(child);
            }
        }

        Map<OptExpression, Pair<OptExpression, Integer>> candidateFrontiers = Maps.newHashMap();
        for (int i = 0; i < root.getInputs().size(); ++i) {
            OptExpression child = root.inputAt(i);
            if (process(child)) {
                candidateFrontiers.put(child, Pair.create(root, i));
            }
        }
        boolean walkUpwards = candidateFrontiers.size() == root.getInputs().size();
        if (!walkUpwards) {
            cpFrontiers.putAll(candidateFrontiers);
            return false;
        }
        return root.getOp().accept(this, root, null);
    }

    // unilateral edge and its inverse counterpart are merged into bilateral edge
    // drop unilateral edge if its bilateral edge already exists
    private Set<CPEdge> mergeCPEdges(Set<CPEdge> cpEdges) {
        Set<CPEdge> mergedEdges = Sets.newHashSet();
        for (CPEdge edge : cpEdges) {
            if (mergedEdges.contains(edge)) {
                continue;
            }

            if (!edge.isUnilateral()) {
                if (!mergedEdges.contains(edge.inverse())) {
                    mergedEdges.add(edge);
                }
            } else if (!mergedEdges.contains(edge.toBiLateral()) &&
                    !mergedEdges.contains(edge.toBiLateral().inverse())) {
                if (mergedEdges.contains(edge.inverse())) {
                    mergedEdges.remove(edge.inverse());
                    mergedEdges.add(edge.toBiLateral());
                } else {
                    mergedEdges.add(edge);
                }
            }
        }
        return mergedEdges;
    }

    // create a CPNode for each Scan Operator, a Scan Operator may be referenced as
    // source node or sink node many times by multiple edges, but only one CPNode is
    // created.
    void prepareCPNode(Set<CPEdge> edges) {
        for (CPEdge e : edges) {
            OptExpression lhs = e.getLhs();
            OptExpression rhs = e.getRhs();
            CPNode lhsNode = optToGraphNode.getOrDefault(lhs, null);
            CPNode rhsNode = optToGraphNode.getOrDefault(rhs, null);
            if (lhsNode == null) {
                lhsNode = CPNode.createNode(lhs);
                optToGraphNode.put(lhs, lhsNode);
            }
            if (rhsNode == null) {
                rhsNode = CPNode.createNode(rhs);
                optToGraphNode.put(rhs, rhsNode);
            }
            if (e.isUnilateral()) {
                rhsNode.addEqColumnRefs(lhs, e.getInverseEqColumnRefs());
            } else {
                rhsNode.addEqColumnRefs(lhs, e.getInverseEqColumnRefs());
                lhsNode.addEqColumnRefs(rhs, e.getEqColumnRefs());
            }
        }
    }

    // Add bilateral edges into cardinality-preserving tree (CPTree)
    // create hub CPNode for bilateral edges, if two bilateral edges can be tied together,
    // the corresponding hub CPNodes must be merged one. Each pair of these Scan Operators
    // coming from tied bilateral edges forms cardinality-preserving relation.
    void processBilateralEdges(List<CPEdge> bilateralEdges) {
        for (CPEdge e : bilateralEdges) {
            CPNode lhsNode = optToGraphNode.get(e.getLhs());
            CPNode rhsNode = optToGraphNode.get(e.getRhs());
            if (lhsNode.getParent() != null && rhsNode.getParent() != null) {
                CPNode.mergeHubNode(lhsNode.getParent(), rhsNode.getParent()).ifPresent(hubNodes::remove);
            } else if (lhsNode.getParent() != null) {
                lhsNode.getParent().addChild(rhsNode);
            } else if (rhsNode.getParent() != null) {
                rhsNode.getParent().addChild(lhsNode);
            } else {
                hubNodes.add(CPNode.createHubNode(lhsNode, rhsNode));
            }
        }
    }

    // Add unilateral edges into cardinality-preserving tree (CPTree)
    void processUnilateralEdges(List<CPEdge> unilateralEdges, Set<CPEdge> edges) {
        for (CPEdge e : unilateralEdges) {
            CPNode lhsNode = optToGraphNode.get(e.getLhs());
            CPNode rhsNode = optToGraphNode.get(e.getRhs());
            if (!rhsNode.isRoot() && rhsNode.getParent().isHub()) {
                rhsNode = rhsNode.getParent();
            }
            if (rhsNode.getParent() != null) {
                CPNode parent = rhsNode.getParent();
                if (parent.isHub()) {
                    continue;
                }
                // choose the best parent
                // if there exist more than one CP chains that ends with rhsNode, for an example(TPCH):
                // cp-chain#0: lineitem->partsupp->part
                // cp-chain#1: lineitem->part
                // we choose cp-chain#1, because if part is unprunable, for cp-chain#0, there are 2 joins, but
                // for cp-chain#1, then are 1 join in the plan.
                //
                // TODO(by satanson): at present, this scenario only consider that there is an edge bridging
                //  old parent and new parent; in future, we should process more than one edges bridging old
                //  parent and and new parent, for an example:
                //  cp-chain#0: A->B->C->D->E
                //  cp-chain#1: A->E
                CPEdge edge = new CPEdge(lhsNode.value, parent.value, true, Collections.emptyMap());
                if (edges.contains(edge)) {
                    parent.getChildren().remove(rhsNode);
                    lhsNode.addChild(rhsNode);
                }
                continue;
            }
            // install rhs to NonCP-children of the Hub
            if (!lhsNode.isRoot() && lhsNode.getParent().isHub() &&
                    lhsNode.getParent().getChildren().contains(lhsNode)) {
                lhsNode.getParent().addNonCPChild(rhsNode);
            } else {
                lhsNode.addChild(rhsNode);
            }
        }
    }

    private void plantCPTree() {
        Set<CPEdge> edges = mergeCPEdges(cpEdges);
        prepareCPNode(edges);
        // split edges into bilateral edges and unilateral edges
        List<CPEdge> unilateralEdges = Lists.newArrayList();
        List<CPEdge> bilateralEdges = Lists.newArrayList();
        for (CPEdge e : edges) {
            if (e.isUnilateral()) {
                unilateralEdges.add(e);
            } else {
                bilateralEdges.add(e);
            }
        }
        processBilateralEdges(bilateralEdges);
        processUnilateralEdges(unilateralEdges, edges);
    }

    public static boolean eqColRefPairsMatch(UnionFind<ColumnRefOperator> colRefEquivClasses,
                                             Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairsA,
                                             Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairsB) {
        if (pairsA.isEmpty() || pairsB.isEmpty()) {
            return false;
        }
        Set<Pair<Integer, Integer>> eqClassPairsA =
                pairsA.stream().map(biCol ->
                        Pair.create(colRefEquivClasses.getGroupIdOrAdd(biCol.first),
                                colRefEquivClasses.getGroupIdOrAdd(biCol.second))
                ).collect(Collectors.toSet());

        Set<Pair<Optional<Integer>, Optional<Integer>>> optEqClassPairsB =
                pairsB.stream().map(biCol ->
                        Pair.create(colRefEquivClasses.getGroupId(biCol.first),
                                colRefEquivClasses.getGroupId(biCol.second))
                ).collect(Collectors.toSet());

        if (!optEqClassPairsB.stream().allMatch(p -> p.first.isPresent() && p.second.isPresent())) {
            return false;
        }
        Set<Pair<Integer, Integer>> eqClassPairsB =
                optEqClassPairsB.stream().map(p -> Pair.create(p.first.get(), p.second.get()))
                        .collect(Collectors.toSet());
        Set<Pair<Integer, Integer>> symDiff = Sets.symmetricDifference(eqClassPairsA, eqClassPairsB);
        return symDiff.isEmpty() || symDiff.stream().allMatch(p -> Objects.equals(p.first, p.second));
    }

    public void collect(OptExpression root) {
        process(root);
        plantCPTree();
    }

    public PruneContext markPrunedTables(OptExpression frontier) {
        Set<OptExpression> scanOpsLeadingByFrontier =
                Utils.getIntStream(scanOps.get(frontier)).map(ordinalToScanNodes::get)
                        .collect(Collectors.toSet());
        PruneContext pruneContext = new PruneContext();
        if (scanOpsLeadingByFrontier.isEmpty()) {
            return pruneContext;
        }
        ColumnRefSet originalColRefSet = new ColumnRefSet();
        frontier.getRowOutputInfo().getOutputColRefs().forEach(colRef ->
                originalColRefSet.union(columnOrigins.get(colRef.getId())));

        List<CPNode> nodes = Stream.concat(optToGraphNode.values().stream(), hubNodes.stream())
                .filter(node -> node.isRoot() && node.intersect(scanOpsLeadingByFrontier))
                .collect(Collectors.toList());
        for (CPNode node : nodes) {
            pruneContext.merge(markPrunedTable(node, originalColRefSet));
        }
        return pruneContext;
    }

    private static class PruneContext {
        Map<ColumnRefOperator, Set<ColumnRefOperator>> rewriteMapping = Maps.newHashMap();
        boolean pruned = true;

        Set<ColumnRefOperator> unprunedPkColRefs = Sets.newHashSet();
        Set<OptExpression> prunedTables = Sets.newHashSet();

        private PruneContext(Map<ColumnRefOperator, Set<ColumnRefOperator>> rewriteMapping, boolean pruned) {
            this.rewriteMapping = rewriteMapping;
            this.pruned = pruned;
        }

        public PruneContext() {
        }

        public PruneContext pruned(Map<ColumnRefOperator, Set<ColumnRefOperator>> rewriteMapping) {
            return new PruneContext(rewriteMapping, true);
        }

        void merge(PruneContext other) {
            this.pruned &= other.pruned;
            other.rewriteMapping.forEach((k, v) -> this.rewriteMapping.merge(k, v, Sets::union));
            this.unprunedPkColRefs.addAll(other.unprunedPkColRefs);
            this.prunedTables.addAll(other.prunedTables);
        }

        public boolean isPruned() {
            return pruned;
        }

        public Map<ColumnRefOperator, Set<ColumnRefOperator>> getRewriteMapping() {
            return rewriteMapping;
        }

        public PruneContext toUnpruned(Collection<ColumnRefOperator> outputColRefs,
                                       Collection<ColumnRefOperator> pkColRefs) {
            outputColRefs.forEach(colRef -> this.rewriteMapping.putIfAbsent(colRef, Sets.newHashSet(colRef)));
            this.unprunedPkColRefs.addAll(pkColRefs);
            this.pruned = false;
            return this;
        }

        public void addPrunedTable(OptExpression scanOp) {
            prunedTables.add(scanOp);
        }

        public Set<OptExpression> getPrunedTables() {
            return prunedTables;
        }

        Set<ColumnRefOperator> getOutputColRefs(OptExpression node, ColumnRefSet requiredColRefSet,
                                                Set<ColumnRefOperator> extraPkColRefs) {
            LogicalScanOperator scanOperator = node.getOp().cast();
            ColumnRefSet usedColRefSet = Optional.ofNullable(scanOperator.getPredicate())
                    .map(ScalarOperator::getUsedColumns)
                    .orElse(new ColumnRefSet());
            usedColRefSet.union(requiredColRefSet);
            usedColRefSet.union(rewriteMapping.keySet());
            if (!isPruned()) {
                usedColRefSet.union(extraPkColRefs);
            }
            Collection<ColumnRefOperator> allColRefs = scanOperator.getColumnMetaToColRefMap().values();
            return allColRefs.stream().filter(usedColRefSet::contains).collect(Collectors.toSet());
        }

        void rewrite(Set<ColumnRefOperator> outputColRefs, Map<ColumnRefOperator, ColumnRefOperator> eqColRefs) {
            for (ColumnRefOperator colRef : outputColRefs) {
                ColumnRefOperator substColRef = eqColRefs.get(colRef);

                Set<ColumnRefOperator> originalColRefs = rewriteMapping.remove(colRef);
                if (originalColRefs == null) {
                    originalColRefs = Sets.newHashSet();
                }
                originalColRefs.add(colRef);
                rewriteMapping.merge(substColRef, originalColRefs, (s0, s1) -> new HashSet<>(Sets.union(s0, s1)));
            }
        }
    }

    private PruneContext markPrunedTableOfHubCPNode(CPNode root, ColumnRefSet originalColRefSet) {
        PruneContext pruneContext = new PruneContext();
        // try to prune non-cardinality-preserving children
        for (CPNode child : root.getNonCPChildren()) {
            pruneContext.merge(markPrunedTable(child, originalColRefSet));
        }

        ColumnRefSet requiredColRefSet = new ColumnRefSet();
        requiredColRefSet.union(originalColRefSet);
        Set<OptExpression> childSet =
                root.getChildren().stream().map(child -> child.value).collect(Collectors.toSet());
        for (ColumnRefOperator colRef : pruneContext.unprunedPkColRefs) {
            Map<OptExpression, ColumnRefOperator> fkColRefMap =
                    foreignKeyColRefs.getOrDefault(colRef, Collections.emptyMap());
            List<ColumnRefOperator> fkColRefs =
                    fkColRefMap.entrySet().stream().filter(e -> childSet.contains(e.getKey()))
                            .map(Map.Entry::getValue).collect(
                                    Collectors.toList());
            requiredColRefSet.union(fkColRefs);
        }
        // try to prune cardinality-preserving children
        // Sort the CPNode by their ordinals in ascend order, the larger the ordinal,
        // the lower the LogicalScanOperator.
        List<CPNode> children = root.getChildren().stream()
                .map(child -> Pair.create(child, scanNodeOrdinals.get(child.getValue())))
                .sorted(Pair.comparingBySecond()).map(p -> p.first).collect(Collectors.toList());

        Map<Long, List<CPNode>> tableIdToChildGroups = Maps.newHashMap();
        for (CPNode child : children) {
            LogicalOlapScanOperator scanOp = child.getValue().getOp().cast();
            Long tableId = scanOp.getTable().getId();
            tableIdToChildGroups.computeIfAbsent(tableId, k -> Lists.newArrayList()).add(child);
        }
        // we prefer to prune higher LogicalScanOperator and retain lower one, since pruning lower one, because
        // the some Operator in mid of the path from the common ancestor to the lower one may use the ColumnRef
        // yielded by the lower one, if the lower one was pruned, the in-mid Operator will missing its dependent
        // ColumnRef. so we sort CP groups according their least ordinals(the last CPNode in the Group has the
        // least ordinal).
        List<List<CPNode>> childGroups = tableIdToChildGroups.values().stream().map(cpNodes -> Pair.create(cpNodes,
                        scanNodeOrdinals.get(cpNodes.get(cpNodes.size() - 1).getValue())))
                .sorted(Pair.comparingBySecond()).map(p -> p.first).collect(Collectors.toList());

        for (int g = 0; g < childGroups.size(); ++g) {
            List<CPNode> group = childGroups.get(g);
            Preconditions.checkArgument(!group.isEmpty());
            CPNode lastNode = group.get(group.size() - 1);
            for (int i = 0; i < group.size() - 1; ++i) {
                CPNode node = group.get(i);
                OptExpression optExpression = node.getValue();
                Set<ColumnRefOperator> pkColRefs =
                        uniqueKeyColRefs.getOrDefault(optExpression, Collections.emptySet());
                Set<ColumnRefOperator> outputColRefs =
                        pruneContext.getOutputColRefs(node.getValue(), requiredColRefSet, pkColRefs);
                pruneContext.addPrunedTable(node.getValue());
                Preconditions.checkArgument(node.getEquivColumnRefs().containsKey(lastNode.getValue()));
                Map<ColumnRefOperator, ColumnRefOperator> eqColRefs =
                        node.getEquivColumnRefs().get(lastNode.getValue());
                Preconditions.checkArgument(eqColRefs.keySet().containsAll(outputColRefs));
                pruneContext.rewrite(outputColRefs, eqColRefs);
            }

            Set<ColumnRefOperator> pkColRefs = uniqueKeyColRefs.get(lastNode.getValue());
            Set<ColumnRefOperator> outputColRefs =
                    pruneContext.getOutputColRefs(lastNode.getValue(), requiredColRefSet, pkColRefs);
            if (!pkColRefs.containsAll(outputColRefs)) {
                pruneContext.toUnpruned(outputColRefs, pkColRefs);
            } else {
                Optional<Map<ColumnRefOperator, ColumnRefOperator>> optEqColRefs =
                        lastNode.getEquivColumnRefs().entrySet().stream()
                                .filter(e -> !pruneContext.getPrunedTables().contains(e.getKey()))
                                .findFirst().map(Map.Entry::getValue);
                if (optEqColRefs.isPresent() && optEqColRefs.get().keySet().containsAll(outputColRefs)) {
                    pruneContext.rewrite(outputColRefs, optEqColRefs.get());
                    pruneContext.addPrunedTable(lastNode.getValue());
                } else {
                    pruneContext.toUnpruned(outputColRefs, pkColRefs);
                }
            }
        }
        return pruneContext;
    }

    private PruneContext markPrunedTableOfNonHubNode(CPNode root, ColumnRefSet originalColRefSet) {
        // visit children of current GraphNode in post-order order.
        PruneContext pruneContext = new PruneContext();
        for (CPNode child : root.getChildren()) {
            pruneContext.merge(markPrunedTable(child, originalColRefSet));
        }

        Set<ColumnRefOperator> pkColRefs = uniqueKeyColRefs.getOrDefault(root.getValue(), Collections.emptySet());

        Set<ColumnRefOperator> outputColRefs =
                pruneContext.getOutputColRefs(root.getValue(), originalColRefSet, pkColRefs);

        if (!pruneContext.isPruned() || root.isRoot()) {
            return pruneContext.toUnpruned(outputColRefs, pkColRefs);
        }

        CPNode parent = root.getParent();
        Map<ColumnRefOperator, ColumnRefOperator> eqColRefMap;
        if (!parent.isHub()) {
            eqColRefMap = root.getEquivColumnRefs().getOrDefault(parent.getValue(), Collections.emptyMap());
        } else {
            eqColRefMap = root.getEquivColumnRefs().values().stream().findFirst().orElse(Collections.emptyMap());
        }
        // eqColRefMap.isEmpty() = true means left join, if rhs of left join has predicates,
        // then it is can not pruned.
        if (eqColRefMap.isEmpty() && root.getValue().getOp().getPredicate() != null) {
            return pruneContext.toUnpruned(outputColRefs, pkColRefs);
        }
        // If there exists any output column that can not substituted by its equivalent
        // column on its parent, then current CPNode can not be pruned.
        if (!eqColRefMap.keySet().containsAll(outputColRefs)) {
            return pruneContext.toUnpruned(outputColRefs, pkColRefs);
        } else {
            pruneContext.rewrite(outputColRefs, eqColRefMap);
            pruneContext.addPrunedTable(root.getValue());
        }
        return pruneContext;
    }

    private PruneContext markPrunedTable(CPNode root, ColumnRefSet originalColRefSet) {
        if (root.isHub()) {
            return markPrunedTableOfHubCPNode(root, originalColRefSet);
        } else {
            return markPrunedTableOfNonHubNode(root, originalColRefSet);
        }
    }

    public static class Pruner extends OptExpressionVisitor<Optional<OptExpression>, Void> {
        private final Set<ColumnRefOperator> substColRefs;
        private final ReplaceColumnRefRewriter columnRefRewriter;
        private final Set<OptExpression> prunedTables;

        private final List<ScalarOperator> prunedPredicates = Lists.newArrayList();
        private final Map<ColumnRefOperator, ScalarOperator> prunedColRefMap = Maps.newHashMap();

        public Pruner(Map<ColumnRefOperator, ScalarOperator> remapping, Set<OptExpression> prunedTables,
                      Set<ColumnRefOperator> substColRefs) {
            this.substColRefs = substColRefs;
            this.columnRefRewriter = new ReplaceColumnRefRewriter(remapping);
            this.prunedTables = prunedTables;
        }

        void gatherPrunedPredicatesAndColumnRefMap(OptExpression optExpression) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = null;
            if (optExpression.getOp() instanceof LogicalProjectOperator) {
                LogicalProjectOperator projectOperator = optExpression.getOp().cast();
                columnRefMap = projectOperator.getColumnRefMap();
            } else if (optExpression.getOp().getProjection() != null) {
                columnRefMap = optExpression.getOp().getProjection().getColumnRefMap();
            }
            if (predicate != null) {
                prunedPredicates.add(columnRefRewriter.rewrite(predicate));
            }
            if (columnRefMap != null && !columnRefMap.isEmpty()) {
                // the same ColumnRefOperator can appear in a Path, Pruner rewrite these ColumnRefOperator
                // in bottom-up, so if the ScalarOperator corresponding to the ColumnRefOperator in offspring
                // Operator has been already rewritten, the ScalarOperators(trivial ColumnRefOperator) in ancestor
                // Operator should overwrite it.
                columnRefMap.forEach(
                        (k, v) -> prunedColRefMap.putIfAbsent((ColumnRefOperator) columnRefRewriter.rewrite(k),
                                columnRefRewriter.rewrite(v)));
            }
        }

        @Override
        public Optional<OptExpression> visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOp = optExpression.getOp().cast();
            if (prunedTables.contains(optExpression)) {
                gatherPrunedPredicatesAndColumnRefMap(optExpression);
                return Optional.empty();
            } else {
                ImmutableMap.Builder<ColumnRefOperator, Column>
                        newColRefToColumnMetaBuilder = new ImmutableMap.Builder<>();
                newColRefToColumnMetaBuilder.putAll(scanOp.getColRefToColumnMetaMap());
                scanOp.getColumnMetaToColRefMap().entrySet()
                        .stream().filter(e -> substColRefs.contains(e.getValue()) &&
                                !scanOp.getColRefToColumnMetaMap().containsKey(e.getValue()))
                        .forEach(e -> newColRefToColumnMetaBuilder.put(e.getValue(), e.getKey()));
                Operator newScanOp = ((LogicalScanOperator.Builder) OperatorBuilderFactory.build(scanOp))
                        .withOperator(scanOp)
                        .setColRefToColumnMetaMap(newColRefToColumnMetaBuilder.build())
                        .build();
                return Optional.of(OptExpression.create(newScanOp, Collections.emptyList()));
            }
        }

        @Override
        public Optional<OptExpression> visit(OptExpression optExpression, Void context) {
            return Optional.of(optExpression);
        }

        @Override
        public Optional<OptExpression> visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(optExpression);
            }
        }

        @Override
        public Optional<OptExpression> visitLogicalProject(OptExpression optExpression, Void context) {
            LogicalProjectOperator projectOp = optExpression.getOp().cast();
            if (optExpression.getInputs().isEmpty()) {
                gatherPrunedPredicatesAndColumnRefMap(optExpression);
                return Optional.empty();
            }
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
            projectOp.getColumnRefMap().forEach((k, v) ->
                    newColumnRefMap.put(
                            columnRefRewriter.rewrite(k).cast(),
                            columnRefRewriter.rewrite(v)));
            LogicalProjectOperator newProjectOp = new LogicalProjectOperator.Builder()
                    .withOperator(projectOp)
                    .setColumnRefMap(newColumnRefMap)
                    .build();
            Preconditions.checkArgument(newProjectOp.getColumnRefMap() != null);
            return Optional.of(OptExpression.create(newProjectOp, optExpression.getInputs()));
        }

        @Override
        public Optional<OptExpression> visitLogicalJoin(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().isEmpty()) {
                gatherPrunedPredicatesAndColumnRefMap(optExpression);
                return Optional.empty();
            }
            ScalarOperator predicate = columnRefRewriter.rewrite(optExpression.getOp().getPredicate());
            if (optExpression.getInputs().size() == 1) {
                OptExpression child = optExpression.inputAt(0);
                predicate = Utils.compoundAnd(child.getOp().getPredicate(), predicate);
                Operator newChild = OperatorBuilderFactory.build(child.getOp())
                        .withOperator(child.getOp()).setPredicate(predicate).build();
                return Optional.of(OptExpression.create(newChild, child.getInputs()));
            } else {
                LogicalJoinOperator joinOperator = optExpression.getOp().cast();
                LogicalJoinOperator newJoinOperator = LogicalJoinOperator.builder().withOperator(joinOperator)
                        .setPredicate(predicate)
                        .setOnPredicate(columnRefRewriter.rewrite(joinOperator.getOnPredicate()))
                        .build();
                return Optional.of(OptExpression.create(newJoinOperator, optExpression.getInputs()));
            }
        }

        public Optional<OptExpression> prune(OptExpression optExpression) {
            List<OptExpression> newInputs = optExpression.getInputs().stream().map(this::prune)
                    .filter(Optional::isPresent).map(Optional::get)
                    .collect(Collectors.toList());
            optExpression.getInputs().clear();
            optExpression.getInputs().addAll(newInputs);
            return optExpression.getOp().accept(this, optExpression, null);
        }
    }

    public static class Grafter extends OptExpressionVisitor<Optional<OptExpression>, Void> {
        private List<ScalarOperator> extraPredicates;
        private Map<ColumnRefOperator, ScalarOperator> extraColRefMap;

        public Grafter(List<ScalarOperator> predicates, Map<ColumnRefOperator, ScalarOperator> colRefMap) {
            this.extraPredicates = predicates;
            this.extraColRefMap = colRefMap;
        }

        @Override
        public Optional<OptExpression> visitLogicalProject(OptExpression optExpression, Void context) {
            LogicalProjectOperator projectOperator = optExpression.getOp().cast();

            Map<ColumnRefOperator, ScalarOperator> colRefMap = projectOperator.getColumnRefMap();
            List<ColumnRefOperator> inputColRefs = optExpression.inputAt(0).getRowOutputInfo().getOutputColRefs();
            if (colRefMap.keySet().containsAll(inputColRefs)) {
                return Optional.empty();
            }
            Map<ColumnRefOperator, ScalarOperator> newColRefMap = Maps.newHashMap(colRefMap);
            inputColRefs.forEach(k -> newColRefMap.put(k, k));
            LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(newColRefMap);
            return Optional.of(OptExpression.create(newProjectOperator, optExpression.getInputs()));
        }

        @Override
        public Optional<OptExpression> visitLogicalTableScan(OptExpression optExpression, Void context) {
            List<ScalarOperator> remainPredicates = Lists.newArrayList();
            List<ScalarOperator> selectedPredicates = Lists.newArrayList();
            LogicalScanOperator scanOperator = optExpression.getOp().cast();
            ColumnRefSet outputColumnRefSet = optExpression.getRowOutputInfo().getOutputColumnRefSet();
            this.extraPredicates.forEach(predicate -> {
                if (outputColumnRefSet.containsAll(predicate.getUsedColumns())) {
                    selectedPredicates.add(predicate);
                } else {
                    remainPredicates.add(predicate);
                }
            });

            OptExpression scanOpt = optExpression;
            if (!selectedPredicates.isEmpty()) {
                if (scanOperator.getPredicate() != null) {
                    selectedPredicates.add(scanOperator.getPredicate());
                }
                ScalarOperator predicate =
                        Utils.compoundAnd(Utils.extractConjuncts(Utils.compoundAnd(selectedPredicates)));
                this.extraPredicates = remainPredicates;
                Operator operator = OperatorBuilderFactory.build(scanOperator)
                        .withOperator(scanOperator).setPredicate(predicate).build();
                scanOpt = OptExpression.create(operator, Collections.emptyList());
            }

            Map<ColumnRefOperator, ScalarOperator> remainColRefMap = Maps.newHashMap();
            Map<ColumnRefOperator, ScalarOperator> selectedColRefMap = Maps.newHashMap();
            this.extraColRefMap.forEach((k, v) -> {
                if (outputColumnRefSet.containsAll(v.getUsedColumns())) {
                    selectedColRefMap.put(k, v);
                } else {
                    remainColRefMap.put(k, v);
                }
            });

            if (!selectedColRefMap.isEmpty()) {
                this.extraColRefMap = remainColRefMap;
                scanOperator.getOutputColumns().forEach(k -> selectedColRefMap.put(k, k));
                LogicalProjectOperator projectOperator = new LogicalProjectOperator(selectedColRefMap);
                return Optional.of(OptExpression.create(projectOperator, Collections.singletonList(scanOpt)));
            }
            return Optional.ofNullable(scanOpt == optExpression ? null : scanOpt);
        }

        @Override
        public Optional<OptExpression> visit(OptExpression optExpression, Void context) {
            return Optional.empty();
        }

        Optional<OptExpression> graft(OptExpression optExpression) {
            List<Optional<OptExpression>> newInputs =
                    optExpression.getInputs().stream().map(this::graft).collect(Collectors.toList());
            Iterator<Optional<OptExpression>> nextNewInput = newInputs.iterator();
            optExpression.getInputs().replaceAll(input -> nextNewInput.next().orElse(input));
            return optExpression.getOp().accept(this, optExpression, null);
        }
    }

    public static class Cleaner extends OptExpressionVisitor<Pair<Operator, ColumnRefSet>, ColumnRefSet> {
        private ColumnRefFactory columnRefFactory;

        Cleaner(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        @Override
        public Pair<Operator, ColumnRefSet> visit(OptExpression optExpression, ColumnRefSet context) {
            return null;
        }

        @Override
        public Pair<Operator, ColumnRefSet> visitLogicalProject(OptExpression optExpression,
                                                                ColumnRefSet requiredColRef) {
            LogicalProjectOperator projectOperator = optExpression.getOp().cast();
            Map<ColumnRefOperator, ScalarOperator> newColRefMap = Maps.newHashMap();
            ColumnRefSet requiredInputColRefSet = new ColumnRefSet();
            projectOperator.getColumnRefMap().forEach((k, v) -> {
                if (requiredColRef.contains(k)) {
                    newColRefMap.put(k, v);
                    requiredInputColRefSet.union(v.getUsedColumns());
                }
            });
            if (newColRefMap.isEmpty()) {
                newColRefMap.put(columnRefFactory.create("auto_fill_col", Type.TINYINT, false),
                        ConstantOperator.createTinyInt((byte) 1));
            }
            LogicalProjectOperator newOperator = LogicalProjectOperator.builder()
                    .withOperator(projectOperator).setColumnRefMap(newColRefMap).build();
            return Pair.create(newOperator, requiredInputColRefSet);
        }

        @Override
        public Pair<Operator, ColumnRefSet> visitLogicalJoin(OptExpression optExpression,
                                                             ColumnRefSet requiredColRefSet) {
            LogicalJoinOperator joinOperator = optExpression.getOp().cast();
            requiredColRefSet.union(joinOperator.getOnPredicate().getUsedColumns());
            return Pair.create(joinOperator, requiredColRefSet);
        }

        @Override
        public Pair<Operator, ColumnRefSet> visitLogicalTableScan(OptExpression optExpression,
                                                                  ColumnRefSet requiredColRefSet) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();
            ColumnRefSet scanRequiredOutputColRefSet = new ColumnRefSet();
            scanRequiredOutputColRefSet.union(requiredColRefSet);
            if (scanOperator.getPredicate() != null) {
                scanRequiredOutputColRefSet.union(scanOperator.getPredicate().getUsedColumns());
            }
            Map<ColumnRefOperator, Column> newColRefToColMetaMap = Maps.newHashMap();
            scanOperator.getColRefToColumnMetaMap().forEach((colRef, col) -> {
                if (scanRequiredOutputColRefSet.contains(colRef)) {
                    newColRefToColMetaMap.put(colRef, col);
                }
            });
            if (newColRefToColMetaMap.isEmpty()) {
                Preconditions.checkArgument(scanOperator.getTable() instanceof OlapTable);
                OlapTable table = (OlapTable) scanOperator.getTable();
                Preconditions.checkArgument(!table.getKeyColumns().isEmpty());
                Column firstKeyColumn = table.getKeyColumns().get(0);
                ColumnRefOperator firstKeyColRef = scanOperator.getColumnMetaToColRefMap().get(firstKeyColumn);
                newColRefToColMetaMap.put(firstKeyColRef, firstKeyColumn);
            }
            LogicalScanOperator.Builder builder =
                    (LogicalScanOperator.Builder) OperatorBuilderFactory.build(scanOperator).withOperator(scanOperator);
            Operator newOp = builder.setColRefToColumnMetaMap(newColRefToColMetaMap).build();
            return Pair.create(newOp, scanRequiredOutputColRefSet);
        }

        OptExpression clean(OptExpression root, ColumnRefSet requiredColRef) {
            Pair<Operator, ColumnRefSet> result =
                    root.getOp().accept(this, root, requiredColRef);
            if (result == null) {
                return root;
            }
            Operator newOp = result.first;
            ColumnRefSet requiredInputColRefSet = result.second;
            List<OptExpression> newInputs = root.getInputs().stream().map(input -> clean(input, requiredInputColRefSet))
                    .collect(Collectors.toList());
            return OptExpression.create(newOp, newInputs);
        }
    }

    public static void pruneFrontier(
            ColumnRefFactory columnRefFactory,
            OptExpression frontier,
            OptExpression parent,
            int idx,
            Set<OptExpression> prunedTables,
            Map<ColumnRefOperator, ScalarOperator> rewriteMapping,
            Set<ColumnRefOperator> substColRefs) {
        // step2: (Bottom-up) prune the sub-plan leading by the frontier, the ColumnRefOperators that escape from
        // the pruned tables substituted by equivalent ColumnRefOperators originates from retained tables.
        // the ScalarOperators in projection and predicate of pruned tables are collected.
        Map<ColumnRefOperator, ScalarOperator> colRefMap = frontier.getRowOutputInfo().getColumnRefMap();
        Pruner pruner = new Pruner(rewriteMapping, prunedTables, substColRefs);
        frontier = pruner.prune(frontier).orElse(frontier);
        // step3: (Bottom-up) seek for locations to graft the pruned ScalarOperators to right Operator.
        Grafter grafter = new Grafter(pruner.prunedPredicates, pruner.prunedColRefMap);
        frontier = grafter.graft(frontier).orElse(frontier);

        // step4: (Top-down) remove the unused ColumnRefOperators.
        colRefMap.replaceAll((k, v) -> pruner.columnRefRewriter.rewrite(v));
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(colRefMap);
        frontier = OptExpression.create(projectOperator, frontier);
        Cleaner cleaner = new Cleaner(columnRefFactory);
        ColumnRefSet requiredColRefSet = new ColumnRefSet(colRefMap.keySet());
        frontier = cleaner.clean(frontier, requiredColRefSet);
        // finally, install the rewritten node to its parent.
        parent.setChild(idx, frontier);
    }

    public void rewrite() {
        for (Map.Entry<OptExpression, Pair<OptExpression, Integer>> e : cpFrontiers.entrySet()) {
            OptExpression frontier = e.getKey();
            OptExpression parent = e.getValue().first;
            int idx = e.getValue().second;
            // step1: (Bottom-up) mark the tables that will be pruned
            PruneContext pruneContext = markPrunedTables(frontier);
            if (pruneContext.getPrunedTables().isEmpty()) {
                continue;
            }
            Set<OptExpression> prunedTables = pruneContext.getPrunedTables();
            Map<ColumnRefOperator, ScalarOperator> rewriteMapping = Maps.newHashMap();
            pruneContext.getRewriteMapping().forEach((substColRef, originalColRefs) ->
                    originalColRefs.forEach(colRef -> rewriteMapping.put(colRef, substColRef)));
            Set<ColumnRefOperator> substColRefs = pruneContext.getRewriteMapping().keySet();
            // step2~4: prune->graft->clean
            pruneFrontier(columnRefFactory, frontier, parent, idx, prunedTables, rewriteMapping, substColRefs);
        }
    }
}
