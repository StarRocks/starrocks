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

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CardinalityPreservingJoinTablePruner extends OptExpressionVisitor<Boolean, Void> {

    Map<ColumnRefOperator, OptExpression> columnToScans = Maps.newHashMap();
    UnionFind<ColumnRefOperator> columnRefEquivClasses = new UnionFind<>();
    Map<OptExpression, Set<OptExpression>> cardinalityPreservingScanOps = Maps.newHashMap();
    Map<OptExpression, Set<OptExpression>> scanOps = Maps.newHashMap();

    Set<CPEdge> cardinalityPreservingEdges = Sets.newHashSet();

    Map<OptExpression, CPNode> optToGraphNode = Maps.newHashMap();
    List<CPNode> hubNodes = Lists.newArrayList();

    public CardinalityPreservingJoinTablePruner() {
    }

    @Override
    public Boolean visit(OptExpression optExpression, Void context) {
        return false;
    }

    @Override
    public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
        LogicalScanOperator scanOp = optExpression.getOp().cast();
        scanOp.getColRefToColumnMetaMap().keySet().forEach(col -> columnToScans.put(col, optExpression));
        scanOps.put(optExpression, ImmutableSet.of(optExpression));
        cardinalityPreservingScanOps.put(optExpression, ImmutableSet.of(optExpression));
        return true;
    }

    public static class UnionFind<T> {
        private final Map<T, Integer> element2Group = Maps.newHashMap();
        private final Map<Integer, Set<T>> eqGroupMap = Maps.newHashMap();

        public Map<T, Set<T>> getEquivGroups(Set<T> elements) {
            Map<T, Set<T>> elm2group = Maps.newHashMap();
            for (T elm : elements) {
                if (!element2Group.containsKey(elm)) {
                    continue;
                }
                Set<T> eqGroup = eqGroupMap.get(element2Group.get(elm));
                if (eqGroup.size() > 1) {
                    elm2group.put(elm, eqGroup);
                }
            }
            return elm2group;
        }

        public Set<T> getEquivGroup(T element) {
            if (element2Group.containsKey(element)) {
                return Collections.unmodifiableSet(eqGroupMap.get(element2Group.get(element)));
            } else {
                return Collections.emptySet();
            }
        }

        public void add(T... elements) {
            for (T elm : elements) {
                if (!find(elm)) {
                    int groupIdx = element2Group.size();
                    element2Group.put(elm, groupIdx);
                    eqGroupMap.put(groupIdx, ImmutableSet.of(elm));
                }
            }
        }

        public int getGroupIdOrAdd(T elem) {
            add(elem);
            return element2Group.get(elem);
        }

        public Optional<Integer> getGroupId(T elem) {
            if (element2Group.containsKey(elem)) {
                return Optional.of(element2Group.get(elem));
            } else {
                return Optional.empty();
            }
        }

        public Set<T> getGroup(int groupId) {
            return eqGroupMap.getOrDefault(groupId, Collections.emptySet());
        }

        public void union(T lhs, T rhs) {
            add(lhs, rhs);
            Integer lhsGroupIdx = element2Group.get(lhs);
            Integer rhsGroupIdx = element2Group.get(rhs);
            if (!lhsGroupIdx.equals(rhsGroupIdx)) {
                Set<T> lhsGroup = eqGroupMap.get(lhsGroupIdx);
                Set<T> rhsGroup = eqGroupMap.get(rhsGroupIdx);
                Set<T> newGroup = Sets.union(lhsGroup, rhsGroup);
                rhsGroup.forEach(s -> {
                    element2Group.put(s, lhsGroupIdx);
                });
                eqGroupMap.put(lhsGroupIdx, newGroup);
                eqGroupMap.remove(rhsGroupIdx);
            }
        }

        public boolean find(T slotId) {
            return element2Group.containsKey(slotId);
        }

        public boolean containsAll(Collection<T> keys) {
            return element2Group.keySet().containsAll(keys);
        }

        public void removesAll(Collection<T> keys) {
            for (T key : keys) {
                if (!element2Group.containsKey(key)) {
                    continue;
                }
                int groupIdx = element2Group.remove(key);
                eqGroupMap.get(groupIdx).remove(key);
            }
        }
    }

    public boolean isForeignKeyConstraintReferenceToUniqueKey(
            ForeignKeyConstraint foreignKeyConstraint,
            OlapTable rhsTable) {
        if (foreignKeyConstraint.getParentTableInfo().getTableId() != rhsTable.getId()) {
            return false;
        }
        Set<String> referencedColumnNames =
                foreignKeyConstraint.getColumnRefPairs().stream().map(p -> p.second).collect(Collectors.toSet());
        return rhsTable.getUniqueConstraints().stream()
                .anyMatch(uk -> new HashSet<>(uk.getUniqueColumns()).equals(referencedColumnNames));
    }

    public static class CardinalityPreservingBiRel {
        private final OptExpression lhs;
        private final OptExpression rhs;
        private final boolean fromForeignKey;
        private final boolean leftToRight;
        private final Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs;

        public CardinalityPreservingBiRel(
                OptExpression lhs,
                OptExpression rhs,
                boolean fromForeignKey,
                boolean leftToRight,
                Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs) {
            this.lhs = lhs;
            this.rhs = rhs;
            this.fromForeignKey = fromForeignKey;
            this.leftToRight = leftToRight;
            this.pairs = pairs;
        }

        public OptExpression getLhs() {
            return lhs;
        }

        public OptExpression getRhs() {
            return rhs;
        }

        public boolean isFromForeignKey() {
            return fromForeignKey;
        }

        public boolean isLeftToRight() {
            return leftToRight;
        }

        public Set<Pair<ColumnRefOperator, ColumnRefOperator>> getPairs() {
            return pairs;
        }

    }

    public static class CPEdge {

        private final OptExpression lhs;
        private final OptExpression rhs;

        private final boolean unilateral;

        final BiMap<ColumnRefOperator, ColumnRefOperator> eqColumnRefs;

        public CPEdge(OptExpression lhs, OptExpression rhs, boolean unilateral,
                      Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs) {
            this.lhs = lhs;
            this.rhs = rhs;
            this.unilateral = unilateral;
            this.eqColumnRefs = HashBiMap.create(eqColumnRefs);
        }

        public CPEdge(OptExpression lhs, OptExpression rhs, boolean unilateral) {
            this(lhs, rhs, unilateral, Collections.emptyMap());
        }

        CPEdge inverse() {
            return new CPEdge(rhs, lhs, unilateral, eqColumnRefs.inverse());
        }

        CPEdge toUniLateral() {
            if (unilateral) {
                return this;
            } else {
                return new CPEdge(lhs, rhs, true, eqColumnRefs);
            }
        }

        CPEdge toBiLateral() {
            if (!unilateral) {
                return this;
            } else {
                return new CPEdge(lhs, rhs, false, eqColumnRefs);
            }
        }

        public OptExpression getLhs() {
            return lhs;
        }

        public OptExpression getRhs() {
            return rhs;
        }

        public Map<ColumnRefOperator, ColumnRefOperator> getEqColumnRefs() {
            return eqColumnRefs;
        }

        public Map<ColumnRefOperator, ColumnRefOperator> getInverseEqColumnRefs() {
            return eqColumnRefs.inverse();
        }

        public boolean isUnilateral() {
            return unilateral;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CPEdge that = (CPEdge) o;
            return unilateral == that.unilateral && Objects.equals(lhs, that.lhs) &&
                    Objects.equals(rhs, that.rhs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lhs, rhs, unilateral);
        }
    }

    public List<CardinalityPreservingBiRel> getCardPreservings(OptExpression lhs, OptExpression rhs,
                                                               boolean leftToRight) {
        LogicalScanOperator lhsScanOp = lhs.getOp().cast();
        LogicalScanOperator rhsScanOp = rhs.getOp().cast();
        if (!(lhsScanOp.getTable() instanceof OlapTable) || !(rhsScanOp.getTable() instanceof OlapTable)) {
            return Collections.emptyList();
        }
        OlapTable lhsTable = (OlapTable) lhsScanOp.getTable();
        OlapTable rhsTable = (OlapTable) rhsScanOp.getTable();
        Map<String, ColumnRefOperator> lhsColumnName2ColRef =
                lhsScanOp.getColumnMetaToColRefMap().entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue()));
        Map<String, ColumnRefOperator> rhsColumnName2ColRef =
                rhsScanOp.getColumnMetaToColRefMap().entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue()));
        List<CardinalityPreservingBiRel> cardinalityPreservingBiRels = Lists.newArrayList();
        if (lhsTable.hasForeignKeyConstraints() && rhsTable.hasUniqueConstraints()) {
            lhsTable.getForeignKeyConstraints().stream()
                    .filter(fk -> isForeignKeyConstraintReferenceToUniqueKey(fk, rhsTable)).forEach(fk -> {
                        Set<String> lhsColumNames =
                                fk.getColumnRefPairs().stream().map(p -> p.first).collect(Collectors.toSet());
                        Set<String> rhsColumNames =
                                fk.getColumnRefPairs().stream().map(p -> p.second).collect(Collectors.toSet());
                        if (lhsColumnName2ColRef.keySet().containsAll(lhsColumNames) &&
                                rhsColumnName2ColRef.keySet().containsAll(rhsColumNames)) {
                            Set<Pair<ColumnRefOperator, ColumnRefOperator>> fkColumnRefPairs =
                                    fk.getColumnRefPairs().stream()
                                            .map(p ->
                                                    Pair.create(
                                                            lhsColumnName2ColRef.get(p.first),
                                                            rhsColumnName2ColRef.get(p.second))
                                            ).collect(Collectors.toSet());
                            cardinalityPreservingBiRels.add(
                                    new CardinalityPreservingBiRel(lhs, rhs, true, leftToRight, fkColumnRefPairs));
                        }
                    });
        }

        if (lhsTable.getId() == rhsTable.getId() && lhsTable.hasUniqueConstraints()) {
            lhsTable.getUniqueConstraints().stream().filter(uk ->
                            lhsColumnName2ColRef.keySet().containsAll(uk.getUniqueColumns()) &&
                                    rhsColumnName2ColRef.keySet().containsAll(uk.getUniqueColumns())
                    ).map(uk ->
                            uk.getUniqueColumns().stream().map(colName ->
                                    Pair.create(
                                            lhsColumnName2ColRef.get(colName),
                                            rhsColumnName2ColRef.get(colName))
                            ).collect(Collectors.toSet()))
                    .forEach(ukColumnRefPairs ->
                            cardinalityPreservingBiRels.add(
                                    new CardinalityPreservingBiRel(lhs, rhs, false, leftToRight,
                                            ukColumnRefPairs)));
        }
        return cardinalityPreservingBiRels;
    }

    public List<CardinalityPreservingBiRel> getCardPreservings(OptExpression optExpression,
                                                               Set<OptExpression> candidateLhsScanOpSet,
                                                               Set<OptExpression> candidateRhsScanOpSet) {
        OptExpression lhs = optExpression.inputAt(0);
        OptExpression rhs = optExpression.inputAt(1);
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        List<CardinalityPreservingBiRel> biRels = Lists.newArrayList();
        if ((joinOp.getJoinType().isInnerJoin() || joinOp.getJoinType().isLeftOuterJoin()) &&
                scanOps.containsKey(lhs) && cardinalityPreservingScanOps.containsKey(rhs)) {
            List<CardinalityPreservingBiRel> leftToRightBiRels = scanOps.get(lhs).stream()
                    .filter(candidateLhsScanOpSet::contains)
                    .flatMap(lhsScan -> cardinalityPreservingScanOps.get(rhs).stream()
                            .filter(candidateRhsScanOpSet::contains)
                            .flatMap(rhsScan ->
                                    getCardPreservings(lhsScan, rhsScan, true).stream()
                            )).collect(Collectors.toList());
            biRels.addAll(leftToRightBiRels);
        }
        if ((joinOp.getJoinType().isInnerJoin() || joinOp.getJoinType().isRightOuterJoin()) &&
                scanOps.containsKey(rhs) && cardinalityPreservingScanOps.containsKey(lhs)) {
            List<CardinalityPreservingBiRel> rightToLeftBiRels = scanOps.get(rhs).stream()
                    .filter(candidateRhsScanOpSet::contains)
                    .flatMap(rhsScan -> cardinalityPreservingScanOps.get(lhs).stream()
                            .filter(candidateLhsScanOpSet::contains)
                            .flatMap(lhsScan ->
                                    getCardPreservings(rhsScan, lhsScan, false).stream()
                            )).collect(Collectors.toList());
            biRels.addAll(rightToLeftBiRels);
        }
        return biRels;
    }

    @Override
    public Boolean visitLogicalJoin(OptExpression optExpression, Void context) {
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        JoinOperator joinType = joinOp.getJoinType();
        if (!joinType.isInnerJoin() && !joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin()) {
            return null;
        }

        Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> onPredicates =
                JoinHelper.separateEqualPredicatesFromOthers(optExpression);
        List<BinaryPredicateOperator> eqOnPredicates = onPredicates.first;
        List<ScalarOperator> otherOnPredicates = onPredicates.second;

        if (!otherOnPredicates.isEmpty() || eqOnPredicates.isEmpty()) {
            return null;
        }

        Set<Pair<ColumnRefOperator, ColumnRefOperator>> eqColumnRefPairs = Sets.newHashSet();
        for (BinaryPredicateOperator eqPredicate : eqOnPredicates) {
            ColumnRefOperator leftCol = eqPredicate.getChild(0).cast();
            ColumnRefOperator rightCol = eqPredicate.getChild(1).cast();
            eqColumnRefPairs.add(Pair.create(leftCol, rightCol));
        }

        Set<Pair<Integer, Integer>> joinColumnEqClasses =
                eqColumnRefPairs.stream().map(biCol ->
                        Pair.create(
                                columnRefEquivClasses.getGroupIdOrAdd(biCol.first),
                                columnRefEquivClasses.getGroupIdOrAdd(biCol.second))
                ).filter(p -> !Objects.equals(p.first, p.second)).collect(Collectors.toSet());

        Set<OptExpression> candidateLhsScanOpSet =
                joinColumnEqClasses.stream()
                        .flatMap(p ->
                                columnRefEquivClasses.getGroup(p.first).stream().map(col -> columnToScans.get(col)))
                        .collect(Collectors.toSet());

        Set<OptExpression> candidateRhsScanOpSet =
                joinColumnEqClasses.stream()
                        .flatMap(p ->
                                columnRefEquivClasses.getGroup(p.second).stream()
                                        .map(col -> columnToScans.get(col)))
                        .collect(Collectors.toSet());

        List<CardinalityPreservingBiRel> biRels =
                getCardPreservings(optExpression, candidateLhsScanOpSet, candidateRhsScanOpSet);
        for (CardinalityPreservingBiRel biRel : biRels) {
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs = biRel.getPairs();
            Set<Pair<Optional<Integer>, Optional<Integer>>> optGroupIdPairs = pairs.stream()
                    .map(p -> Pair.create(
                            columnRefEquivClasses.getGroupId(p.first),
                            columnRefEquivClasses.getGroupId(p.second)))
                    .collect(Collectors.toSet());

            if (optGroupIdPairs.stream().anyMatch(p -> !p.first.isPresent() || !p.second.isPresent())) {
                continue;
            }

            Set<Pair<Integer, Integer>> groupIdPairs =
                    optGroupIdPairs.stream()
                            .map(p -> biRel.isLeftToRight() ?
                                    Pair.create(p.first.get(), p.second.get()) :
                                    Pair.create(p.second.get(), p.first.get()))
                            .filter(p -> !p.first.equals(p.second))
                            .collect(Collectors.toSet());

            if (!joinColumnEqClasses.equals(groupIdPairs)) {
                continue;
            }

            Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs = Maps.newHashMap();
            // two ScanOperators access the same tables, which are joined together on PK/UK
            if (!biRel.isFromForeignKey()) {
                LogicalScanOperator lhsScanOp = biRel.getLhs().getOp().cast();
                LogicalScanOperator rhsScanOp = biRel.getRhs().getOp().cast();
                Set<Column> lhsColumns = lhsScanOp.getColumnMetaToColRefMap().keySet();
                Set<Column> rhsColumns = rhsScanOp.getColumnMetaToColRefMap().keySet();
                Preconditions.checkArgument(lhsColumns.equals(rhsColumns));
                for (Column column : lhsColumns) {
                    ColumnRefOperator lhsColRef = lhsScanOp.getColumnMetaToColRefMap().get(column);
                    ColumnRefOperator rhsColRef = rhsScanOp.getColumnMetaToColRefMap().get(column);
                    columnRefEquivClasses.union(Objects.requireNonNull(lhsColRef),
                            Objects.requireNonNull(rhsColRef));
                    eqColumnRefs.put(lhsColRef, rhsColRef);
                }
            } else if (joinType.isInnerJoin()) {
                pairs.forEach(p -> columnRefEquivClasses.union(p.first, p.second));
                pairs.forEach(p -> eqColumnRefs.put(p.first, p.second));
            }

            OptExpression lhsOpt = optExpression.inputAt(0);
            OptExpression rhsOpt = optExpression.inputAt(1);
            Set<OptExpression> lhsCardPreservingScanOps =
                    cardinalityPreservingScanOps.get(lhsOpt);
            Set<OptExpression> rhsCardPreservingScanOps =
                    cardinalityPreservingScanOps.get(rhsOpt);

            Set<OptExpression> lhsScanOps = scanOps.get(lhsOpt);
            Set<OptExpression> rhsScanOps = scanOps.get(rhsOpt);

            Set<OptExpression> currScanOps = Sets.newHashSet(lhsScanOps);
            currScanOps.addAll(rhsScanOps);
            scanOps.put(optExpression, currScanOps);

            Set<OptExpression> currCPScanOps;
            if (!biRel.isFromForeignKey() &&
                    (lhsCardPreservingScanOps.contains(biRel.lhs) &&
                            rhsCardPreservingScanOps.contains(biRel.rhs)) ||
                    (lhsCardPreservingScanOps.contains(biRel.rhs) &&
                            rhsCardPreservingScanOps.contains(biRel.lhs))) {
                cardinalityPreservingEdges.add(
                        new CPEdge(biRel.getLhs(), biRel.getRhs(), false, eqColumnRefs));

                currCPScanOps = Sets.newHashSet(lhsCardPreservingScanOps);
                currCPScanOps.addAll(rhsCardPreservingScanOps);
            } else {
                if (biRel.isLeftToRight()) {
                    cardinalityPreservingEdges.add(
                            new CPEdge(biRel.getLhs(), biRel.getRhs(), true, eqColumnRefs));
                    currCPScanOps = lhsCardPreservingScanOps;
                } else {
                    cardinalityPreservingEdges.add(
                            new CPEdge(biRel.getRhs(), biRel.getLhs(), true, eqColumnRefs));
                    currCPScanOps = rhsCardPreservingScanOps;
                }
            }

            if (!cardinalityPreservingScanOps.containsKey(optExpression) ||
                    cardinalityPreservingScanOps.get(optExpression).size() < currCPScanOps.size()) {
                cardinalityPreservingScanOps.put(optExpression, currCPScanOps);
            }
        }
        return cardinalityPreservingScanOps.containsKey(optExpression);
    }

    @Override
    public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
        scanOps.put(optExpression, scanOps.get(optExpression.inputAt(0)));
        cardinalityPreservingScanOps.put(optExpression, cardinalityPreservingScanOps.get(optExpression.inputAt(0)));
        return true;
    }

    @Override
    public Boolean visitLogicalFilter(OptExpression optExpression, Void context) {
        scanOps.put(optExpression, scanOps.get(optExpression.inputAt(0)));
        cardinalityPreservingScanOps.put(optExpression, cardinalityPreservingScanOps.get(optExpression.inputAt(0)));
        return true;
    }

    boolean process(OptExpression root) {
        if (!root.getInputs().stream().allMatch(this::process)) {
            return false;
        }
        return root.getOp().accept(this, root, null);
    }

    public static final class CPNode {
        OptExpression value = null;
        CPNode parent = null;
        List<CPNode> children = Collections.emptyList();

        Map<OptExpression, Map<ColumnRefOperator, ColumnRefOperator>> equivColumnRefs = Collections.emptyMap();
        boolean isHub = false;

        List<CPNode> nonCPChildren = Collections.emptyList();

        public CPNode(OptExpression value, CPNode parent, boolean isHub) {
            this.value = value;
            this.parent = parent;
            this.isHub = isHub;
        }

        public static CPNode createNode(OptExpression value) {
            return new CPNode(value, null, false);
        }

        public static CPNode createHubNode(CPNode... children) {
            CPNode hubNode = new CPNode(null, null, true);
            Arrays.stream(children).forEach(hubNode::addChild);
            return hubNode;
        }

        public void addEqColumnRefs(OptExpression optExpr, Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs) {
            if (equivColumnRefs.isEmpty()) {
                equivColumnRefs = Maps.newHashMap();
            }
            equivColumnRefs.put(optExpr, Collections.unmodifiableMap(eqColumnRefs));
        }

        public CPNode getParent() {
            return parent;
        }

        public void setParent(CPNode parent) {
            this.parent = parent;
        }

        public boolean isHub() {
            return isHub;
        }

        public boolean isRoot() {
            return parent == null;
        }

        public boolean isLeaf() {
            return children == null || children.isEmpty();
        }

        public void addChild(CPNode child) {
            if (children.isEmpty()) {
                children = Lists.newArrayList();
            }
            children.add(child);
            child.setParent(this);
        }

        public void addNonCPChild(CPNode child) {
            Preconditions.checkArgument(isHub);
            if (nonCPChildren.isEmpty()) {
                nonCPChildren = Lists.newArrayList();
            }
            nonCPChildren.add(child);
            child.setParent(this);
        }

        public static void mergeHubNode(CPNode lhsNode, CPNode rhsNode) {
            rhsNode.children.forEach(lhsNode::addChild);
            lhsNode.children.addAll(rhsNode.children);
        }

        public List<CPNode> getChildren() {
            return children;
        }

        public List<CPNode> getNonCPChildren() {
            return nonCPChildren;
        }

        public OptExpression getValue() {
            return value;
        }

        public Map<OptExpression, Map<ColumnRefOperator, ColumnRefOperator>> getEquivColumnRefs() {
            return equivColumnRefs;
        }
    }

    public void collect(OptExpression root) {
        process(root);
        Set<CPEdge> edges = Sets.newHashSet();
        for (CPEdge edge : cardinalityPreservingEdges) {
            if (edges.contains(edge)) {
                continue;
            }

            if (!edge.isUnilateral()) {
                if (!edges.contains(edge.inverse())) {
                    edges.add(edge);
                }
            } else if (!edges.contains(edge.toBiLateral()) && !edges.contains(edge.toBiLateral().inverse())) {
                if (edges.contains(edge.inverse())) {
                    edges.remove(edge.inverse());
                    edges.add(edge.toBiLateral());
                } else {
                    edges.add(edge);
                }
            }
        }

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

        for (CPEdge e : edges) {
            if (e.isUnilateral()) {
                continue;
            }

            CPNode lhsNode = optToGraphNode.get(e.getLhs());
            CPNode rhsNode = optToGraphNode.get(e.getRhs());
            if (lhsNode.getParent() == null && rhsNode.getParent() == null) {
                hubNodes.add(CPNode.createHubNode(lhsNode, rhsNode));
            } else if (lhsNode.getParent() != null) {
                lhsNode.getParent().addChild(rhsNode);
            } else if (rhsNode.getParent() != null) {
                rhsNode.getParent().addChild(lhsNode);
            } else {
                CPNode.mergeHubNode(lhsNode.getParent(), rhsNode.getParent());
            }
        }

        for (CPEdge e : edges) {
            if (!e.isUnilateral()) {
                continue;
            }
            CPNode lhsNode = optToGraphNode.get(e.getLhs());
            CPNode rhsNode = optToGraphNode.get(e.getRhs());
            if (!rhsNode.isRoot() && rhsNode.getParent().isHub()) {
                rhsNode = rhsNode.getParent();
            }
            if (!lhsNode.isRoot() && lhsNode.getParent().isHub()) {
                Preconditions.checkArgument(rhsNode.isRoot() || rhsNode.getParent() == lhsNode.getParent());
                lhsNode.getParent().addNonCPChild(rhsNode);
            } else {
                Preconditions.checkArgument(rhsNode.isRoot() || rhsNode.getParent() == lhsNode);
                lhsNode.addChild(rhsNode);
            }
        }
    }

    Map<ColumnRefOperator, ColumnRefOperator> rewriteMapping = Maps.newConcurrentMap();
    Set<OptExpression> prunedTables = Sets.newHashSet();

    public void pruneTables(List<ColumnRefOperator> outputColumnRefs) {
        outputColumnRefs.forEach(colRef -> rewriteMapping.put(colRef, colRef));
        for (CPNode node : Iterables.concat(optToGraphNode.values(), hubNodes)) {
            if (!node.isRoot()) {
                continue;
            }
            pruneTable(node);
        }
    }

    private boolean pruneTable(CPNode root) {
        // visit children of current GraphNode in post-order order.
        if (root.isHub()) {
            // try to prune non-cardinality-preserving children
            boolean prunable = root.getNonCPChildren().stream().allMatch(this::pruneTable);

            // construct equivalent classes from equivalent ColumnRefOperator pairs of
            // cardinality-preserving children.
            UnionFind<ColumnRefOperator> localEqClasses = new UnionFind<>();
            for (CPNode child : root.getChildren()) {
                Preconditions.checkArgument(!child.isHub());
                child.getEquivColumnRefs().values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .forEach(e -> localEqClasses.union(e.getKey(), e.getValue()));
            }

            // try to prune cardinality-preserving children
            for (CPNode child : root.getChildren()) {
                LogicalScanOperator scanOperator = child.getValue().getOp().cast();
                Set<ColumnRefOperator> outputColRefs = rewriteMapping.keySet();
                Set<ColumnRefOperator> currentScanOutputColRefs = scanOperator.getColumnMetaToColRefMap()
                        .values().stream().filter(outputColRefs::contains)
                        .collect(Collectors.toSet());
                // if there exists output columns of current child that can not be remapped
                // to other equivalent column, the child can be prunable.
                if (!currentScanOutputColRefs.isEmpty() &&
                        currentScanOutputColRefs.stream()
                                .anyMatch(colRef -> localEqClasses.getEquivGroup(colRef).size() < 2)) {
                    prunable = false;
                    continue;
                }

                // substitute remapped columns with its other equivalent columns
                for (ColumnRefOperator colRef : currentScanOutputColRefs) {
                    ColumnRefOperator newColRef = Sets.difference(
                            localEqClasses.getEquivGroup(colRef),
                            Collections.singleton(colRef)).iterator().next();
                    ColumnRefOperator originalColRef = rewriteMapping.get(colRef);
                    rewriteMapping.remove(colRef);
                    rewriteMapping.put(newColRef, originalColRef);
                }

                prunedTables.add(child.getValue());
                // all columns of the current child should be removed from equivalent classes
                // to prevent output columns of sequential child from being remapped to it.
                localEqClasses.removesAll(scanOperator.getColumnMetaToColRefMap().values());
            }

            return prunable;
        } else {
            // parent can not be pruned if any of its offsprings is can not be pruned
            if (!root.getChildren().stream().allMatch(this::pruneTable)) {
                return false;
            }

            LogicalScanOperator scanOperator = root.getValue().getOp().cast();
            Set<ColumnRefOperator> outputColRefs = rewriteMapping.keySet();
            Set<ColumnRefOperator> currentScanOutputColRefs = scanOperator.getColumnMetaToColRefMap()
                    .values().stream().filter(outputColRefs::contains)
                    .collect(Collectors.toSet());

            // current ScanOp has nothing to output, so it can be pruned
            if (currentScanOutputColRefs.isEmpty()) {
                prunedTables.add(root.getValue());
                return true;
            }

            // current ScanOp can not be pruned if it has column to output but it has
            // no parent.
            if (root.isRoot()) {
                return false;
            }

            Preconditions.checkArgument(!root.getParent().isHub);
            Map<ColumnRefOperator, ColumnRefOperator> eqColRefs = root.getEquivColumnRefs()
                    .entrySet().stream().flatMap(m -> m.getValue().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // current ScanOp has output columns that can not be remapped to its parent's
            // output columns via equivalence relations, so current ScanOp can not be pruned
            if (eqColRefs.isEmpty() || !eqColRefs.keySet().containsAll(currentScanOutputColRefs)) {
                return false;
            }

            // all the output columns of current ScanOp can be remapped to its parent's
            // output columns, so current ScanOp can be pruned
            for (ColumnRefOperator colRef : currentScanOutputColRefs) {
                ColumnRefOperator originalColRef = rewriteMapping.get(colRef);
                rewriteMapping.remove(colRef);
                rewriteMapping.put(eqColRefs.get(colRef), originalColRef);
            }
            prunedTables.add(root.getValue());
            return true;
        }
    }

    public static class Rewriter extends OptExpressionVisitor<Optional<OptExpression>, Void> {
        private final Map<ColumnRefOperator, ColumnRefOperator> remapping;
        private final ReplaceColumnRefRewriter columnRefRewriter;
        private final Set<OptExpression> prunedTables;

        public Rewriter(Map<ColumnRefOperator, ColumnRefOperator> remapping, Set<OptExpression> prunedTables) {
            this.remapping = remapping;
            Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
            remapping.forEach((k, v) -> replaceMap.put(v, k));
            this.columnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
            this.prunedTables = prunedTables;
        }

        @Override
        public Optional<OptExpression> visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOp = optExpression.getOp().cast();
            if (prunedTables.contains(optExpression)) {
                Preconditions.checkArgument(
                        Sets.intersection(
                                Collections.unmodifiableSet(new HashSet<>(scanOp.getColumnMetaToColRefMap().values())),
                                remapping.keySet()).isEmpty());
                return Optional.empty();
            } else {
                ImmutableMap.Builder<ColumnRefOperator, Column>
                        newColRefToColumnMetaBuilder = new ImmutableMap.Builder<>();
                newColRefToColumnMetaBuilder.putAll(scanOp.getColRefToColumnMetaMap());
                scanOp.getColumnMetaToColRefMap().entrySet()
                        .stream().filter(e -> remapping.containsKey(e.getValue()) &&
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
        public Optional<OptExpression> visitLogicalProject(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().isEmpty()) {
                return Optional.empty();
            }
            LogicalProjectOperator projectOp = optExpression.getOp().cast();
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
                return Optional.empty();
            }
            ScalarOperator predicate = columnRefRewriter.rewrite(optExpression.getOp().getPredicate());
            Operator.Builder builder;
            List<OptExpression> newInputs;
            if (optExpression.getInputs().size() == 1) {
                OptExpression child = optExpression.inputAt(0);
                builder = ((Operator.Builder) OperatorBuilderFactory.build(child.getOp())).withOperator(child.getOp());
                newInputs = child.getInputs();
            } else {
                builder = ((Operator.Builder) OperatorBuilderFactory.build(optExpression.getOp())).withOperator(
                        optExpression.getOp());
                newInputs = optExpression.getInputs();
            }
            Operator newOp = builder.setPredicate(predicate).build();
            return Optional.of(OptExpression.create(newOp, newInputs));
        }

        @Override
        public Optional<OptExpression> visitLogicalFilter(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().isEmpty()) {
                return Optional.empty();
            }
            LogicalFilterOperator filterOp = optExpression.getOp().cast();
            ScalarOperator newPredicate = columnRefRewriter.rewrite(filterOp.getPredicate());
            Operator newOperator =
                    new LogicalFilterOperator.Builder().withOperator(filterOp).setPredicate(newPredicate).build();
            return Optional.of(OptExpression.create(newOperator, optExpression.getInputs()));
        }

        public Optional<OptExpression> process(OptExpression optExpression) {
            List<OptExpression> newInputs = optExpression.getInputs().stream().map(this::process)
                    .filter(Optional::isPresent).map(Optional::get)
                    .collect(Collectors.toList());
            optExpression.getInputs().clear();
            optExpression.getInputs().addAll(newInputs);
            return optExpression.getOp().accept(this, optExpression, null);
        }
    }

    public List<OptExpression> rewrite(OptExpression optExpression) {
        if (prunedTables.isEmpty()) {
            return Collections.singletonList(optExpression);
        }

        Optional<OptExpression> optNewOptExpr = new Rewriter(rewriteMapping, prunedTables).process(optExpression);
        if (optNewOptExpr.isPresent()) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
            rewriteMapping.forEach((k, v) -> columnRefMap.put(v, k));
            return Collections.singletonList(OptExpression.create(new LogicalProjectOperator(columnRefMap),
                    Collections.singletonList(optNewOptExpr.get())));
        } else {
            return Collections.singletonList(optExpression);
        }
    }
}
