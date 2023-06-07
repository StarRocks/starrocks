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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CardinalityPreservingJoinTablePruner extends OptExpressionVisitor<Boolean, Void> {

    Map<ColumnRefOperator, OptExpression> columnToScans = Maps.newHashMap();
    UnionFind<ColumnRefOperator> columnRefEquivClasses = new UnionFind<>();
    Map<OptExpression, Set<OptExpression>> cardinalityPreservingScanOps = Maps.newHashMap();
    Map<OptExpression, ColumnRefSet> cardinalityPreservingColRefs = Maps.newHashMap();
    Map<OptExpression, Set<OptExpression>> scanOps = Maps.newHashMap();

    Optional<OptExpression> optNewRoot = Optional.empty();
    Map<OptExpression, Pair<OptExpression, Integer>> cardinalityPreservingFrontiers = Maps.newHashMap();

    Set<CPEdge> cardinalityPreservingEdges = Sets.newHashSet();

    Map<OptExpression, CPNode> optToGraphNode = Maps.newHashMap();
    List<CPNode> hubNodes = Lists.newArrayList();
    Map<Integer, ColumnRefSet> columnOrigins = Maps.newHashMap();

    public CardinalityPreservingJoinTablePruner() {
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
                scalarOp.getUsedColumns().getStream().forEach(id -> columnRefSet.union(columnOrigins.get(id)));
                columnOrigins.put(col.getId(), columnRefSet);
            }
        });
    }

    @Override
    public Boolean visit(OptExpression optExpression, Void context) {

        return false;
    }

    @Override
    public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
        LogicalScanOperator scanOp = optExpression.getOp().cast();
        if (scanOp.hasLimit()) {
            return false;
        }
        //TODO(by satanson): non-OlapTable will be supported in future
        if (!(scanOp.getTable() instanceof OlapTable)) {
            return false;
        }
        OlapTable table = ((OlapTable) scanOp.getTable());
        // A Table that has no PK/UK/FK can not associate with other tables via
        // cardinality-preserving relations.
        if (!table.hasUniqueConstraints() && !table.hasForeignKeyConstraints()) {
            return false;
        }

        if (table.hasUniqueConstraints()) {
            Map<String, ColumnRefOperator> nameToColumnRefs = scanOp.getColumnNameToColRefMap();
            List<ColumnRefOperator> columnRefs = table.getUniqueConstraints().stream().flatMap(uk ->
                    uk.getUniqueColumns().stream().map(nameToColumnRefs::get)).collect(Collectors.toList());
            cardinalityPreservingColRefs.put(optExpression, new ColumnRefSet(columnRefs));
        }

        scanOp.getColRefToColumnMetaMap().keySet().forEach(col -> {
            columnOrigins.put(col.getId(), new ColumnRefSet(col.getId()));
            columnToScans.put(col, optExpression);
        });

        computeOriginsOfColumnRefs(optExpression);
        scanOps.put(optExpression, ImmutableSet.of(optExpression));
        cardinalityPreservingScanOps.put(optExpression, ImmutableSet.of(optExpression));
        return true;
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
            return false;
        }

        if (joinOp.getPredicate() != null) {
            return false;
        }

        Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> onPredicates =
                JoinHelper.separateEqualPredicatesFromOthers(optExpression);
        List<BinaryPredicateOperator> eqOnPredicates = onPredicates.first;
        List<ScalarOperator> otherOnPredicates = onPredicates.second;

        if (!otherOnPredicates.isEmpty() || eqOnPredicates.isEmpty()) {
            return false;
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
            if (!biRel.isFromForeignKey()) {
                cardinalityPreservingEdges.add(
                        new CPEdge(biRel.getLhs(), biRel.getRhs(), false, eqColumnRefs));
                currCPScanOps = Sets.newHashSet(lhsCardPreservingScanOps);
                currCPScanOps.addAll(rhsCardPreservingScanOps);
                // Predicates in source and sink of edges that represent mutual cardinality-preserving from
                // same table joining on PK/UK can not handle in collect-phase, it will be handled in pruneTables
                // phase.
            } else {
                CPEdge edge;
                if (biRel.isLeftToRight()) {
                    edge = new CPEdge(biRel.getLhs(), biRel.getRhs(), true, eqColumnRefs);
                    currCPScanOps = lhsCardPreservingScanOps;
                } else {
                    edge = new CPEdge(biRel.getRhs(), biRel.getLhs(), true, eqColumnRefs);
                    currCPScanOps = rhsCardPreservingScanOps;
                }

                // If sink of the CPEdge has predicates that reference non-equivalent columns,
                // then the sink of the edge can not be pruned since cardinality-preserving relation
                // is not satisfied.
                OptExpression edgeSink = edge.getRhs();
                ColumnRefSet usedColumns = Optional.ofNullable(edgeSink.getOp().getPredicate())
                        .map(ScalarOperator::getUsedColumns)
                        .orElse(new ColumnRefSet());
                usedColumns.except(eqColumnRefs.keySet());
                if (usedColumns.isEmpty()) {
                    cardinalityPreservingEdges.add(edge);
                } else {
                    currCPScanOps = Collections.emptySet();
                }
            }

            if (!cardinalityPreservingScanOps.containsKey(optExpression) ||
                    cardinalityPreservingScanOps.get(optExpression).size() < currCPScanOps.size()) {
                cardinalityPreservingScanOps.put(optExpression, currCPScanOps);
            }
        }
        return !joinOp.hasLimit() && cardinalityPreservingScanOps.containsKey(optExpression);
    }

    @Override
    public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
        if (optExpression.getOp().hasLimit()) {
            return false;
        }
        scanOps.put(optExpression, scanOps.get(optExpression.inputAt(0)));
        cardinalityPreservingScanOps.put(optExpression, cardinalityPreservingScanOps.get(optExpression.inputAt(0)));
        computeOriginsOfColumnRefs(optExpression);
        return true;
    }

    boolean process(OptExpression root) {
        Map<OptExpression, Pair<OptExpression, Integer>> candidateFrontiers = Maps.newHashMap();
        for (int i = 0; i < root.getInputs().size(); ++i) {
            OptExpression child = root.inputAt(i);
            if (process(child)) {
                candidateFrontiers.put(child, Pair.create(root, i));
            }
        }
        boolean walkUpwards = candidateFrontiers.size() == root.getInputs().size();
        if (!walkUpwards) {
            cardinalityPreservingFrontiers.putAll(candidateFrontiers);
            return false;
        }
        return root.getOp().accept(this, root, null);
    }

    public static final class CPNode {
        OptExpression value = null;
        CPNode parent = null;
        List<CPNode> children = Collections.emptyList();

        Map<OptExpression, Map<ColumnRefOperator, ColumnRefOperator>> equivColumnRefs = Collections.emptyMap();
        boolean hubFlag = false;

        List<CPNode> nonCPChildren = Collections.emptyList();

        public CPNode(OptExpression value, CPNode parent, boolean hubFlag) {
            this.value = value;
            this.parent = parent;
            this.hubFlag = hubFlag;
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
            return hubFlag;
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
            Preconditions.checkArgument(hubFlag);
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

        public boolean intersect(Set<OptExpression> optExpressions) {
            if (hubFlag) {
                return children.stream().anyMatch(node -> optExpressions.contains(node.getValue())) ||
                        nonCPChildren.stream().anyMatch(node -> optExpressions.contains(node.getValue()));
            } else {
                return optExpressions.contains(value);
            }
        }
    }

    public void collect(OptExpression root) {
        if (process(root)) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMaps = Maps.newHashMap();
            root.getRowOutputInfo().getColumnRefMap().keySet().forEach(k -> columnRefMaps.put(k, k));
            OptExpression newRoot =
                    OptExpression.create(new LogicalProjectOperator(columnRefMaps), Collections.singletonList(root));
            optNewRoot = Optional.of(newRoot);
            cardinalityPreservingFrontiers.put(root, Pair.create(newRoot, 0));
        }

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
            if (lhsNode.getParent() != null && rhsNode.getParent() != null) {
                CPNode.mergeHubNode(lhsNode.getParent(), rhsNode.getParent());
            } else if (lhsNode.getParent() != null) {
                lhsNode.getParent().addChild(rhsNode);
            } else if (rhsNode.getParent() != null) {
                rhsNode.getParent().addChild(lhsNode);
            } else {
                hubNodes.add(CPNode.createHubNode(lhsNode, rhsNode));
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

    public void pruneTables(OptExpression frontier,
                            Set<OptExpression> prunedTables,
                            Map<ColumnRefOperator, ColumnRefOperator> rewriteMapping) {

        Set<OptExpression> scanOpsLeadingByFrontier = scanOps.get(frontier);
        ColumnRefSet originColumnSet = new ColumnRefSet();
        frontier.getRowOutputInfo().getColumnRefOps().forEach(colRef ->
                originColumnSet.union(columnOrigins.get(colRef.getId())));

        List<CPNode> nodes = Stream.concat(optToGraphNode.values().stream(), hubNodes.stream())
                .filter(node -> node.isRoot() && node.intersect(scanOpsLeadingByFrontier))
                .collect(Collectors.toList());
        for (CPNode node : nodes) {
            pruneTable(node, prunedTables);
        }
    }

    private static class PruneResult {
        Map<ColumnRefOperator, Set<ColumnRefOperator>> rewriteMapping = Maps.newHashMap();
        boolean pruned = true;

        private PruneResult(Map<ColumnRefOperator, Set<ColumnRefOperator>> rewriteMapping, boolean pruned) {
            this.rewriteMapping = rewriteMapping;
            this.pruned = pruned;
        }

        public PruneResult() {
        }

        public PruneResult pruned(Map<ColumnRefOperator, Set<ColumnRefOperator>> rewriteMapping) {
            return new PruneResult(rewriteMapping, true);
        }

        void merge(PruneResult other) {
            this.pruned &= other.pruned;
            other.rewriteMapping.forEach((k, v) -> {
                this.rewriteMapping.merge(k, v, Sets::union);
            });
        }

        public boolean isPruned() {
            return pruned;
        }

        public Map<ColumnRefOperator, Set<ColumnRefOperator>> getRewriteMapping() {
            return rewriteMapping;
        }

        public PruneResult toUnpruned() {
            if (!this.isPruned()) {
                return this;
            } else {
                return new PruneResult(this.rewriteMapping, false);
            }
        }
    }

    private PruneResult pruneTable(CPNode root, Set<OptExpression> prunedTables) {
        // visit children of current GraphNode in post-order order.
        if (root.isHub()) {
            // try to prune non-cardinality-preserving children
            PruneResult pruneResult = new PruneResult();
            for (CPNode child : root.getNonCPChildren()) {
                PruneResult childPruneResult = pruneTable(child, prunedTables);
                pruneResult.merge(pruneResult);
            }

            // construct equivalent classes from equivalent ColumnRefOperator pairs of
            // cardinality-preserving children.
            UnionFind<ColumnRefOperator> localEqClasses = new UnionFind<>();
            for (CPNode child : root.getChildren()) {
                Preconditions.checkArgument(!child.isHub());
                child.getEquivColumnRefs().values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .forEach(e -> localEqClasses.union(e.getKey(), e.getValue()));
            }

            boolean prunable = pruneResult.isPruned();
            // try to prune cardinality-preserving children
            for (CPNode child : root.getChildren()) {
                LogicalScanOperator scanOperator = child.getValue().getOp().cast();
                Set<ColumnRefOperator> outputColRefs = scanOperator.getColRefToColumnMetaMap().keySet();
                // if there exists output columns of current child that can not be remapped
                // to other equivalent column, the child can be prunable.
                if (!outputColRefs.isEmpty() &&
                        outputColRefs.stream()
                                .anyMatch(colRef -> localEqClasses.getEquivGroup(colRef).size() < 2)) {
                    prunable = false;
                    continue;
                }

                // substitute remapped columns with its other equivalent columns
                for (ColumnRefOperator colRef : outputColRefs) {
                    ColumnRefOperator substColRef = Sets.difference(
                            localEqClasses.getEquivGroup(colRef),
                            Collections.singleton(colRef)).iterator().next();

                    Set<ColumnRefOperator> originalColRefs = pruneResult.getRewriteMapping().remove(colRef);
                    if (originalColRefs == null) {
                        originalColRefs = Sets.newHashSet();
                    }
                    originalColRefs.add(colRef);
                    pruneResult.getRewriteMapping().put(substColRef, originalColRefs);
                }

                prunedTables.add(child.getValue());
                // all columns of the current child should be removed from equivalent classes
                // to prevent output columns of sequential child from being remapped to it.
                localEqClasses.removesAll(outputColRefs);
            }
            if (!prunable) {
                return pruneResult.toUnpruned();
            } else {
                return pruneResult;
            }
        } else {
            // parent can not be pruned if any of its offsprings is can not be pruned
            // NOTICE: if (!root.getChildren().stream().allMatch(this::pruneTable)) can
            // short-circuit.
            PruneResult pruneResult = new PruneResult();
            for (CPNode child : root.getChildren()) {
                PruneResult childPruneResult = pruneTable(child, prunedTables);
                pruneResult.merge(childPruneResult);
            }
            if (pruneResult.isPruned() || root.isRoot()) {
                return pruneResult.toUnpruned();
            }

            LogicalScanOperator scanOperator = root.getValue().getOp().cast();
            Set<ColumnRefOperator> outputColRefs = scanOperator.getColRefToColumnMetaMap().keySet();
            Preconditions.checkArgument(outputColRefs.containsAll(pruneResult.getRewriteMapping().keySet()));

            Map<ColumnRefOperator, ColumnRefOperator> eqColRefMap = root.getEquivColumnRefs()
                    .entrySet().stream().flatMap(m -> m.getValue().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // eqColRefs.isEmpty() is true means current ScanOperator is rhs of left join,
            // so if ScanOperator carries predicate(even constant predicate), it can not be pruned.
            if (eqColRefMap.isEmpty() && scanOperator.getPredicate() != null) {
                return pruneResult.toUnpruned();
            }

            Preconditions.checkArgument(scanOperator.getPredicate() == null ||
                    new ColumnRefSet(outputColRefs).containsAll(scanOperator.getPredicate().getUsedColumns()));

            if (!eqColRefMap.keySet().containsAll(outputColRefs)) {
                return pruneResult.toUnpruned();
            }
            for (ColumnRefOperator colRef : outputColRefs) {
                ColumnRefOperator substColRef = eqColRefMap.get(colRef);
                Set<ColumnRefOperator> originalColRefs = pruneResult.getRewriteMapping().remove(colRef);
                if (originalColRefs == null) {
                    originalColRefs = Sets.newHashSet();
                }
                originalColRefs.add(colRef);
                pruneResult.getRewriteMapping().put(substColRef, originalColRefs);
            }
            prunedTables.add(root.getValue());
            return pruneResult;
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

    public void rewrite() {
        cardinalityPreservingFrontiers.forEach((frontier, parentAndIdx) -> {
            OptExpression parent = parentAndIdx.first;
            int idx = parentAndIdx.second;
            Set<OptExpression> prunedTables = Sets.newHashSet();
            Map<ColumnRefOperator, ColumnRefOperator> rewriteMapping = Maps.newHashMap();
            pruneTables(frontier, prunedTables, rewriteMapping);
            new Rewriter(rewriteMapping, prunedTables).process(frontier)
                    .ifPresent(child -> parent.setChild(idx, child));
        });
    }

    public Optional<OptExpression> getRoot() {
        return optNewRoot;
    }
}
