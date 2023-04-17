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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CPJoinGardener extends OptExpressionVisitor<Boolean, Void> {
    ColumnRefFactory columnRefFactory;
    Map<ColumnRefOperator, OptExpression> columnToScans = Maps.newHashMap();
    UnionFind<ColumnRefOperator> columnRefEquivClasses = new UnionFind<>();
    Map<OptExpression, RoaringBitmap> cpScanOps = Maps.newHashMap();
    Map<OptExpression, RoaringBitmap> scanOps = Maps.newHashMap();
    Map<OptExpression, Pair<OptExpression, Integer>> cpFrontiers = Maps.newHashMap();
    Set<CPEdge> cpEdges = Sets.newHashSet();
    Map<OptExpression, CPNode> optToGraphNode = Maps.newHashMap();
    Set<CPNode> hubNodes = Sets.newHashSet();
    Map<Integer, ColumnRefSet> columnOrigins = Maps.newHashMap();
    Map<OptExpression, Set<ColumnRefOperator>> uniqueKeyColRefs = Maps.newHashMap();
    Map<ColumnRefOperator, Map<OptExpression, ColumnRefOperator>> foreignKeyColRefs = Maps.newHashMap();

    private final Map<OptExpression, Integer> scanNodeOrdinals = Maps.newHashMap();
    private final List<OptExpression> ordinalToScanNodes = Lists.newArrayList();

    private Optional<Long> updateTableId = Optional.empty();

    public CPJoinGardener(
            ColumnRefFactory columnRefFactory,
            long updateTableId) {
        this.columnRefFactory = columnRefFactory;
        if (updateTableId > 0) {
            this.updateTableId = Optional.of(updateTableId);
        }
    }

    public static class CPBiRel {
        private final OptExpression lhs;
        private final OptExpression rhs;
        private final boolean fromForeignKey;
        private final boolean leftToRight;
        private final Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs;

        public CPBiRel(
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
                scalarOp.getUsedColumns().getStream()
                        .forEach(id -> columnRefSet.union(columnOrigins.getOrDefault(id, new ColumnRefSet())));
                columnOrigins.put(col.getId(), columnRefSet);
            }
        });
    }

    public static boolean isForeignKeyConstraintReferenceToUniqueKey(
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

    public static List<CPBiRel> getCardinalityPreserving(OptExpression lhs, OptExpression rhs,
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
        List<CPBiRel> biRels = Lists.newArrayList();
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
                            biRels.add(
                                    new CPBiRel(lhs, rhs, true, leftToRight, fkColumnRefPairs));
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
                            biRels.add(
                                    new CPBiRel(lhs, rhs, false, leftToRight,
                                            ukColumnRefPairs)));
        }
        return biRels;
    }

    Stream<Integer> getIntStream(RoaringBitmap bitmap) {
        Spliterator<Integer> iter = Spliterators.spliteratorUnknownSize(bitmap.iterator(), Spliterator.ORDERED);
        return StreamSupport.stream(iter, false);
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
                    getIntStream(scanOps.get(lhs)).map(ordinalToScanNodes::get)
                            .filter(candidateLhsScanOpSet::contains)
                            .flatMap(lhsScan -> getIntStream(cpScanOps.get(rhs)).map(
                                            ordinalToScanNodes::get)
                                    .filter(candidateRhsScanOpSet::contains)
                                    .flatMap(rhsScan ->
                                            getCardinalityPreserving(lhsScan, rhsScan, true).stream()
                                    )).collect(Collectors.toList());
            biRels.addAll(leftToRightBiRels);
        }
        if ((joinOp.getJoinType().isInnerJoin() || joinOp.getJoinType().isRightOuterJoin()) &&
                scanOps.containsKey(rhs) && cpScanOps.containsKey(lhs)) {
            List<CPBiRel> rightToLeftBiRels =
                    getIntStream(scanOps.get(rhs)).map(ordinalToScanNodes::get)
                            .filter(candidateRhsScanOpSet::contains)
                            .flatMap(rhsScan -> getIntStream(cpScanOps.get(lhs)).map(
                                            ordinalToScanNodes::get)
                                    .filter(candidateLhsScanOpSet::contains)
                                    .flatMap(lhsScan ->
                                            getCardinalityPreserving(rhsScan, lhsScan, false).stream()
                                    )).collect(Collectors.toList());
            biRels.addAll(rightToLeftBiRels);
        }
        return biRels;
    }

    boolean collectCpNodesForPKTableUpdate(OptExpression joinOpt, OptExpression fromChild) {
        RoaringBitmap cpNodes = cpScanOps.getOrDefault(fromChild, RoaringBitmap.bitmapOf());
        scanOps.put(joinOpt, RoaringBitmap.bitmapOf());
        if (updateTableId.isPresent() && !cpNodes.isEmpty()) {
            RoaringBitmap joinCpNodes = RoaringBitmap.bitmapOf();
            getIntStream(cpNodes).map(ordinal -> Pair.create(ordinal, ordinalToScanNodes.get(ordinal)))
                    .filter(e -> ((LogicalScanOperator) e.second.getOp()).getTable().getId() == updateTableId.get())
                    .forEach(e -> joinCpNodes.add(e.first));
            cpScanOps.put(joinOpt, joinCpNodes);
            return true;
        } else {
            return visit(joinOpt, null);
        }
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

        Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> onPredicates =
                JoinHelper.separateEqualPredicatesFromOthers(optExpression);
        List<BinaryPredicateOperator> eqOnPredicates = onPredicates.first;
        List<ScalarOperator> otherOnPredicates = onPredicates.second;

        if (!otherOnPredicates.isEmpty() || eqOnPredicates.isEmpty()) {
            return visit(optExpression, context);
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

        List<CPBiRel> biRels =
                getCardinalityPreserving(optExpression, candidateLhsScanOpSet, candidateRhsScanOpSet);

        if (biRels.isEmpty()) {
            OptExpression lhsChild = optExpression.inputAt(0);
            OptExpression rhsChild = optExpression.inputAt(1);
            if (joinType.isLeftOuterJoin()) {
                return collectCpNodesForPKTableUpdate(optExpression, lhsChild);
            } else if (joinType.isRightJoin()) {
                return collectCpNodesForPKTableUpdate(optExpression, rhsChild);
            } else if (joinType.isInnerJoin()) {
                return collectCpNodesForPKTableUpdate(optExpression, lhsChild) ||
                        collectCpNodesForPKTableUpdate(optExpression, rhsChild);
            }
        }

        for (CPBiRel biRel : biRels) {
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
                            .collect(Collectors.toSet());

            joinColumnEqClasses =
                    eqColumnRefPairs.stream().map(biCol ->
                            Pair.create(
                                    columnRefEquivClasses.getGroupIdOrAdd(biCol.first),
                                    columnRefEquivClasses.getGroupIdOrAdd(biCol.second))
                    ).collect(Collectors.toSet());

            Set<Pair<Integer, Integer>> symDiff = Sets.symmetricDifference(joinColumnEqClasses, groupIdPairs);
            if (!symDiff.isEmpty() && symDiff.stream().anyMatch(p -> !Objects.equals(p.first, p.second))) {
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
            RoaringBitmap lhsCardPreservingScanOps =
                    cpScanOps.get(lhsOpt);
            RoaringBitmap rhsCardPreservingScanOps =
                    cpScanOps.get(rhsOpt);

            RoaringBitmap lhsScanOps = scanOps.get(lhsOpt);
            RoaringBitmap rhsScanOps = scanOps.get(rhsOpt);

            scanOps.put(optExpression, RoaringBitmap.or(lhsScanOps, rhsScanOps));

            RoaringBitmap currCPScanOps;
            if (!biRel.isFromForeignKey()) {
                cpEdges.add(
                        new CPEdge(biRel.getLhs(), biRel.getRhs(), false, eqColumnRefs));
                currCPScanOps = RoaringBitmap.or(lhsCardPreservingScanOps, rhsCardPreservingScanOps);
                uniqueKeyColRefs.computeIfAbsent(biRel.getLhs(), (k) -> Sets.newHashSet())
                        .addAll(pairs.stream().map(p -> p.first).collect(Collectors.toList()));
                uniqueKeyColRefs.computeIfAbsent(biRel.getRhs(), (k) -> Sets.newHashSet())
                        .addAll(pairs.stream().map(p -> p.second).collect(Collectors.toList()));
                pairs.forEach(p -> {
                    foreignKeyColRefs.computeIfAbsent(p.first, (k) -> Maps.newHashMap()).put(biRel.rhs, p.second);
                    foreignKeyColRefs.computeIfAbsent(p.second, (k) -> Maps.newHashMap()).put(biRel.lhs, p.first);
                });
            } else {
                CPEdge edge = new CPEdge(biRel.getLhs(), biRel.getRhs(), true, eqColumnRefs);
                currCPScanOps = biRel.isLeftToRight() ? lhsCardPreservingScanOps : rhsCardPreservingScanOps;
                uniqueKeyColRefs.computeIfAbsent(biRel.getRhs(), (k) -> Sets.newHashSet())
                        .addAll(pairs.stream().map(p -> p.second).collect(Collectors.toList()));
                pairs.forEach(p -> {
                    foreignKeyColRefs.computeIfAbsent(p.second, (k) -> Maps.newHashMap()).put(biRel.lhs, p.first);
                });
                cpEdges.add(edge);
            }

            cpScanOps.merge(optExpression, currCPScanOps, (a, b) -> RoaringBitmap.or(a, b));
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
        return updateTableId.isPresent();
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
        boolean isUpdateTarget = !updateTableId.isPresent() || updateTableId.get() == table.getId();
        // A Table that has no PK/UK/FK can not associate with other tables via
        // cardinality-preserving relations.
        if (!table.hasUniqueConstraints() && !table.hasForeignKeyConstraints() && !isUpdateTarget) {
            return visit(optExpression, context);
        }

        scanOp.getColRefToColumnMetaMap().keySet().forEach(col -> {
            columnOrigins.put(col.getId(), new ColumnRefSet(col.getId()));
            columnToScans.put(col, optExpression);
        });

        int ordinal = scanNodeOrdinals.size();
        scanNodeOrdinals.put(optExpression, ordinal);
        ordinalToScanNodes.add(optExpression);
        computeOriginsOfColumnRefs(optExpression);
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

    public static final class CPNode {
        OptExpression value;
        CPNode parent;
        Set<CPNode> children = Collections.emptySet();

        Map<OptExpression, Map<ColumnRefOperator, ColumnRefOperator>> equivColumnRefs = Collections.emptyMap();
        boolean hubFlag;

        Set<CPNode> nonCPChildren = Collections.emptySet();

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
                children = Sets.newHashSet();
            }
            children.add(child);
            child.setParent(this);
        }

        public void addNonCPChild(CPNode child) {
            Preconditions.checkArgument(hubFlag);
            if (nonCPChildren.isEmpty()) {
                nonCPChildren = Sets.newHashSet();
            }
            nonCPChildren.add(child);
            child.setParent(this);
        }

        public static Optional<CPNode> mergeHubNode(CPNode lhsNode, CPNode rhsNode) {
            if (lhsNode == rhsNode) {
                return Optional.empty();
            }
            rhsNode.children.forEach(lhsNode::addChild);
            rhsNode.nonCPChildren.forEach(lhsNode::addNonCPChild);
            return Optional.of(rhsNode);
        }

        public Set<CPNode> getChildren() {
            return children;
        }

        public Set<CPNode> getNonCPChildren() {
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

    private void plantCPTree() {
        Set<CPEdge> edges = Sets.newHashSet();
        for (CPEdge edge : cpEdges) {
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
        List<CPEdge> unilateralEdges = Lists.newArrayList();
        List<CPEdge> bilateralEdges = Lists.newArrayList();
        for (CPEdge e : edges) {
            if (e.isUnilateral()) {
                unilateralEdges.add(e);
            } else {
                bilateralEdges.add(e);
            }
        }
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

    public void collect(OptExpression root) {
        process(root);
        if (updateTableId.isPresent()) {
            cpFrontiers = cpFrontiers.entrySet().stream()
                    .filter(e -> e.getKey() == root.inputAt(0))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        if (cpFrontiers.isEmpty()) {
            return;
        }
        plantCPTree();
    }

    public PruneContext markPrunedTables(OptExpression frontier) {
        Set<OptExpression> scanOpsLeadingByFrontier =
                getIntStream(scanOps.get(frontier)).map(ordinalToScanNodes::get)
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
            other.rewriteMapping.forEach((k, v) -> {
                this.rewriteMapping.merge(k, v, Sets::union);
            });
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

    private PruneContext markPrunedTable(CPNode root, ColumnRefSet originalColRefSet) {
        // visit children of current GraphNode in post-order order.
        PruneContext pruneContext = new PruneContext();
        if (root.isHub()) {
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
            List<CPNode> children = root.getChildren().stream()
                    .map(child -> Pair.create(child, scanNodeOrdinals.get(child.getValue())))
                    .sorted(Pair.comparingBySecond()).map(p -> p.first).collect(Collectors.toList());
            List<List<CPNode>> childGroups = Lists.newArrayList();

            BiFunction<OptExpression, OptExpression, Boolean> isSameTable = (a, b) ->
                    ((LogicalScanOperator) a.getOp()).getTable().getId() ==
                            ((LogicalScanOperator) b.getOp()).getTable().getId();

            CPNode prevChild = null;
            for (CPNode child : children) {
                if (prevChild == null || !isSameTable.apply(prevChild.getValue(), child.getValue())) {
                    childGroups.add(Lists.newArrayList(child));
                } else {
                    childGroups.get(childGroups.size() - 1).add(child);
                }
                prevChild = child;
            }

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
                boolean isLastGroup = (g == childGroups.size() - 1);
                if (!pkColRefs.containsAll(outputColRefs)) {
                    pruneContext.toUnpruned(outputColRefs, pkColRefs);
                } else {
                    List<CPNode> excludingNodes = isLastGroup ? children : group;
                    Set<OptExpression> excludingScanNodes =
                            excludingNodes.stream().map(CPNode::getValue).collect(Collectors.toSet());

                    Optional<Map<ColumnRefOperator, ColumnRefOperator>> optEqColRefs =
                            lastNode.getEquivColumnRefs().entrySet().stream()
                                    .filter(e -> !excludingScanNodes.contains(e.getKey()))
                                    .findFirst().map(Map.Entry::getValue);
                    if (optEqColRefs.isPresent() && optEqColRefs.get().keySet().containsAll(outputColRefs)) {
                        pruneContext.rewrite(outputColRefs, optEqColRefs.get());
                        pruneContext.addPrunedTable(lastNode.getValue());
                    } else {
                        pruneContext.toUnpruned(outputColRefs, pkColRefs);
                    }
                }
            }
        } else {
            for (CPNode child : root.getChildren()) {
                pruneContext.merge(markPrunedTable(child, originalColRefSet));
            }

            Set<ColumnRefOperator> pkColRefs = uniqueKeyColRefs.getOrDefault(root.getValue(), Collections.emptySet());

            Set<ColumnRefOperator> outputColRefs =
                    pruneContext.getOutputColRefs(root.getValue(), originalColRefSet, pkColRefs);

            //getOutputColRefs(root, originalColRefSet, pruneResult.rewriteMapping);
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
        }
        return pruneContext;
    }

    public static class Pruner extends OptExpressionVisitor<Optional<OptExpression>, Void> {
        private final Map<ColumnRefOperator, ScalarOperator> remapping;
        private final Set<ColumnRefOperator> substColRefs;
        private final ReplaceColumnRefRewriter columnRefRewriter;
        private final Set<OptExpression> prunedTables;

        private final List<ScalarOperator> prunedPredicates = Lists.newArrayList();
        private final Map<ColumnRefOperator, ScalarOperator> prunedColRefMap = Maps.newHashMap();

        public Pruner(Map<ColumnRefOperator, ScalarOperator> remapping, Set<OptExpression> prunedTables,
                      Set<ColumnRefOperator> substColRefs) {
            this.remapping = remapping;
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
                Operator newChild = ((Operator.Builder) OperatorBuilderFactory.build(child.getOp()))
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
        private List<ScalarOperator> predicates;
        private Map<ColumnRefOperator, ScalarOperator> colRefMap;

        public Grafter(List<ScalarOperator> predicates, Map<ColumnRefOperator, ScalarOperator> colRefMap) {
            this.predicates = predicates;
            this.colRefMap = colRefMap;
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
            this.predicates.forEach(predicate -> {
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
                this.predicates = remainPredicates;
                Operator operator = ((LogicalScanOperator.Builder) OperatorBuilderFactory.build(scanOperator))
                        .withOperator(scanOperator).setPredicate(predicate).build();
                scanOpt = OptExpression.create(operator, Collections.emptyList());
            }

            Map<ColumnRefOperator, ScalarOperator> remainColRefMap = Maps.newHashMap();
            Map<ColumnRefOperator, ScalarOperator> selectedColRefMap = Maps.newHashMap();
            this.colRefMap.forEach((k, v) -> {
                if (outputColumnRefSet.containsAll(v.getUsedColumns())) {
                    selectedColRefMap.put(k, v);
                } else {
                    remainColRefMap.put(k, v);
                }
            });

            if (!selectedColRefMap.isEmpty()) {
                this.colRefMap = remainColRefMap;
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
            // step2: (Bottom-up) prune the sub-plan leading by the frontier, the ColumnRefOperators that escape from
            // the pruned tables substituted by equivalent ColumnRefOperators originates from retained tables.
            // the ScalarOperators in projection and predicate of pruned tables are collected.
            Map<ColumnRefOperator, ScalarOperator> rewriteMapping = Maps.newHashMap();
            Map<ColumnRefOperator, ScalarOperator> colRefMap = frontier.getRowOutputInfo().getColumnRefMap();
            pruneContext.getRewriteMapping().forEach((substColRef, originalColRefs) ->
                    originalColRefs.forEach(colRef -> rewriteMapping.put(colRef, substColRef)));
            Pruner pruner = new Pruner(rewriteMapping,
                    pruneContext.getPrunedTables(), pruneContext.rewriteMapping.keySet());
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
    }
}
