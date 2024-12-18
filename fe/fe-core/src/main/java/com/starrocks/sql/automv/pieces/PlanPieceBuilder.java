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

package com.starrocks.sql.automv.pieces;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class PlanPieceBuilder extends OptExpressionVisitor<PlanPiece, PlanPieceBuildContext> {

    private static final PlanPieceBuilder INSTANCE = new PlanPieceBuilder();

    private PlanPieceBuilder() {
    }

    public static PlanPiece createPlanPiece(String name, OptExpression optExpression,
                                            ColumnRefToIdConverter idConverter,
                                            Map<String, FQTable> fqTableMap) {
        PlanPieceBuildContext context =
                PlanPieceBuildContext.of(idConverter, name, fqTableMap, Collections.emptyList());
        return PlanPieceBuilder.INSTANCE.build(optExpression, context);
    }

    private TieredMap<Integer, GenericColumn> convAggCalls(
            final Map<ColumnRefOperator, CallOperator> aggCalls,
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {
        Function<ScalarOperator, GenericColumn> columnConverter =
                OpUtil.toColumnConverter(idConverter, inputColumns);
        return aggCalls.entrySet().stream().collect(TieredMap.toMap(
                e -> idConverter.getId(e.getKey()),
                e -> columnConverter.apply(e.getValue())));
    }

    private TieredMap<Integer, GenericColumn> convColumnRefs(
            final Collection<ColumnRefOperator> colRefs,
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {

        Function<ScalarOperator, GenericColumn> columnConverter =
                OpUtil.toColumnConverter(idConverter, inputColumns);
        return colRefs.stream().collect(TieredMap.toMap(
                idConverter::getId,
                c -> columnConverter.apply(c)));
    }

    private TieredMap<Integer, GenericColumn> convColumnRefMap(
            final Map<ColumnRefOperator, ? extends ScalarOperator> columnRefMap,
            final ColumnRefToIdConverter idConverter,
            final Map<Integer, GenericColumn> inputColumns) {

        Function<ScalarOperator, GenericColumn> columnConverter =
                OpUtil.toColumnConverter(idConverter, inputColumns);
        return columnRefMap.entrySet()
                .stream()
                .filter(e -> !(e.getValue().isColumnRef() && e.getValue().equals(e.getKey())))
                .collect(TieredMap.toMap(
                        e -> idConverter.getId(e.getKey()),
                        e -> columnConverter.apply(e.getValue())));
    }

    private TieredMap<Integer, GenericColumn> handleProject(Operator op,
                                                            Map<Integer, GenericColumn> originColumns,
                                                            ColumnRefToIdConverter idConverter) {
        return Optional.ofNullable(op.getProjection())
                .map(Projection::getColumnRefMap)
                .map(colRefMap -> convColumnRefMap(colRefMap, idConverter, originColumns))
                .orElse(TieredMap.genesis());
    }

    private TieredList<Op> handlePredicate(ScalarOperator predicate,
                                           Map<Integer, GenericColumn> inputColumns,
                                           ColumnRefToIdConverter idConverter) {
        if (predicate == null) {
            return TieredList.genesis();
        } else {
            return OpUtil.toOpConverter(idConverter, inputColumns)
                    .apply(predicate).conjuncts().stream()
                    .collect(TieredList.toList());
        }
    }

    @Override
    public PlanPiece visitLogicalTableScan(OptExpression optExpression, PlanPieceBuildContext args) {
        LogicalScanOperator tableScan = optExpression.getOp().cast();
        ColumnRefToIdConverter idConverter = args.getIdConverter();
        FQTable fqTable = args.getFqTableMap().get(tableScan.getTable().getUUID());
        Function<Column, GenericColumn> columnConverter = OpUtil.toOriginalColumnConverter(fqTable.getFqTableName());
        // Field pruning rule break norm computation of TableScan, so use columnMetaToColumnRefMap instead
        // of columnRefToColumnMetaMap, the former keep invariant after field pruning rule is applied.
        TieredMap<Integer, GenericColumn> originColumns =
                tableScan.getColumnMetaToColRefMap().entrySet()
                        .stream()
                        .collect(TieredMap.toMap(e -> idConverter.getId(e.getValue()),
                                e -> columnConverter.apply(e.getKey())));

        ColumnRefSet usedColumns = ColumnRefSet.of();
        tableScan.getColRefToColumnMetaMap().keySet()
                .stream()
                .map(idConverter::getId)
                .forEach(usedColumns::union);

        TieredMap<Integer, GenericColumn> derivedColumns =
                handleProject(optExpression.getOp(), originColumns, idConverter);
        TieredMap<Integer, GenericColumn> columns = originColumns.merge(derivedColumns);

        // PartitionPruneRule would mutate LogicalScanOperator.predicate, so we do not invoke
        // PartitionPruneRule when using customized RboOptimizer to generate LogicalPlan.
        TieredList<Op> conjuncts = handlePredicate(optExpression.getOp().getPredicate(), columns, idConverter);
        return TablePiece.newBuilder()
                .setTable(args.getFqTableMap().get(tableScan.getTable().getUUID()))
                .setUsedColumns(usedColumns)
                .setColumns(columns)
                .setCommonState(args.getCommonState())
                .setConjuncts(conjuncts).build();
    }

    PlanPiece handleProjectAndFilter(LogicalOperator operator, PlanPiece inputPiece,
                                     ColumnRefToIdConverter idConverter) {
        TieredMap<Integer, GenericColumn> columns = handleProject(operator, inputPiece.getColumns(), idConverter);
        TieredList<Op> conjuncts = handlePredicate(operator.getPredicate(), inputPiece.getColumns(), idConverter);
        columns = inputPiece.getColumns().merge(columns);
        conjuncts = inputPiece.getConjuncts().concat(conjuncts);

        return inputPiece.builder()
                .setColumns(columns)
                .setConjuncts(conjuncts)
                .build();
    }

    @Override
    public PlanPiece visitLogicalProject(OptExpression optExpression, PlanPieceBuildContext args) {
        return handleProjectAndFilter(optExpression.getOp().cast(), args.arg0(), args.getIdConverter());
    }

    @Override
    public PlanPiece visitLogicalFilter(OptExpression optExpression, PlanPieceBuildContext args) {
        return handleProjectAndFilter(optExpression.getOp().cast(), args.arg0(), args.getIdConverter());
    }

    private Pair<JoinOperator, List<PlanPiece>> applyCommutative(LogicalJoinOperator join, PlanPiece lhs,
                                                                 PlanPiece rhs) {
        JoinOperator joinType = join.getJoinType();
        if (joinType.isRightOuterJoin()) {
            return Pair.create(JoinOperator.LEFT_OUTER_JOIN, ImmutableList.of(rhs, lhs));
        } else if (joinType.isRightAntiJoin()) {
            return Pair.create(JoinOperator.LEFT_ANTI_JOIN, ImmutableList.of(rhs, lhs));
        } else if (joinType.isRightSemiJoin()) {
            return Pair.create(JoinOperator.LEFT_SEMI_JOIN, ImmutableList.of(rhs, lhs));
        } else {
            return Pair.create(joinType, ImmutableList.of(lhs, rhs));
        }
    }

    private PlanPiece createStarJoinImpl(
            PlanPiece centre, List<StarJoinPiece.StarCorner> corners,
            List<Op> hoistConjuncts, TieredMap<Integer, GenericColumn> inputColumns) {
        return StarJoinPiece.newBuilder()
                .setCentre(centre)
                .setCorners(corners)
                .setColumns(inputColumns)
                .setConjuncts(TieredList.<Op>genesis().concat(hoistConjuncts))
                .setCommonState(centre.getCommonState())
                .build();
    }

    private PlanPiece createStarJoin(
            JoinOperator joinType,
            PlanPiece lhsPiece,
            PlanPiece rhsPiece,
            List<Op> eqConjuncts,
            List<Op> otherConjuncts,
            List<Op> hoistConjuncts) {

        StarJoinPiece.StarCorner corner = new StarJoinPiece.StarCorner(eqConjuncts, otherConjuncts, joinType, rhsPiece);
        List<StarJoinPiece.StarCorner> corners = Collections.singletonList(corner);
        TieredMap<Integer, GenericColumn> inputColumns;
        if (joinType.isLeftSemiAntiJoin()) {
            inputColumns = lhsPiece.getColumns();
        } else {
            inputColumns = lhsPiece.getColumns().merge(rhsPiece.getColumns());
        }
        return createStarJoinImpl(lhsPiece, corners, hoistConjuncts, inputColumns);
    }

    private PlanPiece mergeStarJoin(
            JoinOperator joinType,
            LogicalOperator join,
            PlanPiece lhsPiece,
            PlanPiece rhsPiece,
            List<Op> eqConjuncts,
            List<Op> otherConjuncts,
            List<Op> hoistConjuncts) {
        Preconditions.checkArgument(lhsPiece.isStarJoin());
        StarJoinPiece lhsJoinPiece = (StarJoinPiece) lhsPiece;
        PlanPiece centre = lhsJoinPiece.getCentre();
        List<StarJoinPiece.StarCorner> corners = lhsJoinPiece.getCorners();
        StarJoinPiece.StarCorner corner = new StarJoinPiece.StarCorner(eqConjuncts, otherConjuncts, joinType, rhsPiece);
        List<StarJoinPiece.StarCorner> newCorners = Lists.newArrayList(corners);
        newCorners.add(corner);
        TieredMap<Integer, GenericColumn> inputColumns;
        if (joinType.isLeftSemiAntiJoin()) {
            inputColumns = lhsJoinPiece.getColumns();
        } else {
            inputColumns = lhsJoinPiece.getColumns().merge(rhsPiece.getColumns());
        }
        return createStarJoinImpl(centre, newCorners, hoistConjuncts, inputColumns);
    }

    private PlanPiece createOrMergeStarJoin(JoinOperator joinType, LogicalOperator join, StarJoinPiece lhsPiece,
                                            PlanPiece rhsPiece, List<Op> eqConjuncts, List<Op> otherConjuncts,
                                            List<Op> hoistConjuncts) {
        PlanPiece centre = lhsPiece.getCentre();
        ColumnRefSet colRefSet = new ColumnRefSet();
        colRefSet.union(ColumnRefSet.createByIds(centre.getColumns().keySet()));
        colRefSet.union(ColumnRefSet.createByIds(rhsPiece.getColumns().keySet()));
        ColumnRefSet joinColRefSet = new ColumnRefSet();
        eqConjuncts.forEach(op -> joinColRefSet.union(op.getIds()));
        otherConjuncts.forEach(op -> joinColRefSet.union(op.getIds()));
        boolean canMerge = colRefSet.containsAll(joinColRefSet);
        if (canMerge) {
            return mergeStarJoin(joinType, join, lhsPiece, rhsPiece, eqConjuncts, otherConjuncts,
                    hoistConjuncts);
        } else {
            return createStarJoin(joinType, lhsPiece, rhsPiece, eqConjuncts, otherConjuncts,
                    hoistConjuncts);
        }
    }

    @Override
    public PlanPiece visitLogicalJoin(OptExpression optExpression, PlanPieceBuildContext args) {
        LogicalJoinOperator join = optExpression.getOp().cast();
        Pair<JoinOperator, List<PlanPiece>> typeAndArgs = applyCommutative(join, args.arg0(), args.arg1());
        JoinOperator joinType = typeAndArgs.first;
        PlanPiece lhsPiece = typeAndArgs.second.get(0);
        PlanPiece rhsPiece = typeAndArgs.second.get(1);
        TieredMap<Integer, GenericColumn> inputColumns =
                rhsPiece.getColumns().merge(lhsPiece.getColumns());

        ColumnRefToIdConverter idConverter = args.getIdConverter();
        // Predicates hoisting
        TieredList<Op> onConjuncts = handlePredicate(join.getOnPredicate(), inputColumns, idConverter);
        Map<Boolean, List<Op>> conjGroups = onConjuncts.stream().collect(Collectors.partitioningBy(Op::isVEV));
        ColumnRefSet lhsIds = ColumnRefSet.createByIds(lhsPiece.getColumns().keySet());
        ColumnRefSet rhsIds = ColumnRefSet.createByIds(rhsPiece.getColumns().keySet());
        Function<Op, Op> vevSwapper = Op.toVEVSwapper(lhsIds, rhsIds);
        List<Op> eqConjuncts = conjGroups.get(true).stream()
                .map(vevSwapper)
                .collect(Collectors.toList());

        List<Op> otherConjuncts = conjGroups.get(false);

        List<HoistTriple> hoistTriples =
                HoistTriple.prepare(eqConjuncts, otherConjuncts, lhsPiece.getConjuncts(), rhsPiece.getConjuncts());

        List<Op> hoistConjuncts = Lists.newArrayList();
        List<Op> newOnConjuncts = Lists.newArrayList();
        List<Op> lhsConjuncts = Lists.newArrayList();
        List<Op> rhsConjuncts = Lists.newArrayList();

        for (HoistTriple triple : hoistTriples) {
            HoistFunction.get(joinType).apply(triple, lhsConjuncts, rhsConjuncts, newOnConjuncts, hoistConjuncts);
        }

        // Trivial IS_NOT_NULL elimination
        List<Op> allConjuncts = Lists.newArrayList(Iterables.concat(eqConjuncts, hoistConjuncts, newOnConjuncts));
        ColumnRefSet eraseIds = OpUtil.eliminateTrivialIsNotNull(allConjuncts);

        Predicate<Op> shouldRetain = op -> !op.isVarIsNotNull() || !eraseIds.contains(op.unmodified().getId());
        newOnConjuncts = newOnConjuncts.stream().filter(shouldRetain).collect(Collectors.toList());
        hoistConjuncts = hoistConjuncts.stream().filter(shouldRetain).collect(Collectors.toList());

        lhsPiece = lhsPiece.setConjuncts(TieredList.<Op>genesis().concat(lhsConjuncts));
        rhsPiece = rhsPiece.setConjuncts(TieredList.<Op>genesis().concat(rhsConjuncts));

        // create/merge StarJoin Piece
        PlanPiece starJoinPiece;
        if (joinType.isFullOuterJoin() ||
                joinType.isLeftOuterJoin() ||
                (!lhsPiece.isStarJoin() && !rhsPiece.isStarJoin()) ||
                (!lhsPiece.isStarJoin() && joinType.isLeftSemiAntiJoin())) {
            starJoinPiece = createStarJoin(joinType, lhsPiece, rhsPiece, eqConjuncts, newOnConjuncts, hoistConjuncts);
        } else if (lhsPiece.isStarJoin()) {
            StarJoinPiece joinPiece = (StarJoinPiece) lhsPiece;
            starJoinPiece = createOrMergeStarJoin(joinType, join, joinPiece, rhsPiece, eqConjuncts, newOnConjuncts,
                    hoistConjuncts);
        } else if (rhsPiece.isStarJoin() && join.isInnerOrCrossJoin()) {
            StarJoinPiece joinPiece = (StarJoinPiece) rhsPiece;
            starJoinPiece = createOrMergeStarJoin(joinType, join, joinPiece, lhsPiece, eqConjuncts, newOnConjuncts,
                    hoistConjuncts);
        } else {
            starJoinPiece = createStarJoin(joinType, lhsPiece, rhsPiece, eqConjuncts, newOnConjuncts, hoistConjuncts);
        }
        return handleProjectAndFilter(join, starJoinPiece, idConverter);
    }

    @Override
    public PlanPiece visitLogicalAggregate(OptExpression optExpression, PlanPieceBuildContext args) {
        LogicalAggregationOperator agg = optExpression.getOp().cast();
        PlanPiece flatTablePiece = args.arg0();
        ColumnRefToIdConverter idConverter = args.getIdConverter();
        TieredMap<Integer, GenericColumn> dimensions =
                convColumnRefs(agg.getGroupingKeys(), idConverter, flatTablePiece.getColumns());

        ColumnRefSet groupKeyColRefSet = ColumnRefSet.createByIds(dimensions.keySet());
        Map<Boolean, TieredList<Op>> conjGroups = flatTablePiece.getConjuncts().stream().collect(
                Collectors.partitioningBy(op -> groupKeyColRefSet.containsAll(op.getIds()), TieredList.<Op>toList()));

        TieredList<Op> hoistConjuncts = conjGroups.get(true);
        TieredList<Op> nonHoistConjuncts = conjGroups.get(false);
        Map<Boolean, TieredList<Op>> nonHoistConjunctGroups = nonHoistConjuncts
                .stream()
                .collect(Collectors.partitioningBy(OpUtil::isStiffPredicate, TieredList.<Op>toList()));

        TieredList<Op> stiffConjuncts = nonHoistConjunctGroups.get(true);
        TieredList<Op> flexibleConjuncts = nonHoistConjunctGroups.get(false);

        Set<Integer> rollupColumnRefs = flexibleConjuncts.stream().flatMap(op -> op.getIdSet().stream())
                .filter(colRef -> !groupKeyColRefSet.contains(colRef)).collect(Collectors.toSet());

        TieredMap<Integer, GenericColumn> rollupDimension = flatTablePiece.getColumns().entrySet()
                .stream().filter(e -> rollupColumnRefs.contains(e.getKey()))
                .collect(TieredMap.toMap());

        TieredMap<Integer, GenericColumn> allMetrics =
                convAggCalls(agg.getAggregations(), idConverter, flatTablePiece.getColumns());
        Map<Boolean, TieredMap<Integer, GenericColumn>> metricsGroup = allMetrics.entrySet()
                .stream()
                .collect(Collectors.partitioningBy(e -> OpUtil.isDistinct(e.getValue()), TieredMap.toMap()));

        TieredMap<Integer, GenericColumn> distinctMetrics = metricsGroup.get(true);

        TieredMap<Integer, GenericColumn> metrics = metricsGroup.get(false);
        flatTablePiece = flatTablePiece.setConjuncts(TieredList.<Op>genesis());
        AggregatePiece.FlatTable flatTable =
                new AggregatePiece.FlatTable(flatTablePiece, stiffConjuncts, flexibleConjuncts);

        AggregatePiece aggPiece = AggregatePiece.newBuilder()
                .setFlatTable(flatTable)
                .setDimensions(dimensions)
                .setRollupDimensions(rollupDimension)
                .setMetrics(metrics)
                .setDistinctMetrics(distinctMetrics)
                .setHoistConjuncts(hoistConjuncts)
                .setNonHoistConjuncts(nonHoistConjuncts)
                .setConjuncts(TieredList.genesis())
                .setCommonState(args.getCommonState())
                .build().cast();

        return handleProjectAndFilter(agg, aggPiece, idConverter);
    }

    private PlanPiece build(OptExpression optExpression, PlanPieceBuildContext context) {
        List<PlanPiece> inputPieces =
                optExpression.getInputs().stream().map(child -> build(child, context)).collect(Collectors.toList());
        PlanPieceBuildContext newContext = context.newContextWithPieces(inputPieces);
        return optExpression.getOp().accept(this, optExpression, newContext);
    }
}
