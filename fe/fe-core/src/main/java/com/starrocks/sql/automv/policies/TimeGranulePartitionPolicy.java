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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.generator.PartitionPolicy;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PieceCommonState;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.StrictOp;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.PartitionPlus;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// TimeGranulePartitionPolicy is used to pick up a partition-by column for MV. At
// first, it tries to use an already-existing time granules which reside in AggregatePiece's
// dimensions or rollupDimensions preferentially, if not exists. it tries to construct
// a default time granule as MV's partition-by column using the base tables' partition-by columns
// which reside in flat table's output columns.
// When multiple time granules are available for partition-by columns, we choose the most
// coarse-grained one.
public class TimeGranulePartitionPolicy extends AggregatePolicy.SimplePolicy {

    // For default time granule's coarseness, we only support HOUR, DAY, MONTH, QUARTER and YEAR.
    // TODO(by satanson): at present, weekly-partition MV is not supported, so there is no
    //  WEEK time granule.
    private static final Set<TimeGranule.Unit> POLICY_SET = ImmutableSet.of(
            TimeGranule.Unit.HOUR,
            TimeGranule.Unit.DAY,
            TimeGranule.Unit.MONTH,
            TimeGranule.Unit.QUARTER,
            TimeGranule.Unit.YEAR);

    private final PartitionExtractor partitionExtractor;
    private final TimeGranule.Unit defaultTimeGranuleUnit;

    private TimeGranulePartitionPolicy(PartitionExtractor partitionExtractor, TimeGranule.Unit defaultTimeGranuleUnit) {
        this.partitionExtractor = partitionExtractor;
        this.defaultTimeGranuleUnit = defaultTimeGranuleUnit;
    }

    public static AbstractAggregatePolicy resolvePolicy(PartitionExtractor extractor, String timeGranuleUnit) {
        try {
            TimeGranule.Unit unit = TimeGranule.Unit.valueOf(timeGranuleUnit.toUpperCase());
            if (POLICY_SET.contains(unit)) {
                return new TimeGranulePartitionPolicy(extractor, unit);
            } else {
                return IDENTITY_POLICY;
            }
        } catch (IllegalArgumentException ignored) {
            return IDENTITY_POLICY;
        }
    }

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        Preconditions.checkArgument(aggPiece.getDistinctMetrics().isEmpty());
        Preconditions.checkArgument(aggPiece.getMetrics().values().stream().allMatch(AggregatePolicies::isRollupAble));

        AggregatePiece.FlatTable flatTable = aggPiece.getFlatTable();
        PlanPiece flatTablePiece = flatTable.getPiece();
        // extract all partition-by columns from base tables.
        List<PartitionPlus> ppList = aggPiece.getPartitionColumns(partitionExtractor).stream()
                .filter(p -> !p.getPartitionColumns().isEmpty())
                .collect(Collectors.toList());
        Predicate<Op> isPartitionExpr = PartitionPolicy.getIsPartitionExprPredicate(aggPiece, partitionExtractor);

        // At first, try to use already-exists time granule which reside in dimensions and rollupDimensions
        // of the AggregatePiece, these granule is never turned into coarse-grained one;
        // then try to complement a default time granule which is constructed from the partition-by columns
        // in flat table. the default time granule's coarseness never less than `defaultTimeGranuleUnit`.
        return addCoarseTimeGranuleAsPartitionByColumn(
                aggPiece,
                aggPiece.getDimensions().merge(aggPiece.getRollupDimensions()),
                ppList,
                isPartitionExpr,
                true
        ).or(() -> addCoarseTimeGranuleAsPartitionByColumn(
                aggPiece,
                flatTablePiece.getColumns(),
                ppList,
                isPartitionExpr,
                false)
        );
    }

    private Optional<AggregatePiece> addCoarseTimeGranuleAsPartitionByColumn(
            AggregatePiece aggPiece,
            TieredMap<Integer, GenericColumn> columns,
            List<PartitionPlus> ppList,
            Predicate<Op> isPartitionExpr,
            boolean columnsFromAgg) {

        ColumnRefSet columnIds = ColumnRefSet.createByIds(columns.keySet());

        TieredList<Op> dimPartOps = OpUtil.columnsToStrictOpMap(columns).keySet()
                .stream()
                .map(StrictOp::getOp)
                .filter(isPartitionExpr)
                .collect(TieredList.<Op>toList());

        TieredList<Op> baseTablePartOps = ppList.stream()
                .map(PartitionPlus::chosePartitionOp)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(op -> columnIds.containsAll(op.getIds()))
                .collect(TieredList.<Op>toList());

        TieredList<Op> partOps = dimPartOps.concat(baseTablePartOps);

        Optional<TimeGranule> optChosenGranule = partOps.stream().map(op -> Optional.ofNullable(TimeGranule.of(op)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .max(Comparator.comparing(Function.identity(), TimeGranule.getComparator()));

        if (optChosenGranule.isPresent()) {
            TimeGranule timeGranule = optChosenGranule.get();
            TimeGranule wellFormedTimeGranule = timeGranule.toWellFormed();
            TimeGranule coarseTimeGranule =
                    Objects.requireNonNull(wellFormedTimeGranule.toCoarse(defaultTimeGranuleUnit));

            boolean exists = aggPiece.getDimensions().merge(aggPiece.getRollupDimensions())
                    .entrySet()
                    .stream()
                    .map(e -> OpUtil.columnToOp(e.getKey(), e.getValue()).strict())
                    .anyMatch(op -> op.equals(coarseTimeGranule.getOp().strict()));

            if (exists) {
                return Optional.of(aggPiece);
            }
            return addPartitionDimension(aggPiece, coarseTimeGranule.getOp(), columnsFromAgg);
        }

        // if partition columns of base tables are not TimeGranules, we use the
        // first non-TimeGranule column.
        Optional<Op> optNonGranuleOp = partOps.stream().filter(Op::isVar).findFirst();
        if (optNonGranuleOp.isPresent()) {
            return addPartitionDimension(aggPiece, optNonGranuleOp.get(), columnsFromAgg);
        }
        return Optional.empty();
    }

    private Optional<AggregatePiece> addPartitionDimension(AggregatePiece aggPiece, Op partitionOp,
                                                           boolean columnsFromAgg) {
        ColumnRefToIdConverter newIdConverter = aggPiece.getCommonState().getIdConverter().duplicate();
        PieceCommonState newCommonState =
                new PieceCommonState(newIdConverter, aggPiece.getCommonState().getCoveredQueries(),
                        aggPiece.getCommonState().getFqTableMap());

        TieredMap<StrictOp, Integer> strictOpToIds =
                OpUtil.columnsToStrictOpMap(aggPiece.getFlatTable().getPiece().getColumns());
        Optional<Integer> optId = Optional.ofNullable(strictOpToIds.get(partitionOp.strict()));
        if (columnsFromAgg && optId.isPresent()) {
            return Optional.of(aggPiece);
        }
        AggregatePiece.FlatTable newFlatTable;
        Pair<Integer, GenericColumn> partitionByColumn;
        if (optId.isPresent()) {
            Integer id = optId.get();
            if (aggPiece.getColumns().containsKey(id)) {
                return Optional.of(aggPiece);
            }
            partitionByColumn = OpUtil.opToColumn(partitionOp, () -> id);
            newFlatTable = aggPiece.getFlatTable();
        } else {
            partitionByColumn = OpUtil.opToColumn(partitionOp, newIdConverter::nextId);
            PlanPiece flatTablePiece = aggPiece.getFlatTable().getPiece();
            TieredMap<Integer, GenericColumn> newColumns = flatTablePiece.getColumns().newTier()
                    .put(partitionByColumn.first, partitionByColumn.second)
                    .build();

            PlanPiece newFlatTablePiece = aggPiece.getFlatTable().getPiece().builder()
                    .setConjuncts(TieredList.genesis())
                    .setColumns(newColumns)
                    .setCommonState(newCommonState)
                    .build();

            newFlatTable =
                    new AggregatePiece.FlatTable(newFlatTablePiece, aggPiece.getFlatTable().getStiffConjuncts(),
                            aggPiece.getFlatTable().getFlexibleConjuncts());
        }

        AggregatePiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(newFlatTable)
                .setDimensions(aggPiece.getDimensions().newTier()
                        .put(partitionByColumn.first, partitionByColumn.second)
                        .build())
                .setCommonState(newCommonState)
                .build().cast();

        return Optional.of(newAggPiece);
    }
}
