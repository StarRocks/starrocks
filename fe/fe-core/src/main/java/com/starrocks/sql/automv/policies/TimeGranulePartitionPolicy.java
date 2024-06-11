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
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PieceCommonState;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private static final Map<TimeGranule.Unit, AbstractAggregatePolicy>
            POLICY_MAP = Stream.of(
                    TimeGranule.Unit.HOUR,
                    TimeGranule.Unit.DAY,
                    TimeGranule.Unit.MONTH,
                    TimeGranule.Unit.QUARTER,
                    TimeGranule.Unit.YEAR)
            .collect(ImmutableMap.toImmutableMap(
                    Function.identity(),
                    TimeGranulePartitionPolicy::new));
    private final TimeGranule.Unit defaultTimeGranuleUnit;

    private TimeGranulePartitionPolicy(TimeGranule.Unit defaultTimeGranuleUnit) {
        this.defaultTimeGranuleUnit = defaultTimeGranuleUnit;
    }

    public static AbstractAggregatePolicy resolvePolicy(String timeGranuleUnit) {
        try {
            TimeGranule.Unit unit = TimeGranule.Unit.valueOf(timeGranuleUnit);
            return POLICY_MAP.getOrDefault(unit, IDENTITY_POLICY);
        } catch (IllegalArgumentException ignored) {
            return IDENTITY_POLICY;
        }
    }

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        Preconditions.checkArgument(aggPiece.getDistinctMetrics().isEmpty());
        Preconditions.checkArgument(aggPiece.getMetrics().values().stream().allMatch(AggregatePolicies::isRollupAble));

        PlanPiece flatTable = aggPiece.getFlatTable();
        // extract all partition-by columns from base tables.
        List<TableAndPartitionColumn>
                tpList = aggPiece.getPartitionColumns().stream()
                .filter(p -> p.second.size() == 1)
                .map(p -> Pair.create(p.first, p.second.entrySet().iterator().next()))
                .map(p -> TableAndPartitionColumn.of(p.first, p.second.getKey(), p.second.getValue()))
                .collect(Collectors.toList());

        ColumnRefSet partitionByColumnIds = ColumnRefSet.of();
        tpList.forEach(tp -> partitionByColumnIds.union(tp.getId()));

        // At first, try to use already-exists time granule which reside in dimensions and rollupDimensions
        // of the AggregatePiece, these granule is never turned into coarse-grained one;
        // then try to complement a default time granule which is constructed from the partition-by columns
        // in flat table. the default time granule's coarseness never less than `defaultTimeGranuleUnit`.
        return addCoarseTimeGranuleAsPartitionByColumn(
                aggPiece,
                aggPiece.getDimensions().merge(aggPiece.getRollupDimensions()),
                partitionByColumnIds,
                true
        ).or(() -> addCoarseTimeGranuleAsPartitionByColumn(
                aggPiece,
                flatTable.getColumns(),
                partitionByColumnIds,
                false)
        );
    }

    private Optional<AggregatePiece> addCoarseTimeGranuleAsPartitionByColumn(
            AggregatePiece aggPiece,
            TieredMap<Integer, GenericColumn> columns,
            ColumnRefSet partitionByColumnIds,
            boolean alreadyExists) {

        Optional<TimeGranule> optChosenGranule = OpUtil.extractPartitionByTimeGranule(columns, partitionByColumnIds)
                .stream()
                .max(Comparator.comparing(p -> p.second, TimeGranule.getComparator()))
                .map(p -> p.second);

        if (optChosenGranule.isEmpty()) {
            return Optional.empty();
        }

        TimeGranule timeGranule = optChosenGranule.get();
        TimeGranule wellFormedTimeGranule = timeGranule.toWellFormed();
        TimeGranule coarseTimeGranule = Objects.requireNonNull(wellFormedTimeGranule.toCoarse(defaultTimeGranuleUnit));

        if (alreadyExists && wellFormedTimeGranule.equals(coarseTimeGranule)) {
            return Optional.of(aggPiece);
        }

        ColumnRefToIdConverter newIdConverter = aggPiece.getCommonState().getIdConverter().duplicate();

        Pair<Integer, GenericColumn> partitionByColumn =
                OpUtil.opToColumn(coarseTimeGranule.getOp(), newIdConverter::nextId);

        PieceCommonState newCommonState =
                new PieceCommonState(newIdConverter, aggPiece.getCommonState().getFqTableMap());
        AggregatePiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setDimensions(aggPiece.getDimensions().newTier()
                        .put(partitionByColumn.first, partitionByColumn.second)
                        .build())
                .setCommonState(newCommonState)
                .build().cast();

        return Optional.of(newAggPiece);
    }

    private static class TableAndPartitionColumn {
        private final TablePiece tablePiece;
        private final Integer id;
        private final GenericColumn column;

        private TableAndPartitionColumn(TablePiece tablePiece, Integer id, GenericColumn column) {
            this.tablePiece = Objects.requireNonNull(tablePiece);
            this.id = Objects.requireNonNull(id);
            this.column = Objects.requireNonNull(column);
        }

        public static TableAndPartitionColumn of(TablePiece tablePiece, Integer id, GenericColumn column) {
            return new TableAndPartitionColumn(tablePiece, id, column);
        }

        public TablePiece getTablePiece() {
            return tablePiece;
        }

        public Integer getId() {
            return id;
        }

        public GenericColumn getColumn() {
            return column;
        }
    }
}
