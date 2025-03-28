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

package com.starrocks.connector.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatsConstants;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectorAnalyzeTask {
    private static final Logger LOG = LogManager.getLogger(ConnectorAnalyzeTask.class);

    private final String catalogName;
    private final Database db;
    private final Table table;
    private Set<String> columns;

    public ConnectorAnalyzeTask(Triple<String, Database, Table> tableTriple, Set<String> columns) {
        this.catalogName = Preconditions.checkNotNull(tableTriple.getLeft());
        this.db = Preconditions.checkNotNull(tableTriple.getMiddle());
        this.table = Preconditions.checkNotNull(tableTriple.getRight());
        this.columns = columns;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void mergeTask(ConnectorAnalyzeTask other) {
        this.columns.addAll(other.columns);
    }

    public void removeColumns(Set<String> columns) {
        this.columns.removeAll(columns);
    }

    private boolean needAnalyze(String column, LocalDateTime lastAnalyzedTime) {
        LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
        // check table update time is after last analyzed time
        if (tableUpdateTime != null) {
            if (lastAnalyzedTime.isAfter(tableUpdateTime)) {
                LOG.info("Table {}.{}.{} column {} last update time: {}, last analyzed time: {}, skip analyze",
                        catalogName, db.getFullName(), table.getName(), column, tableUpdateTime, lastAnalyzedTime);
                return false;
            }
        }
        return true;
    }

    public Optional<AnalyzeStatus> run() {
        // check analyze status first, if the table is analyzing, skip it
        Optional<ExternalAnalyzeStatus> lastAnalyzedStatus = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getAnalyzeStatusMap().values().stream()
                .filter(status -> status instanceof ExternalAnalyzeStatus)
                .map(status -> (ExternalAnalyzeStatus) status)
                .filter(status -> status.getTableUUID().equals(table.getUUID()))
                .filter(status -> !status.getStatus().equals(StatsConstants.ScheduleStatus.FAILED))
                .max(Comparator.comparing(ExternalAnalyzeStatus::getStartTime));
        if (lastAnalyzedStatus.isPresent()) {
            // Do not analyze the table if the last analyze status is PENDING or RUNNING
            StatsConstants.ScheduleStatus lastScheduleStatus = lastAnalyzedStatus.get().getStatus();
            if (lastScheduleStatus == StatsConstants.ScheduleStatus.PENDING ||
                    lastScheduleStatus == StatsConstants.ScheduleStatus.RUNNING) {
                LOG.info("Table {}.{}.{} analyze status is {}, skip it", catalogName, db.getFullName(),
                        table.getName(), lastScheduleStatus);
                return Optional.empty();
            }
        }

        ExternalBasicStatsMeta externalBasicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                getExternalTableBasicStatsMeta(catalogName, db.getFullName(), table.getName());
        Optional<LocalDateTime> lastEarliestAnalyzedTime = Optional.empty();
        if (externalBasicStatsMeta != null) {
            Map<String, LocalDateTime> columnLastAnalyzedTime = externalBasicStatsMeta.getColumnStatsMetaMap().values()
                    .stream().collect(
                            Collectors.toMap(ColumnStatsMeta::getColumnName, ColumnStatsMeta::getUpdateTime));
            Set<String> needAnalyzeColumns = new HashSet<>(columns);

            for (String column : columns) {
                if (columnLastAnalyzedTime.containsKey(column)) {
                    LocalDateTime lastAnalyzedTime = columnLastAnalyzedTime.get(column);
                    Preconditions.checkNotNull(lastAnalyzedTime, "Last analyzed time is null");
                    if (needAnalyze(column, lastAnalyzedTime)) {
                        // need analyze columns, compare the last analyzed time, get the earliest time
                        lastEarliestAnalyzedTime = lastEarliestAnalyzedTime.
                                map(localDateTime -> localDateTime.isAfter(lastAnalyzedTime) ? lastAnalyzedTime : localDateTime).
                                or(() -> Optional.of(lastAnalyzedTime));
                    } else {
                        needAnalyzeColumns.remove(column);
                    }
                } else {
                    lastEarliestAnalyzedTime = Optional.of(LocalDateTime.MIN);
                }
            }
            columns = needAnalyzeColumns;
        }

        if (columns.isEmpty()) {
            LOG.info("Table {}.{}.{} columns {} are all up to date, skip analyze", catalogName, db.getFullName(),
                    table.getName(), columns.stream().map(Object::toString).collect(Collectors.joining(",")));
            return Optional.empty();
        }

        try {
            return Optional.of(executeAnalyze(lastEarliestAnalyzedTime.orElse(LocalDateTime.MIN)));
        } finally {
            ConnectContext.remove();
        }
    }

    public AnalyzeStatus executeAnalyze(LocalDateTime lastAnalyzedTime) {
        // init connect context
        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setThreadLocalInfo();
        // init column names and types
        List<String> columnNames = Lists.newArrayList(columns);
        List<Type> columnTypes = columnNames.stream().map(col ->
                StatisticUtils.getQueryStatisticsColumnType(table, col)).collect(Collectors.toList());
        // init partition names
        List<String> partitionNames = ConnectorPartitionTraits.build(table).getPartitionNames();
        Set<String> updatedPartitions = StatisticUtils.getUpdatedPartitionNames(table, lastAnalyzedTime);
        if (updatedPartitions == null) {
            // assume all partitions is updated if updatedPartitions is null
            updatedPartitions = new HashSet<>(partitionNames);
        }

        StatsConstants.AnalyzeType analyzeType = StatsConstants.AnalyzeType.FULL;
        if (partitionNames.size() > Config.statistic_sample_collect_partition_size) {
            analyzeType = StatsConstants.AnalyzeType.SAMPLE;
        }
        // only collect updated partitions
        partitionNames = new ArrayList<>(updatedPartitions);

        // Init new analyze status
        AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                catalogName, db.getOriginName(), table.getName(),
                table.getUUID(),
                Lists.newArrayList(columns), analyzeType, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap(), LocalDateTime.now());
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.PENDING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

        StatisticExecutor statisticExecutor = new StatisticExecutor();
        return statisticExecutor.collectStatistics(statsConnectCtx,
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                        catalogName, db, table, partitionNames,
                        columnNames, columnTypes,
                        analyzeType, StatsConstants.ScheduleType.ONCE, Maps.newHashMap()),
                analyzeStatus,
                false);
    }
}
