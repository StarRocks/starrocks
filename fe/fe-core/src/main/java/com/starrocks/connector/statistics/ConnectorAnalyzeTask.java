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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatsConstants;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectorAnalyzeTask {
    private static final Logger LOG = LogManager.getLogger(ConnectorAnalyzeTask.class);

    private final String catalogName;
    private final Database db;
    private final Table table;
    private final Set<String> columns;

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

    private boolean skipAnalyzeUseLastUpdateTime(LocalDateTime lastUpdateTime) {
        // do not know the table row count, use small table analyze interval
        if (lastUpdateTime.plusSeconds(Config.connector_table_query_trigger_analyze_small_table_interval).
                isAfter(LocalDateTime.now())) {
            LOG.info("Table {}.{}.{} is already analyzed at {}, skip it", catalogName, db.getFullName(),
                    table.getName(), lastUpdateTime);
            return true;
        } else {
            return false;
        }
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
            StatsConstants.ScheduleStatus lastScheduleStatus = lastAnalyzedStatus.get().getStatus();
            if (lastScheduleStatus == StatsConstants.ScheduleStatus.PENDING ||
                    lastScheduleStatus == StatsConstants.ScheduleStatus.RUNNING) {
                LOG.info("Table {}.{}.{} analyze status is {}, skip it", catalogName, db.getFullName(),
                        table.getName(), lastScheduleStatus);
                return Optional.empty();
            } else {
                // analyze status is Finished
                // check the analyzed columns
                List<String> analyzedColumns = lastAnalyzedStatus.get().getColumns();
                if (analyzedColumns == null || analyzedColumns.isEmpty()) {
                    // analyzed all columns in last analyzed time, check the update time
                    if (skipAnalyzeUseLastUpdateTime(lastAnalyzedStatus.get().getStartTime())) {
                        return Optional.empty();
                    }
                } else {
                    Set<String> lastAnalyzedColumnsSet = new HashSet<>(analyzedColumns);
                    if (lastAnalyzedColumnsSet.containsAll(columns)) {
                        if (skipAnalyzeUseLastUpdateTime(lastAnalyzedStatus.get().getStartTime())) {
                            return Optional.empty();
                        }
                    } else {
                        if (skipAnalyzeUseLastUpdateTime(lastAnalyzedStatus.get().getStartTime())) {
                            columns.removeAll(lastAnalyzedColumnsSet);
                        }
                    }
                }
            }
        }

        // Init new analyze status
        AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                catalogName, db.getOriginName(), table.getName(),
                table.getUUID(),
                Lists.newArrayList(columns), StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap(), LocalDateTime.now());

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.PENDING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setThreadLocalInfo();
        try {
            return Optional.of(executeAnalyze(statsConnectCtx, analyzeStatus));
        } finally {
            ConnectContext.remove();
        }
    }

    public AnalyzeStatus executeAnalyze(ConnectContext statsConnectCtx, AnalyzeStatus analyzeStatus) {
        List<String> columnNames = Lists.newArrayList(columns);
        List<Type> columnTypes = columnNames.stream().map(col -> {
            Column column = table.getColumn(col);
            Preconditions.checkNotNull(column, "Column " + col + " does not exist in table " + table.getName());
            return column.getType();
        }).collect(Collectors.toList());
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        return statisticExecutor.collectStatistics(statsConnectCtx,
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                        catalogName, db, table, null,
                        columnNames, columnTypes,
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE, Maps.newHashMap()),
                analyzeStatus,
                false);
    }
}
