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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectorTableTriggerAnalyzeMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectorTableTriggerAnalyzeMgr.class);

    private final ConnectorAnalyzeTaskQueue connectorAnalyzeTaskQueue = new ConnectorAnalyzeTaskQueue();
    private final ScheduledExecutorService dispatchScheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isStart = new AtomicBoolean(false);

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            dispatchScheduler.scheduleAtFixedRate(connectorAnalyzeTaskQueue::scheduledPendingTask, 0,
                    Config.connector_table_query_trigger_analyze_schedule_interval, TimeUnit.SECONDS);
        }
    }

    public void checkAndUpdateTableStats(Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnStats) {
        if (columnStats == null || columnStats.isEmpty()) {
            return;
        }

        Set<String> analyzeColumns = Sets.newHashSet();
        Triple<String, Database, Table> tableTriple = null;
        boolean tableExist = false;
        String tableUUID = null;
        for (Map.Entry<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> entry : columnStats.entrySet()) {
            ConnectorTableColumnKey columnKey = entry.getKey();
            // first check table exist
            if (!tableExist) {
                try {
                    tableTriple = StatisticsUtils.getTableTripleByUUID(columnKey.tableUUID);
                    // check table could run analyze
                    if (!tableTriple.getRight().isAnalyzableExternalTable()) {
                        return;
                    }
                    tableExist = true;
                    tableUUID = columnKey.tableUUID;
                } catch (Exception e) {
                    LOG.warn("Table {} is not existed", columnKey.tableUUID);
                    return;
                }
            }

            // check column stats exist
            Optional<ConnectorTableColumnStats> columnStatsOptional = entry.getValue();
            if (columnStatsOptional.isEmpty()) {
                analyzeColumns.add(columnKey.column);
            } else {
                // check column stats last update time
                ConnectorTableColumnStats columnStatsValue = columnStatsOptional.get();
                if (columnStatsValue.getUpdateTime() == null) {
                    analyzeColumns.add(columnKey.column);
                }
                LocalDateTime lastUpdateTime = DateUtils.parseStrictDateTime(columnStatsValue.getUpdateTime());
                long rowCount = columnStatsValue.getRowCount();
                long timeInterval = rowCount < Config.connector_table_query_trigger_analyze_small_table_rows ?
                        Config.connector_table_query_trigger_analyze_small_table_interval :
                        Config.connector_table_query_trigger_analyze_large_table_interval;
                if (!lastUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
                    analyzeColumns.add(columnKey.column);
                }
            }
        }

        if (!analyzeColumns.isEmpty()) {
            // need to execute analyze
            if (!this.connectorAnalyzeTaskQueue.
                    addPendingTask(tableUUID, new ConnectorAnalyzeTask(tableTriple, analyzeColumns))) {
                LOG.warn("Add analyze pending task {} failed.", tableUUID);
            }
        }
    }
}
