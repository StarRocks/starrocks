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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectorTableTriggerAnalyzeMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectorTableTriggerAnalyzeMgr.class);

    private final ConnectorAnalyzeTaskQueue connectorAnalyzeTaskQueue = new ConnectorAnalyzeTaskQueue();
    private final Map<ConnectorTableColumnKey, Optional<String>> keyToFileForGlobalDict = new ConcurrentHashMap<>();
    private final ScheduledExecutorService dispatchScheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isStart = new AtomicBoolean(false);

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            dispatchScheduler.scheduleAtFixedRate(this::schedulePendingTask, 0,
                    Config.connector_table_query_trigger_task_schedule_interval, TimeUnit.SECONDS);
        }
    }

    private void schedulePendingTask() {
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            connectorAnalyzeTaskQueue.schedulePendingTask();
        }
        scheduleDictUpdate();
    }

    private void scheduleDictUpdate() {
        for (Map.Entry<ConnectorTableColumnKey, Optional<String>> entry : keyToFileForGlobalDict.entrySet()) {
            Optional<String> value = keyToFileForGlobalDict.remove(entry.getKey());
            String tableUUID = entry.getKey().tableUUID;
            String columnName = entry.getKey().column;
            Runnable task = () -> {
                StatisticExecutor.updateDictSync(tableUUID, columnName, value);
            };
            try {
                ThreadPoolManager.getStatsCacheThreadPoolForLake().submit(task);
            } catch (RejectedExecutionException e) {
                keyToFileForGlobalDict.put(entry.getKey(), value);
                break;
            }
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
                try (ConnectContext.ContextScope scope = ConnectContext.enterOnlyReadIcebergCacheScope(ConnectContext.get())) {
                    tableTriple = StatisticsUtils.getTableTripleByUUID(scope.getContext(), columnKey.tableUUID);
                    // check table could run analyze
                    if (!tableTriple.getRight().isAnalyzableExternalTable()) {
                        return;
                    }
                    tableExist = true;
                    tableUUID = columnKey.tableUUID;
                } catch (Exception e) {
                    LOG.warn("[ExternalStats] trigger skip | table_uuid={} reason=table_not_found", columnKey.tableUUID);
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
                LocalDateTime lastUpdateTime = parseUpdateTime(columnStatsValue.getUpdateTime());
                if (lastUpdateTime == null) {
                    // No usable timestamp, so there is no interval this column can be inside: it is due.
                    // Letting the parse throw instead would abandon the whole table - every other column in
                    // the same batch included - and the exception would vanish into the async callback that
                    // got us here (CachedStatisticStorage#getConnectorTableStatistics), leaving the table
                    // silently without statistics.
                    analyzeColumns.add(columnKey.column);
                    continue;
                }
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
            this.connectorAnalyzeTaskQueue.addPendingTask(tableUUID, new ConnectorAnalyzeTask(tableTriple, analyzeColumns));
        }
    }

    // The recorded collection time, or null when there is none or it cannot be read.
    private static LocalDateTime parseUpdateTime(String updateTime) {
        if (updateTime == null) {
            return null;
        }
        try {
            return DateUtils.parseStrictDateTime(updateTime);
        } catch (Exception e) {
            LOG.warn("[ExternalStats] unparseable statistics update time, treating the column as due | value={}",
                    updateTime, e);
            return null;
        }
    }

    @VisibleForTesting
    public ConnectorAnalyzeTaskQueue getConnectorAnalyzeTaskQueue() {
        return connectorAnalyzeTaskQueue;
    }

    public void addDictUpdateTask(ConnectorTableColumnKey key, Optional<String> fileName) {
        // the subsequent file will invalid previous ones
        Optional<String> old = keyToFileForGlobalDict.get(key);
        if (old == null || old.isPresent()) {
            keyToFileForGlobalDict.put(key, fileName);
        }
    }
}
