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
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTriggerExternalAnalyzeRequest;
import com.starrocks.thrift.TTriggerExternalAnalyzeResponse;
import com.starrocks.thrift.TTriggerExternalAnalyzeTable;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

    // Columns due for analyze that this FE has not queued itself: tableUUID -> columns, in arrival order.
    //
    // Triggered analyze runs only on the leader, because collecting writes the edit log (AnalyzeStatus,
    // ExternalBasicStatsMeta). A follower whose query finds statistics missing or expired buffers the columns
    // here and the scheduler sends them to the leader (FrontendService#triggerExternalAnalyze, fire and forget).
    // On the leader the same buffer receives those requests, and the scheduler resolves each table and queues
    // it, so the RPC handler does no metadata IO. Bounded like the pending queue: a request that does not fit, or
    // is lost on the way, is dropped, and the next query that sees the statistics missing raises it again.
    private final Map<String, Set<String>> triggeredColumns = new LinkedHashMap<>();

    public void start() {
        if (isStart.compareAndSet(false, true)) {
            dispatchScheduler.scheduleAtFixedRate(this::schedulePendingTask, 0,
                    Config.connector_table_query_trigger_task_schedule_interval, TimeUnit.SECONDS);
        }
    }

    // Any exception escaping from here would silently cancel the periodic schedule of scheduleAtFixedRate,
    // which stops both query triggered analyze and global dict update until FE restarts.
    @VisibleForTesting
    void schedulePendingTask() {
        try {
            if (GlobalStateMgr.getCurrentState().isLeader()) {
                queueTriggeredColumns();
                connectorAnalyzeTaskQueue.schedulePendingTask();
            } else {
                forwardTriggeredColumnsToLeader();
            }
        } catch (Throwable e) {
            LOG.warn("[ExternalStats] schedule pending task failed", e);
        }

        try {
            scheduleDictUpdate();
        } catch (Throwable e) {
            LOG.warn("[ExternalStats] schedule dict update failed", e);
        }
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
            if (GlobalStateMgr.getCurrentState().isLeader()) {
                this.connectorAnalyzeTaskQueue.addPendingTask(tableUUID,
                        new ConnectorAnalyzeTask(tableTriple, analyzeColumns));
            } else {
                addTriggeredColumns(tableUUID, analyzeColumns);
            }
        }
    }

    // Buffers columns due for analyze, see triggeredColumns. Also the entry point of a follower's request.
    public void addTriggeredColumns(String tableUUID, Collection<String> columns) {
        if (tableUUID == null || columns == null || columns.isEmpty()) {
            return;
        }
        synchronized (triggeredColumns) {
            Set<String> existing = triggeredColumns.get(tableUUID);
            if (existing != null) {
                existing.addAll(columns);
                return;
            }
            if (triggeredColumns.size() >= Config.connector_table_query_trigger_analyze_max_pending_task_num) {
                LOG.warn("[ExternalStats] trigger drop | table_uuid={} reason=forward_buffer_full buffer_size={}",
                        tableUUID, triggeredColumns.size());
                return;
            }
            triggeredColumns.put(tableUUID, new HashSet<>(columns));
        }
    }

    private Map<String, Set<String>> drainTriggeredColumns() {
        synchronized (triggeredColumns) {
            Map<String, Set<String>> drained = new LinkedHashMap<>(triggeredColumns);
            triggeredColumns.clear();
            return drained;
        }
    }

    // Leader: queues what followers sent (and what this FE buffered before it became leader).
    @VisibleForTesting
    void queueTriggeredColumns() {
        for (Map.Entry<String, Set<String>> entry : drainTriggeredColumns().entrySet()) {
            String tableUUID = entry.getKey();
            Triple<String, Database, Table> tableTriple;
            try {
                tableTriple = StatisticsUtils.getTableTripleByUUID(new ConnectContext(), tableUUID);
                if (!tableTriple.getRight().isAnalyzableExternalTable()) {
                    continue;
                }
            } catch (Exception e) {
                LOG.warn("[ExternalStats] trigger skip | table_uuid={} reason=table_not_found", tableUUID, e);
                continue;
            }
            connectorAnalyzeTaskQueue.addPendingTask(tableUUID, new ConnectorAnalyzeTask(tableTriple, entry.getValue()));
        }
    }

    // Follower: sends the buffered columns to the leader, fire and forget. A lost request is not retried: the next
    // query that finds the statistics missing or expired raises it again.
    @VisibleForTesting
    void forwardTriggeredColumnsToLeader() {
        Map<String, Set<String>> drained = drainTriggeredColumns();
        if (drained.isEmpty()) {
            return;
        }
        TTriggerExternalAnalyzeRequest request = new TTriggerExternalAnalyzeRequest();
        for (Map.Entry<String, Set<String>> entry : drained.entrySet()) {
            TTriggerExternalAnalyzeTable t = new TTriggerExternalAnalyzeTable();
            t.setTable_uuid(entry.getKey());
            t.setColumns(new ArrayList<>(entry.getValue()));
            request.addToTables(t);
        }
        try {
            TTriggerExternalAnalyzeResponse response = sendToLeader(request);
            TStatus status = response.getStatus();
            if (status != null && status.getStatus_code() != TStatusCode.OK) {
                LOG.warn("[ExternalStats] trigger forward rejected | tables={} error={}", drained.size(),
                        status.getError_msgs());
            }
        } catch (Exception e) {
            LOG.warn("[ExternalStats] trigger forward failed | tables={} error={}", drained.size(), e.getMessage());
        }
    }

    @VisibleForTesting
    TTriggerExternalAnalyzeResponse sendToLeader(TTriggerExternalAnalyzeRequest request) throws TException {
        return ThriftRPCRequestExecutor.call(ThriftConnectionPool.frontendPool,
                GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderRpcEndpoint(),
                Config.thrift_rpc_timeout_ms,
                client -> client.triggerExternalAnalyze(request));
    }

    @VisibleForTesting
    int getTriggeredColumnsSize() {
        synchronized (triggeredColumns) {
            return triggeredColumns.size();
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
