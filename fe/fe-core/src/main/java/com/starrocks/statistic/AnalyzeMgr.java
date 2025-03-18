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

package com.starrocks.statistic;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class AnalyzeMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(AnalyzeMgr.class);
    private static final Pair<Long, Long> CHECK_ALL_TABLES =
            new Pair<>(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID);

    private final Map<Long, AnalyzeJob> analyzeJobMap;
    private final Map<Long, AnalyzeStatus> analyzeStatusMap;
    private final Map<Long, BasicStatsMeta> basicStatsMetaMap;
    private final Map<StatsMetaKey, ExternalBasicStatsMeta> externalBasicStatsMetaMap;
    private final Map<Pair<Long, String>, HistogramStatsMeta> histogramStatsMetaMap;
    private final Map<StatsMetaColumnKey, ExternalHistogramStatsMeta> externalHistogramStatsMetaMap;
    private final Map<MultiColumnStatsKey, MultiColumnStatsMeta> multiColumnStatsMetaMap;

    // ConnectContext of all currently running analyze tasks
    private final Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    // only first load of table will trigger analyze, so we don't need limit thread pool queue size
    private static final ExecutorService ANALYZE_TASK_THREAD_POOL = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.statistic_analyze_task_pool_size, Integer.MAX_VALUE,
            "analyze-task-concurrency-pool", true);

    private final Set<Long> dropPartitionIds = new ConcurrentSkipListSet<>();
    private final List<Pair<Long, Long>> checkTableIds = Lists.newArrayList(CHECK_ALL_TABLES);

    private LocalDateTime lastCleanTime;

    public AnalyzeMgr() {
        analyzeJobMap = Maps.newConcurrentMap();
        analyzeStatusMap = Maps.newConcurrentMap();
        basicStatsMetaMap = Maps.newConcurrentMap();
        externalBasicStatsMetaMap = Maps.newConcurrentMap();
        histogramStatsMetaMap = Maps.newConcurrentMap();
        externalHistogramStatsMetaMap = Maps.newConcurrentMap();
        multiColumnStatsMetaMap = Maps.newConcurrentMap();
    }

    public AnalyzeJob getAnalyzeJob(long id) {
        return analyzeJobMap.get(id);
    }

    public AnalyzeStatus getAnalyzeStatus(long id) {
        return analyzeStatusMap.get(id);
    }

    public void addAnalyzeJob(AnalyzeJob job) {
        long id = GlobalStateMgr.getCurrentState().getNextId();
        job.setId(id);
        analyzeJobMap.put(id, job);
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeJob(job);
    }

    public void updateAnalyzeJobWithoutLog(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
    }

    public void updateAnalyzeJobWithLog(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeJob(job);
    }

    public void removeAnalyzeJob(long id) {
        if (analyzeJobMap.containsKey(id)) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeJob(analyzeJobMap.remove(id));
        }
    }

    public List<AnalyzeJob> getAllAnalyzeJobList() {
        return Lists.newLinkedList(analyzeJobMap.values());
    }

    public List<NativeAnalyzeJob> getAllNativeAnalyzeJobList() {
        return analyzeJobMap.values().stream().filter(AnalyzeJob::isNative).map(job -> (NativeAnalyzeJob) job).
                collect(Collectors.toList());
    }

    public List<ExternalAnalyzeJob> getAllExternalAnalyzeJobList() {
        return analyzeJobMap.values().stream().filter(job -> !job.isNative()).map(job -> (ExternalAnalyzeJob) job).
                collect(Collectors.toList());
    }

    public void replayAddAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
    }

    public void replayRemoveAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.remove(job.getId());
    }

    public void addAnalyzeStatus(AnalyzeStatus status) {
        analyzeStatusMap.put(status.getId(), status);
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeStatus(status);
    }

    public void replayAddAnalyzeStatus(AnalyzeStatus status) {
        analyzeStatusMap.put(status.getId(), status);
    }

    public void replayRemoveAnalyzeStatus(AnalyzeStatus status) {
        analyzeStatusMap.remove(status.getId());
    }

    public Map<Long, AnalyzeStatus> getAnalyzeStatusMap() {
        return analyzeStatusMap;
    }

    public void clearExpiredAnalyzeStatus() {
        List<AnalyzeStatus> expireList = Lists.newArrayList();
        for (AnalyzeStatus analyzeStatus : analyzeStatusMap.values()) {
            LocalDateTime now = LocalDateTime.now();
            if (analyzeStatus.getStartTime().plusSeconds(Config.statistic_analyze_status_keep_second).isBefore(now)) {
                expireList.add(analyzeStatus);
            }
        }
        expireList.forEach(status -> analyzeStatusMap.remove(status.getId()));
        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeStatus(status);
        }
    }

    public void dropAnalyzeStatus(Long tableId) {
        List<AnalyzeStatus> expireList = Lists.newArrayList();
        for (AnalyzeStatus analyzeStatus : analyzeStatusMap.values()) {
            if (analyzeStatus.isNative() &&
                    ((NativeAnalyzeStatus) analyzeStatus).getTableId() == tableId) {
                expireList.add(analyzeStatus);
            }
        }
        expireList.forEach(status -> analyzeStatusMap.remove(status.getId()));
        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeStatus(status);
        }
    }

    public void dropExternalAnalyzeStatus(String tableUUID) {
        List<AnalyzeStatus> expireList = analyzeStatusMap.values().stream().
                filter(status -> status instanceof ExternalAnalyzeStatus).
                filter(status -> ((ExternalAnalyzeStatus) status).getTableUUID().equals(tableUUID)).
                collect(Collectors.toList());

        expireList.forEach(status -> analyzeStatusMap.remove(status.getId()));
        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeStatus(status);
        }
    }

    public void dropExternalBasicStatsData(String tableUUID) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalTableStatistics(StatisticUtils.buildConnectContext(), tableUUID);
    }

    public void dropExternalBasicStatsData(String catalogName, String dbName, String tableName) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalTableStatistics(StatisticUtils.buildConnectContext(), catalogName, dbName, tableName);
    }

    public void dropAnalyzeJob(String catalogName, String dbName, String tblName) {
        List<AnalyzeJob> expireList = Lists.newArrayList();
        try {
            for (AnalyzeJob analyzeJob : analyzeJobMap.values()) {
                if (analyzeJob.getCatalogName().equals(catalogName) &&
                        analyzeJob.getDbName().equals(dbName) &&
                        analyzeJob.getTableName().equals(tblName)) {
                    expireList.add(analyzeJob);
                }
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("drop analyze job failed", e);
        }
        expireList.forEach(job -> analyzeJobMap.remove(job.getId()));
        for (AnalyzeJob job : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeJob(job);
        }
    }

    public void addBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddBasicStatsMeta(basicStatsMeta);
    }

    public void replayAddBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
    }

    public void addExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        externalBasicStatsMetaMap.put(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                basicStatsMeta.getDbName(), basicStatsMeta.getTableName()), basicStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddExternalBasicStatsMeta(basicStatsMeta);
    }

    public void replayAddExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        externalBasicStatsMetaMap.put(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                basicStatsMeta.getDbName(), basicStatsMeta.getTableName()), basicStatsMeta);
    }

    public void removeExternalBasicStatsMeta(String catalogName, String dbName, String tableName) {
        StatsMetaKey key = new StatsMetaKey(catalogName, dbName, tableName);
        if (externalBasicStatsMetaMap.containsKey(key)) {
            ExternalBasicStatsMeta basicStatsMeta = externalBasicStatsMetaMap.remove(key);
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveExternalBasicStatsMeta(basicStatsMeta);
        }
    }

    public void removeExternalHistogramStatsMeta(String catalogName, String dbName, String tableName, List<String> columns) {
        for (String column : columns) {
            StatsMetaColumnKey histogramKey = new StatsMetaColumnKey(catalogName, dbName, tableName, column);
            if (externalHistogramStatsMetaMap.containsKey(histogramKey)) {
                GlobalStateMgr.getCurrentState().getEditLog()
                        .logRemoveExternalHistogramStatsMeta(externalHistogramStatsMetaMap.get(histogramKey));
                externalHistogramStatsMetaMap.remove(histogramKey);
            }
        }
    }

    public void replayRemoveExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        externalBasicStatsMetaMap.remove(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                basicStatsMeta.getDbName(), basicStatsMeta.getTableName()));
    }

    public void addMultiColumnStatsMeta(MultiColumnStatsMeta meta) {
        multiColumnStatsMetaMap.put(new MultiColumnStatsKey(meta.getTableId(), meta.getColumnIds(), meta.getStatsType()), meta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddMultiColumnStatsMeta(meta);
    }

    public void replayAddMultiColumnStatsMeta(MultiColumnStatsMeta meta) {
        multiColumnStatsMetaMap.put(new MultiColumnStatsKey(meta.getTableId(), meta.getColumnIds(), meta.getStatsType()), meta);
    }

    public void replayRemoveMultiColumnStatsMeta(MultiColumnStatsMeta meta) {
        multiColumnStatsMetaMap.remove(new MultiColumnStatsKey(meta.getTableId(), meta.getColumnIds(), meta.getStatsType()));
    }

    public void refreshBasicStatisticsCache(Long dbId, Long tableId, List<String> columns, boolean async) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            return;
        }

        if (async) {
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshTableStatistic(table, false);
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshColumnStatistics(table, columns, false);
        } else {
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshTableStatistic(table, true);
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshColumnStatistics(table, columns, true);
        }
    }

    public void refreshConnectorTableBasicStatisticsCache(String catalogName, String dbName, String tableName,
                                                          List<String> columns, boolean async) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
        if (table == null) {
            return;
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage()
                .refreshConnectorTableColumnStatistics(table, columns, !async);
    }

    public void refreshMultiColumnStatisticsCache(long tableId) {
        GlobalStateMgr.getCurrentState().getStatisticStorage().refreshMultiColumnStatistics(tableId);
    }

    public void replayRemoveBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.remove(basicStatsMeta.getTableId());
    }

    public BasicStatsMeta getTableBasicStatsMeta(long tableId) {
        return basicStatsMetaMap.get(tableId);
    }

    public Map<Long, BasicStatsMeta> getBasicStatsMetaMap() {
        return basicStatsMetaMap;
    }

    public long getExistUpdateRows(Long tableId) {
        BasicStatsMeta existInfo =  basicStatsMetaMap.get(tableId);
        return existInfo == null ? 0 : existInfo.getUpdateRows();
    }

    public Map<StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
        return externalBasicStatsMetaMap;
    }

    public ExternalBasicStatsMeta getExternalTableBasicStatsMeta(String catalogName, String dbName, String tableName) {
        return externalBasicStatsMetaMap.get(new StatsMetaKey(catalogName, dbName, tableName));
    }

    public HistogramStatsMeta getHistogramMeta(long tableId, String column) {
        return histogramStatsMetaMap.get(Pair.create(tableId, column));
    }

    public List<HistogramStatsMeta> getHistogramMetaByTable(long tableId) {
        return histogramStatsMetaMap.entrySet().stream()
                .filter(x -> x.getKey().first == tableId)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    public void addHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.put(
                new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()), histogramStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddHistogramStatsMeta(histogramStatsMeta);
    }

    public void replayAddHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.put(
                new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()), histogramStatsMeta);
    }

    public void refreshHistogramStatisticsCache(Long dbId, Long tableId, List<String> columns, boolean async) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (null == db) {
            return;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (null == table) {
            return;
        }

        GlobalStateMgr.getCurrentState().getStatisticStorage().expireHistogramStatistics(table.getId(), columns);
        if (async) {
            GlobalStateMgr.getCurrentState().getStatisticStorage().getHistogramStatistics(table, columns);
        } else {
            GlobalStateMgr.getCurrentState().getStatisticStorage().getHistogramStatisticsSync(table, columns);
        }
    }

    public void replayRemoveHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.remove(new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()));
    }

    public Map<Pair<Long, String>, HistogramStatsMeta> getHistogramStatsMetaMap() {
        return histogramStatsMetaMap;
    }

    public Map<StatsMetaColumnKey, ExternalHistogramStatsMeta> getExternalHistogramStatsMetaMap() {
        return externalHistogramStatsMetaMap;
    }

    public void addExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        externalHistogramStatsMetaMap.put(new StatsMetaColumnKey(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), histogramStatsMeta.getColumn()),
                histogramStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddExternalHistogramStatsMeta(histogramStatsMeta);
    }

    public void replayAddExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        externalHistogramStatsMetaMap.put(new StatsMetaColumnKey(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), histogramStatsMeta.getColumn()),
                histogramStatsMeta);
    }

    public void replayRemoveExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        externalHistogramStatsMetaMap.remove(new StatsMetaColumnKey(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), histogramStatsMeta.getColumn()));
    }

    public void refreshConnectorTableHistogramStatisticsCache(String catalogName, String dbName, String tableName,
                                                              List<String> columns, boolean async) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
        if (table == null) {
            return;
        }

        GlobalStateMgr.getCurrentState().getStatisticStorage().expireConnectorHistogramStatistics(table, columns);
        if (async) {
            GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorHistogramStatistics(table, columns);
        } else {
            GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorHistogramStatisticsSync(table, columns);
        }
    }

    public void clearStatisticFromDroppedTable() {
        clearStatisticFromNativeDroppedTable();
        clearStatisticFromExternalDroppedTable();
    }

    public void clearStatisticFromNativeDroppedTable() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        Set<Long> tables = new HashSet<>();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                continue;
            }

            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                /*
                 * If the meta contains statistical information, but the data is empty,
                 * it means that the table has been truncate or insert overwrite, and it is set to empty,
                 * so it is treated as a table that has been deleted here.
                 */
                if (!StatisticUtils.isEmptyTable(table)) {
                    tables.add(table.getId());
                }
            }
        }

        Set<Long> tableIdHasDeleted = new HashSet<>(basicStatsMetaMap.keySet());
        tableIdHasDeleted.removeAll(tables);

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        try (var guard = statsConnectCtx.bindScope()) {
            statsConnectCtx.setStatisticsConnection(true);

            dropBasicStatsMetaAndData(statsConnectCtx, tableIdHasDeleted);
            dropHistogramStatsMetaAndData(statsConnectCtx, tableIdHasDeleted);
            dropMultiColumnStatsMetaAndData(statsConnectCtx, tableIdHasDeleted);
        }
    }

    public void clearStatisticFromExternalDroppedTable() {
        List<StatsMetaKey> droppedTables = new ArrayList<>();
        for (Map.Entry<StatsMetaKey, ExternalBasicStatsMeta> entry : externalBasicStatsMetaMap.entrySet()) {
            StatsMetaKey tableKey = entry.getKey();
            try {
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableKey.getCatalogName(),
                        tableKey.getDbName(), tableKey.getTableName());
                if (table == null) {
                    LOG.warn("Table {}.{}.{} not exists, clear it's statistics", tableKey.getCatalogName(),
                            tableKey.getDbName(), tableKey.getTableName());
                    droppedTables.add(tableKey);
                }
            } catch (Exception e) {
                LOG.warn("Table {}.{}.{} throw exception, clear it's statistics", tableKey.getCatalogName(),
                        tableKey.getDbName(), tableKey.getTableName(), e);
                droppedTables.add(tableKey);
            }
        }

        for (StatsMetaKey droppedTable : droppedTables) {
            dropExternalBasicStatsMetaAndData(droppedTable.getCatalogName(), droppedTable.getDbName(),
                    droppedTable.getTableName());
            dropExternalHistogramStatsMetaAndData(droppedTable.getCatalogName(), droppedTable.getDbName(),
                    droppedTable.getTableName());
        }
    }

    public void recordDropPartition(long partitionId) {
        dropPartitionIds.add(partitionId);
    }

    public void clearStatisticFromDroppedPartition() {
        clearStaleStatsWhenStarted();
        clearStalePartitionStats();
        dropPartitionStatistics();
    }

    private void dropPartitionStatistics() {
        if (dropPartitionIds.isEmpty()) {
            return;
        }

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setThreadLocalInfo();
        statsConnectCtx.setStatisticsConnection(true);

        List<Long> pids = dropPartitionIds.stream().limit(Config.expr_children_limit / 2).collect(Collectors.toList());

        StatisticExecutor executor = new StatisticExecutor();
        statsConnectCtx.setThreadLocalInfo();
        if (executor.dropPartitionStatistics(statsConnectCtx, pids)) {
            pids.forEach(dropPartitionIds::remove);
        }
    }

    private void clearStalePartitionStats() {
        // It means FE is restarted, the previous step had cleared the stats.
        if (lastCleanTime == null) {
            lastCleanTime = LocalDateTime.now();
            return;
        }

        //  do the clear task once every 12 hours.
        if (Duration.between(lastCleanTime, LocalDateTime.now()).toSeconds() < Config.clear_stale_stats_interval_sec) {
            return;
        }

        List<Table> tables = Lists.newArrayList();
        LocalDateTime workTime = LocalDateTime.now();
        for (Map.Entry<Long, AnalyzeStatus> entry : analyzeStatusMap.entrySet()) {
            AnalyzeStatus analyzeStatus = entry.getValue();
            LocalDateTime endTime = analyzeStatus.getEndTime();
            // After the last cleanup, if a table has successfully undergone a statistics collection,
            // and the collection completion time is after the last cleanup time,
            // then during the next cleanup process, the stale column statistics would be cleared.
            if (analyzeStatus instanceof NativeAnalyzeStatus
                    && analyzeStatus.getStatus() == StatsConstants.ScheduleStatus.FINISH
                    && Duration.between(endTime, lastCleanTime).toMinutes() < 30) {
                NativeAnalyzeStatus nativeAnalyzeStatus = (NativeAnalyzeStatus) analyzeStatus;
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeStatus.getDbId());
                if (db != null && GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getId(), nativeAnalyzeStatus.getTableId()) != null) {
                    tables.add(GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getId(), nativeAnalyzeStatus.getTableId()));
                }
            }
        }

        if (tables.isEmpty()) {
            lastCleanTime = workTime;
        }

        List<Long> tableIds = Lists.newArrayList();
        List<Long> partitionIds = Lists.newArrayList();
        int exprLimit = Config.expr_children_limit / 2;
        for (Table table : tables) {
            List<Long> pids = table.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());
            if (pids.size() > exprLimit) {
                tableIds.clear();
                partitionIds.clear();
                tableIds.add(table.getId());
                partitionIds.addAll(pids);
                break;
            } else if ((tableIds.size() + partitionIds.size() + pids.size()) > exprLimit) {
                break;
            }
            tableIds.add(table.getId());
            partitionIds.addAll(pids);
        }

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();
        StatisticExecutor executor = new StatisticExecutor();
        statsConnectCtx.getSessionVariable().setExprChildrenLimit(partitionIds.size() * 3);
        boolean res = executor.dropTableInvalidPartitionStatistics(statsConnectCtx, tableIds, partitionIds);
        if (!res) {
            LOG.debug("failed to clean stale column statistics before time: {}", lastCleanTime);
        }
        lastCleanTime = LocalDateTime.now();
    }

    private void clearStaleStatsWhenStarted() {
        if (!Config.statistic_check_expire_partition || checkTableIds.isEmpty()) {
            return;
        }

        if (checkTableIds.contains(CHECK_ALL_TABLES)) {
            checkTableIds.clear();
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                    if (table == null || !(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
                        continue;
                    }
                    checkTableIds.add(new Pair<>(dbId, table.getId()));
                }
            }

        }

        List<Pair<Long, Long>> checkDbTableIds = Lists.newArrayList();
        List<Long> checkPartitionIds = Lists.newArrayList();
        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);

        int exprLimit = Config.expr_children_limit / 2;
        for (Pair<Long, Long> dbTableId : checkTableIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbTableId.first);
            if (null == db) {
                continue;
            }

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), dbTableId.second);
            if (table == null) {
                continue;
            }

            List<Long> pids = table.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());

            // SQL parse will limit expr number, so we need modify it in the session
            // Of course, it's low probability to reach the limit
            if (pids.size() > exprLimit) {
                checkDbTableIds.clear();
                checkPartitionIds.clear();
                checkDbTableIds.add(dbTableId);
                checkPartitionIds.addAll(pids);
                break;
            } else if ((checkDbTableIds.size() + checkPartitionIds.size() + pids.size()) > exprLimit) {
                break;
            }

            checkDbTableIds.add(dbTableId);
            checkPartitionIds.addAll(pids);
        }

        if (checkDbTableIds.isEmpty() || checkPartitionIds.isEmpty()) {
            return;
        }

        statsConnectCtx.setThreadLocalInfo();
        StatisticExecutor executor = new StatisticExecutor();
        List<Long> tables = checkDbTableIds.stream().map(p -> p.second).collect(Collectors.toList());

        statsConnectCtx.getSessionVariable().setExprChildrenLimit(checkPartitionIds.size() * 3);
        if (executor.dropTableInvalidPartitionStatistics(statsConnectCtx, tables, checkPartitionIds)) {
            checkDbTableIds.forEach(checkTableIds::remove);
        }
    }

    public void dropBasicStatsMetaAndData(ConnectContext statsConnectCtx, Set<Long> tableIdHasDeleted) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        try (var guard = statsConnectCtx.bindScope()) {
            for (Long tableId : tableIdHasDeleted) {
                BasicStatsMeta basicStatsMeta = basicStatsMetaMap.get(tableId);
                if (basicStatsMeta == null) {
                    continue;
                }
                // Both types of tables need to be deleted, because there may have been a switch of
                // collecting statistics types, leaving some discarded statistics data.
                statisticExecutor.dropTableStatistics(statsConnectCtx, tableId, StatsConstants.AnalyzeType.SAMPLE);
                statisticExecutor.dropTableStatistics(statsConnectCtx, tableId, StatsConstants.AnalyzeType.FULL);

                statisticExecutor.dropTableMultiColumnStatistics(statsConnectCtx, tableId);
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveBasicStatsMeta(basicStatsMetaMap.get(tableId));
                basicStatsMetaMap.remove(tableId);
            }
        }
    }

    public void dropHistogramStatsMetaAndData(ConnectContext statsConnectCtx, Set<Long> tableIdHasDeleted) {
        Map<Long, List<String>> expireHistogram = new HashMap<>();
        for (Pair<Long, String> entry : histogramStatsMetaMap.keySet()) {
            if (tableIdHasDeleted.contains(entry.first)) {
                if (expireHistogram.containsKey(entry.first)) {
                    expireHistogram.get(entry.first).add(entry.second);
                } else {
                    expireHistogram.put(entry.first, Lists.newArrayList(entry.second));
                }
            }
        }

        for (Map.Entry<Long, List<String>> histogramItem : expireHistogram.entrySet()) {
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            statisticExecutor.dropHistogram(statsConnectCtx, histogramItem.getKey(), histogramItem.getValue());

            for (String histogramColumn : histogramItem.getValue()) {
                Pair<Long, String> histogramKey = new Pair<>(histogramItem.getKey(), histogramColumn);
                GlobalStateMgr.getCurrentState().getEditLog()
                        .logRemoveHistogramStatsMeta(histogramStatsMetaMap.get(histogramKey));
                histogramStatsMetaMap.remove(histogramKey);
            }
        }
    }

    public void dropMultiColumnStatsMetaAndData(ConnectContext statsConnectCtx, Set<Long> tableIdHasDeleted) {
        List<MultiColumnStatsKey> keysToRemove = new ArrayList<>();

        for (Map.Entry<MultiColumnStatsKey, MultiColumnStatsMeta> entry : multiColumnStatsMetaMap.entrySet()) {
            MultiColumnStatsKey key = entry.getKey();
            MultiColumnStatsMeta value = entry.getValue();

            StatisticExecutor statisticExecutor = new StatisticExecutor();
            statisticExecutor.dropTableMultiColumnStatistics(statsConnectCtx, key.tableId);

            if (tableIdHasDeleted.contains(key.tableId)) {
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveMultiColumnStatsMeta(value);
                keysToRemove.add(key);
            }
        }

        keysToRemove.forEach(multiColumnStatsMetaMap::remove);
    }

    public void dropExternalBasicStatsMetaAndData(String catalogName, String dbName, String tableName) {
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropExternalBasicStatsData(catalogName, dbName, tableName);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().removeExternalBasicStatsMeta(catalogName, dbName, tableName);
    }

    public void dropExternalHistogramStatsMetaAndData(String catalogName, String dbName, String tableName) {
        List<String> columns = Lists.newArrayList();
        StatsMetaKey tableKey = new StatsMetaKey(catalogName, dbName, tableName);
        for (StatsMetaColumnKey histogramKey : externalHistogramStatsMetaMap.keySet()) {
            if (histogramKey.getTableKey().equals(tableKey)) {
                columns.add(histogramKey.getColumnName());
            }
        }

        dropExternalHistogramStatsMetaAndData(catalogName, dbName, tableName, columns);
    }

    public void dropExternalHistogramStatsMetaAndData(String catalogName, String dbName, String tableName,
                                                      List<String> columns) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalHistogram(StatisticUtils.buildConnectContext(), catalogName, dbName, tableName,
                columns);

        removeExternalHistogramStatsMeta(catalogName, dbName, tableName, columns);
    }

    public void dropExternalHistogramStatsMetaAndData(ConnectContext statsConnectCtx,
                                                      TableName tableName, Table table,
                                                      List<String> columns) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalHistogram(statsConnectCtx, table.getUUID(), columns);

        removeExternalHistogramStatsMeta(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), columns);
    }

    public void registerConnection(long analyzeID, ConnectContext ctx) {
        connectionMap.put(analyzeID, ctx);
    }

    public void unregisterConnection(long analyzeID, boolean killExecutor) {
        ConnectContext context = connectionMap.remove(analyzeID);
        if (killExecutor) {
            if (context != null) {
                context.kill(false, "kill analyze unregisterConnection");
            } else {
                throw new SemanticException("There is no running task with analyzeId " + analyzeID);
            }
        }
    }

    public void killConnection(long analyzeID) {
        ConnectContext context = connectionMap.get(analyzeID);
        if (context != null) {
            context.kill(false, "kill analyze");
        } else {
            throw new SemanticException("There is no running task with analyzeId " + analyzeID);
        }
    }

    public ExecutorService getAnalyzeTaskThreadPool() {
        return ANALYZE_TASK_THREAD_POOL;
    }

    public void updateLoadRows(TransactionState transactionState) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(transactionState.getDbId());
        if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
            return;
        }
        TxnCommitAttachment attachment = transactionState.getTxnCommitAttachment();
        if (attachment instanceof RLTaskTxnCommitAttachment) {
            if (!transactionState.getTableIdList().isEmpty()) {
                long tableId = transactionState.getTableIdList().get(0);
                long loadedRows = ((RLTaskTxnCommitAttachment) attachment).getLoadedRows();
                updateBasicStatsMeta(db.getId(), tableId, loadedRows);
            }
        } else if (attachment instanceof ManualLoadTxnCommitAttachment) {
            if (!transactionState.getTableIdList().isEmpty()) {
                long tableId = transactionState.getTableIdList().get(0);
                long loadedRows = ((ManualLoadTxnCommitAttachment) attachment).getLoadedRows();
                updateBasicStatsMeta(db.getId(), tableId, loadedRows);
            }
        } else if (attachment instanceof LoadJobFinalOperation) {
            LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) attachment;
            loadJobFinalOperation.getLoadingStatus().travelTableCounters(
                    kv -> updateBasicStatsMeta(db.getId(), kv.getKey(), kv.getValue().get(TableMetricsEntity.TABLE_LOAD_ROWS))
            );
        } else if (attachment instanceof InsertTxnCommitAttachment) {
            if (!transactionState.getTableIdList().isEmpty()) {
                long tableId = transactionState.getTableIdList().get(0);
                long loadRows = ((InsertTxnCommitAttachment) attachment).getLoadedRows();
                if (loadRows == 0) {
                    OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getId(), tableId);
                    loadRows = table != null ? table.getRowCount() : 0;
                }
                updateBasicStatsMeta(db.getId(), tableId, loadRows);
            }
        }
    }

    public void readFields(DataInputStream dis) throws IOException {
        // read job
        String s = Text.readString(dis);
        SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);

        if (null != data) {
            if (null != data.jobs) {
                for (NativeAnalyzeJob job : data.jobs) {
                    replayAddAnalyzeJob(job);
                }
            }

            if (null != data.nativeStatus) {
                for (AnalyzeStatus status : data.nativeStatus) {
                    replayAddAnalyzeStatus(status);
                }
            }

            if (null != data.basicStatsMeta) {
                for (BasicStatsMeta meta : data.basicStatsMeta) {
                    replayAddBasicStatsMeta(meta);
                }
            }

            if (null != data.histogramStatsMeta) {
                for (HistogramStatsMeta meta : data.histogramStatsMeta) {
                    replayAddHistogramStatsMeta(meta);
                }
            }

            if (null != data.multiColumnStatsMeta) {
                for (MultiColumnStatsMeta meta : data.multiColumnStatsMeta) {
                    replayAddMultiColumnStatsMeta(meta);
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // save history
        SerializeData data = new SerializeData();
        data.jobs = getAllNativeAnalyzeJobList();
        data.nativeStatus = new ArrayList<>(getAnalyzeStatusMap().values().stream().
                filter(AnalyzeStatus::isNative).
                map(status -> (NativeAnalyzeStatus) status).collect(Collectors.toSet()));
        data.basicStatsMeta = new ArrayList<>(getBasicStatsMetaMap().values());
        data.histogramStatsMeta = new ArrayList<>(getHistogramStatsMetaMap().values());
        data.multiColumnStatsMeta = new ArrayList<>(multiColumnStatsMetaMap.values());

        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(out, s);
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<AnalyzeStatus> analyzeStatuses = getAnalyzeStatusMap().values().stream()
                .distinct().collect(Collectors.toList());

        int numJson = 1 + analyzeJobMap.size()
                + 1 + analyzeStatuses.size()
                + 1 + basicStatsMetaMap.size()
                + 1 + histogramStatsMetaMap.size()
                + 1 + externalBasicStatsMetaMap.size()
                + 1 + externalHistogramStatsMetaMap.size()
                + 1 + multiColumnStatsMetaMap.size();

        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.ANALYZE_MGR, numJson);

        writer.writeInt(analyzeJobMap.size());
        for (AnalyzeJob analyzeJob : analyzeJobMap.values()) {
            writer.writeJson(analyzeJob);
        }

        writer.writeInt(analyzeStatuses.size());
        for (AnalyzeStatus analyzeStatus : analyzeStatuses) {
            writer.writeJson(analyzeStatus);
        }

        writer.writeInt(basicStatsMetaMap.size());
        for (BasicStatsMeta basicStatsMeta : basicStatsMetaMap.values()) {
            writer.writeJson(basicStatsMeta);
        }

        writer.writeInt(histogramStatsMetaMap.size());
        for (HistogramStatsMeta histogramStatsMeta : histogramStatsMetaMap.values()) {
            writer.writeJson(histogramStatsMeta);
        }

        writer.writeInt(externalBasicStatsMetaMap.size());
        for (ExternalBasicStatsMeta basicStatsMeta : externalBasicStatsMetaMap.values()) {
            writer.writeJson(basicStatsMeta);
        }

        writer.writeInt(externalHistogramStatsMetaMap.size());
        for (ExternalHistogramStatsMeta histogramStatsMeta : externalHistogramStatsMetaMap.values()) {
            writer.writeJson(histogramStatsMeta);
        }

        writer.writeInt(multiColumnStatsMetaMap.size());
        for (MultiColumnStatsMeta multiColumnStatsMeta : multiColumnStatsMetaMap.values()) {
            writer.writeJson(multiColumnStatsMeta);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(AnalyzeJob.class, this::replayAddAnalyzeJob);

        reader.readCollection(AnalyzeStatus.class, this::replayAddAnalyzeStatus);

        reader.readCollection(BasicStatsMeta.class, this::replayAddBasicStatsMeta);

        reader.readCollection(HistogramStatsMeta.class, this::replayAddHistogramStatsMeta);

        reader.readCollection(ExternalBasicStatsMeta.class, this::replayAddExternalBasicStatsMeta);

        reader.readCollection(ExternalHistogramStatsMeta.class, this::replayAddExternalHistogramStatsMeta);

        reader.readCollection(MultiColumnStatsMeta.class, this::replayAddMultiColumnStatsMeta);
    }

    private void updateBasicStatsMeta(long dbId, long tableId, long loadedRows) {
        BasicStatsMeta basicStatsMeta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(tableId);
        if (basicStatsMeta == null) {
            // first load without analyze op, we need fill a meta with loaded rows for cardinality estimation
            BasicStatsMeta meta = new BasicStatsMeta(dbId, tableId, Lists.newArrayList(),
                    StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                    StatsConstants.buildInitStatsProp(), loadedRows);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().put(tableId, meta);
        } else {
            basicStatsMeta.increaseDeltaRows(loadedRows);
        }
    }

    private static class SerializeData {
        @SerializedName("analyzeJobs")
        public List<NativeAnalyzeJob> jobs;

        @SerializedName("analyzeStatus")
        public List<NativeAnalyzeStatus> nativeStatus;

        @SerializedName("basicStatsMeta")
        public List<BasicStatsMeta> basicStatsMeta;

        @SerializedName("histogramStatsMeta")
        public List<HistogramStatsMeta> histogramStatsMeta;

        @SerializedName("multiColumnStatsMeta")
        public List<MultiColumnStatsMeta> multiColumnStatsMeta;
    }

    public static class StatsMetaKey {
        private final String catalogName;
        private final String dbName;
        private final String tableName;

        public StatsMetaKey(String catalogName, String dbName, String tableName) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StatsMetaKey)) {
                return false;
            }
            StatsMetaKey that = (StatsMetaKey) o;
            return Objects.equal(catalogName, that.catalogName) &&
                    Objects.equal(dbName, that.dbName) &&
                    Objects.equal(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(catalogName, dbName, tableName);
        }
    }

    public static class StatsMetaColumnKey {
        private final StatsMetaKey tableKey;
        private final String columnName;
        public StatsMetaColumnKey(String catalogName, String dbName, String tableName, String columnName) {
            this.tableKey = new StatsMetaKey(catalogName, dbName, tableName);
            this.columnName = columnName;
        }

        public StatsMetaKey getTableKey() {
            return tableKey;
        }

        public String getColumnName() {
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StatsMetaColumnKey)) {
                return false;
            }
            StatsMetaColumnKey that = (StatsMetaColumnKey) o;
            return Objects.equal(tableKey, that.tableKey) &&
                    Objects.equal(columnName, that.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tableKey, columnName);
        }
    }

    public static class MultiColumnStatsKey {
        private final long tableId;
        private final Set<Integer> columnIds;
        private final StatsConstants.StatisticsType statisticsType;

        public MultiColumnStatsKey(long tableId, Set<Integer> columnIds, StatsConstants.StatisticsType statisticsType) {
            this.tableId = tableId;
            this.columnIds = columnIds;
            this.statisticsType = statisticsType;
        }

        public long getTableId() {
            return tableId;
        }

        public Set<Integer> getColumnIds() {
            return columnIds;
        }

        public StatsConstants.StatisticsType getStatisticsType() {
            return statisticsType;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MultiColumnStatsKey that = (MultiColumnStatsKey) o;
            return tableId == that.tableId && java.util.Objects.equals(columnIds, that.columnIds) &&
                    statisticsType == that.statisticsType;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(tableId, columnIds, statisticsType);
        }
    }
}
