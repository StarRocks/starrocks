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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
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

    // ConnectContext of all currently running analyze tasks
    private final Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    // only first load of table will trigger analyze, so we don't need limit thread pool queue size
    private static final ExecutorService ANALYZE_TASK_THREAD_POOL = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.statistic_collect_concurrency, Integer.MAX_VALUE,
            "analyze-task-concurrency-pool", true);

    private final Set<Long> dropPartitionIds = new ConcurrentSkipListSet<>();
    private final List<Pair<Long, Long>> checkTableIds = Lists.newArrayList(CHECK_ALL_TABLES);

    public AnalyzeMgr() {
        analyzeJobMap = Maps.newConcurrentMap();
        analyzeStatusMap = Maps.newConcurrentMap();
        basicStatsMetaMap = Maps.newConcurrentMap();
        externalBasicStatsMetaMap = Maps.newConcurrentMap();
        histogramStatsMetaMap = Maps.newConcurrentMap();
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
        statisticExecutor.dropTableStatistics(StatisticUtils.buildConnectContext(), tableUUID);
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

    public void replayRemoveExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        externalBasicStatsMetaMap.remove(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                basicStatsMeta.getDbName(), basicStatsMeta.getTableName()));
    }

    public void refreshBasicStatisticsCache(Long dbId, Long tableId, List<String> columns, boolean async) {
        Table table;
        try {
            table = MetaUtils.getTable(dbId, tableId);
        } catch (SemanticException e) {
            return;
        }

        GlobalStateMgr.getCurrentStatisticStorage().expireTableAndColumnStatistics(table, columns);
        if (async) {
            GlobalStateMgr.getCurrentStatisticStorage().refreshTableStatistic(table);
            GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistics(table, columns);
        } else {
            GlobalStateMgr.getCurrentStatisticStorage().refreshTableStatisticSync(table);
            GlobalStateMgr.getCurrentStatisticStorage().getColumnStatisticsSync(table, columns);
        }
    }

    public void refreshConnectorTableBasicStatisticsCache(String catalogName, String dbName, String tableName,
                                                          List<String> columns, boolean async) {
        Table table;
        try {
            table = MetaUtils.getTable(catalogName, dbName, tableName);
        } catch (Exception e) {
            return;
        }

        GlobalStateMgr.getCurrentStatisticStorage().expireConnectorTableColumnStatistics(table, columns);
        if (async) {
            GlobalStateMgr.getCurrentStatisticStorage().getConnectorTableStatistics(table, columns);
        } else {
            GlobalStateMgr.getCurrentStatisticStorage().getConnectorTableStatisticsSync(table, columns);
        }
    }

    public void replayRemoveBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.remove(basicStatsMeta.getTableId());
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (null == db) {
            return;
        }
        Table table = db.getTable(tableId);
        if (null == table) {
            return;
        }

        GlobalStateMgr.getCurrentStatisticStorage().expireHistogramStatistics(table.getId(), columns);
        if (async) {
            GlobalStateMgr.getCurrentStatisticStorage().getHistogramStatistics(table, columns);
        } else {
            GlobalStateMgr.getCurrentStatisticStorage().getHistogramStatisticsSync(table, columns);
        }
    }

    public void replayRemoveHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.remove(new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()));
    }

    public Map<Pair<Long, String>, HistogramStatsMeta> getHistogramStatsMetaMap() {
        return histogramStatsMetaMap;
    }

    public void clearStatisticFromDroppedTable() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        Set<Long> tables = new HashSet<>();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                continue;
            }

            for (Table table : db.getTables()) {
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
        statsConnectCtx.setThreadLocalInfo();
        statsConnectCtx.setStatisticsConnection(true);

        dropBasicStatsMetaAndData(statsConnectCtx, tableIdHasDeleted);
        dropHistogramStatsMetaAndData(statsConnectCtx, tableIdHasDeleted);
    }

    public void dropPartition(long partitionId) {
        dropPartitionIds.add(partitionId);
    }

    public void clearStatisticFromDroppedPartition() {
        checkAndDropPartitionStatistics();
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

    private void checkAndDropPartitionStatistics() {
        if (!Config.statistic_check_expire_partition || checkTableIds.isEmpty()) {
            return;
        }

        if (checkTableIds.contains(CHECK_ALL_TABLES)) {
            checkTableIds.clear();
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                for (Table table : db.getTables()) {
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
            Database db = GlobalStateMgr.getCurrentState().getDb(dbTableId.first);
            if (null == db) {
                continue;
            }

            Table table = db.getTable(dbTableId.second);
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
        for (Long tableId : tableIdHasDeleted) {
            BasicStatsMeta basicStatsMeta = basicStatsMetaMap.get(tableId);
            if (basicStatsMeta == null) {
                continue;
            }
            // Both types of tables need to be deleted, because there may have been a switch of
            // collecting statistics types, leaving some discarded statistics data.
            statisticExecutor.dropTableStatistics(statsConnectCtx, tableId, StatsConstants.AnalyzeType.SAMPLE);
            statisticExecutor.dropTableStatistics(statsConnectCtx, tableId, StatsConstants.AnalyzeType.FULL);
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveBasicStatsMeta(basicStatsMetaMap.get(tableId));
            basicStatsMetaMap.remove(tableId);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(transactionState.getDbId());
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
                    OlapTable table = (OlapTable) db.getTable(tableId);
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

        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(out, s);
    }

    public long loadAnalyze(DataInputStream dis, long checksum) throws IOException {
        try {
            readFields(dis);
            LOG.info("finished replay analyze job from image");
        } catch (EOFException e) {
            LOG.info("none analyze job replay.");
        }
        return checksum;
    }

    public long saveAnalyze(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        List<AnalyzeStatus> analyzeStatuses = getAnalyzeStatusMap().values().stream()
                .distinct().collect(Collectors.toList());

        int numJson = 1 + analyzeJobMap.size()
                + 1 + analyzeStatuses.size()
                + 1 + basicStatsMetaMap.size()
                + 1 + histogramStatsMetaMap.size()
                + 1 + externalBasicStatsMetaMap.size();

        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.ANALYZE_MGR, numJson);

        writer.writeJson(analyzeJobMap.size());
        for (AnalyzeJob analyzeJob : analyzeJobMap.values()) {
            writer.writeJson(analyzeJob);
        }

        writer.writeJson(analyzeStatuses.size());
        for (AnalyzeStatus analyzeStatus : analyzeStatuses) {
            writer.writeJson(analyzeStatus);
        }

        writer.writeJson(basicStatsMetaMap.size());
        for (BasicStatsMeta basicStatsMeta : basicStatsMetaMap.values()) {
            writer.writeJson(basicStatsMeta);
        }

        writer.writeJson(histogramStatsMetaMap.size());
        for (HistogramStatsMeta histogramStatsMeta : histogramStatsMetaMap.values()) {
            writer.writeJson(histogramStatsMeta);
        }

        writer.writeJson(externalBasicStatsMetaMap.size());
        for (ExternalBasicStatsMeta basicStatsMeta : externalBasicStatsMetaMap.values()) {
            writer.writeJson(basicStatsMeta);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int analyzeJobSize = reader.readInt();
        for (int i = 0; i < analyzeJobSize; ++i) {
            AnalyzeJob analyzeJob = reader.readJson(AnalyzeJob.class);
            replayAddAnalyzeJob(analyzeJob);
        }

        int analyzeStatusSize = reader.readInt();
        for (int i = 0; i < analyzeStatusSize; ++i) {
            AnalyzeStatus analyzeStatus = reader.readJson(AnalyzeStatus.class);
            replayAddAnalyzeStatus(analyzeStatus);
        }

        int basicStatsMetaSize = reader.readInt();
        for (int i = 0; i < basicStatsMetaSize; ++i) {
            BasicStatsMeta basicStatsMeta = reader.readJson(BasicStatsMeta.class);
            replayAddBasicStatsMeta(basicStatsMeta);
        }

        int histogramStatsMetaSize = reader.readInt();
        for (int i = 0; i < histogramStatsMetaSize; ++i) {
            HistogramStatsMeta histogramStatsMeta = reader.readJson(HistogramStatsMeta.class);
            replayAddHistogramStatsMeta(histogramStatsMeta);
        }

        int externalBasicStatsMetaSize = reader.readInt();
        for (int i = 0; i < externalBasicStatsMetaSize; ++i) {
            ExternalBasicStatsMeta basicStatsMeta = reader.readJson(ExternalBasicStatsMeta.class);
            replayAddExternalBasicStatsMeta(basicStatsMeta);
        }
    }

    private void updateBasicStatsMeta(long dbId, long tableId, long loadedRows) {
        BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(tableId);
        if (basicStatsMeta == null) {
            // first load without analyze op, we need fill a meta with loaded rows for cardinality estimation
            BasicStatsMeta meta = new BasicStatsMeta(dbId, tableId, Lists.newArrayList(),
                    StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                    StatsConstants.buildInitStatsProp(), loadedRows);
            GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().put(tableId, meta);
        } else {
            basicStatsMeta.increaseUpdateRows(loadedRows);
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
    }

    public static class StatsMetaKey {
        String catalogName;
        String dbName;
        String tableName;
        public StatsMetaKey(String catalogName, String dbName, String tableName) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
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
}
