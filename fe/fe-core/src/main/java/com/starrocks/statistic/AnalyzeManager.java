// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
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
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class AnalyzeManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(AnalyzeManager.class);

    private final Map<Long, AnalyzeJob> analyzeJobMap;
    private final Map<Long, AnalyzeStatus> analyzeStatusMap;
    private final Map<Long, BasicStatsMeta> basicStatsMetaMap;
    private final Map<Pair<Long, String>, HistogramStatsMeta> histogramStatsMetaMap;

    private static final ExecutorService executor =
            ThreadPoolManager.newDaemonFixedThreadPool(1, 16, "analyze-replay-pool", true);

    public AnalyzeManager() {
        analyzeJobMap = Maps.newConcurrentMap();
        analyzeStatusMap = Maps.newConcurrentMap();
        basicStatsMetaMap = Maps.newConcurrentMap();
        histogramStatsMetaMap = Maps.newConcurrentMap();
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

    public void replayAddAnalyzeJob(AnalyzeJob job) {
        executor.submit(new AnalyzeReplayTask(job));
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
            if (analyzeStatus.getTableId() == tableId) {
                expireList.add(analyzeStatus);
            }
        }
        expireList.forEach(status -> analyzeStatusMap.remove(status.getId()));
        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeStatus(status);
        }
    }

    public void addBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddBasicStatsMeta(basicStatsMeta);
    }

    public void replayAddBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
    }

    public void replayRemoveBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.remove(basicStatsMeta.getTableId());
    }

    public Map<Long, BasicStatsMeta> getBasicStatsMetaMap() {
        return basicStatsMetaMap;
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

            db.getTables().stream().map(Table::getId).forEach(tables::add);
        }

        Set<Long> tableIdHasDeleted = new HashSet<>(basicStatsMetaMap.keySet());
        tableIdHasDeleted.removeAll(tables);

        for (BasicStatsMeta basicStatsMeta : basicStatsMetaMap.values()) {
            Database db = GlobalStateMgr.getCurrentState().getDb(basicStatsMeta.getDbId());
            Table table = db.getTable(basicStatsMeta.getTableId());
            /*
             * If the meta contains statistical information, but the data is empty,
             * it means that the table has been truncate or insert overwrite, and it is set to empty,
             * so it is treated as a table that has been deleted here.
             */
            if (StatisticUtils.isEmptyTable(table)) {
                tableIdHasDeleted.add(table.getId());
            }
        }

        dropBasicStatsMetaAndData(tableIdHasDeleted);
        dropHistogramStatsMetaAndData(tableIdHasDeleted);
    }

    public void dropBasicStatsMetaAndData(Set<Long> tableIdHasDeleted) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        for (Long tableId : tableIdHasDeleted) {
            BasicStatsMeta basicStatsMeta = basicStatsMetaMap.get(tableId);
            if (basicStatsMeta == null) {
                continue;
            }
            // Both types of tables need to be deleted, because there may have been a switch of
            // collecting statistics types, leaving some discarded statistics data.
            statisticExecutor.dropTableStatistics(tableId, StatsConstants.AnalyzeType.SAMPLE);
            statisticExecutor.dropTableStatistics(tableId, StatsConstants.AnalyzeType.FULL);
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveBasicStatsMeta(basicStatsMetaMap.get(tableId));
            basicStatsMetaMap.remove(tableId);
        }
    }

    public void dropHistogramStatsMetaAndData(Set<Long> tableIdHasDeleted) {
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
            statisticExecutor.dropHistogram(histogramItem.getKey(), histogramItem.getValue());

            for (String histogramColumn : histogramItem.getValue()) {
                Pair<Long, String> histogramKey = new Pair<>(histogramItem.getKey(), histogramColumn);
                GlobalStateMgr.getCurrentState().getEditLog()
                        .logRemoveHistogramStatsMeta(histogramStatsMetaMap.get(histogramKey));
                histogramStatsMetaMap.remove(histogramKey);
            }
        }
    }

    public void updateLoadRows(TransactionState transactionState) {
        Database db = GlobalStateMgr.getCurrentState().getDb(transactionState.getDbId());
        if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
            return;
        }
        TxnCommitAttachment attachment = transactionState.getTxnCommitAttachment();
        if (attachment instanceof RLTaskTxnCommitAttachment) {
            BasicStatsMeta basicStatsMeta =
                    GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(transactionState.getTableIdList().get(0));
            if (basicStatsMeta != null) {
                basicStatsMeta.increaseUpdateRows(((RLTaskTxnCommitAttachment) attachment).getLoadedRows());
            }
        } else if (attachment instanceof ManualLoadTxnCommitAttachment) {
            BasicStatsMeta basicStatsMeta =
                    GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(transactionState.getTableIdList().get(0));
            if (basicStatsMeta != null) {
                basicStatsMeta.increaseUpdateRows(((ManualLoadTxnCommitAttachment) attachment).getLoadedRows());
            }
        } else if (attachment instanceof LoadJobFinalOperation) {
            LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) attachment;
            loadJobFinalOperation.getLoadingStatus().travelTableCounters(
                    kv -> {
                        BasicStatsMeta basicStatsMeta =
                                GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(kv.getKey());
                        if (basicStatsMeta != null) {
                            basicStatsMeta.increaseUpdateRows(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_ROWS));
                        }
                    }
            );
        } else if (attachment instanceof InsertTxnCommitAttachment) {
            BasicStatsMeta basicStatsMeta =
                    GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(transactionState.getTableIdList().get(0));
            if (basicStatsMeta != null) {
                basicStatsMeta.increaseUpdateRows(((InsertTxnCommitAttachment) attachment).getLoadedRows());
            }
        }
    }

    public void readFields(DataInputStream dis) throws IOException {
        // read job
        String s = Text.readString(dis);
        SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);

        if (null != data) {
            if (null != data.jobs) {
                for (AnalyzeJob job : data.jobs) {
                    replayAddAnalyzeJob(job);
                }
            }

            if (null != data.status) {
                for (AnalyzeStatus status : data.status) {
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
        data.jobs = getAllAnalyzeJobList();
        data.status = new ArrayList<>(getAnalyzeStatusMap().values());
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

    private static class SerializeData {
        @SerializedName("analyzeJobs")
        public List<AnalyzeJob> jobs;

        @SerializedName("analyzeStatus")
        public List<AnalyzeStatus> status;

        @SerializedName("basicStatsMeta")
        public List<BasicStatsMeta> basicStatsMeta;

        @SerializedName("histogramStatsMeta")
        public List<HistogramStatsMeta> histogramStatsMeta;
    }

    // This task is used to expire cached statistics
    public class AnalyzeReplayTask implements Runnable {
        private AnalyzeJob analyzeJob;

        public AnalyzeReplayTask(AnalyzeJob job) {
            this.analyzeJob = job;
        }

        public void checkAndExpireCachedStatistics(Table table, AnalyzeJob job) {
            if (null == table || !table.isNativeTable()) {
                return;
            }

            // check table has update
            // use job last work time compare table update time to determine whether to expire cached statistics
            LocalDateTime updateTime = StatisticUtils.getTableLastUpdateTime(table);
            LocalDateTime jobLastWorkTime = LocalDateTime.MIN;
            if (analyzeJobMap.containsKey(job.getId())) {
                jobLastWorkTime = analyzeJobMap.get(job.getId()).getWorkTime();
            }
            if (jobLastWorkTime.isBefore(updateTime)) {
                List<String> columns = (job.getColumns() == null || job.getColumns().isEmpty()) ?
                        table.getFullSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                                .collect(Collectors.toList()) : job.getColumns();
                GlobalStateMgr.getCurrentStatisticStorage().expireColumnStatistics(table, columns);
            }
        }

        public void expireCachedStatistics(AnalyzeJob job) {
            if (job.getScheduleType().equals(StatsConstants.ScheduleType.ONCE)) {
                Database db = GlobalStateMgr.getCurrentState().getDb(job.getDbId());
                if (null == db) {
                    return;
                }
                GlobalStateMgr.getCurrentStatisticStorage()
                        .expireColumnStatistics(db.getTable(job.getTableId()), job.getColumns());
            } else {
                List<Table> tableNeedCheck = new ArrayList<>();
                if (job.getDbId() == StatsConstants.DEFAULT_ALL_ID) {
                    List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
                    for (Long dbId : dbIds) {
                        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                        if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                            continue;
                        }
                        tableNeedCheck.addAll(db.getTables());
                    }
                } else if (job.getDbId() != StatsConstants.DEFAULT_ALL_ID &&
                        job.getTableId() == StatsConstants.DEFAULT_ALL_ID) {
                    Database db = GlobalStateMgr.getCurrentState().getDb(job.getDbId());
                    if (null == db) {
                        return;
                    }
                    tableNeedCheck.addAll(db.getTables());
                } else {
                    Database db = GlobalStateMgr.getCurrentState().getDb(job.getDbId());
                    if (null == db) {
                        return;
                    }
                    tableNeedCheck.add(db.getTable(job.getTableId()));
                }

                for (Table table : tableNeedCheck) {
                    checkAndExpireCachedStatistics(table, job);
                }
            }
        }

        @Override
        public void run() {
            expireCachedStatistics(analyzeJob);
        }
    }
}
