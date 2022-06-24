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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    // expire finish job
    public void expireAnalyzeJob() {
        List<AnalyzeJob> expireList = Lists.newArrayList();

        LocalDateTime now = LocalDateTime.now();
        for (AnalyzeJob job : analyzeJobMap.values()) {
            if (Constants.ScheduleStatus.FINISH != job.getStatus()) {
                continue;
            }

            if (AnalyzeJob.DEFAULT_ALL_ID == job.getDbId() || AnalyzeJob.DEFAULT_ALL_ID == job.getTableId()) {
                // finish job must be schedule once job, must contains db and table
                LOG.warn("expire analyze job check failed, contain default id job: " + job.getId());
                continue;
            }

            // check db/table
            Database db = GlobalStateMgr.getCurrentState().getDb(job.getDbId());
            if (null == db) {
                expireList.add(job);
                continue;
            }

            Table table = db.getTable(job.getTableId());
            if (null == table) {
                expireList.add(job);
                continue;
            }

            if (table.getType() != Table.TableType.OLAP) {
                expireList.add(job);
                continue;
            }

            // keep show 1 day
            if (job.getWorkTime().plusDays(1).isBefore(now)) {
                expireList.add(job);
            }
        }

        expireList.forEach(d -> analyzeJobMap.remove(d.getId()));
        for (AnalyzeJob job : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveAnalyzeJob(job);
        }
    }

    public void replayAddAnalyzeJob(AnalyzeJob job) {
        executor.submit(new AnalyzeReplayTask(job));
        analyzeJobMap.put(job.getId(), job);
    }

    public void replayRemoveAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.remove(job.getId());
    }

    public void addAnalyzeStatus(AnalyzeStatus status) {
        if (Config.enable_collect_full_statistics) {
            analyzeStatusMap.put(status.getId(), status);
            GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeStatus(status);
        }
    }

    public void replayAddAnalyzeStatus(AnalyzeStatus status) {
        if (status.getStatus().equals(Constants.ScheduleStatus.RUNNING)) {
            status.setStatus(Constants.ScheduleStatus.FAILED);
        }
        analyzeStatusMap.put(status.getId(), status);
    }

    public Map<Long, AnalyzeStatus> getAnalyzeStatusMap() {
        return analyzeStatusMap;
    }

    public void addBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        if (Config.enable_collect_full_statistics) {
            basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
            GlobalStateMgr.getCurrentState().getEditLog().logAddBasicStatsMeta(basicStatsMeta);
        }
    }

    public void replayAddBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
    }

    public Map<Long, BasicStatsMeta> getBasicStatsMetaMap() {
        return basicStatsMetaMap;
    }

    public void addHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.put(
                new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()), histogramStatsMeta);
        GlobalStateMgr.getCurrentState().getEditLog().logAddHistogramMeta(histogramStatsMeta);
    }

    public void replayAddHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.put(
                new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()), histogramStatsMeta);
    }

    public Map<Pair<Long, String>, HistogramStatsMeta> getHistogramStatsMetaMap() {
        return histogramStatsMetaMap;
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
            if (null == table || !Table.TableType.OLAP.equals(table.getType())) {
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
            if (job.getScheduleType().equals(Constants.ScheduleType.ONCE)) {
                Database db = GlobalStateMgr.getCurrentState().getDb(job.getDbId());
                if (null == db) {
                    return;
                }
                GlobalStateMgr.getCurrentStatisticStorage()
                        .expireColumnStatistics(db.getTable(job.getTableId()), job.getColumns());
            } else {
                List<Table> tableNeedCheck = new ArrayList<>();
                if (job.getDbId() == AnalyzeJob.DEFAULT_ALL_ID) {
                    List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
                    for (Long dbId : dbIds) {
                        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                        if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                            continue;
                        }
                        tableNeedCheck.addAll(db.getTables());
                    }
                } else if (job.getDbId() != AnalyzeJob.DEFAULT_ALL_ID &&
                        job.getTableId() == AnalyzeJob.DEFAULT_ALL_ID) {
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
