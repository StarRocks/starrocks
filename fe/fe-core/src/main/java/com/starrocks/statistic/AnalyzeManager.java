// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
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
import java.time.Clock;
import java.time.Instant;
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

    private static final ExecutorService executor =
            ThreadPoolManager.newDaemonFixedThreadPool(1, 16, "analyze-replay-pool", true);

    public AnalyzeManager() {
        analyzeJobMap = Maps.newConcurrentMap();
        analyzeStatusMap = Maps.newConcurrentMap();
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

            long maxTime = ((OlapTable) table).getPartitions().stream().map(Partition::getVisibleVersionTime)
                    .max(Long::compareTo).orElse(0L);

            LocalDateTime updateTime =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(maxTime), Clock.systemDefaultZone().getZone());

            // keep show 1 day
            if (updateTime.plusDays(1).isBefore(now)) {
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

    public void addAnalyzeStatus(AnalyzeJob job) {
        AnalyzeStatus analyzeStatus = new AnalyzeStatus(job.getId(), job.getDbId(), job.getTableId(),
                job.getType(),
                job.getScheduleType(),
                LocalDateTime.now());
        analyzeStatusMap.put(job.getTableId(), analyzeStatus);
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeStatus(analyzeStatus);
    }

    public void updateAnalyzeStatusWithLog(AnalyzeJob job) {
        AnalyzeStatus analyzeStatus = new AnalyzeStatus(job.getId(), job.getDbId(), job.getTableId(),
                job.getType(),
                job.getScheduleType(),
                LocalDateTime.now());
        analyzeStatusMap.put(job.getTableId(), analyzeStatus);
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeStatus(analyzeStatus);
    }

    public void replayAddAnalyzeStatus(AnalyzeStatus status) {
        analyzeStatusMap.put(status.getTableId(), status);
    }

    public List<AnalyzeStatus> getAllAnalyzeStatus() {
        return Lists.newLinkedList(analyzeStatusMap.values());
    }

    public void readFields(DataInputStream dis) throws IOException {
        // read job
        String s = Text.readString(dis);
        SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);

        if (null != data && null != data.jobs) {
            for (AnalyzeJob job : data.jobs) {
                replayAddAnalyzeJob(job);
            }
        }

        if (null != data && null != data.status) {
            for (AnalyzeStatus status : data.status) {
                replayAddAnalyzeStatus(status);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // save history
        SerializeData data = new SerializeData();
        data.jobs = getAllAnalyzeJobList();
        data.status = getAllAnalyzeStatus();

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
