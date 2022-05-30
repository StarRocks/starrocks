// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.Constants.AnalyzeType;
import com.starrocks.statistic.Constants.ScheduleStatus;
import com.starrocks.statistic.Constants.ScheduleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticAutoCollector extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticAutoCollector.class);

    private static final StatisticExecutor statisticExecutor = new StatisticExecutor();

    public StatisticAutoCollector() {
        super("AutoStatistic", Config.statistic_collect_interval_sec * 1000);
    }

    private static class TableCollectJob {
        public AnalyzeJob job;
        public Database db;
        public Table table;
        public List<String> columns;

        public List<Long> partitionIdList = new ArrayList<>();

        private void tryCollect() throws Exception {
            if (AnalyzeType.FULL == job.getType()) {
                if (Config.enable_collect_full_statistics) {
                    for (Long partitionId : partitionIdList) {
                        statisticExecutor.fullCollectStatisticSyncV2(db.getId(), table.getId(), partitionId, columns);
                    }
                } else {
                    statisticExecutor.fullCollectStatisticSync(db.getId(), table.getId(), columns);
                }
            } else if (AnalyzeType.SAMPLE == job.getType()) {
                statisticExecutor
                        .sampleCollectStatisticSync(db.getId(), table.getId(), columns, job.getSampleCollectRows());
            }
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        // update interval
        if (getInterval() != Config.statistic_collect_interval_sec * 1000) {
            setInterval(Config.statistic_collect_interval_sec * 1000);
        }

        GlobalStateMgr.getCurrentAnalyzeMgr().expireAnalyzeJob();

        if (!Config.enable_statistic_collect) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        initDefaultJob();

        List<TableCollectJob> allJobs;
        if (Config.enable_collect_full_statistics) {
            allJobs = generateFullAnalyzeJobs();
        } else {
            allJobs = generateAllJobs();
        }

        collectStatistics(allJobs);

        expireStatistic();
    }

    private void initDefaultJob() {
        // Add a default sample job if was't been collect
        List<AnalyzeJob> allAnalyzeJobs = GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
        if (allAnalyzeJobs.stream().anyMatch(j -> j.getScheduleType() == ScheduleType.SCHEDULE)) {
            return;
        }

        AnalyzeJob analyzeJob = new AnalyzeJob();
        // all databases
        analyzeJob.setDbId(AnalyzeJob.DEFAULT_ALL_ID);
        analyzeJob.setTableId(AnalyzeJob.DEFAULT_ALL_ID);
        analyzeJob.setColumns(Collections.emptyList());
        analyzeJob.setScheduleType(ScheduleType.SCHEDULE);
        analyzeJob.setType(AnalyzeType.SAMPLE);
        analyzeJob.setStatus(ScheduleStatus.PENDING);
        analyzeJob.setWorkTime(LocalDateTime.MIN);

        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeJob(analyzeJob);
    }

    private void collectStatistics(List<TableCollectJob> allJobs) {
        // AnalyzeJob-List<TableCollectJob> for update AnalyzeJob status
        Map<AnalyzeJob, List<TableCollectJob>> analyzeJobMap = Maps.newHashMap();

        for (TableCollectJob tjb : allJobs) {
            if (!analyzeJobMap.containsKey(tjb.job)) {
                analyzeJobMap.put(tjb.job, Lists.newArrayList(tjb));
            } else {
                analyzeJobMap.get(tjb.job).add(tjb);
            }
        }

        for (Map.Entry<AnalyzeJob, List<TableCollectJob>> entry : analyzeJobMap.entrySet()) {
            AnalyzeJob analyzeJob = entry.getKey();
            if (analyzeJob.getStatus() == ScheduleStatus.FINISH) {
                continue;
            }

            if (ScheduleType.SCHEDULE == analyzeJob.getScheduleType() &&
                    LocalDateTime.now().minusSeconds(analyzeJob.getUpdateIntervalSec())
                            .isBefore(analyzeJob.getWorkTime())) {
                continue;
            }

            analyzeJob.setStatus(ScheduleStatus.RUNNING);
            analyzeJob.setReason("");
            // only update job
            GlobalStateMgr.getCurrentAnalyzeMgr().updateAnalyzeJobWithoutLog(analyzeJob);
            for (TableCollectJob tcj : entry.getValue()) {
                try {
                    LOG.info("Statistic collect work job: {}, type: {}, db: {}, table: {}",
                            analyzeJob.getId(), analyzeJob.getType(), tcj.db.getFullName(), tcj.table.getName());
                    tcj.tryCollect();

                    GlobalStateMgr.getCurrentStatisticStorage().expireColumnStatistics(tcj.table, tcj.columns);
                } catch (Exception e) {
                    LOG.warn("Statistic collect work job: {}, type: {}, db: {}, table: {}. throw exception.",
                            analyzeJob.getId(), analyzeJob.getType(), tcj.db.getFullName(), tcj.table.getName(), e);

                    if (analyzeJob.getReason().length() < 40) {
                        String error =
                                analyzeJob.getReason() + "\n" + tcj.db.getFullName() + "." + tcj.table.getName() +
                                        ": " + e.getMessage();
                        analyzeJob.setReason(error);
                    }
                }
            }

            // update & record
            if (ScheduleType.ONCE == analyzeJob.getScheduleType()) {
                analyzeJob.setStatus(ScheduleStatus.FINISH);
            } else {
                analyzeJob.setStatus(ScheduleStatus.PENDING);
            }
            analyzeJob.setWorkTime(LocalDateTime.now());
            GlobalStateMgr.getCurrentAnalyzeMgr().updateAnalyzeJobWithLog(analyzeJob);
        }
    }

    private List<TableCollectJob> generateFullAnalyzeJobs() {
        AnalyzeJob analyzeJob = new AnalyzeJob();
        analyzeJob.setDbId(AnalyzeJob.DEFAULT_ALL_ID);
        analyzeJob.setTableId(AnalyzeJob.DEFAULT_ALL_ID);
        analyzeJob.setColumns(Lists.newArrayList());
        analyzeJob.setType(AnalyzeType.FULL);
        analyzeJob.setScheduleType(ScheduleType.SCHEDULE);
        analyzeJob.setWorkTime(LocalDateTime.now());
        analyzeJob.setStatus(Constants.ScheduleStatus.PENDING);

        Map<Long, TableCollectJob> allTableJobMap = Maps.newHashMap();

        // all database
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();

        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                continue;
            }

            for (Table table : db.getTables()) {
                createTableJobs(allTableJobMap, analyzeJob, db, table);
            }
        }

        List<TableCollectJob> tableCollectJobs = new ArrayList<>(allTableJobMap.values());
        for (TableCollectJob tableCollectJob : tableCollectJobs) {
            List<Partition> partitions = Lists.newArrayList(((OlapTable) tableCollectJob.table).getPartitions());
            for (Partition partition : partitions) {
                LocalDateTime updateTime = StatisticUtils.getPartitionLastUpdateTime(partition);

                if ((ScheduleType.SCHEDULE.equals(tableCollectJob.job.getScheduleType())
                        && !StatisticUtils.isEmptyPartition(partition)
                        && tableCollectJob.job.getWorkTime().isBefore(updateTime))) {
                    tableCollectJob.partitionIdList.add(partition.getId());
                }
            }
        }

        return tableCollectJobs;
    }

    private List<TableCollectJob> generateAllJobs() {
        List<AnalyzeJob> allAnalyzeJobs = GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
        // The jobs need to be sorted in order of execution to avoid duplicate collections
        allAnalyzeJobs.sort(Comparator.comparing(AnalyzeJob::getId));

        Map<Long, TableCollectJob> allTableJobMap = Maps.newHashMap();

        for (AnalyzeJob analyzeJob : allAnalyzeJobs) {
            if (AnalyzeJob.DEFAULT_ALL_ID == analyzeJob.getDbId()) {

                // all database
                List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();

                for (Long dbId : dbIds) {
                    Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                    if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                        continue;
                    }

                    for (Table table : db.getTables()) {
                        createTableJobs(allTableJobMap, analyzeJob, db, table);
                    }
                }
            } else if (AnalyzeJob.DEFAULT_ALL_ID == analyzeJob.getTableId()
                    && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getDbId()) {
                // all table
                Database db = GlobalStateMgr.getCurrentState().getDb(analyzeJob.getDbId());
                if (null == db) {
                    continue;
                }

                for (Table table : db.getTables()) {
                    createTableJobs(allTableJobMap, analyzeJob, db, table);
                }
            } else if ((null == analyzeJob.getColumns() || analyzeJob.getColumns().isEmpty())
                    && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getTableId()
                    && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getDbId()) {
                // all column
                Database db = GlobalStateMgr.getCurrentState().getDb(analyzeJob.getDbId());
                if (null == db) {
                    continue;
                }

                createTableJobs(allTableJobMap, analyzeJob, db, db.getTable(analyzeJob.getTableId()));
            } else if (!analyzeJob.getColumns().isEmpty() && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getTableId()
                    && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getDbId()) {
                // some column
                Database db = GlobalStateMgr.getCurrentState().getDb(analyzeJob.getDbId());
                if (null == db) {
                    continue;
                }

                createTableJobs(allTableJobMap, analyzeJob, db, db.getTable(analyzeJob.getTableId()),
                        analyzeJob.getColumns());
            }
        }

        return new ArrayList<>(allTableJobMap.values());
    }

    private void createTableJobs(Map<Long, TableCollectJob> tableJobs, AnalyzeJob job,
                                 Database db, Table table) {
        if (null == table || !Table.TableType.OLAP.equals(table.getType())) {
            return;
        }

        List<String> columns = table.getFullSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                .collect(Collectors.toList());
        createTableJobs(tableJobs, job, db, table, columns);
    }

    private void createTableJobs(Map<Long, TableCollectJob> tableJobs, AnalyzeJob job,
                                 Database db, Table table, List<String> columns) {
        // check table has update
        LocalDateTime updateTime = StatisticUtils.getTableLastUpdateTime(table);

        // 1. If job is schedule and the table has update, we need re-collect data
        // 2. If job is once and is happened after the table update, we need add it to avoid schedule-job cover data
        if ((ScheduleType.SCHEDULE.equals(job.getScheduleType()) && !StatisticUtils.isEmptyTable(table) &&
                job.getWorkTime().isBefore(updateTime)) ||
                (ScheduleType.ONCE.equals(job.getScheduleType()) && job.getWorkTime().isAfter(updateTime))) {
            createJobs(tableJobs, job, db, table, columns);
        } else {
            LOG.debug("Skip collect on table: " + table.getName() + ", updateTime: " + updateTime +
                    ", JobId: " + job.getId() + ", lastCollectTime: " + job.getWorkTime());
        }
    }

    private void createJobs(Map<Long, TableCollectJob> result,
                            AnalyzeJob job, Database db, Table table, List<String> columns) {
        TableCollectJob tableJob = new TableCollectJob();
        tableJob.job = job;
        tableJob.db = db;
        tableJob.table = table;
        tableJob.columns = columns;
        result.put(table.getId(), tableJob);
    }

    private void expireStatistic() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        List<Long> tables = Lists.newArrayList();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                continue;
            }

            db.getTables().stream().map(Table::getId).forEach(tables::add);
        }
        try {
            List<String> expireTables = statisticExecutor.queryExpireTableSync(tables);

            if (expireTables.isEmpty()) {
                return;
            }
            LOG.info("Statistic expire tableIds: {}", expireTables);
            statisticExecutor.expireStatisticSync(expireTables);
        } catch (Exception e) {
            LOG.warn("expire statistic failed.", e);
        }
    }
}
