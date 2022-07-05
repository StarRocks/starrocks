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
import com.starrocks.common.FeConstants;
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

    @Override
    protected void runAfterCatalogReady() {
        // update interval
        if (getInterval() != Config.statistic_collect_interval_sec * 1000) {
            setInterval(Config.statistic_collect_interval_sec * 1000);
        }

        GlobalStateMgr.getCurrentAnalyzeMgr().expireAnalyzeJob();

        if (!Config.enable_statistic_collect || FeConstants.runningUnitTest) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        initDefaultJob();

        List<StatisticsCollectJob> allJobs;
        if (Config.enable_collect_full_statistics) {
            allJobs = generateFullAnalyzeJobs();
        } else {
            allJobs = generateAllJobs();
        }

        for (StatisticsCollectJob tcj : allJobs) {
            statisticExecutor.collectStatistics(tcj);
        }

        expireStatistic();
    }

    private void initDefaultJob() {
        // Add a default sample job if wasn't collect
        List<AnalyzeJob> allAnalyzeJobs = GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
        if (allAnalyzeJobs.stream().anyMatch(j -> j.getScheduleType() == ScheduleType.SCHEDULE)) {
            return;
        }

        AnalyzeJob analyzeJob = new AnalyzeJob(AnalyzeJob.DEFAULT_ALL_ID, AnalyzeJob.DEFAULT_ALL_ID, Collections.emptyList(),
                AnalyzeType.SAMPLE, ScheduleType.SCHEDULE, Maps.newHashMap(), ScheduleStatus.PENDING, LocalDateTime.MIN);
        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeJob(analyzeJob);
    }

    private List<StatisticsCollectJob> generateFullAnalyzeJobs() {
        List<StatisticsCollectJob> collectJobs = new ArrayList<>();

        // all database
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                continue;
            }

            for (Table table : db.getTables()) {
                AnalyzeJob analyzeJob;
                if (((OlapTable) table).getPartitions().stream().anyMatch(
                        p -> p.getDataSize() > Config.statistics_max_full_collect_data_size)) {
                    analyzeJob = new AnalyzeJob(dbId, table.getId(), Lists.newArrayList(),
                            AnalyzeType.SAMPLE, ScheduleType.SCHEDULE, Maps.newHashMap(),
                            ScheduleStatus.PENDING, LocalDateTime.MIN);
                } else {
                    analyzeJob = new AnalyzeJob(dbId, table.getId(), Lists.newArrayList(),
                            AnalyzeType.FULL, ScheduleType.SCHEDULE, Maps.newHashMap(),
                            ScheduleStatus.PENDING, LocalDateTime.MIN);
                }

                if (!Table.TableType.OLAP.equals(table.getType())) {
                    continue;
                }

                List<String> columns = table.getFullSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                        .collect(Collectors.toList());

                BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(table.getId());
                List<Partition> partitions = Lists.newArrayList(((OlapTable) table).getPartitions());
                List<Long> partitionIdList = new ArrayList<>();
                if (basicStatsMeta == null) {
                    partitions.stream().map(Partition::getId).forEach(partitionIdList::add);
                } else {
                    if (basicStatsMeta.getHealthy() > Config.statistics_auto_collect_ratio) {
                        continue;
                    }
                    LocalDateTime statsLastUpdateTime = basicStatsMeta.getUpdateTime();
                    for (Partition partition : partitions) {
                        LocalDateTime updateTime = StatisticUtils.getPartitionLastUpdateTime(partition);

                        if (statsLastUpdateTime.isBefore(updateTime)) {
                            partitionIdList.add(partition.getId());
                        }
                    }
                }
                if (!partitionIdList.isEmpty()) {
                    collectJobs.add(new FullStatisticsCollectJob(analyzeJob, db, (OlapTable) table, partitionIdList, columns));
                }
            }
        }

        return collectJobs;
    }

    private List<StatisticsCollectJob> generateAllJobs() {
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

                OlapTable olapTable = (OlapTable) db.getTable(analyzeJob.getTableId());
                createTableJobs(allTableJobMap, analyzeJob, db, olapTable, analyzeJob.getColumns());
            }
        }

        return new ArrayList<>(allTableJobMap.values());
    }

    private void createTableJobs(Map<Long, TableCollectJob> tableJobs, AnalyzeJob job,
                                 Database db, Table table) {
        if (null == table || !table.isNativeTable()) {
            return;
        }

        List<String> columns = table.getFullSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                .collect(Collectors.toList());
        createTableJobs(tableJobs, job, db, (OlapTable) table, columns);
    }

    private void createTableJobs(Map<Long, TableCollectJob> tableJobs, AnalyzeJob job,
                                 Database db, OlapTable table, List<String> columns) {
        // check table has updated
        LocalDateTime updateTime = StatisticUtils.getTableLastUpdateTime(table);

        // 1. If job is schedule and the table has update, we need re-collect data
        // 2. If job is once and is happened after the table update, we need add it to avoid schedule-job cover data
        if ((ScheduleType.SCHEDULE.equals(job.getScheduleType()) && !StatisticUtils.isEmptyTable(table) &&
                job.getWorkTime().isBefore(updateTime)) ||
                (ScheduleType.ONCE.equals(job.getScheduleType()) && job.getWorkTime().isAfter(updateTime))) {

            TableCollectJob tableCollectJob = new TableCollectJob(job, db, table, columns);
            tableJobs.put(table.getId(), tableCollectJob);
        } else {
            LOG.debug("Skip collect on table: " + table.getName() + ", updateTime: " + updateTime +
                    ", JobId: " + job.getId() + ", lastCollectTime: " + job.getWorkTime());
        }
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
