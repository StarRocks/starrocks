// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.statistic.Constants.AnalyzeType;
import com.starrocks.statistic.Constants.ScheduleStatus;
import com.starrocks.statistic.Constants.ScheduleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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

        private void tryCollect() throws Exception {
            if (AnalyzeType.FULL == job.getType()) {
                statisticExecutor.fullCollectStatisticSync(db.getId(), table.getId(), columns);
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

        Catalog.getCurrentAnalyzeMgr().expireAnalyzeJob();

        if (!Config.enable_statistic_collect) {
            return;
        }

        // none statistic table
        if (null == StatisticUtils.getStatisticsTable()) {
            return;
        }

        initDefaultJob();

        List<TableCollectJob> allJobs = generateAllJobs();

        collectStatistics(allJobs);

        expireStatistic();
    }

    private void initDefaultJob() {
        // Add a default sample job if was't been collect
        List<AnalyzeJob> allAnalyzeJobs = Catalog.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
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

        Catalog.getCurrentAnalyzeMgr().addAnalyzeJob(analyzeJob);
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
            Catalog.getCurrentAnalyzeMgr().updateAnalyzeJobWithoutLog(analyzeJob);
            for (TableCollectJob tcj : entry.getValue()) {
                try {
                    LOG.info("Statistic collect work job: {}, type: {}, db: {}, table: {}",
                            analyzeJob.getId(), analyzeJob.getType(), tcj.db.getFullName(), tcj.table.getName());
                    tcj.tryCollect();

                    Catalog.getCurrentStatisticStorage().expireColumnStatistics(tcj.table, tcj.columns);
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
            Catalog.getCurrentAnalyzeMgr().updateAnalyzeJobWithLog(analyzeJob);
        }
    }

    private List<TableCollectJob> generateAllJobs() {
        List<AnalyzeJob> allAnalyzeJobs = Catalog.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
        // The jobs need to be sorted in order of execution to avoid duplicate collections
        allAnalyzeJobs.sort(Comparator.comparing(AnalyzeJob::getId));

        Map<Long, List<TableCollectJob>> allTableJobMap = Maps.newHashMap();

        for (AnalyzeJob analyzeJob : allAnalyzeJobs) {
            if (AnalyzeJob.DEFAULT_ALL_ID == analyzeJob.getDbId()) {
                // all database
                List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();

                for (Long dbId : dbIds) {
                    Database db = Catalog.getCurrentCatalog().getDb(dbId);
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
                Database db = Catalog.getCurrentCatalog().getDb(analyzeJob.getDbId());
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
                Database db = Catalog.getCurrentCatalog().getDb(analyzeJob.getDbId());
                if (null == db) {
                    continue;
                }

                createTableJobs(allTableJobMap, analyzeJob, db, db.getTable(analyzeJob.getTableId()));
            } else if (!analyzeJob.getColumns().isEmpty() && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getTableId()
                    && AnalyzeJob.DEFAULT_ALL_ID != analyzeJob.getDbId()) {
                // some column
                Database db = Catalog.getCurrentCatalog().getDb(analyzeJob.getDbId());
                if (null == db) {
                    continue;
                }

                createTableJobs(allTableJobMap, analyzeJob, db, db.getTable(analyzeJob.getTableId()),
                        analyzeJob.getColumns());
            }
        }

        List<TableCollectJob> list = Lists.newLinkedList();
        allTableJobMap.values().forEach(list::addAll);
        return list;
    }

    private void createTableJobs(Map<Long, List<TableCollectJob>> tableJobs, AnalyzeJob job,
                                 Database db, Table table) {
        if (null == table || !Table.TableType.OLAP.equals(table.getType())) {
            return;
        }

        List<String> columns = table.getFullSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                .collect(Collectors.toList());
        createTableJobs(tableJobs, job, db, table, columns);
    }

    private void createTableJobs(Map<Long, List<TableCollectJob>> tableJobs, AnalyzeJob job,
                                 Database db, Table table, List<String> columns) {
        // check table has update
        LocalDateTime updateTime = StatisticUtils.getTableLastUpdateTime(table);

        // 1. If job is schedule and the table has update, we need re-collect data
        // 2. If job is once and is happened after the table update, we need add it to avoid schedule-job cover data
        if ((ScheduleType.SCHEDULE.equals(job.getScheduleType()) && job.getWorkTime().isBefore(updateTime))
                || (ScheduleType.ONCE.equals(job.getScheduleType()) && job.getWorkTime().isAfter(updateTime))) {
            createJobs(tableJobs, job, db, table, columns);
        } else {
            LOG.debug("Skip collect on table: " + table.getName() + ", updateTime: " + updateTime +
                    ", JobId: " + job.getId() + ", lastCollectTime: " + job.getWorkTime());
        }
    }

    private void createJobs(Map<Long, List<TableCollectJob>> result,
                            AnalyzeJob job, Database db, Table table, List<String> columns) {
        if (result.containsKey(table.getId())) {
            List<TableCollectJob> tableJobs = result.get(table.getId());
            Iterator<TableCollectJob> iter = tableJobs.iterator();
            while (iter.hasNext()) {
                TableCollectJob tJob = iter.next();
                tJob.columns.removeAll(columns);

                if (tJob.columns.isEmpty()) {
                    iter.remove();
                }
            }
        } else {
            result.put(table.getId(), Lists.newLinkedList());
        }

        TableCollectJob tableJob = new TableCollectJob();
        tableJob.job = job;
        tableJob.db = db;
        tableJob.table = table;
        tableJob.columns = columns;
        result.get(table.getId()).add(tableJob);
    }

    private void expireStatistic() {
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        List<Long> tables = Lists.newArrayList();
        for (Long dbId : dbIds) {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
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
