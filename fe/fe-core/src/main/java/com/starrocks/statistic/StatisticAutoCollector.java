// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

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

        if (!Config.enable_statistic_collect) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        initDefaultJob();

        if (Config.enable_collect_full_statistics) {
            List<StatisticsCollectJob> allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                    new AnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID, null,
                            AnalyzeType.FULL, ScheduleType.SCHEDULE,
                            Maps.newHashMap(),
                            ScheduleStatus.PENDING,
                            LocalDateTime.MIN));
            for (StatisticsCollectJob statsJob : allJobs) {
                statisticExecutor.collectStatistics(statsJob);
            }
        } else {
            List<AnalyzeJob> allAnalyzeJobs = GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
            for (AnalyzeJob analyzeJob : allAnalyzeJobs) {
                analyzeJob.run(statisticExecutor);
            }
        }

        expireStatistic();
    }

    private void initDefaultJob() {
        // Add a default sample job if wasn't collect
        List<AnalyzeJob> allAnalyzeJobs = GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
        if (allAnalyzeJobs.stream().anyMatch(j -> j.getScheduleType() == ScheduleType.SCHEDULE)) {
            return;
        }

        AnalyzeJob analyzeJob = new AnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID,
                Collections.emptyList(), AnalyzeType.SAMPLE, ScheduleType.SCHEDULE, Maps.newHashMap(),
                ScheduleStatus.PENDING, LocalDateTime.MIN);
        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeJob(analyzeJob);
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
