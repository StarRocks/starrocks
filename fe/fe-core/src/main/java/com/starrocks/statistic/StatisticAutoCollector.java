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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class StatisticAutoCollector extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticAutoCollector.class);

    private static final StatisticExecutor STATISTIC_EXECUTOR = new StatisticExecutor();

    public StatisticAutoCollector() {
        super("AutoStatistic", Config.statistic_collect_interval_sec * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        // update interval
        if (getInterval() != Config.statistic_collect_interval_sec * 1000) {
            setInterval(Config.statistic_collect_interval_sec * 1000);
        }

        if (!Config.enable_statistic_collect || FeConstants.runningUnitTest) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        initDefaultJob();

        if (Config.enable_collect_full_statistic) {
            List<StatisticsCollectJob> allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                    new AnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID, null,
                            AnalyzeType.FULL, ScheduleType.SCHEDULE,
                            Maps.newHashMap(),
                            ScheduleStatus.PENDING,
                            LocalDateTime.MIN));
            for (StatisticsCollectJob statsJob : allJobs) {
                AnalyzeStatus analyzeStatus = new AnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                        statsJob.getDb().getId(), statsJob.getTable().getId(), statsJob.getColumns(),
                        statsJob.getType(), statsJob.getScheduleType(), statsJob.getProperties(), LocalDateTime.now());
                analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
                GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();
                STATISTIC_EXECUTOR.collectStatistics(statsConnectCtx, statsJob, analyzeStatus, true);
            }
        } else {
            List<AnalyzeJob> allAnalyzeJobs = GlobalStateMgr.getCurrentAnalyzeMgr().getAllAnalyzeJobList();
            allAnalyzeJobs.sort((o1, o2) -> Long.compare(o2.getId(), o1.getId()));
            for (AnalyzeJob analyzeJob : allAnalyzeJobs) {
                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();
                analyzeJob.run(statsConnectCtx, STATISTIC_EXECUTOR);
            }
        }
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
}
