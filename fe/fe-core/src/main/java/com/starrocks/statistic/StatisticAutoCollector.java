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
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StatisticAutoCollector extends FrontendDaemon {
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

        if (!checkoutAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()))) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        initDefaultJob();

        if (Config.enable_collect_full_statistic) {
            LOG.info("auto collect full statistic on all databases start");
            List<StatisticsCollectJob> allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                    new NativeAnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID, null,
                            AnalyzeType.FULL, ScheduleType.SCHEDULE,
                            Maps.newHashMap(),
                            ScheduleStatus.PENDING,
                            LocalDateTime.MIN));
            for (StatisticsCollectJob statsJob : allJobs) {
                AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                        statsJob.getDb().getId(), statsJob.getTable().getId(), statsJob.getColumns(),
                        statsJob.getType(), statsJob.getScheduleType(), statsJob.getProperties(), LocalDateTime.now());
                analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();
                STATISTIC_EXECUTOR.collectStatistics(statsConnectCtx, statsJob, analyzeStatus, true);
            }
            LOG.info("auto collect full statistic on all databases end");
        } else {
            List<NativeAnalyzeJob> allNativeAnalyzeJobs =
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllNativeAnalyzeJobList();
            allNativeAnalyzeJobs.sort((o1, o2) -> Long.compare(o2.getId(), o1.getId()));
            String jobIds = allNativeAnalyzeJobs.stream().map(j -> String.valueOf(j.getId()))
                    .collect(Collectors.joining(", "));
            LOG.info("auto collect statistic on analyze job[{}] start", jobIds);
            for (NativeAnalyzeJob nativeAnalyzeJob : allNativeAnalyzeJobs) {
                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();
                nativeAnalyzeJob.run(statsConnectCtx, STATISTIC_EXECUTOR);
            }
            LOG.info("auto collect statistic on analyze job[{}] end", jobIds);
        }

        // collect external table statistic
        List<ExternalAnalyzeJob> allExternalAnalyzeJobs =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllExternalAnalyzeJobList();
        if (!allExternalAnalyzeJobs.isEmpty()) {
            allExternalAnalyzeJobs.sort((o1, o2) -> Long.compare(o2.getId(), o1.getId()));
            String jobIds = allExternalAnalyzeJobs.stream().map(j -> String.valueOf(j.getId()))
                    .collect(Collectors.joining(", "));
            LOG.info("auto collect external statistic on analyze job[{}] start", jobIds);
            for (ExternalAnalyzeJob externalAnalyzeJob : allExternalAnalyzeJobs) {
                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();
                externalAnalyzeJob.run(statsConnectCtx, STATISTIC_EXECUTOR);
            }
            LOG.info("auto collect external statistic on analyze job[{}] end", jobIds);
        }
    }

    private void initDefaultJob() {
        // Add a default sample job if wasn't collect
        List<NativeAnalyzeJob> allNativeAnalyzeJobs =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllNativeAnalyzeJobList();
        if (allNativeAnalyzeJobs.stream().anyMatch(j -> j.getScheduleType() == ScheduleType.SCHEDULE)) {
            return;
        }

        NativeAnalyzeJob nativeAnalyzeJob = new NativeAnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID,
                Collections.emptyList(), AnalyzeType.SAMPLE, ScheduleType.SCHEDULE, Maps.newHashMap(),
                ScheduleStatus.PENDING, LocalDateTime.MIN);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeJob(nativeAnalyzeJob);
    }

    private boolean checkoutAnalyzeTime(LocalTime now) {
        try {
            LocalTime start = LocalTime.parse(Config.statistic_auto_analyze_start_time, DateUtils.TIME_FORMATTER);
            LocalTime end = LocalTime.parse(Config.statistic_auto_analyze_end_time, DateUtils.TIME_FORMATTER);

            if (start.isAfter(end) && (now.isAfter(start) || now.isBefore(end))) {
                return true;
            } else if (now.isAfter(start) && now.isBefore(end)) {
                return true;
            } else {
                return false;
            }
        } catch (DateTimeParseException e) {
            LOG.warn("Parse analyze start/end time format fail : " + e.getMessage());

            // If the time format configuration is incorrect,
            // processing can be run at any time without affecting the normal process
            return true;
        }
    }
}
