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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.starrocks.common.AlreadyExistsException;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StatisticAutoCollector extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticAutoCollector.class);

    private static final StatisticExecutor STATISTIC_EXECUTOR = new StatisticExecutor();
    public static final String DEFAULT_JOB_FLAG = "default_job_flag";

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

        if (!checkoutAnalyzeTime()) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            LOG.warn("Statistic table state check failed, skip auto collection");
            return;
        }

        prepareDefaultJob();

        runJobs();
    }

    @VisibleForTesting
    public List<StatisticsCollectJob> runJobs() {
        List<StatisticsCollectJob> result = Lists.newArrayList();

        // TODO: define the priority in the job instead
        List<NativeAnalyzeJob> allNativeAnalyzeJobs =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllNativeAnalyzeJobList();
        allNativeAnalyzeJobs.sort((o1, o2) -> Long.compare(o2.getId(), o1.getId()));
        String analyzeJobIds = allNativeAnalyzeJobs.stream().map(j -> String.valueOf(j.getId()))
                .collect(Collectors.joining(", "));

        LOG.info("auto collect statistic on analyze job[{}] start", analyzeJobIds);
        for (NativeAnalyzeJob nativeAnalyzeJob : allNativeAnalyzeJobs) {
            if (nativeAnalyzeJob.isDefaultJob() && !Config.enable_auto_collect_statistics) {
                continue;
            }
            List<StatisticsCollectJob> jobs = nativeAnalyzeJob.instantiateJobs();
            result.addAll(jobs);
            ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
            statsConnectCtx.setThreadLocalInfo();
            nativeAnalyzeJob.run(statsConnectCtx, STATISTIC_EXECUTOR, jobs);
        }
        LOG.info("auto collect statistic on analyze job[{}] end", analyzeJobIds);

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
                List<StatisticsCollectJob> jobs = externalAnalyzeJob.instantiateJobs();
                result.addAll(jobs);
                externalAnalyzeJob.run(statsConnectCtx, STATISTIC_EXECUTOR, jobs);
            }
            LOG.info("auto collect external statistic on analyze job[{}] end", jobIds);
        }
        return result;
    }

    /**
     * Choose user-created jobs first, fallback to default job if it doesn't exist
     */
    public void prepareDefaultJob() {
        List<NativeAnalyzeJob> allNativeAnalyzeJobs =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllNativeAnalyzeJobList();
        Optional<NativeAnalyzeJob> defaultJob = allNativeAnalyzeJobs.stream().filter(NativeAnalyzeJob::isDefaultJob).findFirst();
        // Compatible with old version.
        // since the default_job will be persisted and not cleaned up when it's first created.
        // we need to ensure that auto collection uses the correct analyze_type according to user config.
        if (defaultJob.isPresent()) {
            AnalyzeType analyzeType = Config.enable_collect_full_statistic ? AnalyzeType.FULL : AnalyzeType.SAMPLE;
            defaultJob.get().setType(analyzeType);
            return;
        }

        NativeAnalyzeJob job = createDefaultJobAnalyzeAll();
        try {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeJob(job);
        } catch (AlreadyExistsException e) {
            LOG.info("analyze job already exists");
        }
    }

    /**
     * Create a default job to analyze all tables in the system
     */
    private NativeAnalyzeJob createDefaultJobAnalyzeAll() {
        AnalyzeType analyzeType = Config.enable_collect_full_statistic ? AnalyzeType.FULL : AnalyzeType.SAMPLE;
        return new NativeAnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID,
                Collections.emptyList(), Collections.emptyList(), analyzeType, ScheduleType.SCHEDULE,
                Map.of(DEFAULT_JOB_FLAG, "true"), ScheduleStatus.PENDING, LocalDateTime.MIN);
    }

    /**
     * Check if it's a proper time to run auto analyze
     *
     * @return true if it's a good time
     */
    public static boolean checkoutAnalyzeTime() {
        LocalTime now = LocalTime.now(TimeUtils.getTimeZone().toZoneId());
        return checkoutAnalyzeTime(now);
    }

    private static boolean checkoutAnalyzeTime(LocalTime now) {
        String startTimeStr = stripQuotes(Config.statistic_auto_analyze_start_time);
        String endTimeStr = stripQuotes(Config.statistic_auto_analyze_end_time);

        try {
            LocalTime start = LocalTime.parse(startTimeStr, DateUtils.TIME_FORMATTER);
            LocalTime end = LocalTime.parse(endTimeStr, DateUtils.TIME_FORMATTER);

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

    private static String stripQuotes(String str) {
        return str != null ? str.replaceAll("[\"']", "") : str;
    }
}
