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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/metric/MetricRepo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.metric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.common.util.NetUtils;
import com.starrocks.http.HttpMetricRegistry;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.metric.Metric.MetricType;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.monitor.jvm.JvmStatCollector;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.proto.PKafkaOffsetProxyRequest;
import com.starrocks.proto.PKafkaOffsetProxyResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.DatabaseTransactionMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public final class MetricRepo {
    private static final Logger LOG = LogManager.getLogger(MetricRepo.class);

    private static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    private static final StarRocksMetricRegistry STARROCKS_METRIC_REGISTER = new StarRocksMetricRegistry();

    public static volatile boolean hasInit = false;
    public static final SystemMetrics SYSTEM_METRICS = new SystemMetrics();

    public static final String TABLET_NUM = "tablet_num";
    public static final String TABLET_MAX_COMPACTION_SCORE = "tablet_max_compaction_score";

    public static LongCounterMetric COUNTER_REQUEST_ALL;
    public static LongCounterMetric COUNTER_QUERY_ALL;
    public static LongCounterMetric COUNTER_QUERY_ERR;
    public static LongCounterMetric COUNTER_QUERY_TIMEOUT;
    public static LongCounterMetric COUNTER_QUERY_SUCCESS;
    public static LongCounterMetric COUNTER_SLOW_QUERY;

    public static LongCounterMetric COUNTER_QUERY_QUEUE_PENDING;
    public static LongCounterMetric COUNTER_QUERY_QUEUE_TOTAL;
    public static LongCounterMetric COUNTER_QUERY_QUEUE_TIMEOUT;

    public static LongCounterMetric COUNTER_QUERY_QUEUE_SLOT_PENDING;
    public static LongCounterMetric COUNTER_QUERY_QUEUE_SLOT_RUNNING;

    public static LongCounterMetric COUNTER_QUERY_ANALYSIS_ERR;
    public static LongCounterMetric COUNTER_QUERY_INTERNAL_ERR;

    public static final MetricWithLabelGroup<LongCounterMetric> COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING =
            new MetricWithLabelGroup<>("category",
                    () -> new LongCounterMetric("query_queue_v2_category_pending_slots", MetricUnit.REQUESTS,
                            "the number of current pending slots for each category"));
    public static final MetricWithLabelGroup<LongCounterMetric> COUNTER_QUERY_QUEUE_CATEGORY_SLOT_RUNNING =
            new MetricWithLabelGroup<>("category",
                    () -> new LongCounterMetric("query_queue_v2_category_running_slots", MetricUnit.REQUESTS,
                            "the number of current running slots for each category"));
    public static final MetricWithLabelGroup<LongCounterMetric> COUNTER_QUERY_QUEUE_CATEGORY_SLOT_ALLOCATED_TOTAL =
            new MetricWithLabelGroup<>("category",
                    () -> new LongCounterMetric("query_queue_v2_category_total_allocated_slots", MetricUnit.REQUESTS,
                            "the accumulated value of allocated slots for each category"));
    public static final MetricWithLabelGroup<GaugeMetricImpl<Integer>> GAUGE_QUERY_QUEUE_CATEGORY_WEIGHT =
            new MetricWithLabelGroup<>("category",
                    () -> new GaugeMetricImpl<>("query_queue_v2_category_weight", MetricUnit.REQUESTS,
                            "the weight of each category"));
    public static final MetricWithLabelGroup<GaugeMetricImpl<Integer>> GAUGE_QUERY_QUEUE_CATEGORY_SLOT_MIN_SLOTS =
            new MetricWithLabelGroup<>("category",
                    () -> new GaugeMetricImpl<>("query_queue_v2_category_min_slots", MetricUnit.REQUESTS,
                            "the min slots of each category"));
    public static final MetricWithLabelGroup<LongCounterMetric> COUNTER_QUERY_QUEUE_CATEGORY_SLOT_STATE =
            new MetricWithLabelGroup<>("category",
                    () -> new LongCounterMetric("query_queue_v2_category_state", MetricUnit.REQUESTS,
                            "the current state of each category"));

    public static LongCounterMetric COUNTER_UNFINISHED_BACKUP_JOB;
    public static LongCounterMetric COUNTER_UNFINISHED_RESTORE_JOB;

    public static LongCounterMetric COUNTER_LOAD_ADD;
    public static LongCounterMetric COUNTER_LOAD_FINISHED;
    public static LongCounterMetric COUNTER_EDIT_LOG_WRITE;
    public static LongCounterMetric COUNTER_EDIT_LOG_READ;
    public static LongCounterMetric COUNTER_EDIT_LOG_SIZE_BYTES;
    public static LongCounterMetric COUNTER_IMAGE_WRITE;
    public static LongCounterMetric COUNTER_IMAGE_PUSH;
    public static LongCounterMetric COUNTER_TXN_REJECT;
    public static LongCounterMetric COUNTER_TXN_BEGIN;
    public static LongCounterMetric COUNTER_TXN_FAILED;
    public static LongCounterMetric COUNTER_TXN_SUCCESS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ROWS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_RECEIVED_BYTES;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ERROR_ROWS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_PAUSED;
    public static LongCounterMetric COUNTER_SHORTCIRCUIT_QUERY;
    public static LongCounterMetric COUNTER_SHORTCIRCUIT_RPC;
    public static LongCounterMetric COUNTER_BACKEND_SERVICE_RPC;
    public static LongCounterMetric COUNTER_LAKE_SERVICE_RPC;
    public static LongCounterMetric COUNTER_BRPC_EXEC_PLAN_FRAGMENT;
    public static LongCounterMetric COUNTER_BRPC_EXEC_PLAN_FRAGMENT_ERROR;

    public static Histogram HISTO_QUERY_LATENCY;
    public static Histogram HISTO_EDIT_LOG_WRITE_LATENCY;
    public static Histogram HISTO_JOURNAL_WRITE_LATENCY;
    public static Histogram HISTO_JOURNAL_WRITE_BATCH;
    public static Histogram HISTO_JOURNAL_WRITE_BYTES;
    public static Histogram HISTO_SHORTCIRCUIT_RPC_LATENCY;
    public static Histogram HISTO_DEPLOY_PLAN_FRAGMENTS_LATENCY;

    // following metrics will be updated by metric calculator
    public static GaugeMetricImpl<Double> GAUGE_QUERY_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_REQUEST_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_ERR_RATE;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_INTERNAL_ERR_RATE;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_ANALYSIS_ERR_RATE;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_TIMEOUT_RATE;

    // these query latency is different from HISTO_QUERY_LATENCY, for these only summarize the latest queries, but HISTO_QUERY_LATENCY summarizes all queries.
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_MEAN;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_MEDIAN;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P75;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P90;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P95;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P99;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P999;
    public static GaugeMetricImpl<Long> GAUGE_MAX_TABLET_COMPACTION_SCORE;
    public static GaugeMetricImpl<Long> GAUGE_STACKED_JOURNAL_NUM;

    public static GaugeMetricImpl<Long> GAUGE_ENCRYPTION_KEY_NUM;

    public static List<GaugeMetricImpl<Long>> GAUGE_ROUTINE_LOAD_LAGS;

    public static List<GaugeMetricImpl<Long>> GAUGE_MEMORY_USAGE_STATS;
    public static List<GaugeMetricImpl<Long>> GAUGE_OBJECT_COUNT_STATS;

    // Currently, we use gauge for safe mode metrics, since we do not have unTyped metrics till now
    public static GaugeMetricImpl<Integer> GAUGE_SAFE_MODE;

    private static final ScheduledThreadPoolExecutor METRIC_TIMER =
            ThreadPoolManager.newDaemonScheduledThreadPool(1, "Metric-Timer-Pool", true);
    private static final MetricCalculator METRIC_CALCULATOR = new MetricCalculator();

    public static synchronized void init() {
        if (hasInit) {
            return;
        }

        GAUGE_ROUTINE_LOAD_LAGS = new ArrayList<>();
        GAUGE_MEMORY_USAGE_STATS = new ArrayList<>();
        GAUGE_OBJECT_COUNT_STATS = new ArrayList<>();

        // 1. gauge
        // load jobs
        LoadMgr loadManger = GlobalStateMgr.getCurrentState().getLoadMgr();
        for (EtlJobType jobType : EtlJobType.values()) {
            // Only broker/spark/insert loads are stored into LoadManager,
            // so these 3 types of jobs are displayed, others are always 0.
            if (!(jobType == EtlJobType.BROKER || jobType == EtlJobType.SPARK || jobType == EtlJobType.INSERT)) {
                continue;
            }

            for (JobState state : JobState.values()) {
                GaugeMetric<Long> gauge = new GaugeMetric<Long>("job",
                        MetricUnit.NOUNIT, "job statistics") {
                    @Override
                    public Long getValue() {
                        if (!GlobalStateMgr.getCurrentState().isLeader()) {
                            return 0L;
                        }
                        return loadManger.getLoadJobNum(state, jobType);
                    }
                };
                gauge.addLabel(new MetricLabel("job", "load"))
                        .addLabel(new MetricLabel("type", jobType.name()))
                        .addLabel(new MetricLabel("state", state.name()));
                STARROCKS_METRIC_REGISTER.addMetric(gauge);
            }
        }

        // running alter job
        AlterJobMgr alter = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        for (AlterJobV2.JobType jobType : AlterJobV2.JobType.values()) {
            if (jobType != AlterJobV2.JobType.SCHEMA_CHANGE && jobType != AlterJobV2.JobType.ROLLUP) {
                continue;
            }

            GaugeMetric<Long> gauge = new GaugeMetric<Long>("job",
                    MetricUnit.NOUNIT, "job statistics") {
                @Override
                public Long getValue() {
                    if (!GlobalStateMgr.getCurrentState().isLeader()) {
                        return 0L;
                    }
                    if (jobType == AlterJobV2.JobType.SCHEMA_CHANGE) {
                        return alter.getSchemaChangeHandler()
                                .getAlterJobV2Num(AlterJobV2.JobState.RUNNING);
                    } else {
                        return alter.getMaterializedViewHandler()
                                .getAlterJobV2Num(AlterJobV2.JobState.RUNNING);
                    }
                }
            };
            gauge.addLabel(new MetricLabel("job", "alter"))
                    .addLabel(new MetricLabel("type", jobType.name()))
                    .addLabel(new MetricLabel("state", "running"));
            STARROCKS_METRIC_REGISTER.addMetric(gauge);
        }

        // capacity
        generateBackendsTabletMetrics();

        // journal id
        GaugeMetric<Long> maxJournalId = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "max_journal_id", MetricUnit.NOUNIT, "max journal id of this frontends") {
            @Override
            public Long getValue() {
                return GlobalStateMgr.getCurrentState().getMaxJournalId();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(maxJournalId);

        // meta log total count
        GaugeMetric<Long> metaLogCount = new GaugeMetric<Long>(
                "meta_log_count", MetricUnit.NOUNIT, "meta log total count") {
            @Override
            public Long getValue() {
                return GlobalStateMgr.getCurrentState().getMaxJournalId() -
                        GlobalStateMgr.getCurrentState().getImageJournalId();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(metaLogCount);

        // scheduled tablet num
        GaugeMetric<Long> scheduledTabletNum = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "scheduled_tablet_num", MetricUnit.NOUNIT, "number of tablets being scheduled") {
            @Override
            public Long getValue() {
                if (!GlobalStateMgr.getCurrentState().isLeader()) {
                    return 0L;
                }
                return (long) GlobalStateMgr.getCurrentState().getTabletScheduler().getTotalNum();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(scheduledTabletNum);

        // routine load jobs
        RoutineLoadMgr routineLoadManger = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
        for (RoutineLoadJob.JobState state : RoutineLoadJob.JobState.values()) {
            GaugeMetric<Long> gauge = new GaugeMetric<Long>("routine_load_jobs",
                    MetricUnit.NOUNIT, "routine load jobs") {
                @Override
                public Long getValue() {
                    if (null == routineLoadManger) {
                        return 0L;
                    }
                    return (long) routineLoadManger.getRoutineLoadJobByState(Sets.newHashSet(state)).size();
                }
            };
            gauge.addLabel(new MetricLabel("state", state.name()));
            STARROCKS_METRIC_REGISTER.addMetric(gauge);
        }

        GaugeMetric<Long> routineLoadUnstableJobsGauge = new GaugeMetric<Long>("routine_load_jobs",
                MetricUnit.NOUNIT, "routine load jobs") {
            @Override
            public Long getValue() {
                if (null == routineLoadManger) {
                    return 0L;
                }
                return routineLoadManger.numUnstableJobs();
            }
        };
        routineLoadUnstableJobsGauge.addLabel(new MetricLabel("state", "UNSTABLE"));
        STARROCKS_METRIC_REGISTER.addMetric(routineLoadUnstableJobsGauge);

        // qps, rps, error rate and query latency
        // these metrics should be set an init value, in case that metric calculator is not running
        GAUGE_QUERY_PER_SECOND = new GaugeMetricImpl<>("qps", MetricUnit.NOUNIT, "query per second");
        GAUGE_QUERY_PER_SECOND.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_PER_SECOND);

        GAUGE_REQUEST_PER_SECOND = new GaugeMetricImpl<>("rps", MetricUnit.NOUNIT, "request per second");
        GAUGE_REQUEST_PER_SECOND.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_REQUEST_PER_SECOND);

        GAUGE_QUERY_ERR_RATE = new GaugeMetricImpl<>("query_err_rate", MetricUnit.NOUNIT, "query error rate");
        GAUGE_QUERY_ERR_RATE.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_ERR_RATE);

        GAUGE_QUERY_INTERNAL_ERR_RATE =
                new GaugeMetricImpl<>("query_internal_err_rate", MetricUnit.NOUNIT, "query internal error rate");
        GAUGE_QUERY_INTERNAL_ERR_RATE.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_INTERNAL_ERR_RATE);

        GAUGE_QUERY_ANALYSIS_ERR_RATE =
                new GaugeMetricImpl<>("query_analysis_err_rate", MetricUnit.NOUNIT, "query analysis error rate");
        GAUGE_QUERY_ANALYSIS_ERR_RATE.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_ANALYSIS_ERR_RATE);

        GAUGE_QUERY_TIMEOUT_RATE =
                new GaugeMetricImpl<>("query_timeout_rate", MetricUnit.NOUNIT, "query timeout rate");
        GAUGE_QUERY_TIMEOUT_RATE.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_TIMEOUT_RATE);

        GAUGE_MAX_TABLET_COMPACTION_SCORE = new GaugeMetricImpl<>("max_tablet_compaction_score",
                MetricUnit.NOUNIT, "max tablet compaction score of all backends");
        GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(0L);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_MAX_TABLET_COMPACTION_SCORE);

        GAUGE_STACKED_JOURNAL_NUM = new GaugeMetricImpl<>(
                "editlog_stacked_num", MetricUnit.OPERATIONS, "counter of edit log that are stacked");
        GAUGE_STACKED_JOURNAL_NUM.setValue(0L);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_STACKED_JOURNAL_NUM);

        GAUGE_ENCRYPTION_KEY_NUM = new GaugeMetricImpl<>(
                "encryption_key_num", MetricUnit.NOUNIT, "number of encryption keys in key manager");
        GAUGE_ENCRYPTION_KEY_NUM.setValue(0L);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_ENCRYPTION_KEY_NUM);

        GAUGE_QUERY_LATENCY_MEAN =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "mean of query latency");
        GAUGE_QUERY_LATENCY_MEAN.addLabel(new MetricLabel("type", "mean"));
        GAUGE_QUERY_LATENCY_MEAN.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_MEAN);

        GAUGE_QUERY_LATENCY_MEDIAN =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "median of query latency");
        GAUGE_QUERY_LATENCY_MEDIAN.addLabel(new MetricLabel("type", "50_quantile"));
        GAUGE_QUERY_LATENCY_MEDIAN.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_MEDIAN);

        GAUGE_QUERY_LATENCY_P75 =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "p75 of query latency");
        GAUGE_QUERY_LATENCY_P75.addLabel(new MetricLabel("type", "75_quantile"));
        GAUGE_QUERY_LATENCY_P75.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_P75);

        GAUGE_QUERY_LATENCY_P90 =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "p90 of query latency");
        GAUGE_QUERY_LATENCY_P90.addLabel(new MetricLabel("type", "90_quantile"));
        GAUGE_QUERY_LATENCY_P90.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_P90);

        GAUGE_QUERY_LATENCY_P95 =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "p95 of query latency");
        GAUGE_QUERY_LATENCY_P95.addLabel(new MetricLabel("type", "95_quantile"));
        GAUGE_QUERY_LATENCY_P95.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_P95);

        GAUGE_QUERY_LATENCY_P99 =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "p99 of query latency");
        GAUGE_QUERY_LATENCY_P99.addLabel(new MetricLabel("type", "99_quantile"));
        GAUGE_QUERY_LATENCY_P99.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_P99);

        GAUGE_QUERY_LATENCY_P999 =
                new GaugeMetricImpl<>("query_latency", MetricUnit.MILLISECONDS, "p999 of query latency");
        GAUGE_QUERY_LATENCY_P999.addLabel(new MetricLabel("type", "999_quantile"));
        GAUGE_QUERY_LATENCY_P999.setValue(0.0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_QUERY_LATENCY_P999);

        GAUGE_SAFE_MODE = new GaugeMetricImpl<>("safe_mode", MetricUnit.NOUNIT, "safe mode flag");
        GAUGE_SAFE_MODE.addLabel(new MetricLabel("type", "safe_mode"));
        GAUGE_SAFE_MODE.setValue(0);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_SAFE_MODE);

        GaugeMetric<Long> gaugeReportQueueSize = new GaugeMetric<Long>(
                "report_queue_size", MetricUnit.NOUNIT, "report queue size") {
            @Override
            public Long getValue() {
                return (long) GlobalStateMgr.getCurrentState().getReportHandler().getReportQueueSize();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(gaugeReportQueueSize);

        // 2. counter
        COUNTER_REQUEST_ALL = new LongCounterMetric("request_total", MetricUnit.REQUESTS, "total request");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_REQUEST_ALL);
        COUNTER_QUERY_ALL = new LongCounterMetric("query_total", MetricUnit.REQUESTS, "total query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_ALL);
        COUNTER_QUERY_ERR = new LongCounterMetric("query_err", MetricUnit.REQUESTS, "total error query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_ERR);
        COUNTER_QUERY_TIMEOUT = new LongCounterMetric("query_timeout", MetricUnit.REQUESTS, "total timeout query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_TIMEOUT);
        COUNTER_QUERY_SUCCESS = new LongCounterMetric("query_success", MetricUnit.REQUESTS, "total success query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_SUCCESS);
        COUNTER_SLOW_QUERY = new LongCounterMetric("slow_query", MetricUnit.REQUESTS, "total slow query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_SLOW_QUERY);
        COUNTER_QUERY_QUEUE_PENDING = new LongCounterMetric("query_queue_pending", MetricUnit.REQUESTS,
                "total pending query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_QUEUE_PENDING);

        COUNTER_QUERY_QUEUE_SLOT_PENDING = new LongCounterMetric("query_queue_slot_pending", MetricUnit.REQUESTS,
                "total pending query slot");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_QUEUE_SLOT_PENDING);
        COUNTER_QUERY_QUEUE_SLOT_RUNNING = new LongCounterMetric("query_queue_slot_running", MetricUnit.REQUESTS,
                "total running query slot");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_QUEUE_SLOT_RUNNING);

        COUNTER_QUERY_QUEUE_TOTAL = new LongCounterMetric("query_queue_total", MetricUnit.REQUESTS,
                "total history queued query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_QUEUE_TOTAL);
        COUNTER_QUERY_QUEUE_TIMEOUT = new LongCounterMetric("query_queue_timeout", MetricUnit.REQUESTS,
                "total history query for timeout in queue");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_QUEUE_TIMEOUT);
        COUNTER_LOAD_ADD = new LongCounterMetric("load_add", MetricUnit.REQUESTS, "total load submit");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_LOAD_ADD);
        COUNTER_ROUTINE_LOAD_PAUSED =
                new LongCounterMetric("routine_load_paused", MetricUnit.REQUESTS, "counter of routine load paused");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_ROUTINE_LOAD_PAUSED);
        COUNTER_LOAD_FINISHED = new LongCounterMetric("load_finished", MetricUnit.REQUESTS, "total load finished");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_LOAD_FINISHED);
        COUNTER_EDIT_LOG_WRITE =
                new LongCounterMetric("edit_log_write", MetricUnit.OPERATIONS, "counter of edit log write into bdbje");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_EDIT_LOG_WRITE);
        COUNTER_EDIT_LOG_READ =
                new LongCounterMetric("edit_log_read", MetricUnit.OPERATIONS, "counter of edit log read from bdbje");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_EDIT_LOG_READ);
        COUNTER_EDIT_LOG_SIZE_BYTES =
                new LongCounterMetric("edit_log_size_bytes", MetricUnit.BYTES, "size of edit log");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_EDIT_LOG_SIZE_BYTES);
        COUNTER_IMAGE_WRITE = new LongCounterMetric("image_write", MetricUnit.OPERATIONS, "counter of image generated");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_IMAGE_WRITE);
        COUNTER_IMAGE_PUSH = new LongCounterMetric("image_push", MetricUnit.OPERATIONS,
                "counter of image succeeded in pushing to other frontends");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_IMAGE_PUSH);

        COUNTER_SHORTCIRCUIT_QUERY = new LongCounterMetric("shortcircuit_query", MetricUnit.REQUESTS, "total shortcircuit query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_SHORTCIRCUIT_QUERY);
        COUNTER_SHORTCIRCUIT_RPC = new LongCounterMetric("shortcircuit_rpc", MetricUnit.REQUESTS, "total shortcircuit rpc");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_SHORTCIRCUIT_RPC);
        COUNTER_BACKEND_SERVICE_RPC = new LongCounterMetric("brpc_backend_service", MetricUnit.REQUESTS,
                "total backend service rpc");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_BACKEND_SERVICE_RPC);
        COUNTER_LAKE_SERVICE_RPC = new LongCounterMetric("brpc_lake_service", MetricUnit.REQUESTS, "total lake service rpc");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_LAKE_SERVICE_RPC);
        GaugeMetric<Long> brpcTotal = (GaugeMetric<Long>) new GaugeMetric<Long>("brpc_total", MetricUnit.REQUESTS, "total brpc") {
            @Override
            public Long getValue() {
                return COUNTER_BACKEND_SERVICE_RPC.getValue() + COUNTER_LAKE_SERVICE_RPC.getValue();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(brpcTotal);
        COUNTER_BRPC_EXEC_PLAN_FRAGMENT = new LongCounterMetric(
                "brpc_exec_plan_fragment", MetricUnit.REQUESTS, "total brpc exec plan fragment");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_BRPC_EXEC_PLAN_FRAGMENT);
        COUNTER_BRPC_EXEC_PLAN_FRAGMENT_ERROR = new LongCounterMetric(
                "brpc_exec_plan_fragment_error", MetricUnit.REQUESTS, "total brpc exec plan fragment error");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_BRPC_EXEC_PLAN_FRAGMENT_ERROR);

        COUNTER_QUERY_ANALYSIS_ERR = new LongCounterMetric("query_analysis_err", MetricUnit.REQUESTS,
                                                           "total analysis error query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_ANALYSIS_ERR);

        COUNTER_QUERY_INTERNAL_ERR = new LongCounterMetric("query_internal_err", MetricUnit.REQUESTS, 
                                                           "total internal error query");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_QUERY_INTERNAL_ERR);

        COUNTER_TXN_REJECT =
                new LongCounterMetric("txn_reject", MetricUnit.REQUESTS, "counter of rejected transactions");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_TXN_REJECT);
        COUNTER_TXN_BEGIN = new LongCounterMetric("txn_begin", MetricUnit.REQUESTS, "counter of beginning transactions");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_TXN_BEGIN);
        COUNTER_TXN_SUCCESS =
                new LongCounterMetric("txn_success", MetricUnit.REQUESTS, "counter of success transactions");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_TXN_SUCCESS);
        COUNTER_TXN_FAILED = new LongCounterMetric("txn_failed", MetricUnit.REQUESTS, "counter of failed transactions");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_TXN_FAILED);

        COUNTER_ROUTINE_LOAD_ROWS =
                new LongCounterMetric("routine_load_rows", MetricUnit.ROWS, "total rows of routine load");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_ROUTINE_LOAD_ROWS);
        COUNTER_ROUTINE_LOAD_RECEIVED_BYTES = new LongCounterMetric("routine_load_receive_bytes", MetricUnit.BYTES,
                "total received bytes of routine load");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_ROUTINE_LOAD_RECEIVED_BYTES);
        COUNTER_ROUTINE_LOAD_ERROR_ROWS = new LongCounterMetric("routine_load_error_rows", MetricUnit.ROWS,
                "total error rows of routine load");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_ROUTINE_LOAD_ERROR_ROWS);

        COUNTER_UNFINISHED_BACKUP_JOB = new LongCounterMetric("unfinished_backup_job", MetricUnit.REQUESTS,
                "current unfinished backup job");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_UNFINISHED_BACKUP_JOB);
        COUNTER_UNFINISHED_RESTORE_JOB = new LongCounterMetric("unfinished_restore_job", MetricUnit.REQUESTS,
                "current unfinished restore job");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_UNFINISHED_RESTORE_JOB);
        List<Database> dbs = Lists.newArrayList();
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb() != null) {
            for (Map.Entry<Long, Database> entry : GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().entrySet()) {
                dbs.add(entry.getValue());
            }

            for (Database db : dbs) {
                AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
                if (jobI instanceof BackupJob && !((BackupJob) jobI).isDone()) {
                    COUNTER_UNFINISHED_BACKUP_JOB.increase(1L);
                } else if (jobI instanceof RestoreJob && !((RestoreJob) jobI).isDone()) {
                    COUNTER_UNFINISHED_RESTORE_JOB.increase(1L);
                }

            }
        }

        // 3. histogram
        HISTO_QUERY_LATENCY = METRIC_REGISTER.histogram(MetricRegistry.name("query", "latency", "ms"));
        HISTO_EDIT_LOG_WRITE_LATENCY =
                METRIC_REGISTER.histogram(MetricRegistry.name("editlog", "write", "latency", "ms"));
        HISTO_JOURNAL_WRITE_LATENCY =
                METRIC_REGISTER.histogram(MetricRegistry.name("journal", "write", "latency", "ms"));
        HISTO_JOURNAL_WRITE_BATCH =
                METRIC_REGISTER.histogram(MetricRegistry.name("journal", "write", "batch"));
        HISTO_JOURNAL_WRITE_BYTES =
                METRIC_REGISTER.histogram(MetricRegistry.name("journal", "write", "bytes"));
        HISTO_SHORTCIRCUIT_RPC_LATENCY = METRIC_REGISTER.histogram(MetricRegistry.name("shortcircuit", "latency", "ms"));
        HISTO_DEPLOY_PLAN_FRAGMENTS_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("deploy_plan_fragments", "latency", "ms"));

        // init system metrics
        initSystemMetrics();

        updateMetrics();
        hasInit = true;

        if (Config.enable_metric_calculator) {
            METRIC_TIMER.scheduleAtFixedRate(METRIC_CALCULATOR, 0, 15 * 1000L, TimeUnit.MILLISECONDS);
        }
    }

    private static void initSystemMetrics() {
        // TCP retransSegs
        GaugeMetric<Long> tcpRetransSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "All TCP packets retransmitted") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpRetransSegs;
            }
        };
        tcpRetransSegs.addLabel(new MetricLabel("name", "tcp_retrans_segs"));
        STARROCKS_METRIC_REGISTER.addMetric(tcpRetransSegs);

        // TCP inErrs
        GaugeMetric<Long> tpcInErrs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all problematic TCP packets received") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpInErrs;
            }
        };
        tpcInErrs.addLabel(new MetricLabel("name", "tcp_in_errs"));
        STARROCKS_METRIC_REGISTER.addMetric(tpcInErrs);

        // TCP inSegs
        GaugeMetric<Long> tpcInSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all TCP packets received") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpInSegs;
            }
        };
        tpcInSegs.addLabel(new MetricLabel("name", "tcp_in_segs"));
        STARROCKS_METRIC_REGISTER.addMetric(tpcInSegs);

        // TCP outSegs
        GaugeMetric<Long> tpcOutSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all TCP packets send with RST") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpOutSegs;
            }
        };
        tpcOutSegs.addLabel(new MetricLabel("name", "tcp_out_segs"));
        STARROCKS_METRIC_REGISTER.addMetric(tpcOutSegs);
    }

    // to generate the metrics related to tablets of each backend
    // this metric is reentrant, so that we can add or remove metric along with the backend add or remove
    // at runtime.
    public static void generateBackendsTabletMetrics() {
        // remove all previous 'tablet' metric
        STARROCKS_METRIC_REGISTER.removeMetrics(TABLET_NUM);
        STARROCKS_METRIC_REGISTER.removeMetrics(TABLET_MAX_COMPACTION_SCORE);

        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        for (Long beId : infoService.getBackendIds(false)) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            // tablet number of each backend
            GaugeMetric<Long> tabletNum = (GaugeMetric<Long>) new GaugeMetric<Long>(TABLET_NUM,
                    MetricUnit.NOUNIT, "tablet number") {
                @Override
                public Long getValue() {
                    if (!GlobalStateMgr.getCurrentState().isLeader()) {
                        return 0L;
                    }
                    return invertedIndex.getTabletNumByBackendId(beId);
                }
            };
            tabletNum.addLabel(new MetricLabel("backend",
                    NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())));
            STARROCKS_METRIC_REGISTER.addMetric(tabletNum);

            // max compaction score of tablets on each backend
            GaugeMetric<Long> tabletMaxCompactionScore = (GaugeMetric<Long>) new GaugeMetric<Long>(
                    TABLET_MAX_COMPACTION_SCORE, MetricUnit.NOUNIT,
                    "tablet max compaction score") {
                @Override
                public Long getValue() {
                    if (!GlobalStateMgr.getCurrentState().isLeader()) {
                        return 0L;
                    }
                    return be.getTabletMaxCompactionScore();
                }
            };
            tabletMaxCompactionScore.addLabel(new MetricLabel("backend",
                    NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())));
            STARROCKS_METRIC_REGISTER.addMetric(tabletMaxCompactionScore);

        } // end for backends
    }

    public static void updateMemoryUsageMetrics() {
        if (GAUGE_MEMORY_USAGE_STATS.size() ==
                MemoryUsageTracker.MEMORY_USAGE.values().stream().mapToInt(Map::size).sum()) {
            return;
        }

        List<GaugeMetricImpl<Long>> memoryUsageGauges = new ArrayList<>();
        List<GaugeMetricImpl<Long>> objectCountGauges = new ArrayList<>();
        MemoryUsageTracker.MEMORY_USAGE.forEach((moduleName, module) -> {
            if (module != null) {
                module.forEach((className, memoryState) -> {
                    GaugeMetricImpl<Long> metricBytes =
                            new GaugeMetricImpl<>("memory_usage", MetricUnit.BYTES,
                                    "The bytes of module");
                    metricBytes.addLabel(new MetricLabel("module", moduleName));
                    metricBytes.addLabel(new MetricLabel("className", className));
                    metricBytes.setValue(memoryState.getCurrentConsumption());
                    memoryUsageGauges.add(metricBytes);

                    Map<String, Long> counterMap = memoryState.getCounterMap();
                    counterMap.forEach((name, value) -> {
                        GaugeMetricImpl<Long> metricCount =
                                new GaugeMetricImpl<>("object_count", MetricUnit.NOUNIT,
                                        "The count of object");
                        metricCount.addLabel(new MetricLabel("module", moduleName));
                        metricCount.addLabel(new MetricLabel("className", className));
                        metricCount.addLabel(new MetricLabel("objectName", name));
                        metricCount.setValue(value);
                        objectCountGauges.add(metricCount);
                    });
                });

            }
        });

        GAUGE_MEMORY_USAGE_STATS = memoryUsageGauges;
        GAUGE_OBJECT_COUNT_STATS = objectCountGauges;
    }

    public static void updateRoutineLoadProcessMetrics() {
        List<RoutineLoadJob> jobs = GlobalStateMgr.getCurrentState().getRoutineLoadMgr().getRoutineLoadJobByState(
                Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE,
                        RoutineLoadJob.JobState.PAUSED,
                        RoutineLoadJob.JobState.RUNNING));

        List<RoutineLoadJob> allKafkaJobs = jobs.stream()
                .filter(job -> (job instanceof KafkaRoutineLoadJob)
                        && ((KafkaProgress) job.getProgress()).hasPartition())
                .collect(Collectors.toList());

        if (allKafkaJobs.size() <= 0) {
            return;
        }

        Map<Long, List<RoutineLoadJob>> kafkaJobsMp = allKafkaJobs.stream().collect(
                Collectors.groupingBy(RoutineLoadJob::getWarehouseId)
        );

        // get all partitions offset in a batch api
        for (Map.Entry<Long, List<RoutineLoadJob>> entry : kafkaJobsMp.entrySet()) {
            long warehouseId = entry.getKey();
            List<RoutineLoadJob> kafkaJobs = entry.getValue();

            List<PKafkaOffsetProxyRequest> requests = new ArrayList<>();

            for (RoutineLoadJob job : kafkaJobs) {
                KafkaRoutineLoadJob kJob = (KafkaRoutineLoadJob) job;
                try {
                    kJob.convertCustomProperties(false);
                } catch (DdlException e) {
                    LOG.warn("convert custom properties failed", e);
                    return;
                }
                PKafkaOffsetProxyRequest offsetProxyRequest = new PKafkaOffsetProxyRequest();
                offsetProxyRequest.kafkaInfo = KafkaUtil.genPKafkaLoadInfo(kJob.getBrokerList(), kJob.getTopic(),
                        ImmutableMap.copyOf(kJob.getConvertedCustomProperties()), warehouseId);
                offsetProxyRequest.partitionIds = new ArrayList<>(
                        ((KafkaProgress) kJob.getProgress()).getPartitionIdToOffset().keySet());
                requests.add(offsetProxyRequest);
            }

            List<PKafkaOffsetProxyResult> offsetProxyResults;
            try {
                offsetProxyResults = KafkaUtil.getBatchOffsets(requests);
            } catch (StarRocksException e) {
                LOG.warn("get batch offsets failed", e);
                return;
            }

            List<GaugeMetricImpl<Long>> routineLoadLags = new ArrayList<>();

            for (int i = 0; i < kafkaJobs.size(); i++) {
                KafkaRoutineLoadJob kJob = (KafkaRoutineLoadJob) kafkaJobs.get(i);
                ImmutableMap<Integer, Long> partitionIdToProgress =
                        ((KafkaProgress) kJob.getProgress()).getPartitionIdToOffset();

                // offset of partitionIds[i] is beginningOffsets[i] and latestOffsets[i]
                List<Integer> partitionIds = offsetProxyResults.get(i).partitionIds;
                List<Long> beginningOffsets = offsetProxyResults.get(i).beginningOffsets;
                List<Long> latestOffsets = offsetProxyResults.get(i).latestOffsets;

                long maxLag = Long.MIN_VALUE;
                for (int j = 0; j < partitionIds.size(); j++) {
                    int partitionId = partitionIds.get(j);
                    if (!partitionIdToProgress.containsKey(partitionId)) {
                        continue;
                    }
                    long progress = partitionIdToProgress.get(partitionId);
                    if (progress == KafkaProgress.OFFSET_BEGINNING_VAL) {
                        progress = beginningOffsets.get(j);
                    }

                    maxLag = Math.max(latestOffsets.get(j) - progress, maxLag);
                }
                if (maxLag >= Config.min_routine_load_lag_for_metrics) {
                    GaugeMetricImpl<Long> metric =
                            new GaugeMetricImpl<>("routine_load_max_lag_of_partition", MetricUnit.NOUNIT,
                                    "routine load kafka lag");
                    metric.addLabel(new MetricLabel("job_name", kJob.getName()));
                    metric.setValue(maxLag);
                    routineLoadLags.add(metric);
                }
            }

            GAUGE_ROUTINE_LOAD_LAGS = routineLoadLags;
        }
    }

    public static synchronized String getMetric(MetricVisitor visitor, MetricsAction.RequestParams requestParams) {
        if (!hasInit) {
            return "";
        }

        // update the metrics first
        updateMetrics();

        // jvm
        JvmStatCollector jvmStatCollector = new JvmStatCollector();
        JvmStats jvmStats = jvmStatCollector.stats();
        visitor.visitJvm(jvmStats);

        // starrocks metrics
        for (Metric metric : STARROCKS_METRIC_REGISTER.getMetrics()) {
            visitor.visit(metric);
        }

        // database metrics
        collectDatabaseMetrics(visitor);

        // table metrics
        if (requestParams.isCollectTableMetrics()) {
            collectTableMetrics(visitor, requestParams.isMinifyTableMetrics());
        }

        // materialized view metrics
        if (requestParams.isCollectMVMetrics()) {
            MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, requestParams.isMinifyMVMetrics());
        }

        // histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            visitor.visitHistogram(entry.getKey(), entry.getValue());
        }
        ResourceGroupMetricMgr.visitQueryLatency();

        // collect routine load process metrics
        if (Config.enable_routine_load_lag_metrics) {
            collectRoutineLoadProcessMetrics(visitor);
        }

        if (Config.memory_tracker_enable) {
            collectMemoryUsageMetrics(visitor);
        }

        // collect http metrics
        HttpMetricRegistry.getInstance().visit(visitor);


        //collect connections for per user
        collectUserConnMetrics(visitor);

        // collect runnning txns of per db
        collectDbRunningTxnMetrics(visitor);

        // collect starmgr related metrics as well
        StarMgrServer.getCurrentState().visitMetrics(visitor);

        // collect brpc pool metrics
        collectBrpcMetrics(visitor);

        // node info
        visitor.getNodeInfo();
        return visitor.build();
    }

    // update some metrics to make a ready to be visited
    private static void updateMetrics() {
        SYSTEM_METRICS.update();
    }

    // collect table-level metrics
    private static void collectTableMetrics(MetricVisitor visitor, boolean minifyTableMetrics) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getLocalMetastore().listDbNames();
        for (String dbName : dbNames) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (null == db) {
                continue;
            }

            // NOTE: avoid holding database lock here, since we only read all tables, and immutable fields of table
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                long tableId = table.getId();
                String tableName = table.getName();

                if (table.isNativeTableOrMaterializedView()) {
                    // table size metrics
                    GaugeMetric<Long> tableSizeBytesTotal = new GaugeMetric<Long>("table_size_bytes",
                            MetricUnit.BYTES, "total size of table in bytes") {
                        @Override
                        public Long getValue() {
                            OlapTable olapTable = (OlapTable) table;
                            return olapTable.getDataSize();
                        }
                    };
                    tableSizeBytesTotal.addLabel(new MetricLabel("db_name", dbName))
                            .addLabel(new MetricLabel("tbl_name", tableName))
                            .addLabel(new MetricLabel("tbl_id", String.valueOf(tableId)));
                    visitor.visit(tableSizeBytesTotal);
                }

                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(tableId);
                for (Metric m : entity.getMetrics()) {
                    if (minifyTableMetrics && (null == m.getValue() ||
                            (MetricType.COUNTER == m.type && (Long) m.getValue() == 0L))) {
                        continue;
                    }
                    m.addLabel(new MetricLabel("db_name", dbName))
                            .addLabel(new MetricLabel("tbl_name", tableName))
                            .addLabel(new MetricLabel("tbl_id", String.valueOf(tableId)));
                    visitor.visit(m);
                }
            }
        }
    }

    private static void collectDatabaseMetrics(MetricVisitor visitor) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getLocalMetastore().listDbNames();
        GaugeMetricImpl<Integer> databaseNum = new GaugeMetricImpl<>(
                "database_num", MetricUnit.OPERATIONS, "count of database");
        int dbNum = 0;
        for (String dbName : dbNames) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (null == db) {
                continue;
            }
            dbNum++;
            GaugeMetricImpl<Integer> tableNum = new GaugeMetricImpl<>(
                    "table_num", MetricUnit.OPERATIONS, "count of table");
            tableNum.setValue(db.getTableNumber());
            tableNum.addLabel(new MetricLabel("db_name", dbName));
            visitor.visit(tableNum);

            GaugeMetric<Long> dbSizeBytesTotal = new GaugeMetric<Long>("db_size_bytes",
                    MetricUnit.BYTES, "total size of db in bytes") {
                @Override
                public Long getValue() {
                    return db.usedDataQuotaBytes.get();
                }
            };
            dbSizeBytesTotal.addLabel(new MetricLabel("db_name", dbName));
            visitor.visit(dbSizeBytesTotal);
        }
        databaseNum.setValue(dbNum);
        visitor.visit(databaseNum);
    }

    private static void collectBrpcMetrics(MetricVisitor visitor) {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName pattern = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=*");
            Set<ObjectName> objectNames = mBeanServer.queryNames(pattern, null);
            if (objectNames.size() == 0) {
                LOG.warn("failed to get GenericObjectPoolMXBean");
                return;
            }
            String[] attrNames = new String[] {"NumActive", "NumIdle", "NumWaiters", "BorrowedCount",
                    "ReturnedCount", "CreatedCount", "DestroyedCount", "MeanActiveTimeMillis", "MeanIdleTimeMillis",
                    "MeanBorrowWaitTimeMillis"};
            for (ObjectName objectName : objectNames) {
                String name = objectName.getKeyProperty("name");
                AttributeList attrs = mBeanServer.getAttributes(objectName, attrNames);
                if (attrs.size() != attrNames.length) {
                    LOG.warn("failed to get GenericObjectPoolMXBean attributes, attrs.size={}, attrNames.size={}",
                            attrs.size(), attrNames.length);
                    return;
                }
                for (int i = 0; i < attrs.size(); i++) {
                    String attrName = attrNames[i];
                    Object attr = ((Attribute) attrs.get(i)).getValue();
                    if (attr instanceof Integer) {
                        GaugeMetricImpl<Integer> metric = new GaugeMetricImpl<>(
                                "brpc_pool_" + attrName.toLowerCase(), MetricUnit.NOUNIT, "brpc pool " + attrName);
                        metric.addLabel(new MetricLabel("name", name));
                        metric.setValue((Integer) attr);
                        visitor.visit(metric);
                    } else if (attr instanceof Long) {
                        GaugeMetricImpl<Long> metric = new GaugeMetricImpl<>(
                                "brpc_pool_" + attrName.toLowerCase(), MetricUnit.NOUNIT, "brpc pool " + attrName);
                        metric.addLabel(new MetricLabel("name", name));
                        metric.setValue((Long) attr);
                        visitor.visit(metric);
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn("failed to collect brpc metrics", e);
        }
    }

    private static void collectRoutineLoadProcessMetrics(MetricVisitor visitor) {
        for (GaugeMetricImpl<Long> metric : GAUGE_ROUTINE_LOAD_LAGS) {
            visitor.visit(metric);
        }
    }

    private static void collectMemoryUsageMetrics(MetricVisitor visitor) {
        for (GaugeMetricImpl<Long> metric : GAUGE_MEMORY_USAGE_STATS) {
            visitor.visit(metric);
        }
        for (GaugeMetricImpl<Long> metric : GAUGE_OBJECT_COUNT_STATS) {
            visitor.visit(metric);
        }
    }

    // collect connections of per user
    private static void collectUserConnMetrics(MetricVisitor visitor) {

        Map<String, AtomicInteger> userConnectionMap = ExecuteEnv.getInstance().getScheduler().getUserConnectionMap();

        userConnectionMap.forEach((username, connValue) -> {
            GaugeMetricImpl<Integer> metricConnect =
                    new GaugeMetricImpl<>("connection_total", MetricUnit.CONNECTIONS,
                        "total connection");
            metricConnect.addLabel(new MetricLabel("user", username));
            metricConnect.setValue(connValue.get());
            visitor.visit(metricConnect);
        });
    }

    // collect runnning txns of per db
    private static void collectDbRunningTxnMetrics(MetricVisitor visitor) {
        Map<Long, DatabaseTransactionMgr> dbIdToDatabaseTransactionMgrs =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getAllDatabaseTransactionMgrs();
        for (DatabaseTransactionMgr mgr : dbIdToDatabaseTransactionMgrs.values()) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mgr.getDbId());
            if (null == db) {
                continue;
            }
            GaugeMetricImpl<Integer> txnNum = new GaugeMetricImpl<>("txn_running", MetricUnit.NOUNIT,
                     "number of running transactions");
            txnNum.addLabel(new MetricLabel("db", db.getFullName()));
            txnNum.setValue(mgr.getRunningTxnNums());
            visitor.visit(txnNum);
        }
    }

    public static synchronized List<Metric> getMetricsByName(String name) {
        return STARROCKS_METRIC_REGISTER.getMetricsByName(name);
    }

    public static void addMetric(Metric<?> metric) {
        init();
        STARROCKS_METRIC_REGISTER.addMetric(metric);
    }
}

