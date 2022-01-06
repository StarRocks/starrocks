// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Sets;
import com.starrocks.alter.Alter;
import com.starrocks.alter.AlterJob.JobType;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadManager;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadManager;
import com.starrocks.metric.Metric.MetricType;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.monitor.jvm.JvmService;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.proto.PKafkaOffsetProxyRequest;
import com.starrocks.proto.PKafkaOffsetProxyResult;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class MetricRepo {
    private static final Logger LOG = LogManager.getLogger(MetricRepo.class);

    private static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    private static final StarRocksMetricRegistry STARROCKS_METRIC_REGISTER = new StarRocksMetricRegistry();

    public static volatile boolean isInit = false;
    public static final SystemMetrics SYSTEM_METRICS = new SystemMetrics();

    public static final String TABLET_NUM = "tablet_num";
    public static final String TABLET_MAX_COMPACTION_SCORE = "tablet_max_compaction_score";

    public static LongCounterMetric COUNTER_REQUEST_ALL;
    public static LongCounterMetric COUNTER_QUERY_ALL;
    public static LongCounterMetric COUNTER_QUERY_ERR;
    public static LongCounterMetric COUNTER_QUERY_TIMEOUT;
    public static LongCounterMetric COUNTER_QUERY_SUCCESS;
    public static LongCounterMetric COUNTER_SLOW_QUERY;
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

    public static Histogram HISTO_QUERY_LATENCY;
    public static Histogram HISTO_EDIT_LOG_WRITE_LATENCY;

    // following metrics will be updated by metric calculator
    public static GaugeMetricImpl<Double> GAUGE_QUERY_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_REQUEST_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_ERR_RATE;
    // these query latency is different from HISTO_QUERY_LATENCY, for these only summarize the latest queries, but HISTO_QUERY_LATENCY summarizes all queries.
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_MEAN;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_MEDIAN;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P75;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P90;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P95;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P99;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_LATENCY_P999;
    public static GaugeMetricImpl<Long> GAUGE_MAX_TABLET_COMPACTION_SCORE;

    private static ScheduledThreadPoolExecutor metricTimer =
            ThreadPoolManager.newDaemonScheduledThreadPool(1, "Metric-Timer-Pool", true);
    private static MetricCalculator metricCalculator = new MetricCalculator();

    public static synchronized void init() {
        if (isInit) {
            return;
        }

        // 1. gauge
        // load jobs
        LoadManager loadManger = Catalog.getCurrentCatalog().getLoadManager();
        for (EtlJobType jobType : EtlJobType.values()) {
            if (jobType == EtlJobType.MINI || jobType == EtlJobType.UNKNOWN) {
                continue;
            }

            for (JobState state : JobState.values()) {
                GaugeMetric<Long> gauge = (GaugeMetric<Long>) new GaugeMetric<Long>("job",
                        MetricUnit.NOUNIT, "job statistics") {
                    @Override
                    public Long getValue() {
                        if (!Catalog.getCurrentCatalog().isMaster()) {
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
        Alter alter = Catalog.getCurrentCatalog().getAlterInstance();
        for (JobType jobType : JobType.values()) {
            if (jobType != JobType.SCHEMA_CHANGE && jobType != JobType.ROLLUP) {
                continue;
            }

            GaugeMetric<Long> gauge = (GaugeMetric<Long>) new GaugeMetric<Long>("job",
                    MetricUnit.NOUNIT, "job statistics") {
                @Override
                public Long getValue() {
                    if (!Catalog.getCurrentCatalog().isMaster()) {
                        return 0L;
                    }
                    if (jobType == JobType.SCHEMA_CHANGE) {
                        return alter.getSchemaChangeHandler()
                                .getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.RUNNING);
                    } else {
                        return alter.getMaterializedViewHandler()
                                .getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.RUNNING);
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

        // connections
        GaugeMetric<Integer> conections = (GaugeMetric<Integer>) new GaugeMetric<Integer>(
                "connection_total", MetricUnit.CONNECTIONS, "total connections") {
            @Override
            public Integer getValue() {
                return ExecuteEnv.getInstance().getScheduler().getConnectionNum();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(conections);

        // journal id
        GaugeMetric<Long> maxJournalId = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "max_journal_id", MetricUnit.NOUNIT, "max journal id of this frontends") {
            @Override
            public Long getValue() {
                return Catalog.getCurrentCatalog().getMaxJournalId();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(maxJournalId);

        // meta log total count
        GaugeMetric<Long> metaLogCount = new GaugeMetric<Long>(
                "meta_log_count", MetricUnit.NOUNIT, "meta log total count") {
            @Override
            public Long getValue() {
                return Catalog.getCurrentCatalog().getMaxJournalId() - Catalog.getCurrentCatalog().getImageJournalId();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(metaLogCount);

        // scheduled tablet num
        GaugeMetric<Long> scheduledTabletNum = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "scheduled_tablet_num", MetricUnit.NOUNIT, "number of tablets being scheduled") {
            @Override
            public Long getValue() {
                if (!Catalog.getCurrentCatalog().isMaster()) {
                    return 0L;
                }
                return (long) Catalog.getCurrentCatalog().getTabletScheduler().getTotalNum();
            }
        };
        STARROCKS_METRIC_REGISTER.addMetric(scheduledTabletNum);

        // routine load jobs
        RoutineLoadManager routineLoadManger = Catalog.getCurrentCatalog().getRoutineLoadManager();
        for (RoutineLoadJob.JobState state : RoutineLoadJob.JobState.values()) {
            GaugeMetric<Long> gauge = (GaugeMetric<Long>) new GaugeMetric<Long>("routine_load_jobs",
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

        GAUGE_MAX_TABLET_COMPACTION_SCORE = new GaugeMetricImpl<>("max_tablet_compaction_score",
                MetricUnit.NOUNIT, "max tablet compaction score of all backends");
        GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(0L);
        STARROCKS_METRIC_REGISTER.addMetric(GAUGE_MAX_TABLET_COMPACTION_SCORE);

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

        COUNTER_TXN_REJECT =
                new LongCounterMetric("txn_reject", MetricUnit.REQUESTS, "counter of rejected transactions");
        STARROCKS_METRIC_REGISTER.addMetric(COUNTER_TXN_REJECT);
        COUNTER_TXN_BEGIN = new LongCounterMetric("txn_begin", MetricUnit.REQUESTS, "counter of begining transactions");
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

        // 3. histogram
        HISTO_QUERY_LATENCY = METRIC_REGISTER.histogram(MetricRegistry.name("query", "latency", "ms"));
        HISTO_EDIT_LOG_WRITE_LATENCY =
                METRIC_REGISTER.histogram(MetricRegistry.name("editlog", "write", "latency", "ms"));

        // init system metrics
        initSystemMetrics();

        updateMetrics();
        isInit = true;

        if (Config.enable_metric_calculator) {
            metricTimer.scheduleAtFixedRate(metricCalculator, 0, 15 * 1000L, TimeUnit.MILLISECONDS);
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

    // to generate the metrics related to tablets of each backends
    // this metric is reentrant, so that we can add or remove metric along with the backend add or remove
    // at runtime.
    public static void generateBackendsTabletMetrics() {
        // remove all previous 'tablet' metric
        STARROCKS_METRIC_REGISTER.removeMetrics(TABLET_NUM);
        STARROCKS_METRIC_REGISTER.removeMetrics(TABLET_MAX_COMPACTION_SCORE);

        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();

        for (Long beId : infoService.getBackendIds(false)) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            // tablet number of each backends
            GaugeMetric<Long> tabletNum = (GaugeMetric<Long>) new GaugeMetric<Long>(TABLET_NUM,
                    MetricUnit.NOUNIT, "tablet number") {
                @Override
                public Long getValue() {
                    if (!Catalog.getCurrentCatalog().isMaster()) {
                        return 0L;
                    }
                    return invertedIndex.getTabletNumByBackendId(beId);
                }
            };
            tabletNum.addLabel(new MetricLabel("backend", be.getHost() + ":" + be.getHeartbeatPort()));
            STARROCKS_METRIC_REGISTER.addMetric(tabletNum);

            // max compaction score of tablets on each backends
            GaugeMetric<Long> tabletMaxCompactionScore = (GaugeMetric<Long>) new GaugeMetric<Long>(
                    TABLET_MAX_COMPACTION_SCORE, MetricUnit.NOUNIT,
                    "tablet max compaction score") {
                @Override
                public Long getValue() {
                    if (!Catalog.getCurrentCatalog().isMaster()) {
                        return 0L;
                    }
                    return be.getTabletMaxCompactionScore();
                }
            };
            tabletMaxCompactionScore.addLabel(new MetricLabel("backend", be.getHost() + ":" + be.getHeartbeatPort()));
            STARROCKS_METRIC_REGISTER.addMetric(tabletMaxCompactionScore);

        } // end for backends
    }

    public static synchronized String getMetric(MetricVisitor visitor, boolean collectTableMetrics,
                                                boolean minifyTableMetrics) {
        if (!isInit) {
            return "";
        }

        // update the metrics first
        updateMetrics();

        // jvm
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        visitor.visitJvm(jvmStats);

        // starrocks metrics
        for (Metric metric : STARROCKS_METRIC_REGISTER.getMetrics()) {
            visitor.visit(metric);
        }

        // table metrics
        if (collectTableMetrics) {
            collectTableMetrics(visitor, minifyTableMetrics);
        }

        // histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            visitor.visitHistogram(entry.getKey(), entry.getValue());
        }

        // collect routine load process metrics
        if (Config.enable_routine_load_lag_metrics) {
            collectRoutineLoadProcessMetrics(visitor);
        }

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
        Catalog catalog = Catalog.getCurrentCatalog();
        List<String> dbNames = catalog.getDbNames();
        for (String dbName : dbNames) {
            Database db = Catalog.getCurrentCatalog().getDb(dbName);
            if (null == db) {
                continue;
            }
            String dbShortName = dbName.replace("default_cluster:", "");
            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(table.getId());
                    for (Metric m : entity.getMetrics()) {
                        if (minifyTableMetrics && (null == m.getValue() ||
                                (MetricType.COUNTER == m.type && ((Long) m.getValue()).longValue() == 0L))) {
                            continue;
                        }
                        m.addLabel(new MetricLabel("db_name", dbShortName))
                                .addLabel(new MetricLabel("tbl_name", table.getName()))
                                .addLabel(new MetricLabel("tbl_id", String.valueOf(table.getId())));
                        visitor.visit(m);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
    }

    private static void collectRoutineLoadProcessMetrics(MetricVisitor visitor) {
        List<RoutineLoadJob> jobs = Catalog.getCurrentCatalog().getRoutineLoadManager().getRoutineLoadJobByState(
                Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE, RoutineLoadJob.JobState.RUNNING));

        List<RoutineLoadJob> kafkaJobs = jobs.stream()
                .filter(job -> (job instanceof KafkaRoutineLoadJob)
                        && ((KafkaProgress) job.getProgress()).hasPartition())
                .collect(Collectors.toList());

        if (kafkaJobs.size() <= 0) {
            return;
        }

        // get all partitions offset in a batch api
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
                    ImmutableMap.copyOf(kJob.getConvertedCustomProperties()));
            offsetProxyRequest.partitionIds = new ArrayList<>(
                    ((KafkaProgress) kJob.getProgress()).getPartitionIdToOffset().keySet());
            requests.add(offsetProxyRequest);
        }
        List<PKafkaOffsetProxyResult> offsetProxyResults;
        try {
            offsetProxyResults = KafkaUtil.getBatchOffsets(requests);
        } catch (UserException e) {
            LOG.warn("get batch offsets failed", e);
            return;
        }

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
                        new GaugeMetricImpl<>("routine_load_max_lag_of_partition", MetricUnit.NOUNIT, "routine load kafka lag");
                metric.addLabel(new MetricLabel("job_name", kJob.getName()));
                metric.setValue(maxLag);
                visitor.visit(metric);
            }
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

