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

package com.starrocks.load.batchwrite;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.starrocks.metric.CounterMetric;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.GaugeMetricImpl;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricVisitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MergeCommitMetricRegistry {

    private static final MergeCommitMetricRegistry INSTANCE = new MergeCommitMetricRegistry();

    private final GaugeMetricImpl<Integer> jobNum = new GaugeMetricImpl<>("merge_commit_job_num",
            Metric.MetricUnit.NOUNIT, "the number of merge commit jobs");
    private final LongCounterMetric taskSuccessNum = new LongCounterMetric("merge_commit_task_success_num",
            Metric.MetricUnit.NOUNIT, "the number of merge commit success tasks");
    private final LongCounterMetric taskFailNum = new LongCounterMetric("merge_commit_task_fail_num",
            Metric.MetricUnit.NOUNIT, "the number of merge commit fail tasks");
    private final LongCounterMetric taskRunningNum = new LongCounterMetric("merge_commit_task_running_num",
            Metric.MetricUnit.NOUNIT, "the number of merge commit running tasks");
    private final LongCounterMetric loadRows = new LongCounterMetric("merge_commit_load_rows",
            Metric.MetricUnit.NOUNIT, "the number of load rows");
    private final LongCounterMetric loadBytes = new LongCounterMetric("merge_commit_load_bytes",
            Metric.MetricUnit.NOUNIT, "the number of load bytes");
    private final Histogram loadLatencyMs;

    private final LongCounterMetric txnDispatchSuccessNum = new LongCounterMetric("merge_commit_txn_dispatch_success_num",
            Metric.MetricUnit.NOUNIT, "the number of successful txn dispatch");
    private final LongCounterMetric txnDispatchFailNum = new LongCounterMetric("merge_commit_txn_dispatch_fail_num",
            Metric.MetricUnit.NOUNIT, "the number of failed txn dispatch");
    private final LongCounterMetric txnDispatchPendingNum = new LongCounterMetric("merge_commit_txn_dispatch_pending_num",
            Metric.MetricUnit.NOUNIT, "the number of pending txn dispatch");
    private final LongCounterMetric txnDispatchRetryNum = new LongCounterMetric("merge_commit_txn_dispatch_retry_num",
            Metric.MetricUnit.NOUNIT, "the number of txn dispatch retry");
    private final Histogram txnDispatchLatencyUs;

    private final GaugeMetricImpl<Integer> warehouseNum = new GaugeMetricImpl<>("merge_commit_warehouse_num",
            Metric.MetricUnit.NOUNIT, "the number of warehouses used for merge commit");
    private final GaugeMetricImpl<Integer> computeNodeNum = new GaugeMetricImpl<>("merge_commit_cn_num",
            Metric.MetricUnit.NOUNIT, "the number of compute nodes used for merge commit");
    private final GaugeMetricImpl<Integer> totalExpectParallel = new GaugeMetricImpl<>("merge_commit_total_expect_parallel",
            Metric.MetricUnit.NOUNIT, "the total expected parallelisms of all merge commit jobs");
    private final GaugeMetricImpl<Integer> totalActualParallel = new GaugeMetricImpl<>("merge_commit_total_actual_parallel",
            Metric.MetricUnit.NOUNIT, "the total actual parallelisms of all merge commit jobs");

    private final Map<String, GaugeMetric<?>> gaugeMetrics = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric<?>> counterMetrics = new ConcurrentHashMap<>();
    private final MetricRegistry histoMetricRegistry = new MetricRegistry();

    public static MergeCommitMetricRegistry getInstance() {
        return INSTANCE;
    }

    private MergeCommitMetricRegistry() {
        registerGauge(jobNum);
        registerCounter(taskSuccessNum);
        registerCounter(taskFailNum);
        registerCounter(taskRunningNum);
        registerCounter(loadRows);
        registerCounter(loadBytes);
        this.loadLatencyMs = registerHistogram("merge_commit_load_latency_ms");
        registerCounter(txnDispatchSuccessNum);
        registerCounter(txnDispatchFailNum);
        registerCounter(txnDispatchPendingNum);
        registerCounter(txnDispatchRetryNum);
        this.txnDispatchLatencyUs = registerHistogram("merge_commit_txn_dispatch_latency_us");
        registerGauge(warehouseNum);
        registerGauge(computeNodeNum);
        registerGauge(totalExpectParallel);
        registerGauge(totalActualParallel);
    }

    public void visit(MetricVisitor visitor) {
        for (GaugeMetric<?> metric : gaugeMetrics.values()) {
            visitor.visit(metric);
        }

        for (CounterMetric<?> metric : counterMetrics.values()) {
            visitor.visit(metric);
        }

        for (Map.Entry<String, Histogram> entry : histoMetricRegistry.getHistograms().entrySet()) {
            visitor.visitHistogram(entry.getKey(), entry.getValue());
        }
    }

    private void registerGauge(GaugeMetric<?> gaugeMetric) {
        gaugeMetrics.put(gaugeMetric.getName(), gaugeMetric);
    }

    private void registerCounter(CounterMetric<?> counterMetric) {
        counterMetrics.put(counterMetric.getName(), counterMetric);
    }

    private Histogram registerHistogram(String name) {
        return histoMetricRegistry.histogram(name);
    }

    public void setJobNum(int num) {
        jobNum.setValue(num);
    }

    public void incSuccessTask() {
        taskSuccessNum.increase(1L);
    }

    public void incFailTask() {
        taskFailNum.increase(1L);
    }

    public void updateRunningTask(long delta) {
        taskRunningNum.increase(delta);
    }

    public void incLoadData(long rows, long bytes) {
        loadRows.increase(rows);
        loadBytes.increase(bytes);
    }

    public void updateLoadLatency(long latencyMs) {
        loadLatencyMs.update(latencyMs);
    }

    public void updatePendingTxnDispatch(long delta) {
        txnDispatchPendingNum.increase(delta);
    }

    public void updateTxnDispatchResult(boolean success, long numRetry, long latencyUs) {
        if (success) {
            txnDispatchSuccessNum.increase(1L);
        } else {
            txnDispatchFailNum.increase(1L);
        }
        txnDispatchRetryNum.increase(numRetry);
        txnDispatchLatencyUs.update(latencyUs);
    }

    public void updateBackendAssigner(int warehouseNum, int cnNum, int totalExpectParallel, int totalActualParallel) {
        this.warehouseNum.setValue(warehouseNum);
        this.computeNodeNum.setValue(cnNum);
        this.totalExpectParallel.setValue(totalExpectParallel);
        this.totalActualParallel.setValue(totalActualParallel);
    }
}
