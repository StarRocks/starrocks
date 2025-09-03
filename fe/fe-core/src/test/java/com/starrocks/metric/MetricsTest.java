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

import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.proc.JvmMonitorProcDir;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.monitor.jvm.JvmStatCollector;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class MetricsTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testTcpMetrics() {
        List<Metric> metrics = MetricRepo.getMetricsByName("snmp");
        Assertions.assertEquals(4, metrics.size());
        for (Metric metric : metrics) {
            GaugeMetric<Long> gm = (GaugeMetric<Long>) metric;
            String metricName = gm.getLabels().get(0).getValue();
            if (metricName.equals("tcp_retrans_segs")) {
                Assertions.assertEquals(Long.valueOf(826271L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_in_errs")) {
                Assertions.assertEquals(Long.valueOf(12712L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_in_segs")) {
                Assertions.assertEquals(Long.valueOf(1034019111L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_out_segs")) {
                Assertions.assertEquals(Long.valueOf(1166716939L), (Long) gm.getValue());
            } else {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testJsonJvmStats() {
        JsonMetricVisitor jsonMetricVisitor = new JsonMetricVisitor("sr_fe_jvm_stat_test");
        JvmStatCollector jvmStatCollector = new JvmStatCollector();
        JvmStats jvmStats = jvmStatCollector.stats();
        jsonMetricVisitor.visitJvm(jvmStats);
        String output = jsonMetricVisitor.build();
        System.out.println(output);
        List<String> metricNames = Arrays.asList(
                "jvm_old_gc",
                "jvm_young_gc",
                "jvm_young_size_bytes",
                "jvm_heap_size_bytes",
                "jvm_old_size_bytes",
                "jvm_direct_buffer_pool_size_bytes"
        );
        for (String metricName : metricNames) {
            Assertions.assertTrue(output.contains(metricName));
        }
    }

    @Test
    public void testPrometheusJvmStats() {
        PrometheusMetricVisitor prometheusMetricVisitor = new PrometheusMetricVisitor("sr_fe_jvm_stat_test");
        JvmStatCollector jvmStatCollector = new JvmStatCollector();
        System.out.println(jvmStatCollector.toString());
        JvmStats jvmStats = jvmStatCollector.stats();
        prometheusMetricVisitor.visitJvm(jvmStats);
        String output = prometheusMetricVisitor.build();
        System.out.println(output);
        List<String> metricNames = Arrays.asList(
                "jvm_old_gc",
                "jvm_young_gc",
                "jvm_young_size_bytes",
                "jvm_heap_size_bytes",
                "jvm_old_size_bytes",
                "jvm_direct_buffer_pool_size_bytes"
        );
        for (String metricName : metricNames) {
            Assertions.assertTrue(output.contains(metricName));
        }
    }

    private boolean jvmProcDirResultRowsContains(List<List<String>> rows, String metricName) {
        for (List<String> row : rows) {
            if (row.contains(metricName)) {
                return true;
            }
        }

        return false;
    }

    @Test
    public void testProcDirJvmStats() throws AnalysisException {
        JvmMonitorProcDir jvmMonitorProcDir = new JvmMonitorProcDir();
        List<List<String>> rows = jvmMonitorProcDir.fetchResult().getRows();
        System.out.println(rows);
        List<String> metricNames = Arrays.asList(
                "gc old collection count",
                "gc old collection time",
                "gc young collection time",
                "gc young collection time",
                "mem pool old committed",
                "mem pool old used"
        );
        for (String metricName : metricNames) {
            System.out.println(metricName);
            Assertions.assertTrue(jvmProcDirResultRowsContains(rows, metricName));
        }
    }

    @Test
    public void testAddLabel() {
        LongCounterMetric m = new LongCounterMetric("test_metric", Metric.MetricUnit.BYTES, "test");
        m.addLabel(new MetricLabel("k1", "v0"));
        m.addLabel(new MetricLabel("k2", "v2"));
        m.addLabel(new MetricLabel("k1", "v1"));
        Assertions.assertEquals(m.getLabels().size(), 2);
        Assertions.assertEquals(m.getLabels().get(0).getValue(), "v1");
        Assertions.assertEquals(m.getLabels().get(1).getValue(), "v2");
    }

    @Test
    public void testPrometheusHistogramMetrics() {
        PrometheusMetricVisitor prometheusMetricVisitor = new PrometheusMetricVisitor("sr");
        HistogramMetric histogramMetric = new HistogramMetric("duration");
        histogramMetric.addLabel(new MetricLabel("k1", "v1"));
        histogramMetric.addLabel(new MetricLabel("k2", "v2"));
        prometheusMetricVisitor.visitHistogram(histogramMetric);
        String output = prometheusMetricVisitor.build();
        List<String> metricNames = Arrays.asList(
                "sr_duration{quantile=\"0.75\", k1=\"v1\", k2=\"v2\"}",
                "sr_duration{quantile=\"0.95\", k1=\"v1\", k2=\"v2\"}",
                "sr_duration{quantile=\"0.98\", k1=\"v1\", k2=\"v2\"}",
                "sr_duration{quantile=\"0.99\", k1=\"v1\", k2=\"v2\"}",
                "sr_duration{quantile=\"0.999\", k1=\"v1\", k2=\"v2\"}",
                "sr_duration_sum",
                "sr_duration_count"
        );
        for (String metricName : metricNames) {
            Assertions.assertTrue(output.contains(metricName));
        }
    }

    @Test
    public void testJsonHistogramMetrics1() {
        JsonMetricVisitor visitor = new JsonMetricVisitor("sr");
        HistogramMetric histogramMetric = new HistogramMetric("duration");
        histogramMetric.addLabel(new MetricLabel("k1", "v1"));
        histogramMetric.addLabel(new MetricLabel("k2", "v2"));
        visitor.visitHistogram(histogramMetric);
        String output = visitor.build();
        List<String> metricNames = Arrays.asList(
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.75\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.95\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.98\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.99\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.999\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration_sum\",\"k1\":\"v1\",\"k2\":\"v2\"},\"unit\":\"milliseconds\",\"value\":0" +
                        ".0},\n",
                "{\"tags\":{\"metric\":\"sr_duration_count\",\"k1\":\"v1\",\"k2\":\"v2\"},\"unit\":\"nounit\",\"value\":0}"
        );
        for (String metricName : metricNames) {
            Assertions.assertTrue(output.contains(metricName));
        }
    }

    @Test
    public void testJsonHistogramMetrics2() {
        JsonMetricVisitor visitor = new JsonMetricVisitor("sr");
        HistogramMetric histogramMetric = new HistogramMetric("duration");
        histogramMetric.addLabel(new MetricLabel("k1", "v1"));
        histogramMetric.addLabel(new MetricLabel("k2", "v2"));
        visitor.visitHistogram("", histogramMetric);
        String output = visitor.build();
        List<String> metricNames = Arrays.asList(
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.75\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.95\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.98\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.99\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration\",\"k1\":\"v1\",\"k2\":\"v2\",\"quantile\":\"0.999\"}," +
                        "\"unit\":\"milliseconds\",\"value\":0.0},\n",
                "{\"tags\":{\"metric\":\"sr_duration_sum\",\"k1\":\"v1\",\"k2\":\"v2\"},\"unit\":\"milliseconds\",\"value\":0" +
                        ".0},\n",
                "{\"tags\":{\"metric\":\"sr_duration_count\",\"k1\":\"v1\",\"k2\":\"v2\"},\"unit\":\"nounit\",\"value\":0}"
        );
        for (String metricName : metricNames) {
            Assertions.assertTrue(output.contains(metricName));
        }
    }

    @Test
    public void testCloneMetrics() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);

        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();
        TabletSchedulerStat stat = tabletScheduler.getStat();
        stat.counterCloneTask.incrementAndGet(); // 1
        stat.counterCloneTaskSucceeded.incrementAndGet(); // 1
        stat.counterCloneTaskInterNodeCopyBytes.addAndGet(100L);
        stat.counterCloneTaskInterNodeCopyDurationMs.addAndGet(10L);
        stat.counterCloneTaskIntraNodeCopyBytes.addAndGet(101L);
        stat.counterCloneTaskIntraNodeCopyDurationMs.addAndGet(11L);

        TabletSchedCtx ctx1 = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, 1L, 2L, 3L, 4L, 1001L, System.currentTimeMillis());
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx1);
        TabletSchedCtx ctx2 = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, 1L, 2L, 3L, 4L, 1002L, System.currentTimeMillis());
        Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx2);
        Set<Long> allTabletIds = Deencapsulation.getField(tabletScheduler, "allTabletIds");
        allTabletIds.add(1001L);
        allTabletIds.add(1002L);

        // check
        List<Metric> scheduledTabletNum = MetricRepo.getMetricsByName("scheduled_tablet_num");
        Assertions.assertEquals(1, scheduledTabletNum.size());
        Assertions.assertEquals(2L, (Long) scheduledTabletNum.get(0).getValue());

        List<Metric> scheduledPendingTabletNums = MetricRepo.getMetricsByName("scheduled_pending_tablet_num");
        Assertions.assertEquals(2, scheduledPendingTabletNums.size());
        for (Metric metric : scheduledPendingTabletNums) {
            // label 0 is is_leader
            MetricLabel label = (MetricLabel) metric.getLabels().get(1);
            String type = label.getValue();
            if (type.equals("REPAIR")) {
                Assertions.assertEquals(0L, metric.getValue());
            } else if (type.equals("BALANCE")) {
                Assertions.assertEquals(1L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> scheduledRunningTabletNums = MetricRepo.getMetricsByName("scheduled_running_tablet_num");
        Assertions.assertEquals(2, scheduledRunningTabletNums.size());
        for (Metric metric : scheduledRunningTabletNums) {
            // label 0 is is_leader
            MetricLabel label = (MetricLabel) metric.getLabels().get(1);
            String type = label.getValue();
            if (type.equals("REPAIR")) {
                Assertions.assertEquals(1L, metric.getValue());
            } else if (type.equals("BALANCE")) {
                Assertions.assertEquals(0L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> cloneTaskTotal = MetricRepo.getMetricsByName("clone_task_total");
        Assertions.assertEquals(1, cloneTaskTotal.size());
        Assertions.assertEquals(1L, (Long) cloneTaskTotal.get(0).getValue());

        List<Metric> cloneTaskSuccess = MetricRepo.getMetricsByName("clone_task_success");
        Assertions.assertEquals(1, cloneTaskSuccess.size());
        Assertions.assertEquals(1L, (Long) cloneTaskSuccess.get(0).getValue());

        List<Metric> cloneTaskCopyBytes = MetricRepo.getMetricsByName("clone_task_copy_bytes");
        Assertions.assertEquals(2, cloneTaskCopyBytes.size());
        for (Metric metric : cloneTaskCopyBytes) {
            MetricLabel label = (MetricLabel) metric.getLabels().get(0);
            String type = label.getValue();
            if (type.equals("INTER_NODE")) {
                Assertions.assertEquals(100L, metric.getValue());
            } else if (type.equals("INTRA_NODE")) {
                Assertions.assertEquals(101L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> cloneTaskCopyDurationMs = MetricRepo.getMetricsByName("clone_task_copy_duration_ms");
        Assertions.assertEquals(2, cloneTaskCopyDurationMs.size());
        for (Metric metric : cloneTaskCopyDurationMs) {
            MetricLabel label = (MetricLabel) metric.getLabels().get(0);
            String type = label.getValue();
            if (type.equals("INTER_NODE")) {
                Assertions.assertEquals(10L, metric.getValue());
            } else if (type.equals("INTRA_NODE")) {
                Assertions.assertEquals(11L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }
    }
}
