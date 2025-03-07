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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.JvmMonitorProcDir;
import com.starrocks.monitor.jvm.JvmStatCollector;
import com.starrocks.monitor.jvm.JvmStats;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MetricsTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testTcpMetrics() {
        List<Metric> metrics = MetricRepo.getMetricsByName("snmp");
        Assert.assertEquals(4, metrics.size());
        for (Metric metric : metrics) {
            GaugeMetric<Long> gm = (GaugeMetric<Long>) metric;
            String metricName = gm.getLabels().get(0).getValue();
            if (metricName.equals("tcp_retrans_segs")) {
                Assert.assertEquals(Long.valueOf(826271L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_in_errs")) {
                Assert.assertEquals(Long.valueOf(12712L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_in_segs")) {
                Assert.assertEquals(Long.valueOf(1034019111L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_out_segs")) {
                Assert.assertEquals(Long.valueOf(1166716939L), (Long) gm.getValue());
            } else {
                Assert.fail();
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
            Assert.assertTrue(output.contains(metricName));
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
            Assert.assertTrue(output.contains(metricName));
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
            Assert.assertTrue(jvmProcDirResultRowsContains(rows, metricName));
        }
    }

    @Test
    public void testAddLabel() {
        LongCounterMetric m = new LongCounterMetric("test_metric", Metric.MetricUnit.BYTES, "test");
        m.addLabel(new MetricLabel("k1", "v0"));
        m.addLabel(new MetricLabel("k2", "v2"));
        m.addLabel(new MetricLabel("k1", "v1"));
        Assert.assertEquals(m.getLabels().size(), 2);
        Assert.assertEquals(m.getLabels().get(0).getValue(), "v1");
        Assert.assertEquals(m.getLabels().get(1).getValue(), "v2");
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
            Assert.assertTrue(output.contains(metricName));
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
            Assert.assertTrue(output.contains(metricName));
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
            Assert.assertTrue(output.contains(metricName));
        }
    }
}
