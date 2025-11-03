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

package com.starrocks.transaction;

import com.codahale.metrics.Histogram;
import com.starrocks.common.Config;
import com.starrocks.metric.HistogramMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.monitor.jvm.JvmStats;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionMetricRegistryTest {

    private String oldGroupConfig;

    @BeforeEach
    public void setUp() {
        oldGroupConfig = Config.txn_latency_metric_report_groups;
    }

    @AfterEach
    public void tearDown() {
        Config.txn_latency_metric_report_groups = oldGroupConfig;
        TransactionMetricRegistry.getInstance().updateConfig();
    }

    @Test
    public void testUpdateVisibleRecordsLatenciesOnAllGroup() throws Exception {
        // ensure no per-group interference
        Config.txn_latency_metric_report_groups = "";
        TransactionMetricRegistry registry = TransactionMetricRegistry.getInstance();
        registry.updateConfig();

        Map<String, Long> before = collectCountsByType(registry, "all");

        TransactionState txn = buildTxn(100, 200, 250, 280, 300,
                TransactionStatus.VISIBLE, TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        registry.update(txn);

        Map<String, Long> after = collectCountsByType(registry, "all");
        assertSixIncrements(before, after);
    }

    @Test
    public void testUpdateIgnoresNonVisibleTransactions() throws Exception {
        Config.txn_latency_metric_report_groups = "";
        TransactionMetricRegistry registry = TransactionMetricRegistry.getInstance();
        registry.updateConfig();

        Map<String, Long> before = collectCountsByType(registry, "all");

        TransactionState txn = buildTxn(100, 200, 250, 280, 300,
                TransactionStatus.COMMITTED, TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        registry.update(txn);

        Map<String, Long> after = collectCountsByType(registry, "all");
        assertNoChanges(before, after);
    }

    @Test
    public void testEnableDisablePerGroupReporting() throws Exception {
        TransactionMetricRegistry registry = TransactionMetricRegistry.getInstance();

        Config.txn_latency_metric_report_groups = "stream_load,insert";
        registry.updateConfig();

        Map<String, Long> beforeStream = collectCountsByType(registry, "stream_load");
        Map<String, Long> beforeInsert = collectCountsByType(registry, "insert");
        // should have six histograms for each enabled type
        Assertions.assertEquals(6, beforeStream.size());
        Assertions.assertEquals(6, beforeInsert.size());

        TransactionState tStream = buildTxn(10, 20, 25, 28, 30,
                TransactionStatus.VISIBLE, TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        TransactionState tInsert = buildTxn(11, 21, 26, 29, 31,
                TransactionStatus.VISIBLE, TransactionState.LoadJobSourceType.INSERT_STREAMING);
        registry.update(tStream);
        registry.update(tInsert);

        Map<String, Long> afterStream = collectCountsByType(registry, "stream_load");
        Map<String, Long> afterInsert = collectCountsByType(registry, "insert");
        assertSixIncrements(beforeStream, afterStream);
        assertSixIncrements(beforeInsert, afterInsert);

        // disable all groups and ensure they are not reported
        Config.txn_latency_metric_report_groups = "";
        registry.updateConfig();
        List<HistogramMetric> reported = collectAll(registry);
        for (HistogramMetric h : reported) {
            String type = getLabelValue(h, "type");
            Assertions.assertNotEquals("stream_load", type);
            Assertions.assertNotEquals("insert", type);
        }
    }

    @Test
    public void testStreamingSourceTypeMapsToStreamLoadGroup() throws Exception {
        TransactionMetricRegistry registry = TransactionMetricRegistry.getInstance();

        // verify mapping data structures via reflection
        Map<?, ?> groups = (Map<?, ?>) FieldUtils.readDeclaredField(registry, "metricGroupsByName", true);
        Object[] indexToGroup = (Object[]) FieldUtils.readDeclaredField(registry, "sourceTypeIndexToGroup", true);

        // each LoadJobSourceType should appear exactly once across groups
        Set<TransactionState.LoadJobSourceType> seen = new HashSet<>();
        for (Object groupObj : groups.values()) {
            @SuppressWarnings("unchecked")
            EnumSet<TransactionState.LoadJobSourceType> types =
                    (EnumSet<TransactionState.LoadJobSourceType>) FieldUtils.readDeclaredField(groupObj, "sourceTypes", true);
            for (TransactionState.LoadJobSourceType t : types) {
                Assertions.assertTrue(seen.add(t), "duplicate mapping for " + t);
                Object mapped = indexToGroup[t.ordinal()];
                Assertions.assertSame(groupObj, mapped, "indexToGroup mismatch for " + t);
            }
        }
        // specifically streaming types map to group name "stream_load"
        Object streamGroup = groups.get("stream_load");
        Assertions.assertNotNull(streamGroup);
        @SuppressWarnings("unchecked")
        EnumSet<TransactionState.LoadJobSourceType> streamTypes =
                (EnumSet<TransactionState.LoadJobSourceType>) FieldUtils.readDeclaredField(streamGroup, "sourceTypes", true);
        Assertions.assertTrue(streamTypes.contains(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assertions.assertTrue(streamTypes.contains(TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assertions.assertTrue(streamTypes.contains(TransactionState.LoadJobSourceType.MULTI_STATEMENT_STREAMING));

        // functionally, updating a FRONTEND_STREAMING txn should increment type=stream_load
        Config.txn_latency_metric_report_groups = "stream_load";
        registry.updateConfig();
        Map<String, Long> before = collectCountsByType(registry, "stream_load");
        Assertions.assertEquals(6, before.size());
        TransactionState txn = buildTxn(100, 200, 220, 240, 260,
                TransactionStatus.VISIBLE, TransactionState.LoadJobSourceType.FRONTEND_STREAMING);
        registry.update(txn);
        Map<String, Long> after = collectCountsByType(registry, "stream_load");
        assertSixIncrements(before, after);
    }

    // --------------------- helpers ---------------------

    private static TransactionState buildTxn(long prepare, long commit, long publish, long publishFinish, long finish,
                                             TransactionStatus status,
                                             TransactionState.LoadJobSourceType sourceType) throws Exception {
        TransactionState txn = new TransactionState();
        txn.setPrepareTime(prepare);
        txn.setCommitTime(commit);
        txn.setFinishTime(finish);
        FieldUtils.writeDeclaredField(txn, "publishVersionTime", publish, true);
        FieldUtils.writeDeclaredField(txn, "publishVersionFinishTime", publishFinish, true);
        FieldUtils.writeDeclaredField(txn, "sourceType", sourceType, true);
        txn.setTransactionStatus(status);
        return txn;
    }

    private static class CollectingVisitor extends MetricVisitor {
        final List<HistogramMetric> histograms = new ArrayList<>();

        public CollectingVisitor() {
            super("");
        }

        @Override
        public void visitJvm(JvmStats jvmStats) {
        }

        @Override
        public void visit(Metric metric) {
        }

        @Override
        public void visitHistogram(String name, Histogram histogram) {
        }

        @Override
        public void visitHistogram(HistogramMetric histogram) {
            histograms.add(histogram);
        }

        @Override
        public void getNodeInfo() {
        }

        @Override
        public String build() {
            return "";
        }
    }

    private static List<HistogramMetric> collectAll(TransactionMetricRegistry registry) {
        CollectingVisitor v = new CollectingVisitor();
        registry.report(v);
        return v.histograms;
    }

    private static String getLabelValue(HistogramMetric metric, String key) {
        for (MetricLabel l : metric.getLabels()) {
            if (key.equals(l.getKey())) {
                return l.getValue();
            }
        }
        return "";
    }

    private static Map<String, Long> collectCountsByType(TransactionMetricRegistry registry, String typeValue) {
        List<HistogramMetric> all = collectAll(registry);
        Map<String, Long> counts = new HashMap<>();
        for (HistogramMetric h : all) {
            if (!typeValue.equals(getLabelValue(h, "type"))) {
                continue;
            }
            counts.put(h.getName(), h.getCount());
        }
        return counts;
    }

    private static void assertSixIncrements(Map<String, Long> before, Map<String, Long> after) {
        for (String name : metricNames()) {
            long b = before.getOrDefault(name, 0L);
            long a = after.getOrDefault(name, 0L);
            Assertions.assertEquals(b + 1, a, "expect +1 for " + name);
        }
    }

    private static void assertNoChanges(Map<String, Long> before, Map<String, Long> after) {
        for (String name : metricNames()) {
            long b = before.getOrDefault(name, 0L);
            long a = after.getOrDefault(name, 0L);
            Assertions.assertEquals(b, a, "expect unchanged for " + name);
        }
    }

    private static List<String> metricNames() {
        List<String> names = new ArrayList<>();
        names.add("txn_total_latency_ms");
        names.add("txn_write_latency_ms");
        names.add("txn_publish_latency_ms");
        names.add("txn_publish_schedule_latency_ms");
        names.add("txn_publish_execute_latency_ms");
        names.add("txn_publish_ack_latency_ms");
        return names;
    }
}


