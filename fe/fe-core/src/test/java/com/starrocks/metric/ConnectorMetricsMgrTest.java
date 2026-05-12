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

package com.starrocks.metric;

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectorMetricsMgrTest extends StarRocksTestBase {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    // ======================= Write Metrics =======================

    @Test
    public void testIcebergWriteMetrics() {
        ConnectorMetricsMgr.increaseWriteTotalSuccess("iceberg", "insert");
        ConnectorMetricsMgr.increaseWriteRows("iceberg", 100L, "insert");
        ConnectorMetricsMgr.increaseWriteBytes("iceberg", 2048L, "insert");
        ConnectorMetricsMgr.increaseWriteFiles("iceberg", 3L, "insert");
        ConnectorMetricsMgr.increaseWriteDurationMs("iceberg", 500L, "insert");

        LongCounterMetric total = findCounter("iceberg_write_total",
                "status", "success", "reason", "none", "write_type", "insert");
        Assertions.assertNotNull(total);
        Assertions.assertTrue(total.getValue() >= 1);

        LongCounterMetric rows = findCounter("iceberg_write_rows", "write_type", "insert");
        Assertions.assertNotNull(rows);
        Assertions.assertTrue(rows.getValue() >= 100);

        LongCounterMetric bytes = findCounter("iceberg_write_bytes", "write_type", "insert");
        Assertions.assertNotNull(bytes);
        Assertions.assertTrue(bytes.getValue() >= 2048);

        LongCounterMetric files = findCounter("iceberg_write_files", "write_type", "insert");
        Assertions.assertNotNull(files);
        Assertions.assertTrue(files.getValue() >= 3);

        LongCounterMetric duration = findCounter("iceberg_write_duration_ms_total", "write_type", "insert");
        Assertions.assertNotNull(duration);
        Assertions.assertTrue(duration.getValue() >= 500);
    }

    @Test
    public void testHiveWriteMetrics() {
        ConnectorMetricsMgr.increaseWriteTotalSuccess("hive", "insert");
        ConnectorMetricsMgr.increaseWriteRows("hive", 200L, "insert");
        ConnectorMetricsMgr.increaseWriteBytes("hive", 4096L, "insert");
        ConnectorMetricsMgr.increaseWriteFiles("hive", 5L, "insert");
        ConnectorMetricsMgr.increaseWriteDurationMs("hive", 300L, "insert");

        LongCounterMetric total = findCounter("hive_write_total",
                "status", "success", "reason", "none", "write_type", "insert");
        Assertions.assertNotNull(total);
        Assertions.assertTrue(total.getValue() >= 1);

        LongCounterMetric rows = findCounter("hive_write_rows", "write_type", "insert");
        Assertions.assertNotNull(rows);
        Assertions.assertTrue(rows.getValue() >= 200);
    }

    @Test
    public void testWriteFailMetrics() {
        ConnectorMetricsMgr.increaseWriteTotalFail("iceberg", new RuntimeException("timeout"), "overwrite");

        LongCounterMetric fail = findCounter("iceberg_write_total",
                "status", "failed", "reason", "timeout", "write_type", "overwrite");
        Assertions.assertNotNull(fail);
        Assertions.assertTrue(fail.getValue() >= 1);

        ConnectorMetricsMgr.increaseWriteTotalFail("hive", "out of memory", "insert");

        LongCounterMetric hiveFail = findCounter("hive_write_total",
                "status", "failed", "reason", "oom", "write_type", "insert");
        Assertions.assertNotNull(hiveFail);
        Assertions.assertTrue(hiveFail.getValue() >= 1);
    }

    @Test
    public void testWriteTypeOverwriteAndCtas() {
        ConnectorMetricsMgr.increaseWriteTotalSuccess("iceberg", "overwrite");
        ConnectorMetricsMgr.increaseWriteTotalSuccess("iceberg", "ctas");
        ConnectorMetricsMgr.increaseWriteTotalSuccess("hive", "overwrite");

        Assertions.assertNotNull(findCounter("iceberg_write_total",
                "status", "success", "write_type", "overwrite"));
        Assertions.assertNotNull(findCounter("iceberg_write_total",
                "status", "success", "write_type", "ctas"));
        Assertions.assertNotNull(findCounter("hive_write_total",
                "status", "success", "write_type", "overwrite"));
    }

    // ======================= Delete Metrics =======================

    @Test
    public void testDeleteMetrics() {
        ConnectorMetricsMgr.increaseDeleteTotalSuccess("iceberg", "position");
        ConnectorMetricsMgr.increaseDeleteRows("iceberg", 50L, "position");
        ConnectorMetricsMgr.increaseDeleteBytes("iceberg", 1024L, "position");
        ConnectorMetricsMgr.increaseDeleteDurationMs("iceberg", 200L, "position");

        LongCounterMetric total = findCounter("iceberg_delete_total",
                "status", "success", "reason", "none", "delete_type", "position");
        Assertions.assertNotNull(total);
        Assertions.assertTrue(total.getValue() >= 1);

        LongCounterMetric rows = findCounter("iceberg_delete_rows", "delete_type", "position");
        Assertions.assertNotNull(rows);
        Assertions.assertTrue(rows.getValue() >= 50);
    }

    @Test
    public void testDeleteFailMetrics() {
        ConnectorMetricsMgr.increaseDeleteTotalFail("iceberg", new java.util.concurrent.TimeoutException(), "metadata");

        LongCounterMetric fail = findCounter("iceberg_delete_total",
                "status", "failed", "reason", "timeout", "delete_type", "metadata");
        Assertions.assertNotNull(fail);
        Assertions.assertTrue(fail.getValue() >= 1);
    }

    // ======================= Compaction Metrics =======================

    @Test
    public void testCompactionMetrics() {
        ConnectorMetricsMgr.increaseCompactionTotalSuccess("iceberg", "manual");
        ConnectorMetricsMgr.increaseCompactionDurationMs("iceberg", 120L, "manual");
        ConnectorMetricsMgr.increaseCompactionInputFiles("iceberg", 3L, "manual");
        ConnectorMetricsMgr.increaseCompactionOutputFiles("iceberg", 2L, "manual");
        ConnectorMetricsMgr.increaseCompactionRemovedDeleteFiles("iceberg", 1L, "manual");

        LongCounterMetric total = findCounter("iceberg_compaction_total",
                "compaction_type", "manual", "status", "success", "reason", "none");
        Assertions.assertNotNull(total);
        Assertions.assertTrue(total.getValue() >= 1);

        LongCounterMetric duration = findCounter("iceberg_compaction_duration_ms_total",
                "compaction_type", "manual");
        Assertions.assertNotNull(duration);
        Assertions.assertTrue(duration.getValue() >= 120);

        LongCounterMetric input = findCounter("iceberg_compaction_input_files_total",
                "compaction_type", "manual");
        Assertions.assertNotNull(input);
        Assertions.assertTrue(input.getValue() >= 3);

        LongCounterMetric output = findCounter("iceberg_compaction_output_files_total",
                "compaction_type", "manual");
        Assertions.assertNotNull(output);
        Assertions.assertTrue(output.getValue() >= 2);

        LongCounterMetric removed = findCounter("iceberg_compaction_removed_delete_files_total",
                "compaction_type", "manual");
        Assertions.assertNotNull(removed);
        Assertions.assertTrue(removed.getValue() >= 1);
    }

    @Test
    public void testCompactionFailMetrics() {
        ConnectorMetricsMgr.increaseCompactionTotal("iceberg", "failed", "timeout", "auto");

        LongCounterMetric fail = findCounter("iceberg_compaction_total",
                "compaction_type", "auto", "status", "failed", "reason", "timeout");
        Assertions.assertNotNull(fail);
        Assertions.assertTrue(fail.getValue() >= 1);
    }

    // ======================= Normalization & Classification =======================

    @Test
    public void testNormalization() {
        Assertions.assertEquals("success", ConnectorMetricsMgr.normalizeStatus("SUCCESS"));
        Assertions.assertEquals("failed", ConnectorMetricsMgr.normalizeStatus("FAILED"));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.normalizeStatus(null));
        Assertions.assertEquals("custom", ConnectorMetricsMgr.normalizeStatus("custom"));

        Assertions.assertEquals("unknown", ConnectorMetricsMgr.normalizeReason(null));
        Assertions.assertEquals("oom", ConnectorMetricsMgr.normalizeReason("oom"));

        Assertions.assertEquals("insert", ConnectorMetricsMgr.normalizeWriteType("INSERT"));
        Assertions.assertEquals("overwrite", ConnectorMetricsMgr.normalizeWriteType("OVERWRITE"));
        Assertions.assertEquals("ctas", ConnectorMetricsMgr.normalizeWriteType("CTAS"));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.normalizeWriteType(null));

        Assertions.assertEquals("position", ConnectorMetricsMgr.normalizeDeleteType("POSITION"));
        Assertions.assertEquals("metadata", ConnectorMetricsMgr.normalizeDeleteType("METADATA"));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.normalizeDeleteType(null));

        Assertions.assertEquals("manual", ConnectorMetricsMgr.normalizeCompactionType("MANUAL"));
        Assertions.assertEquals("auto", ConnectorMetricsMgr.normalizeCompactionType("AUTO"));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.normalizeCompactionType(null));
    }

    @Test
    public void testClassifyFailReason() {
        // From error message
        Assertions.assertEquals("timeout", ConnectorMetricsMgr.classifyFailReason("connection timeout"));
        Assertions.assertEquals("timeout", ConnectorMetricsMgr.classifyFailReason("request timed out"));
        Assertions.assertEquals("oom", ConnectorMetricsMgr.classifyFailReason("java.lang.OutOfMemoryError"));
        Assertions.assertEquals("oom", ConnectorMetricsMgr.classifyFailReason("out of memory"));
        Assertions.assertEquals("access_denied", ConnectorMetricsMgr.classifyFailReason("access denied"));
        Assertions.assertEquals("access_denied", ConnectorMetricsMgr.classifyFailReason("permission denied"));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.classifyFailReason("some other error"));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.classifyFailReason((String) null));

        // From throwable
        Assertions.assertEquals("oom", ConnectorMetricsMgr.classifyFailReason(new OutOfMemoryError()));
        Assertions.assertEquals("timeout",
                ConnectorMetricsMgr.classifyFailReason(new java.util.concurrent.TimeoutException()));
        Assertions.assertEquals("unknown",
                ConnectorMetricsMgr.classifyFailReason(new RuntimeException("unknown error")));
        Assertions.assertEquals("unknown", ConnectorMetricsMgr.classifyFailReason((Throwable) null));
    }

    // ======================= Prometheus Exposure =======================

    @Test
    public void testMetricRegistration() {
        // Trigger metric creation for multiple connectors
        ConnectorMetricsMgr.increaseWriteTotalSuccess("iceberg", "insert");
        ConnectorMetricsMgr.increaseWriteTotalSuccess("hive", "insert");
        ConnectorMetricsMgr.increaseDeleteTotalSuccess("iceberg", "position");
        ConnectorMetricsMgr.increaseCompactionTotalSuccess("iceberg", "manual");

        // Verify metrics are registered in MetricRepo
        Assertions.assertFalse(MetricRepo.getMetricsByName("iceberg_write_total").isEmpty(),
                "iceberg_write_total should be registered");
        Assertions.assertFalse(MetricRepo.getMetricsByName("hive_write_total").isEmpty(),
                "hive_write_total should be registered");
        Assertions.assertFalse(MetricRepo.getMetricsByName("iceberg_delete_total").isEmpty(),
                "iceberg_delete_total should be registered");
        Assertions.assertFalse(MetricRepo.getMetricsByName("iceberg_compaction_total").isEmpty(),
                "iceberg_compaction_total should be registered");
    }

    // ======================= Connector Isolation =======================

    @Test
    public void testConnectorIsolation() {
        // Metrics for different connectors should be independent
        long icebergWritesBefore = getCounterValue("iceberg_write_total",
                "status", "success", "reason", "none", "write_type", "insert");
        long hiveWritesBefore = getCounterValue("hive_write_total",
                "status", "success", "reason", "none", "write_type", "insert");

        ConnectorMetricsMgr.increaseWriteTotalSuccess("iceberg", "insert");

        long icebergWritesAfter = getCounterValue("iceberg_write_total",
                "status", "success", "reason", "none", "write_type", "insert");
        long hiveWritesAfter = getCounterValue("hive_write_total",
                "status", "success", "reason", "none", "write_type", "insert");

        Assertions.assertEquals(icebergWritesBefore + 1, icebergWritesAfter);
        Assertions.assertEquals(hiveWritesBefore, hiveWritesAfter);
    }

    // ======================= Helpers =======================

    private long getCounterValue(String name, String... labels) {
        LongCounterMetric counter = findCounter(name, labels);
        return counter == null ? 0 : counter.getValue();
    }

    private LongCounterMetric findCounter(String name, String... labels) {
        List<Metric> metrics = MetricRepo.getMetricsByName(name);
        for (Metric<?> metric : metrics) {
            if (!(metric instanceof LongCounterMetric)) {
                continue;
            }
            Map<String, String> labelMap = metric.getLabels().stream()
                    .collect(Collectors.toMap(MetricLabel::getKey, MetricLabel::getValue));
            boolean match = true;
            for (int i = 0; i < labels.length; i += 2) {
                if (!labels[i + 1].equals(labelMap.get(labels[i]))) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return (LongCounterMetric) metric;
            }
        }
        return null;
    }
}
