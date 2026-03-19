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

public class IcebergMetricsMgrTest extends StarRocksTestBase {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testCompactionTotalMetrics() {
        LongCounterMetric successBefore = findCounter("iceberg_compaction_total",
                "compaction_type", "manual", "status", "success", "reason", "none");
        long successBase = successBefore == null ? 0 : successBefore.getValue();

        IcebergMetricsMgr.increaseIcebergCompactionTotalSuccess();

        LongCounterMetric successAfter = findCounter("iceberg_compaction_total",
                "compaction_type", "manual", "status", "success", "reason", "none");
        Assertions.assertNotNull(successAfter);
        Assertions.assertEquals(successBase + 1, successAfter.getValue());

        LongCounterMetric failBefore = findCounter("iceberg_compaction_total",
                "compaction_type", "auto", "status", "failed", "reason", "timeout");
        long failBase = failBefore == null ? 0 : failBefore.getValue();

        IcebergMetricsMgr.increaseIcebergCompactionTotal("failed", "timeout", "auto");

        LongCounterMetric failAfter = findCounter("iceberg_compaction_total",
                "compaction_type", "auto", "status", "failed", "reason", "timeout");
        Assertions.assertNotNull(failAfter);
        Assertions.assertEquals(failBase + 1, failAfter.getValue());
    }

    @Test
    public void testCompactionFileAndDurationMetrics() {
        LongCounterMetric durationBefore = findCounter("iceberg_compaction_duration_ms_total",
                "compaction_type", "manual");
        long durationBase = durationBefore == null ? 0 : durationBefore.getValue();

        LongCounterMetric inputBefore = findCounter("iceberg_compaction_input_files_total",
                "compaction_type", "manual");
        long inputBase = inputBefore == null ? 0 : inputBefore.getValue();

        LongCounterMetric outputBefore = findCounter("iceberg_compaction_output_files_total",
                "compaction_type", "manual");
        long outputBase = outputBefore == null ? 0 : outputBefore.getValue();

        LongCounterMetric removedBefore = findCounter("iceberg_compaction_removed_delete_files_total",
                "compaction_type", "manual");
        long removedBase = removedBefore == null ? 0 : removedBefore.getValue();

        IcebergMetricsMgr.increaseIcebergCompactionDurationMs(120L, "manual");
        IcebergMetricsMgr.increaseIcebergCompactionInputFiles(3L, "manual");
        IcebergMetricsMgr.increaseIcebergCompactionOutputFiles(2L, "manual");
        IcebergMetricsMgr.increaseIcebergCompactionRemovedDeleteFiles(1L, "manual");

        LongCounterMetric durationAfter = findCounter("iceberg_compaction_duration_ms_total",
                "compaction_type", "manual");
        LongCounterMetric inputAfter = findCounter("iceberg_compaction_input_files_total",
                "compaction_type", "manual");
        LongCounterMetric outputAfter = findCounter("iceberg_compaction_output_files_total",
                "compaction_type", "manual");
        LongCounterMetric removedAfter = findCounter("iceberg_compaction_removed_delete_files_total",
                "compaction_type", "manual");

        Assertions.assertNotNull(durationAfter);
        Assertions.assertEquals(durationBase + 120L, durationAfter.getValue());

        Assertions.assertNotNull(inputAfter);
        Assertions.assertEquals(inputBase + 3L, inputAfter.getValue());

        Assertions.assertNotNull(outputAfter);
        Assertions.assertEquals(outputBase + 2L, outputAfter.getValue());

        Assertions.assertNotNull(removedAfter);
        Assertions.assertEquals(removedBase + 1L, removedAfter.getValue());
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
                String key = labels[i];
                String value = labels[i + 1];
                if (!value.equals(labelMap.get(key))) {
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
