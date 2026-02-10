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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class PipeMetricMgrTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    private static long getNextRandomId() {
        return ThreadLocalRandom.current().nextLong(10000);
    }

    private LongCounterMetric getCounterMetric(String name, long dbId, String... labelKeyValues) {
        List<Metric> metrics = MetricRepo.getMetricsByName(name);
        Optional<Metric> target = metrics.stream().filter(m -> {
            boolean dbMatch = m.getLabels().stream().anyMatch(l -> ((MetricLabel) l).getKey().equals("db_id") &&
                    ((MetricLabel) l).getValue().equals(String.valueOf(dbId)));
            if (!dbMatch) {
                return false;
            }

            for (int i = 0; i < labelKeyValues.length; i += 2) {
                String key = labelKeyValues[i];
                String val = labelKeyValues[i + 1];
                boolean labelMatch = m.getLabels().stream().anyMatch(
                        l -> ((MetricLabel) l).getKey().equals(key) && ((MetricLabel) l).getValue().equals(val));
                if (!labelMatch) {
                    return false;
                }
            }
            return true;
        }).findFirst();
        return target.map(metric -> (LongCounterMetric) metric).orElse(null);
    }


    @Test
    public void testLifecycleMetrics() {
        long dbId = getNextRandomId();

        PipeMetricMgr.incPipeCreation(dbId, "FILE");
        LongCounterMetric creation = getCounterMetric("pipe_creation", dbId, "pipe_type", "FILE");
        Assertions.assertNotNull(creation);
        Assertions.assertEquals(1L, creation.getValue());

        PipeMetricMgr.incPipeCreation(dbId, "FILE");
        Assertions.assertEquals(2L, creation.getValue());

        PipeMetricMgr.incPipeCreation(dbId, "KAFKA");
        LongCounterMetric creationKafka = getCounterMetric("pipe_creation", dbId, "pipe_type", "KAFKA");
        Assertions.assertNotNull(creationKafka);
        Assertions.assertEquals(1L, creationKafka.getValue());

        PipeMetricMgr.incPipeDrop(dbId, "FILE");
        PipeMetricMgr.incPipeAlter(dbId, "FILE");

        Assertions.assertNotNull(getCounterMetric("pipe_drop", dbId, "pipe_type", "FILE"));
        Assertions.assertNotNull(getCounterMetric("pipe_alter", dbId, "pipe_type", "FILE"));
    }

    @Test
    public void testScheduleMetric() {
        long dbId = getNextRandomId();
        PipeMetricMgr.incPipeSchedule(dbId, "FILE");
        LongCounterMetric schedule = getCounterMetric("pipe_schedule_count", dbId, "pipe_type", "FILE");
        Assertions.assertNotNull(schedule);
        Assertions.assertEquals(1L, schedule.getValue());

        PipeMetricMgr.incPipeSchedule(dbId, "FILE");
        Assertions.assertEquals(2L, schedule.getValue());
    }


    @Test
    public void testTaskMetrics() {
        long dbId = getNextRandomId();

        // Complete tasks
        PipeMetricMgr.incPipeCompleteTasks(dbId, "FILE", "SUCCESS", 10);
        List<Metric> completed = MetricRepo.getMetricsByName("pipe_complete_tasks");
        Optional<Metric> m = completed.stream().filter(metric -> metric.getLabels().stream().anyMatch(
                l -> ((MetricLabel) l).getKey().equals("pipe_type") && ((MetricLabel) l).getValue().equals("FILE")) &&
                metric.getLabels().stream().anyMatch(l -> ((MetricLabel) l).getKey().equals("done_status") &&
                        ((MetricLabel) l).getValue().equals("SUCCESS")) &&
                metric.getLabels().stream().anyMatch(l -> ((MetricLabel) l).getKey().equals("db_id") &&
                        ((MetricLabel) l).getValue().equals(String.valueOf(dbId)))).findFirst();

        Assertions.assertTrue(m.isPresent());
        Assertions.assertEquals(10L, m.get().getValue());
    }

    @Test
    public void testDataLoadMetrics() {
        long dbId = getNextRandomId();

        // Loaded stats counters
        PipeMetricMgr.incPipeLoadedFiles(dbId, "FILE", 100);
        LongCounterMetric loadedFiles = getCounterMetric("pipe_loaded_files", dbId, "pipe_type", "FILE");
        Assertions.assertEquals(100L, loadedFiles.getValue());

        PipeMetricMgr.incPipeLoadedBytes(dbId, "FILE", 1024);
        LongCounterMetric loadedBytes = getCounterMetric("pipe_loaded_bytes", dbId, "pipe_type", "FILE");
        Assertions.assertEquals(1024L, loadedBytes.getValue());

        PipeMetricMgr.incPipeLoadedRows(dbId, "FILE", 500);
        LongCounterMetric loadedRows = getCounterMetric("pipe_loaded_rows", dbId, "pipe_type", "FILE");
        Assertions.assertEquals(500L, loadedRows.getValue());
    }

    @Test
    public void testCompleteTasksPartitioning() {
        long dbId1 = getNextRandomId();
        long dbId2 = getNextRandomId();

        PipeMetricMgr.incPipeCompleteTasks(dbId1, "FILE", "SUCCESS", 10);
        PipeMetricMgr.incPipeCompleteTasks(dbId2, "FILE", "SUCCESS", 20);

        Assertions.assertNotNull(getCounterMetric("pipe_complete_tasks", dbId1,
                "pipe_type", "FILE", "done_status", "SUCCESS"));
        Assertions.assertEquals(10L, getCounterMetric("pipe_complete_tasks", dbId1,
                "pipe_type", "FILE", "done_status", "SUCCESS").getValue());

        Assertions.assertNotNull(getCounterMetric("pipe_complete_tasks", dbId2,
                "pipe_type", "FILE", "done_status", "SUCCESS"));
        Assertions.assertEquals(20L, getCounterMetric("pipe_complete_tasks", dbId2,
                "pipe_type", "FILE", "done_status", "SUCCESS").getValue());
    }

    @Test
    public void testMetricExpiration() throws InterruptedException {
        long dbId = getNextRandomId();
        int originalExpireMinutes = Config.pipe_metric_expire_minutes;

        try {
            // Setup Metrics
            PipeMetricMgr.incPipeCreation(dbId, "FILE"); // Counter
            LongCounterMetric creation = getCounterMetric("pipe_creation", dbId, "pipe_type", "FILE");
            Assertions.assertNotNull(creation);

            // Set Expiration to 0 (Immediate expiration)
            Config.pipe_metric_expire_minutes = 0;
            Thread.sleep(10); // Ensure time advances past lastAccessTime

            // Trigger Cleanup
            PipeMetricMgr.cleanupExpiredMetrics();

            // Counter should be expired and removed because we don't protect counters (lifecycle metrics usually accumulate,
            // but the test logic in PipeMetricMgr removes them if expired)
            // Wait: Lifecycle metrics are usually kept?
            // "Lifecycle metrics (creation, drop, alter) are kept" comment in removeMetrics(dbId, type).
            // But cleanupExpiredMetrics iterates METRIC_MAP and removes anything expired.
            // Let's verify the implementation of cleanupExpiredMetrics:
            // It iterates METRIC_MAP and removes if expired. It does NOT check isLifecycleGroup.
            // So they should be gone.
            Assertions.assertNull(getCounterMetric("pipe_creation", dbId, "pipe_type", "FILE"));
        } finally {
            Config.pipe_metric_expire_minutes = originalExpireMinutes;
        }
    }

    @Test
    public void testRefreshPipeStateGaugesNoPipes() {
        // When there are no pipes (or PipeManager is not available), refreshPipeStateGauges should not throw
        // This tests the error handling path
        PipeMetricMgr.refreshPipeStateGauges();
        // Should complete without exception
    }

    private GaugeMetricImpl<Long> getGaugeMetric(String name, long dbId, String... labelKeyValues) {
        List<Metric> metrics = MetricRepo.getMetricsByName(name);
        Optional<Metric> target = metrics.stream().filter(m -> {
            boolean dbMatch = m.getLabels().stream().anyMatch(l -> ((MetricLabel) l).getKey().equals("db_id") &&
                    ((MetricLabel) l).getValue().equals(String.valueOf(dbId)));
            if (!dbMatch) {
                return false;
            }

            for (int i = 0; i < labelKeyValues.length; i += 2) {
                String key = labelKeyValues[i];
                String val = labelKeyValues[i + 1];
                boolean labelMatch = m.getLabels().stream().anyMatch(
                        l -> ((MetricLabel) l).getKey().equals(key) && ((MetricLabel) l).getValue().equals(val));
                if (!labelMatch) {
                    return false;
                }
            }
            return true;
        }).findFirst();
        return target.map(metric -> (GaugeMetricImpl<Long>) metric).orElse(null);
    }
}
