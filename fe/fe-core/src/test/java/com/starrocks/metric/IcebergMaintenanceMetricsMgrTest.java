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
import com.starrocks.connector.iceberg.IcebergMaintenanceTaskRecord;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;
import com.starrocks.utframe.StarRocksTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class IcebergMaintenanceMetricsMgrTest extends StarRocksTestBase {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testRecordCheck() {
        IcebergMaintenanceMetricsMgr.recordCheck("cat_check", 123L);
        IcebergMaintenanceMetricsMgr.recordCheck("cat_check", 7L);

        LongCounterMetric total = findCounter("iceberg_amm_check_total", "catalog", "cat_check");
        Assertions.assertNotNull(total);
        Assertions.assertEquals(2L, total.getValue());

        LongCounterMetric duration = findCounter("iceberg_amm_check_duration_ms_total", "catalog", "cat_check");
        Assertions.assertNotNull(duration);
        Assertions.assertEquals(130L, duration.getValue());
    }

    @Test
    public void testRecordExecute() {
        IcebergMaintenanceMetricsMgr.recordExecute("cat_exec",
                IcebergMaintenanceMetricsMgr.ACTION_EXPIRE_SNAPSHOTS, IcebergMaintenanceTaskRecord.STATUS_SUCCESS, 100L);
        IcebergMaintenanceMetricsMgr.recordExecute("cat_exec",
                IcebergMaintenanceMetricsMgr.ACTION_EXPIRE_SNAPSHOTS, IcebergMaintenanceTaskRecord.STATUS_FAILED, 50L);
        IcebergMaintenanceMetricsMgr.recordExecute("cat_exec",
                IcebergMaintenanceMetricsMgr.ACTION_EXPIRE_SNAPSHOTS, IcebergMaintenanceTaskRecord.STATUS_SKIPPED, 5L);

        LongCounterMetric success = findCounter("iceberg_amm_execute_total",
                "catalog", "cat_exec", "action", "expire_snapshots", "status", "success");
        Assertions.assertNotNull(success);
        Assertions.assertEquals(1L, success.getValue());

        LongCounterMetric failed = findCounter("iceberg_amm_execute_total",
                "catalog", "cat_exec", "action", "expire_snapshots", "status", "failed");
        Assertions.assertNotNull(failed);
        Assertions.assertEquals(1L, failed.getValue());

        // skipped (ran but nothing to do) is counted separately, not folded into success
        LongCounterMetric skipped = findCounter("iceberg_amm_execute_total",
                "catalog", "cat_exec", "action", "expire_snapshots", "status", "skipped");
        Assertions.assertNotNull(skipped);
        Assertions.assertEquals(1L, skipped.getValue());

        LongCounterMetric duration = findCounter("iceberg_amm_execute_duration_ms_total",
                "catalog", "cat_exec", "action", "expire_snapshots");
        Assertions.assertNotNull(duration);
        Assertions.assertEquals(155L, duration.getValue());
    }

    @Test
    public void testReportExpireSnapshotsEffect() throws Exception {
        // committed run that actually expired snapshots (output < input): material change,
        // so both the input and output counters are reported
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(10L);
        setLongField(stats, "snapshotCountOutput", 3L);
        stats.setExecuted(true);
        stats.setCommitted(true);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_expire", stats);

        LongCounterMetric input = findCounter("iceberg_amm_snapshot_count_input", "catalog", "cat_expire");
        Assertions.assertNotNull(input);
        Assertions.assertEquals(10L, input.getValue());
        LongCounterMetric output = findCounter("iceberg_amm_snapshot_count_output", "catalog", "cat_expire");
        Assertions.assertNotNull(output);
        Assertions.assertEquals(3L, output.getValue());
    }

    @Test
    public void testReportExpireSnapshotsSkippedReportsNothing() throws Exception {
        // committed but nothing was expired (output == input): no material change, so the
        // effect counters must not move — skip visibility lives in execute_total{status=skipped}
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(10L);
        setLongField(stats, "snapshotCountOutput", 10L);
        stats.setExecuted(true);
        stats.setCommitted(true);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_expire_skip", stats);

        Assertions.assertNull(findCounter("iceberg_amm_snapshot_count_input", "catalog", "cat_expire_skip"));
        Assertions.assertNull(findCounter("iceberg_amm_snapshot_count_output", "catalog", "cat_expire_skip"));
    }

    @Test
    public void testReportRemoveOrphanFilesSkippedReportsNothing() {
        // orphan scan that removed nothing: no material change, so no counter is even created
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REMOVE_ORPHAN_FILES);
        stats.addOrphanDetected(5);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_orphan_skip", stats);

        Assertions.assertNull(findCounter("iceberg_amm_orphan_file_removed_total", "catalog", "cat_orphan_skip"));
        Assertions.assertNull(findCounter("iceberg_amm_orphan_bytes_removed_total", "catalog", "cat_orphan_skip"));
    }

    @Test
    public void testReportExpireSnapshotsNotExecuted() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(10L);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_expire_noop", stats);
        Assertions.assertNull(findCounter("iceberg_amm_snapshot_count_input", "catalog", "cat_expire_noop"));
    }

    @Test
    public void testReportEffectNotCommittedIsSkipped() {
        // the procedure staged work into the transaction (executed=true) but the
        // commitTransaction() failed (committed=false): nothing was published, so
        // expire/rewrite effect metrics must not be reported
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(10L);
        stats.setExecuted(true);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_uncommitted", stats);
        Assertions.assertNull(findCounter("iceberg_amm_snapshot_count_input", "catalog", "cat_uncommitted"));

        IcebergMaintenanceTaskStats rewriteStats = new IcebergMaintenanceTaskStats();
        rewriteStats.setOperation(IcebergTableOperation.REWRITE_MANIFESTS);
        rewriteStats.setManifestCountInput(8L);
        rewriteStats.setExecuted(true);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_uncommitted", rewriteStats);
        Assertions.assertNull(findCounter("iceberg_amm_manifest_file_count_input", "catalog", "cat_uncommitted"));
    }

    @Test
    public void testReportRewriteManifestsEffect() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REWRITE_MANIFESTS);
        stats.setManifestCountInput(8L);
        stats.setManifestBytesInput(1024L);
        stats.setExecuted(true);
        stats.setCommitted(true);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_rewrite", stats);

        LongCounterMetric input = findCounter("iceberg_amm_manifest_file_count_input", "catalog", "cat_rewrite");
        Assertions.assertNotNull(input);
        Assertions.assertEquals(8L, input.getValue());
    }

    @Test
    public void testReportRemoveOrphanFilesEffectEvenWhenPartial() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REMOVE_ORPHAN_FILES);
        stats.addOrphanDetected(5);
        stats.addOrphanRemoved(3, 300L);
        stats.setPartiallyApplied(true);
        // executed stays false: the run failed mid-way, but deletions already happened
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_orphan", stats);

        LongCounterMetric files = findCounter("iceberg_amm_orphan_file_removed_total", "catalog", "cat_orphan");
        Assertions.assertNotNull(files);
        Assertions.assertEquals(3L, files.getValue());

        LongCounterMetric bytes = findCounter("iceberg_amm_orphan_bytes_removed_total", "catalog", "cat_orphan");
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(300L, bytes.getValue());
    }

    @Test
    public void testReportEffectMetricsNullSafe() {
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_null", null);
        IcebergMaintenanceMetricsMgr.reportEffectMetrics("cat_null", new IcebergMaintenanceTaskStats());
        Assertions.assertNull(findCounter("iceberg_amm_orphan_file_removed_total", "catalog", "cat_null"));
    }

    @Test
    public void testPlanningLatencyHistogramRecorded() {
        HistogramMetric histogram = MetricRepo.getOrCreateIcebergPlanningLatencyHistogram("cat_plan");
        long before = histogram.getCount();

        IcebergMaintenanceMetricsMgr.recordPlanningLatencyMs("cat_plan", 100L);
        IcebergMaintenanceMetricsMgr.recordPlanningLatencyMs("cat_plan", 20L);

        Assertions.assertEquals(before + 2, histogram.getCount());
        // the histogram captures the samples (mean lies between the two values)
        double mean = histogram.getSnapshot().getMean();
        Assertions.assertTrue(mean >= 20.0 && mean <= 100.0, "mean was " + mean);
        // it is the same per-catalog instance, registered under the iceberg planning label
        Assertions.assertEquals("catalog=\"cat_plan\"", histogram.getTagName());
    }

    @Test
    public void testPlanningLatencyConcurrent() throws Exception {
        HistogramMetric histogram = MetricRepo.getOrCreateIcebergPlanningLatencyHistogram("cat_plan_conc");
        long before = histogram.getCount();
        int threads = 8;
        int perThread = 1000;
        CountDownLatch latch = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        IcebergMaintenanceMetricsMgr.recordPlanningLatencyMs("cat_plan_conc", 50L);
                    }
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        Assertions.assertEquals(before + (long) threads * perThread, histogram.getCount());
    }

    // output-side stats fields have no setter (only IcebergMaintenanceTaskStats.collectOutputs
    // fills them from a refreshed table); set them directly to build a material-change scenario
    private static void setLongField(IcebergMaintenanceTaskStats stats, String name, long value) throws Exception {
        Field field = IcebergMaintenanceTaskStats.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setLong(stats, value);
    }

    private LongCounterMetric findCounter(String name, String... labels) {
        Metric<?> metric = findMetric(name, labels);
        return metric instanceof LongCounterMetric ? (LongCounterMetric) metric : null;
    }

    private Metric<?> findMetric(String name, String... labels) {
        List<Metric> metrics = MetricRepo.getMetricsByName(name);
        for (Metric<?> metric : metrics) {
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
                return metric;
            }
        }
        return null;
    }
}
