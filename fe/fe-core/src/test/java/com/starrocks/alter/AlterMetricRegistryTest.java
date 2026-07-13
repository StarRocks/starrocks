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

package com.starrocks.alter;

import com.starrocks.alter.AlterMetricRegistry.AlterColumnExecutionMode;
import com.starrocks.alter.AlterMetricRegistry.AlterColumnOperationType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.lake.LakeTable;
import com.starrocks.metric.PrometheusMetricVisitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link AlterMetricRegistry}, covering both writers end-to-end.
 *
 * <p>Extends {@link LakeFastSchemaChangeTestBase} so the shared-data lake paths are available and the
 * cluster runs as leader (both metrics are leader-aware and report real values only on the leader).
 * {@code isFastSchemaEvolutionV2()} returns {@code true}, so the inherited tests and
 * {@link #executeAlterAndWaitFinish} exercise the synchronous FSE-v2 path; the asynchronous legacy
 * path is driven manually in {@link #asyncLegacyJobRecordsLegacyFseDuration}.
 *
 * <p>The registry singleton and its series are process-global, so assertions use before/after deltas,
 * resilient to shared state across tests.
 */
public class AlterMetricRegistryTest extends LakeFastSchemaChangeTestBase {

    @Override
    protected boolean isFastSchemaEvolutionV2() {
        return true;
    }

    // ---- counter ----

    @Test
    public void updateAlterColumnCounterBumpsPerType() {
        AlterMetricRegistry registry = AlterMetricRegistry.getInstance();
        for (AlterColumnOperationType type : AlterColumnOperationType.values()) {
            long before = registry.getAlterColumnCount(type);
            registry.updateAlterColumnCounter(type);
            Assertions.assertEquals(before + 1L, registry.getAlterColumnCount(type),
                    "updateAlterColumnCounter must bump " + type + " exactly once");
        }
    }

    @Test
    public void countsAddDropModifyThroughSchemaChangeHandler() throws Exception {
        AlterMetricRegistry registry = AlterMetricRegistry.getInstance();
        createTable(connectContext,
                "CREATE TABLE t_op (c0 INT, v0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 "
                        + "PROPERTIES('cloud_native_fast_schema_evolution_v2'='true');");

        long add0 = registry.getAlterColumnCount(AlterColumnOperationType.ADD);
        alterTable(connectContext, "ALTER TABLE t_op ADD COLUMN c1 BIGINT");
        Assertions.assertEquals(add0 + 1, registry.getAlterColumnCount(AlterColumnOperationType.ADD),
                "ADD COLUMN must bump type=add");

        long drop0 = registry.getAlterColumnCount(AlterColumnOperationType.DROP);
        alterTable(connectContext, "ALTER TABLE t_op DROP COLUMN c1");
        Assertions.assertEquals(drop0 + 1, registry.getAlterColumnCount(AlterColumnOperationType.DROP),
                "DROP COLUMN must bump type=drop");

        // The counter fires during analysis (in SchemaChangeHandler's per-clause loop), independent of the
        // execution path the clause takes. On this shared-data v2 table an INT->BIGINT widening on a value
        // column is fast-schema-evolution-eligible and applies synchronously, leaving the table NORMAL.
        long mod0 = registry.getAlterColumnCount(AlterColumnOperationType.MODIFY);
        alterTable(connectContext, "ALTER TABLE t_op MODIFY COLUMN v0 BIGINT");
        Assertions.assertEquals(mod0 + 1, registry.getAlterColumnCount(AlterColumnOperationType.MODIFY),
                "MODIFY COLUMN must bump type=modify");
    }

    // ---- emission (report -> MetricVisitor) ----

    @Test
    public void reportEmitsCounterAndHistogramSeries() {
        AlterMetricRegistry registry = AlterMetricRegistry.getInstance();
        // Ensure at least one series exists for each label value.
        registry.updateAlterColumnCounter(AlterColumnOperationType.ADD);
        registry.updateAlterColumnCounter(AlterColumnOperationType.DROP);
        registry.updateAlterColumnCounter(AlterColumnOperationType.MODIFY);
        registry.updateAlterColumnDuration(AlterColumnExecutionMode.FAST_SCHEMA_EVOLUTION, 5L);
        registry.updateAlterColumnDuration(AlterColumnExecutionMode.LEGACY_FAST_SCHEMA_EVOLUTION, 7L);

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("starrocks_fe");
        registry.report(visitor);
        String out = visitor.build();

        // Counter: one series per type, emitted by report() via visit(Metric). The counter is leader-aware,
        // so it also carries an is_leader label; assert label-order-independently.
        Assertions.assertTrue(out.contains("starrocks_fe_alter_column_operation_total{"), out);
        Assertions.assertTrue(out.contains("type=\"add\""), out);
        Assertions.assertTrue(out.contains("type=\"drop\""), out);
        Assertions.assertTrue(out.contains("type=\"modify\""), out);
        // The three counter series share a single HELP/TYPE header (deduped by metric name).
        Assertions.assertEquals(1, countOccurrences(out, "# TYPE starrocks_fe_alter_column_operation_total"), out);

        // Histogram: one set of quantile/_sum/_count lines per execution_mode, emitted via visitHistogram().
        Assertions.assertTrue(out.contains("starrocks_fe_alter_column_duration_ms{quantile=\"0.75\""), out);
        Assertions.assertTrue(out.contains("starrocks_fe_alter_column_duration_ms_count{"), out);
        Assertions.assertTrue(out.contains("execution_mode=\"fse\""), out);
        Assertions.assertTrue(out.contains("execution_mode=\"legacy_fse\""), out);
        // The cluster runs as leader; assert is_leader="true" is bound to each family's series (not a single
        // global match that one family could satisfy for both).
        Assertions.assertTrue(anyLineContains(out, "starrocks_fe_alter_column_operation_total{", "is_leader=\"true\""), out);
        Assertions.assertTrue(anyLineContains(out, "starrocks_fe_alter_column_duration_ms", "is_leader=\"true\""), out);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }

    /** True if some line of {@code haystack} contains both {@code a} and {@code b}. */
    private static boolean anyLineContains(String haystack, String a, String b) {
        for (String line : haystack.split("\n")) {
            if (line.contains(a) && line.contains(b)) {
                return true;
            }
        }
        return false;
    }

    // ---- duration histogram ----

    @Test
    public void syncFseRecordsFseDuration() throws Exception {
        AlterMetricRegistry registry = AlterMetricRegistry.getInstance();
        LakeTable table = createTable(connectContext,
                "CREATE TABLE t_sync (c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 "
                        + "PROPERTIES('cloud_native_fast_schema_evolution_v2'='true');");
        long before = registry.getAlterColumnDurationCount(AlterColumnExecutionMode.FAST_SCHEMA_EVOLUTION);
        executeAlterAndWaitFinish(table, "ALTER TABLE t_sync ADD COLUMN c1 BIGINT", true);
        Assertions.assertEquals(before + 1L,
                registry.getAlterColumnDurationCount(AlterColumnExecutionMode.FAST_SCHEMA_EVOLUTION),
                "synchronous FSE add-column must record one fse duration observation");
    }

    @Test
    public void asyncLegacyJobRecordsLegacyFseDuration() throws Exception {
        AlterMetricRegistry registry = AlterMetricRegistry.getInstance();
        LakeTable table = createTable(connectContext,
                "CREATE TABLE t_legacy (c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 "
                        + "PROPERTIES('cloud_native_fast_schema_evolution_v2'='false');");
        long before = registry.getAlterColumnDurationCount(AlterColumnExecutionMode.LEGACY_FAST_SCHEMA_EVOLUTION);
        alterTable(connectContext, "ALTER TABLE t_legacy ADD COLUMN c1 BIGINT");
        runAsyncAlterToFinish(table);
        Assertions.assertEquals(before + 1L,
                registry.getAlterColumnDurationCount(AlterColumnExecutionMode.LEGACY_FAST_SCHEMA_EVOLUTION),
                "async legacy FSE add-column must record one legacy_fse duration observation on FINISHED");
    }

    /** Drive the single unfinished async schema-change job for {@code table} to FINISHED. */
    private void runAsyncAlterToFinish(LakeTable table) throws Exception {
        List<AlterJobV2> jobs = schemaChangeHandler.getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size(), "expected exactly one async schema-change job");
        AlterJobV2 job = jobs.get(0);
        long deadline = System.currentTimeMillis() + 60_000;
        while (job.getJobState() != AlterJobV2.JobState.FINISHED
                || table.getState() != OlapTable.OlapTableState.NORMAL) {
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException("legacy FSE job did not finish; state=" + job.getJobState());
            }
            job.run();
            Thread.sleep(100);
        }
    }
}
