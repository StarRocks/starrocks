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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies MV refresh metrics are counted once per refresh job (on the terminal task run), not once per task run.
 */
public class MaterializedViewMetricsJobLevelTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "depts");
    }

    /**
     * Single-run refresh: the one run IS the terminal run, so counters must still equal 1.
     * This guards against regressions where the terminal-run gate accidentally suppresses
     * single-batch refreshes.
     */
    @Test
    public void testSingleBatchRefreshCountsOnce() throws Exception {
        String partitionTable = "CREATE TABLE mjl_single_t1 (dt date, v int)\n" +
                "PARTITION BY date_trunc('day', dt)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("mjl_single_t1", "p1", "2024-01-01", "2024-01-02");

        String mvSql = "CREATE MATERIALIZED VIEW mjl_single_mv1 " +
                "PARTITION BY date_trunc('day', dt) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"partition_refresh_number\"=\"-1\") " +
                "AS SELECT dt, sum(v) FROM mjl_single_t1 GROUP BY dt";
        starRocksAssert.withMaterializedView(mvSql);

        // Insert data after MV creation so the base table has a newer version than the MV's snapshot.
        executeInsertSql("insert into mjl_single_t1 partition(p1) values('2024-01-01', 1)");

        try {
            MaterializedView mv = getMv("test", "mjl_single_mv1");

            TaskRun taskRun = buildMVTaskRun(mv, "test");
            initAndExecuteTaskRun(taskRun);

            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
            MaterializedViewMetricsEntity metrics = (MaterializedViewMetricsEntity) iEntity;

            Assertions.assertEquals(1, metrics.counterRefreshJobTotal.getValue(),
                    "single-batch job should count as 1 job");
            Assertions.assertEquals(1, metrics.counterRefreshJobSuccessTotal.getValue(),
                    "single-batch job should count as 1 success");
            Assertions.assertEquals(1, metrics.histRefreshJobDuration.getCount(),
                    "single-batch job should record exactly 1 duration sample");
        } finally {
            starRocksAssert.dropMaterializedView("mjl_single_mv1");
            starRocksAssert.dropTable("mjl_single_t1");
        }
    }

    /**
     * A second refresh on an already-up-to-date MV is SKIPPED: it is counted once as a job (in the
     * empty bucket), not as a success or failure.
     */
    @Test
    public void skippedRefreshCountedOncePerJob() throws Exception {
        String mvSql = "CREATE MATERIALIZED VIEW mjl_skipped_mv1 " +
                "REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts WHERE deptno > 10";
        starRocksAssert.withMaterializedView(mvSql);

        try {
            MaterializedView mv = getMv("test", "mjl_skipped_mv1");

            refreshMaterializedView(DB_NAME, "mjl_skipped_mv1");

            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
            MaterializedViewMetricsEntity entity = (MaterializedViewMetricsEntity) iEntity;

            assertEquals(1L, entity.counterRefreshJobTotal.getValue());
            assertEquals(1L, entity.counterRefreshJobSuccessTotal.getValue());
            assertEquals(0L, entity.counterRefreshJobFailedTotal.getValue());
            assertEquals(0L, entity.counterRefreshJobEmptyTotal.getValue());

            // Second refresh: base table unchanged → SKIPPED (counts once as an empty job).
            refreshMaterializedView(DB_NAME, "mjl_skipped_mv1");

            assertEquals(2L, entity.counterRefreshJobTotal.getValue());
            assertEquals(1L, entity.counterRefreshJobSuccessTotal.getValue());
            assertEquals(0L, entity.counterRefreshJobFailedTotal.getValue());
            assertEquals(1L, entity.counterRefreshJobEmptyTotal.getValue());
        } finally {
            starRocksAssert.dropMaterializedView("mjl_skipped_mv1");
        }
    }

    /**
     * Multi-batch refresh (partition_refresh_number=1 with 2 stale partitions):
     * - The first task run calls generateNextTaskRunIfNeeded() which returns true (spawned a successor).
     *   => it is NOT terminal; counters must NOT be bumped.
     * - The second task run has no successor => it IS terminal; counters must be bumped exactly once.
     *
     * So after the full job: total=1, success=1, duration-samples=1 (not 2).
     */
    @Test
    public void testMultiBatchRefreshCountsOnce() throws Exception {
        String partitionTable = "CREATE TABLE mjl_multi_t1 (dt date, v int)\n" +
                "PARTITION BY date_trunc('day', dt)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("mjl_multi_t1", "p1", "2024-02-01", "2024-02-02");
        addRangePartition("mjl_multi_t1", "p2", "2024-02-02", "2024-02-03");

        String mvSql = "CREATE MATERIALIZED VIEW mjl_multi_mv1 " +
                "PARTITION BY date_trunc('day', dt) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"partition_refresh_number\"=\"1\") " +
                "AS SELECT dt, sum(v) FROM mjl_multi_t1 GROUP BY dt";
        starRocksAssert.withMaterializedView(mvSql);

        // Insert into both partitions AFTER MV creation so both are stale and need refreshing.
        executeInsertSql("insert into mjl_multi_t1 partition(p1) values('2024-02-01', 1)");
        executeInsertSql("insert into mjl_multi_t1 partition(p2) values('2024-02-02', 2)");

        try {
            MaterializedView mv = getMv("test", "mjl_multi_mv1");

            // Batch 1: IS_TEST=true so the spawned next-run is stored in nextTaskRun, not submitted.
            // Pin processStartTime so the terminal duration can be shown to use the first run's start
            // without looking up task-run history.
            TaskRun taskRun1 = buildMVTaskRun(mv, "test");
            taskRun1.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            long firstProcessStart = System.currentTimeMillis() - 30_000L;
            taskRun1.getStatus().setProcessStartTime(firstProcessStart);
            taskRun1.executeTaskRun();

            MVPCTRefreshProcessor processor1 = getPartitionBasedRefreshProcessor(taskRun1);
            TaskRun taskRun2 = processor1.getNextTaskRun();

            Assertions.assertNotNull(taskRun2,
                    "expected a second batch task run for 2-partition MV with partition_refresh_number=1");
            Assertions.assertEquals(String.valueOf(firstProcessStart),
                    taskRun2.getProperties().get(TaskRun.MV_REFRESH_JOB_PROCESS_START_TIME),
                    "the successor run must carry the first run's process start");

            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
            MaterializedViewMetricsEntity metrics = (MaterializedViewMetricsEntity) iEntity;

            Assertions.assertEquals(0, metrics.counterRefreshJobTotal.getValue(),
                    "job counter must not be bumped after an intermediate batch");
            Assertions.assertEquals(0, metrics.histRefreshJobDuration.getCount(),
                    "duration must not be sampled after an intermediate batch");

            // Batch 2: terminal run (no more partitions left). Do not add batch 1 to history:
            // duration must come from the propagated property, not lookupLastJobOfTasks.
            initAndExecuteTaskRun(taskRun2);

            MVPCTRefreshProcessor processor2 = getPartitionBasedRefreshProcessor(taskRun2);
            Assertions.assertNull(processor2.getNextTaskRun(),
                    "no third batch expected");

            Assertions.assertEquals(1, metrics.counterRefreshJobTotal.getValue(),
                    "multi-batch job must be counted exactly once");
            Assertions.assertEquals(1, metrics.counterRefreshJobSuccessTotal.getValue(),
                    "multi-batch job must record exactly one success");
            Assertions.assertEquals(1, metrics.histRefreshJobDuration.getCount(),
                    "multi-batch job must record exactly one duration sample");
            Assertions.assertTrue(metrics.histRefreshJobDuration.getSnapshot().getMax() >= 25_000L,
                    "duration must use the first run's process start, not only the terminal run");
        } finally {
            starRocksAssert.dropMaterializedView("mjl_multi_mv1");
            starRocksAssert.dropTable("mjl_multi_t1");
        }
    }

    /**
     * A successful batch whose pending next batch cannot be enqueued (rejected submit / queue full)
     * leaves the refresh incomplete. The run must be failed so the metric agrees with task history and
     * materialized_view_refresh_jobs, which would otherwise record this terminal run as a success.
     */
    @Test
    public void incompleteRefreshWhenSuccessorRejectedCountsAsFailed() throws Exception {
        String partitionTable = "CREATE TABLE mjl_reject_t1 (dt date, v int)\n" +
                "PARTITION BY date_trunc('day', dt)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("mjl_reject_t1", "p1", "2024-03-01", "2024-03-02");
        addRangePartition("mjl_reject_t1", "p2", "2024-03-02", "2024-03-03");

        String mvSql = "CREATE MATERIALIZED VIEW mjl_reject_mv1 " +
                "PARTITION BY date_trunc('day', dt) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"partition_refresh_number\"=\"1\") " +
                "AS SELECT dt, sum(v) FROM mjl_reject_t1 GROUP BY dt";
        starRocksAssert.withMaterializedView(mvSql);

        executeInsertSql("insert into mjl_reject_t1 partition(p1) values('2024-03-01', 1)");
        executeInsertSql("insert into mjl_reject_t1 partition(p2) values('2024-03-02', 2)");

        // Simulate a rejected successor submit: the first batch succeeds but cannot enqueue its next
        // batch, while hasNextBatchRun() stays genuinely true (p2 is still pending).
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public boolean generateNextTaskRunIfNeeded() {
                return false;
            }
        };

        try {
            MaterializedView mv = getMv("test", "mjl_reject_mv1");
            TaskRun taskRun = buildMVTaskRun(mv, "test");

            Assertions.assertThrows(Exception.class, () -> initAndExecuteTaskRun(taskRun),
                    "an incomplete refresh (successor rejected) must fail the run");

            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
            MaterializedViewMetricsEntity metrics = (MaterializedViewMetricsEntity) iEntity;

            Assertions.assertEquals(1, metrics.counterRefreshJobTotal.getValue(),
                    "incomplete refresh must be counted exactly once");
            Assertions.assertEquals(1, metrics.counterRefreshJobFailedTotal.getValue(),
                    "incomplete refresh must count as a failed job");
            Assertions.assertEquals(0, metrics.counterRefreshJobSuccessTotal.getValue(),
                    "incomplete refresh must not count as a success");
            Assertions.assertEquals(1, metrics.histRefreshJobDuration.getCount(),
                    "incomplete refresh must record exactly one duration sample");
        } finally {
            starRocksAssert.dropMaterializedView("mjl_reject_mv1");
            starRocksAssert.dropTable("mjl_reject_t1");
        }
    }

    @Test
    public void zeroJobProcessStartTimeSkipsDurationMetric() throws Exception {
        assertInvalidJobProcessStartTimeSkipsDuration("mjl_zero_t1", "mjl_zero_mv1", "0");
    }

    @Test
    public void nonNumericJobProcessStartTimeSkipsDurationMetric() throws Exception {
        assertInvalidJobProcessStartTimeSkipsDuration("mjl_nan_t1", "mjl_nan_mv1", "not-a-number");
    }

    private void assertInvalidJobProcessStartTimeSkipsDuration(String table, String mvName, String raw)
            throws Exception {
        String partitionTable = "CREATE TABLE " + table + " (dt date, v int)\n" +
                "PARTITION BY date_trunc('day', dt)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition(table, "p1", "2024-04-01", "2024-04-02");

        String mvSql = "CREATE MATERIALIZED VIEW " + mvName + " " +
                "PARTITION BY date_trunc('day', dt) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"partition_refresh_number\"=\"-1\") " +
                "AS SELECT dt, sum(v) FROM " + table + " GROUP BY dt";
        starRocksAssert.withMaterializedView(mvSql);
        executeInsertSql("insert into " + table + " partition(p1) values('2024-04-01', 1)");

        try {
            MaterializedView mv = getMv("test", mvName);
            TaskRun taskRun = buildMVTaskRun(mv, "test");
            taskRun.getProperties().put(TaskRun.MV_REFRESH_JOB_PROCESS_START_TIME, raw);
            initAndExecuteTaskRun(taskRun);

            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
            MaterializedViewMetricsEntity metrics = (MaterializedViewMetricsEntity) iEntity;

            Assertions.assertEquals(1, metrics.counterRefreshJobSuccessTotal.getValue(),
                    "invalid duration property must not skip the job-success counter");
            Assertions.assertEquals(0, metrics.histRefreshJobDuration.getCount(),
                    "invalid " + TaskRun.MV_REFRESH_JOB_PROCESS_START_TIME + " must skip the duration sample");
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
            starRocksAssert.dropTable(table);
        }
    }
}
