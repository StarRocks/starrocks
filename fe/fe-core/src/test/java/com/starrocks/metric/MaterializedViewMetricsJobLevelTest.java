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
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

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
            TaskRun taskRun1 = buildMVTaskRun(mv, "test");
            initAndExecuteTaskRun(taskRun1);

            MVPCTRefreshProcessor processor1 = getPartitionBasedRefreshProcessor(taskRun1);
            TaskRun taskRun2 = processor1.getNextTaskRun();

            Assertions.assertNotNull(taskRun2,
                    "expected a second batch task run for 2-partition MV with partition_refresh_number=1");

            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
            MaterializedViewMetricsEntity metrics = (MaterializedViewMetricsEntity) iEntity;

            Assertions.assertEquals(0, metrics.counterRefreshJobTotal.getValue(),
                    "job counter must not be bumped after an intermediate batch");
            Assertions.assertEquals(0, metrics.histRefreshJobDuration.getCount(),
                    "duration must not be sampled after an intermediate batch");

            // Mirror production: the scheduler always adds batch N to history before dispatching batch N+1.
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunHistory().addHistory(taskRun1.getStatus());

            // Regression: Database.getFullName() is unqualified while a stored TaskRunStatus keeps the
            // "default_cluster:" prefix; matchByTaskName compares against the stripped getDbName(), so the
            // roll-up must still find earlier in-memory batches (not only archived ones) via the plain db name.
            String mvTaskName = TaskBuilder.getMvTaskName(mv.getId());
            boolean foundEarlierBatch = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunHistory()
                    .lookupLastJobOfTasks(DB_NAME, Collections.singleton(mvTaskName))
                    .stream().anyMatch(s -> taskRun1.getStatus().getQueryId().equals(s.getQueryId()));
            Assertions.assertTrue(foundEarlierBatch,
                    "duration roll-up must find the earlier in-memory batch via the unqualified db name");

            // Batch 2: terminal run (no more partitions left).
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
        } finally {
            starRocksAssert.dropMaterializedView("mjl_multi_mv1");
            starRocksAssert.dropTable("mjl_multi_t1");
        }
    }
}
