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

import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.scheduler.Constants;
import com.starrocks.task.AgentBatchTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

/**
 * In-place leader demotion keeps AlterJobV2 objects alive in memory, unlike a restart which
 * reloads them from the image/journal. resetToLastDurableState() must therefore map the
 * deliberately-unlogged in-memory states (e.g. WAITING_TXN -> RUNNING) back to the last durable
 * state and drop leader-session transients (batch tasks, futures), so a re-elected leader
 * resumes exactly like a restarted FE instead of waiting on stale dispatch state.
 */
public class LeaderHandoffResetTest {

    @Test
    public void testSchemaChangeRunningMapsBackAndBatchIsFresh() {
        SchemaChangeJobV2 job = new SchemaChangeJobV2(1L, 2L, 3L, "tbl", 100000L);
        job.setJobState(JobState.RUNNING);
        AgentBatchTask staleBatch = job.schemaChangeBatchTask;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState(),
                "unlogged RUNNING must map back to its durable predecessor");
        Assertions.assertNotSame(staleBatch, job.schemaChangeBatchTask,
                "batch must be replaced: runWaitingTxnJob appends, a stale batch double-adds");
        Assertions.assertEquals(0, job.schemaChangeBatchTask.getTaskNum());
    }

    @Test
    public void testSchemaChangePendingDiscardsUnloggedWatershed() {
        SchemaChangeJobV2 job = new SchemaChangeJobV2(1L, 2L, 3L, "tbl", 100000L);
        // Simulate a fenced PENDING -> WAITING_TXN attempt: watershed allocated in memory but
        // the journal write never committed, so the durable image still has none.
        job.watershedTxnId = 42L;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.PENDING, job.getJobState());
        Assertions.assertEquals(-1L, job.watershedTxnId,
                "an unlogged watershed on a durable-PENDING job must be discarded");
    }

    @Test
    public void testRollupRunningMapsBackAndBatchIsFresh() {
        RollupJobV2 job = new RollupJobV2();
        job.setJobState(JobState.RUNNING);
        AgentBatchTask staleBatch = job.rollupBatchTask;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        Assertions.assertNotSame(staleBatch, job.rollupBatchTask);
    }

    @Test
    public void testOptimizeRunningRestoresDurableSnapshot() {
        OptimizeJobV2 job = new OptimizeJobV2(1L, 2L, 3L, "tbl", 100000L);
        job.setJobState(JobState.RUNNING);
        job.rewriteTasks.add(null);

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        Assertions.assertTrue(job.rewriteTasks.isEmpty(),
                "rewriteTasks must be restored to the durable WAITING_TXN snapshot (empty)");
    }

    @Test
    public void testOptimizePendingUndoesPartialTmpPartitionAppend() {
        OptimizeJobV2 job = new OptimizeJobV2(1L, 2L, 3L, "tbl", 100000L);
        // Simulate a fenced runPendingJob: tmp partition ids appended in memory while the
        // durable PENDING image has none - re-running with the stale list would pair 2N tmp
        // partitions against N sources.
        job.getTmpPartitionIds().add(101L);

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.PENDING, job.getJobState());
        Assertions.assertTrue(job.getTmpPartitionIds().isEmpty());
    }

    @Test
    public void testOnlineOptimizeCancelsStaleInsertFuture() {
        OnlineOptimizeJobV2 job = new OnlineOptimizeJobV2(1L, 2L, 3L, "tbl", 100000L);
        job.setJobState(JobState.RUNNING);
        CompletableFuture<Constants.TaskRunState> staleFuture = new CompletableFuture<>();
        job.future = staleFuture;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        Assertions.assertNull(job.future,
                "a stale INSERT future would be consumed by a NEW task whose INSERT never ran");
        Assertions.assertTrue(staleFuture.isCancelled());
    }

    @Test
    public void testLakeSchemaChangeRunningMapsBackAndBatchIsFresh() {
        LakeTableSchemaChangeJob job = new LakeTableSchemaChangeJob();
        job.setJobState(JobState.RUNNING);
        AgentBatchTask staleBatch = job.schemaChangeBatchTask;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        Assertions.assertNotSame(staleBatch, job.schemaChangeBatchTask);
    }

    @Test
    public void testLakeRollupRunningMapsBackAndBatchIsFresh() {
        LakeRollupJob job = new LakeRollupJob();
        job.setJobState(JobState.RUNNING);
        AgentBatchTask staleBatch = job.rollupBatchTask;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        Assertions.assertNotSame(staleBatch, job.rollupBatchTask);
    }

    @Test
    public void testLakeAlterMetaRunningMapsBackToPending() {
        // This family skips WAITING_TXN on the live path; PENDING is the durable predecessor
        // (its same-state re-log persisted the watershed) and replay() throws on RUNNING.
        LakeTableAlterMetaJob job = new LakeTableAlterMetaJob();
        job.setJobState(JobState.RUNNING);
        job.batchTask = new AgentBatchTask();

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.PENDING, job.getJobState());
        Assertions.assertNull(job.batchTask);
    }

    @Test
    public void testLakeFastPathBothInMemoryStatesMapToPendingAndBatchNulled() {
        // Both WAITING_TXN and RUNNING are in-memory only for the fast-path family, and
        // runRunningJob dispatches ONLY behind a null guard - a stale batch would silently
        // suppress the re-send.
        LakeTableAddIndexJob job = new LakeTableAddIndexJob();
        job.setJobState(JobState.WAITING_TXN);
        job.batchTask = new AgentBatchTask();

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.PENDING, job.getJobState());
        Assertions.assertNull(job.batchTask);

        job.setJobState(JobState.RUNNING);
        job.batchTask = new AgentBatchTask();
        job.resetToLastDurableState();
        Assertions.assertEquals(JobState.PENDING, job.getJobState());
        Assertions.assertNull(job.batchTask);
    }

    @Test
    public void testFinalStatesAreUntouched() {
        SchemaChangeJobV2 job = new SchemaChangeJobV2(1L, 2L, 3L, "tbl", 100000L);
        job.setJobState(JobState.FINISHED);
        AgentBatchTask batch = job.schemaChangeBatchTask;
        CompletableFuture<Boolean> publishFuture = new CompletableFuture<>();
        job.publishVersionFuture = publishFuture;

        job.resetToLastDurableState();

        Assertions.assertEquals(JobState.FINISHED, job.getJobState());
        Assertions.assertSame(batch, job.schemaChangeBatchTask, "final states must not be reset");
        Assertions.assertSame(publishFuture, job.publishVersionFuture);
        Assertions.assertFalse(publishFuture.isCancelled());
    }

    @Test
    public void testPublishVersionFutureCancelledForNonFinalStates() {
        LakeRollupJob job = new LakeRollupJob();
        job.setJobState(JobState.FINISHED_REWRITING);
        CompletableFuture<Boolean> staleFuture = new CompletableFuture<>();
        job.publishVersionFuture = staleFuture;

        job.resetToLastDurableState();

        // FINISHED_REWRITING itself is durable (stays), but the previous session's publish
        // future must not short-circuit the new session's first publishVersion() tick.
        Assertions.assertEquals(JobState.FINISHED_REWRITING, job.getJobState());
        Assertions.assertNull(job.publishVersionFuture);
        Assertions.assertTrue(staleFuture.isCancelled());
    }

    @Test
    public void testHandlerSweepResetsJobsAndClearsRunningGate() {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        RollupJobV2 job = new RollupJobV2();
        job.setJobState(JobState.RUNNING);
        handler.alterJobsV2.put(job.getJobId(), job);
        handler.getTableRunningJobMap().computeIfAbsent(7L, k -> new java.util.HashSet<>()).add(job.getJobId());

        handler.onStopped();

        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState(),
                "onStopped must sweep alterJobsV2 and reset each non-final job");
        Assertions.assertTrue(handler.getTableRunningJobMap().isEmpty(),
                "the leader-only concurrency gate must be cleared on demotion");
    }
}
