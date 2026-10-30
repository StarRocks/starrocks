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

import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link AlterJobMgr#unfinishedAlterJobsAllowConcurrentPartitionCreation(long)},
 * the shared guard helper that the G1 (manual ADD PARTITION), G2 (automatic creation during load)
 * and G3 (dynamic-partition scheduler) guards consult to decide whether partition creation may run
 * concurrently with the unfinished alter jobs on a table.
 *
 * <p>These tests drive the helper against the REAL {@link SchemaChangeHandler}/
 * {@link MaterializedViewHandler} registries (constructed without {@code start()}, so no daemon
 * runs) with real {@link AlterJobV2} instances registered via {@code addAlterJobV2}, so they
 * exercise the actual {@code getUnfinishedAlterJobV2ByTableId} filtering (tableId match, and
 * FINISHED/CANCELLED exclusion) rather than a mocked lookup. The end-to-end executor wiring is
 * covered by {@link ConcurrentAddPartitionDuringAlterE2ETest} and the shared-data SQL test.
 */
public class ConcurrentAddPartitionDuringAlterTest {

    private static final long TABLE_ID = 1000L;

    /**
     * Wire {@code GlobalStateMgr.getCurrentState()} to a mock whose schema-change and rollup
     * handlers are REAL (un-started) handler instances, then hand them to {@code body} so the
     * test can register real jobs before invoking the helper.
     */
    private void withRealHandlers(BiConsumer<SchemaChangeHandler, MaterializedViewHandler> body) {
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr gsm = mock(GlobalStateMgr.class);
            // The constructors do NOT start the scheduler daemon (start() is separate), so the
            // registered jobs stay put and the test is fully deterministic.
            SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
            MaterializedViewHandler rollupHandler = new MaterializedViewHandler();
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            when(gsm.getSchemaChangeHandler()).thenReturn(schemaChangeHandler);
            when(gsm.getRollupHandler()).thenReturn(rollupHandler);
            body.accept(schemaChangeHandler, rollupHandler);
        }
    }

    /** A lake ADD INDEX fast-path job for {@code tableId} — declares allowConcurrentPartitionCreation()==true. */
    private LakeTableAddIndexJob safeJob(long jobId, long tableId) {
        return new LakeTableAddIndexJob(jobId, 2L, tableId, "t", 60_000L, new ArrayList<>(), new ArrayList<>());
    }

    /** A lake full-rewrite schema-change job — does not declare the capability (false). */
    private LakeTableSchemaChangeJob unsafeJob(long jobId, long tableId) {
        return new LakeTableSchemaChangeJob(jobId, 2L, tableId, "t", 60_000L);
    }

    @Test
    public void testNoUnfinishedJobsIsNotTolerable() {
        // Anomaly conservatism: a non-NORMAL state with no unfinished job (e.g. stale state after a
        // crash) must NOT be treated as tolerable; the legacy rejection applies.
        withRealHandlers((sc, rollup) ->
                assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }

    @Test
    public void testAllSafeSchemaChangeJobsTolerable() {
        withRealHandlers((sc, rollup) -> {
            sc.addAlterJobV2(safeJob(1L, TABLE_ID));
            assertTrue(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testUnsafeSchemaChangeJobNotTolerable() {
        withRealHandlers((sc, rollup) -> {
            sc.addAlterJobV2(unsafeJob(1L, TABLE_ID));
            assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testMixedSafeAndUnsafeNotTolerable() {
        // allMatch: a single unsafe job in the set forces the conservative answer.
        withRealHandlers((sc, rollup) -> {
            sc.addAlterJobV2(safeJob(1L, TABLE_ID));
            sc.addAlterJobV2(unsafeJob(2L, TABLE_ID));
            assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testUnsafeRollupJobNotTolerable() {
        // The rollup handler's jobs are included in the check; an unsafe rollup job vetoes.
        withRealHandlers((sc, rollup) -> {
            sc.addAlterJobV2(safeJob(1L, TABLE_ID));
            rollup.addAlterJobV2(unsafeJob(2L, TABLE_ID));
            assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testSafeJobsAcrossBothHandlersTolerable() {
        withRealHandlers((sc, rollup) -> {
            sc.addAlterJobV2(safeJob(1L, TABLE_ID));
            rollup.addAlterJobV2(safeJob(2L, TABLE_ID));
            assertTrue(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testFinishedSafeJobIsExcluded() {
        // A FINISHED job is no longer "unfinished" -> filtered out by getUnfinishedAlterJobV2ByTableId
        // -> the set is empty -> not tolerable (the table should already be NORMAL once it finished).
        withRealHandlers((sc, rollup) -> {
            LakeTableAddIndexJob job = safeJob(1L, TABLE_ID);
            job.setJobState(AlterJobV2.JobState.FINISHED);
            sc.addAlterJobV2(job);
            assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testCancelledSafeJobIsExcluded() {
        withRealHandlers((sc, rollup) -> {
            LakeTableAddIndexJob job = safeJob(1L, TABLE_ID);
            job.setJobState(AlterJobV2.JobState.CANCELLED);
            sc.addAlterJobV2(job);
            assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
        });
    }

    @Test
    public void testJobsForOtherTableAreNotConsidered() {
        // The check is per-table: a safe job on a DIFFERENT table must not make THIS table tolerable.
        withRealHandlers((sc, rollup) -> {
            long otherTableId = TABLE_ID + 1;
            sc.addAlterJobV2(safeJob(1L, otherTableId));
            assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID));
            assertTrue(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(otherTableId));
        });
    }
}
