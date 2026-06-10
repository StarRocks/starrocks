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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link AlterJobMgr#unfinishedAlterJobsAllowConcurrentPartitionCreation(long)},
 * the shared guard helper that the G1 (manual ADD PARTITION), G2 (automatic creation during load)
 * and G3 (dynamic-partition scheduler) guards consult to decide whether partition creation may run
 * concurrently with the unfinished alter jobs on a table. The end-to-end executor wiring is covered
 * by shared-data SQL integration tests.
 */
public class ConcurrentAddPartitionDuringAlterTest {

    private static final long TABLE_ID = 1000L;

    /**
     * Wire {@code GlobalStateMgr.getCurrentState()} to a mock whose schema-change and rollup
     * handlers return the supplied unfinished-job lists for {@link #TABLE_ID}, then run the body.
     */
    private void runWithUnfinishedJobs(List<AlterJobV2> schemaChangeJobs, List<AlterJobV2> rollupJobs,
                                       Runnable body) {
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr gsm = mock(GlobalStateMgr.class);
            SchemaChangeHandler schemaChangeHandler = mock(SchemaChangeHandler.class);
            MaterializedViewHandler rollupHandler = mock(MaterializedViewHandler.class);
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            when(gsm.getSchemaChangeHandler()).thenReturn(schemaChangeHandler);
            when(gsm.getRollupHandler()).thenReturn(rollupHandler);
            when(schemaChangeHandler.getUnfinishedAlterJobV2ByTableId(TABLE_ID)).thenReturn(schemaChangeJobs);
            when(rollupHandler.getUnfinishedAlterJobV2ByTableId(TABLE_ID)).thenReturn(rollupJobs);
            body.run();
        }
    }

    /** A lake ADD INDEX fast-path job — declares allowConcurrentPartitionCreation() == true. */
    private AlterJobV2 safeJob() {
        return new LakeTableAddIndexJob(1L, 2L, TABLE_ID, "t", 60_000L, new ArrayList<>(), new ArrayList<>());
    }

    /** A lake full-rewrite schema-change job — does not declare the capability (false). */
    private AlterJobV2 unsafeJob() {
        return new LakeTableSchemaChangeJob(2L, 2L, TABLE_ID, "t", 60_000L);
    }

    @Test
    public void testNoUnfinishedJobsIsNotTolerable() {
        // Anomaly conservatism: a non-NORMAL state with no unfinished job (e.g. stale state after a
        // crash) must NOT be treated as tolerable; the legacy rejection applies.
        runWithUnfinishedJobs(new ArrayList<>(), new ArrayList<>(), () ->
                assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }

    @Test
    public void testAllSafeSchemaChangeJobsTolerable() {
        runWithUnfinishedJobs(Collections.singletonList(safeJob()), new ArrayList<>(), () ->
                assertTrue(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }

    @Test
    public void testUnsafeSchemaChangeJobNotTolerable() {
        runWithUnfinishedJobs(Collections.singletonList(unsafeJob()), new ArrayList<>(), () ->
                assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }

    @Test
    public void testMixedSafeAndUnsafeNotTolerable() {
        // allMatch: a single unsafe job in the set forces the conservative answer.
        runWithUnfinishedJobs(Arrays.asList(safeJob(), unsafeJob()), new ArrayList<>(), () ->
                assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }

    @Test
    public void testUnsafeRollupJobNotTolerable() {
        // The rollup handler's jobs are included in the check; an unsafe rollup job vetoes.
        runWithUnfinishedJobs(Collections.singletonList(safeJob()), Collections.singletonList(unsafeJob()), () ->
                assertFalse(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }

    @Test
    public void testSafeJobsAcrossBothHandlersTolerable() {
        runWithUnfinishedJobs(Collections.singletonList(safeJob()), Collections.singletonList(safeJob()), () ->
                assertTrue(AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(TABLE_ID)));
    }
}
