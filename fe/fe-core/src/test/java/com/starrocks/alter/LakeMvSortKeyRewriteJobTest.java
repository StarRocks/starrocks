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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Light unit coverage for {@link LakeMvSortKeyRewriteJob}'s MV-specific deltas over
 * {@link LakeRangeRewriteSchemaChangeJob}: the non-inactivating MV hook, the system-INSERT seam, and
 * copy/serialization round-trips of the new {@code refreshWasActiveAtSubmit} field.
 */
public class LakeMvSortKeyRewriteJobTest {

    private static final long DB_ID = 30001L;
    private static final long MV_ID = 30002L;
    private static final String MV_NAME = "mv_sortkey_refresh_test";

    // Real MV, registered under GlobalStateMgr so onReplayReconcile's ensureMvStateSchemaChangeOnReplay
    // can resolve and mutate it via getTable()/AutoCloseableLock -- but with no live TaskManager /
    // InsertOverwriteJobMgr, since tests inject a mocked RefreshCoordinator instead.
    private MaterializedView stubbedMv;

    // Tracks a Task registered via registerRefreshTask() (if any) so tearDownPersistTest() can drop it --
    // TaskManager.nameToTaskMap is keyed by a name derived from MV_ID and outlives a single test method.
    private Task registeredTask;

    @BeforeEach
    public void setUpStubbedCatalog() {
        // Needed only by the DefaultRefreshCoordinator tests below, which register a real refresh Task
        // (TaskManager.createTask journals) instead of injecting a mocked RefreshCoordinator.
        UtFrameUtils.setUpForPersistTest();

        Database db = new Database(DB_ID, "db_mv_sortkey_refresh_test");
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(db);

        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", IntegerType.INT);
        k1.setIsKey(true);
        columns.add(k1);
        columns.add(new Column("v1", IntegerType.INT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(k1));
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);

        stubbedMv = new MaterializedView(MV_ID, DB_ID, MV_NAME, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        db.registerTableUnlocked(stubbedMv);
    }

    @AfterEach
    public void tearDownPersistTest() {
        if (registeredTask != null) {
            GlobalStateMgr.getCurrentState().getTaskManager().dropTasks(List.of(registeredTask.getId()));
        }
        UtFrameUtils.tearDownForPersisTest();
    }

    private LakeMvSortKeyRewriteJob newJobWithStubbedCatalog() {
        return new LakeMvSortKeyRewriteJob(1L, DB_ID, MV_ID, MV_NAME, 3600_000L);
    }

    /** Registers stubbedMv's refresh Task with the (pseudo-journaled) live TaskManager. */
    private Task registerRefreshTask() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_ID);
        Task task = TaskBuilder.buildMvTask(stubbedMv, db.getFullName());
        GlobalStateMgr.getCurrentState().getTaskManager().createTask(task);
        registeredTask = task;
        return task;
    }

    @Test
    public void testAffectedColumnsForMvInactivationIsEmpty() {
        // A sort-key reorder on the MV itself is output-preserving: it must not inactivate dependents.
        LakeMvSortKeyRewriteJob job = new LakeMvSortKeyRewriteJob(1L, 2L, 3L, "mv", 1000L);
        OlapTable mv = mock(OlapTable.class);
        assertTrue(job.affectedColumnsForMvInactivation(mv).isEmpty());
        verify(mv, never()).getSchemaByIndexMetaId(anyLong());  // does not even inspect the schema
    }

    @Test
    public void testConfigureRewriteInsertSetsSystem() {
        LakeMvSortKeyRewriteJob job = new LakeMvSortKeyRewriteJob(1L, 2L, 3L, "mv", 1000L);
        InsertStmt insertStmt = mock(InsertStmt.class);
        job.configureRewriteInsert(insertStmt);
        verify(insertStmt).setSystem(true);
    }

    @Test
    public void testCopyForPersistPreservesTypeAndFlag() {
        // copyForPersist is what AlterJobV2.persistStateChange journals — the flag must survive.
        for (boolean flag : new boolean[] {true, false}) {
            LakeMvSortKeyRewriteJob job = new LakeMvSortKeyRewriteJob(1L, 2L, 3L, "mv", 1000L);
            job.setRefreshWasActiveAtSubmit(flag);
            AlterJobV2 copy = job.copyForPersist();
            assertTrue(copy instanceof LakeMvSortKeyRewriteJob);
            assertEquals(flag, ((LakeMvSortKeyRewriteJob) copy).isRefreshWasActiveAtSubmit());
        }
    }

    @Test
    public void testApplyNewMvSortKeysPersistsIntoPropertiesMap() {
        // TableProperty.mvSortKeys has no @SerializedName: on deserialize it is rebuilt from the
        // persisted "properties" map, not from the transient field. So the flip must call
        // putMvSortKeys() (which writes mv_sort_keys back into that map), not just setMvSortKeys() -
        // otherwise the new order silently reverts to the pre-rewrite value after an FE restart or a
        // follower checkpoint-image reload, even though physical storage stays rewritten.
        MaterializedView mv = new MaterializedView();
        TableProperty property = new TableProperty();
        mv.setTableProperty(property);

        LakeMvSortKeyRewriteJob job = new LakeMvSortKeyRewriteJob(1L, 2L, 3L, "mv", 1000L);
        job.setNewSortKeyColumns(List.of(new Column("k2", IntegerType.INT), new Column("k1", IntegerType.INT)));

        job.applyNewMvSortKeys(mv);

        assertEquals(List.of("k2", "k1"), property.getMvSortKeys());
        assertEquals("k2,k1", property.getProperties().get(PropertyAnalyzer.PROPERTY_MV_SORT_KEYS));
    }

    @Test
    public void testGsonRoundTripPreservesSubtypeAndFlag() {
        // The new subtype AND the new serialized field must round-trip (both true and false).
        for (boolean flag : new boolean[] {true, false}) {
            LakeMvSortKeyRewriteJob job = new LakeMvSortKeyRewriteJob(1L, 2L, 3L, "mv", 1000L);
            job.setRefreshWasActiveAtSubmit(flag);
            String json = GsonUtils.GSON.toJson(job, AlterJobV2.class);
            AlterJobV2 back = GsonUtils.GSON.fromJson(json, AlterJobV2.class);
            assertTrue(back instanceof LakeMvSortKeyRewriteJob);
            assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, back.getType());
            assertEquals(flag, ((LakeMvSortKeyRewriteJob) back).isRefreshWasActiveAtSubmit());
        }
    }

    @Test
    public void testBeforeShadowBuildSuspendsAndDrainsRefresh() throws Exception {
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        // hasRunningOverwriteJob returns true once (in-flight), then false (drained)
        when(coord.hasRunningOverwriteJob()).thenReturn(true, false);
        job.setRefreshCoordinator(coord);

        job.beforeShadowBuild();

        InOrder inOrder = inOrder(coord);
        inOrder.verify(coord).suspendRefresh();          // suspend BEFORE draining
        inOrder.verify(coord, atLeast(1)).hasRunningOverwriteJob();  // drained until false
        inOrder.verify(coord, never()).resumeRefresh();
    }

    @Test
    public void testAfterJobSettledResumesWhenActiveAtSubmit() {
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(true);
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        job.setRefreshCoordinator(coord);
        job.afterJobSettled(false);
        job.afterJobSettled(true);
        verify(coord, times(2)).resumeRefresh();
    }

    @Test
    public void testAfterJobSettledSwallowsResumeFailure() {
        // The terminal state is already journaled by the time afterJobSettled runs (called from
        // runFinishedRewritingJob/cancelImpl), so a resumeRefresh failure (e.g. task-lock contention)
        // must be swallowed, not propagated -- an escaping exception would skip sibling alter jobs in
        // the runAlterJobV2 scheduler loop.
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(true);
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        doThrow(new RuntimeException("lock contention")).when(coord).resumeRefresh();
        job.setRefreshCoordinator(coord);

        assertDoesNotThrow(() -> job.afterJobSettled(false));

        verify(coord).resumeRefresh();
    }

    @Test
    public void testAfterJobSettledDoesNotResurrectPrePausedRefresh() {
        // Refresh was PAUSED by the user at submit -- never resumed on finish or cancel.
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(false);
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        job.setRefreshCoordinator(coord);
        job.afterJobSettled(false);
        job.afterJobSettled(true);
        verify(coord, never()).resumeRefresh();
    }

    @Test
    public void testTerminalReplayDoesNotResurrectPrePausedRefresh() {
        // Same guard on the terminal-replay path.
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(false);
        job.setJobState(AlterJobV2.JobState.FINISHED);
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        job.setRefreshCoordinator(coord);
        job.onReplayReconcile();
        verify(coord, never()).resumeRefreshForReplay();
    }

    @Test
    public void testDrainTimeoutThrows() {
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        when(coord.hasRunningOverwriteJob()).thenReturn(true);  // never drains
        job.setRefreshCoordinator(coord);
        job.setDrainTimeoutMs(50);
        assertThrows(AlterCancelException.class, job::beforeShadowBuild);
    }

    @Test
    public void testDrainWaitsForTempPartitions() throws Exception {
        // Overwrite job gone but a temp partition lingers -> keep draining.
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        when(coord.hasRunningOverwriteJob()).thenReturn(false);
        when(coord.hasTempPartitions()).thenReturn(true, false);
        job.setRefreshCoordinator(coord);
        job.beforeShadowBuild();
        verify(coord, atLeast(2)).hasTempPartitions();
    }

    @Test
    public void testOnReplayReconcileTerminalResumesViaReplayOp() {
        // Replay must use the no-journal resume, never the live one.
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(true);
        job.setJobState(AlterJobV2.JobState.FINISHED);
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        job.setRefreshCoordinator(coord);
        job.onReplayReconcile();
        verify(coord).resumeRefreshForReplay();
        verify(coord, never()).resumeRefresh();        // never the journaling live op on replay
        verify(coord, never()).hasRunningOverwriteJob(); // never drains on replay
    }

    @Test
    public void testOnReplayReconcileNonTerminalForcesSchemaChangeAndSuspends() {
        // PENDING replay must force SCHEMA_CHANGE (reject overwrites post-failover) AND converge the
        // refresh task to PAUSE via the no-journal replay suspend.
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();  // stub table state = NORMAL
        LakeMvSortKeyRewriteJob.RefreshCoordinator coord = mock(LakeMvSortKeyRewriteJob.RefreshCoordinator.class);
        job.setRefreshCoordinator(coord);
        job.setJobState(AlterJobV2.JobState.PENDING);
        job.onReplayReconcile();
        assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, stubbedMv.getState());
        verify(coord).suspendRefreshForReplay();          // task converges to PAUSE
        verify(coord, never()).suspendRefresh();           // never the journaling live op on replay
        verify(coord, never()).hasRunningOverwriteJob();   // never drains on replay
    }

    // The tests below inject no RefreshCoordinator, so the job lazily resolves the production
    // DefaultRefreshCoordinator (getRefreshCoordinator()) and drives the real TaskManager /
    // InsertOverwriteJobMgr instead of a mock.

    @Test
    public void testDefaultRefreshCoordinatorSuspendsAndResumesRealTask() throws Exception {
        Task task = registerRefreshTask();
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(true);

        job.beforeShadowBuild();
        assertEquals(Constants.TaskState.PAUSE, task.getState());

        job.afterJobSettled(false);
        assertEquals(Constants.TaskState.ACTIVE, task.getState());
    }

    @Test
    public void testDefaultRefreshCoordinatorReplayResumesRealTask() throws Exception {
        Task task = registerRefreshTask();
        task.setState(Constants.TaskState.PAUSE);  // simulate suspended-for-rewrite state pre-replay
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();
        job.setRefreshWasActiveAtSubmit(true);
        job.setJobState(AlterJobV2.JobState.FINISHED);

        job.onReplayReconcile();

        assertEquals(Constants.TaskState.ACTIVE, task.getState());
    }

    @Test
    public void testDefaultRefreshCoordinatorReplayNonTerminalSuspendsRealTask() throws Exception {
        Task task = registerRefreshTask();
        LakeMvSortKeyRewriteJob job = newJobWithStubbedCatalog();  // stub table state = NORMAL
        job.setJobState(AlterJobV2.JobState.PENDING);

        job.onReplayReconcile();

        assertEquals(Constants.TaskState.PAUSE, task.getState());
        assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, stubbedMv.getState());
    }

    @Test
    public void testDefaultRefreshCoordinatorNoOpWhenMvOrTaskAbsent() {
        // stubbedMv has no registered refresh Task -- resolveTask() returns null.
        LakeMvSortKeyRewriteJob jobNoTask = newJobWithStubbedCatalog();
        jobNoTask.setRefreshWasActiveAtSubmit(true);
        assertDoesNotThrow(jobNoTask::beforeShadowBuild);
        assertDoesNotThrow(() -> jobNoTask.afterJobSettled(false));

        // Table id does not resolve to any table at all -- resolveMv() returns null.
        LakeMvSortKeyRewriteJob jobNoMv = new LakeMvSortKeyRewriteJob(2L, DB_ID, MV_ID + 999, "missing_mv", 3600_000L);
        jobNoMv.setRefreshWasActiveAtSubmit(true);
        assertDoesNotThrow(jobNoMv::beforeShadowBuild);
        assertDoesNotThrow(() -> jobNoMv.afterJobSettled(false));
    }
}
