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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.thrift.TTaskType;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LakeTableIndexFastPathJobBase} lifecycle methods that can
 * be exercised in isolation: getInfo, getTransactionId, replay branches, and
 * cancelImpl. Heavy lifecycle methods (runPendingJob / runRunningJob etc.)
 * require a working catalog + lock manager and are covered elsewhere.
 *
 * <p>{@link LakeTableAddIndexJob} is used as a concrete instantiation of the
 * abstract base.
 */
public class LakeTableIndexFastPathJobBaseTest {

    private LakeTableAddIndexJob newJob() {
        return new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(), new ArrayList<>());
    }

    private static void setField(Object o, String name, Object value) throws Exception {
        Field f = findField(o.getClass(), name);
        f.setAccessible(true);
        f.set(o, value);
    }

    private static Field findField(Class<?> c, String name) throws NoSuchFieldException {
        Class<?> cur = c;
        while (cur != null) {
            try {
                return cur.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                cur = cur.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    // -------- getTransactionId --------

    @Test
    public void testGetTransactionIdEmptyBeforeWatershed() {
        LakeTableAddIndexJob job = newJob();
        assertEquals(Optional.empty(), job.getTransactionId());
    }

    @Test
    public void testGetTransactionIdPresentAfterWatershed() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "watershedTxnId", 99L);
        assertEquals(Optional.of(99L), job.getTransactionId());
    }

    // -------- getInfo --------

    @Test
    public void testGetInfoSharedDataMode() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "watershedTxnId", 12345L);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        WarehouseManager wm = mock(WarehouseManager.class);
        Warehouse wh = mock(Warehouse.class);
        when(wh.getName()).thenReturn("wh1");
        when(wm.getWarehouseAllowNull(anyLong())).thenReturn(wh);
        when(gsm.getWarehouseMgr()).thenReturn(wm);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> rmStatic = Mockito.mockStatic(RunMode.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            rmStatic.when(RunMode::getCurrentRunMode).thenReturn(RunMode.SHARED_DATA);
            List<List<Comparable>> infos = new ArrayList<>();
            job.getInfo(infos);
            assertEquals(1, infos.size());
            List<Comparable> row = infos.get(0);
            assertEquals(14, row.size());
            assertEquals("wh1", row.get(13));
        }
    }

    @Test
    public void testGetInfoSharedNothingMode() {
        LakeTableAddIndexJob job = newJob();
        try (MockedStatic<RunMode> rmStatic = Mockito.mockStatic(RunMode.class)) {
            rmStatic.when(RunMode::getCurrentRunMode).thenReturn(RunMode.SHARED_NOTHING);
            List<List<Comparable>> infos = new ArrayList<>();
            job.getInfo(infos);
            assertEquals(1, infos.size());
            assertEquals(13, infos.get(0).size());
        }
    }

    @Test
    public void testGetInfoSharedDataModeNullWarehouse() {
        LakeTableAddIndexJob job = newJob();
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        WarehouseManager wm = mock(WarehouseManager.class);
        when(wm.getWarehouseAllowNull(anyLong())).thenReturn(null);
        when(gsm.getWarehouseMgr()).thenReturn(wm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> rmStatic = Mockito.mockStatic(RunMode.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            rmStatic.when(RunMode::getCurrentRunMode).thenReturn(RunMode.SHARED_DATA);
            List<List<Comparable>> infos = new ArrayList<>();
            job.getInfo(infos);
            assertEquals("null", infos.get(0).get(13));
        }
    }

    // -------- cancelImpl --------

    /** Build a {@link GlobalStateMgr} mock wired up for catalog reads + lock fallback. */
    private GlobalStateMgr buildLockableGsm(Database db, OlapTable table) {
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(anyLong())).thenReturn(db);
        when(lm.getTable(anyLong(), anyLong())).thenReturn(table);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        // Locker.lockDatabase falls through to MetadataMgr.getDb when the lock
        // manager is disabled; supply the same Database mock there.
        MetadataMgr mm = mock(MetadataMgr.class);
        when(mm.getDb(anyLong())).thenReturn(db);
        when(gsm.getMetadataMgr()).thenReturn(mm);
        when(gsm.getEditLog()).thenReturn(mockEditLog());
        return gsm;
    }

    @Test
    public void testCancelImpl_FromPending_ResetsState() {
        LakeTableAddIndexJob job = newJob();
        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.SCHEMA_CHANGE);

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertTrue(job.cancelImpl("err"));
            verify(table).setState(OlapTable.OlapTableState.NORMAL);
            assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        }
    }

    @Test
    public void testCancelImpl_AlreadyFinishedReturnsFalse() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.FINISHED);
        assertFalse(job.cancelImpl("err"));
    }

    @Test
    public void testCancelImpl_RemovesAgentTasks() throws Exception {
        LakeTableAddIndexJob job = newJob();
        AgentBatchTask batch = new AgentBatchTask();
        AgentTask t = mock(AgentTask.class);
        when(t.getBackendId()).thenReturn(11L);
        when(t.getSignature()).thenReturn(22L);
        batch.addTask(t);
        setField(job, "batchTask", batch);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        GlobalStateMgr gsm = buildLockableGsm(db, table);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<AgentTaskQueue> queueStatic = Mockito.mockStatic(AgentTaskQueue.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertTrue(job.cancelImpl("oops"));
            queueStatic.verify(() -> AgentTaskQueue.removeTask(11L, TTaskType.ALTER, 22L), times(1));
        }
    }

    @Test
    public void testCancelImpl_NullDatabaseStillCancels() {
        LakeTableAddIndexJob job = newJob();
        GlobalStateMgr gsm = buildLockableGsm(null, null);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertTrue(job.cancelImpl("err"));
            assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        }
    }

    // -------- replay --------

    private LakeTableAddIndexJob makeReplayDriver(AlterJobV2.JobState state) throws Exception {
        LakeTableAddIndexJob other = newJob();
        setField(other, "jobState", state);
        setField(other, "watershedTxnId", 7L);
        return other;
    }

    private void runReplay(LakeTableAddIndexJob target, LakeTableAddIndexJob other,
                           Database db, OlapTable table) {
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(2L)).thenReturn(db);
        if (db != null) {
            when(lm.getTable(2L, 3L)).thenReturn(table);
        }
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            target.replay(other);
        }
    }

    @Test
    public void testReplay_PendingFlipsTableToSchemaChange() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.PENDING);
        OlapTable table = mock(OlapTable.class);
        runReplay(target, other, new Database(2L, "db"), table);
        verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    @Test
    public void testReplay_WaitingTxnFlipsToSchemaChange() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.WAITING_TXN);
        OlapTable table = mock(OlapTable.class);
        runReplay(target, other, new Database(2L, "db"), table);
        verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    @Test
    public void testReplay_RunningFlipsToSchemaChange() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.RUNNING);
        OlapTable table = mock(OlapTable.class);
        runReplay(target, other, new Database(2L, "db"), table);
        verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    @Test
    public void testReplay_FinishedRewritingFlipsToSchemaChange() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.FINISHED_REWRITING);
        OlapTable table = mock(OlapTable.class);
        runReplay(target, other, new Database(2L, "db"), table);
        verify(table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    @Test
    public void testReplay_FinishedAppliesCatalogMutationAndNormal() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.FINISHED);
        // Empty commitVersionMap → no partition lookup, just applyCatalogMutation + setState.
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        runReplay(target, other, new Database(2L, "db"), table);
        verify(table).setState(OlapTable.OlapTableState.NORMAL);
        verify(table).getIndexes(); // confirms applyCatalogMutation ran.
    }

    @Test
    public void testReplay_FinishedBumpsVisibleVersionWhenStale() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.FINISHED);
        Map<Long, Long> commitMap = new HashMap<>();
        commitMap.put(100L, 5L);
        setField(other, "commitVersionMap", commitMap);
        setField(other, "finishedTimeMs", 12345L);

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getVisibleVersion()).thenReturn(2L);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        runReplay(target, other, new Database(2L, "db"), table);
        verify(pp).setVisibleVersion(5L, 12345L);
    }

    @Test
    public void testReplay_FinishedSkipsVisibleVersionBumpWhenAdvanced() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.FINISHED);
        Map<Long, Long> commitMap = new HashMap<>();
        commitMap.put(100L, 5L);
        setField(other, "commitVersionMap", commitMap);

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getVisibleVersion()).thenReturn(10L);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        runReplay(target, other, new Database(2L, "db"), table);
        verify(pp, never()).setVisibleVersion(anyLong(), anyLong());
    }

    @Test
    public void testReplay_CancelledFlipsToNormal() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.CANCELLED);
        OlapTable table = mock(OlapTable.class);
        runReplay(target, other, new Database(2L, "db"), table);
        verify(table).setState(OlapTable.OlapTableState.NORMAL);
    }

    @Test
    public void testReplay_TableMissingIsNoOp() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.PENDING);
        Database db = new Database(2L, "db");
        // table=null path: replay should not throw.
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(2L)).thenReturn(db);
        when(lm.getTable(2L, 3L)).thenReturn(null);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            target.replay(other); // must not throw
        }
        // Base fields should still be copied even when the table is missing.
        assertEquals(7L, target.getTransactionId().orElse(-1L));
    }

    @Test
    public void testReplay_DatabaseMissingIsNoOp() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.PENDING);
        runReplay(target, other, null, null);
        // No table call → no setState happened on a non-null table; basics still replicated.
        assertEquals(AlterJobV2.JobState.PENDING, target.getJobState());
    }

    @Test
    public void testReplay_CopiesBaseFields() throws Exception {
        LakeTableAddIndexJob target = newJob();
        LakeTableAddIndexJob other = makeReplayDriver(AlterJobV2.JobState.WAITING_TXN);
        Map<Long, List<Long>> p2t = new HashMap<>();
        p2t.put(100L, Collections.singletonList(200L));
        Map<Long, Long> t2i = new HashMap<>();
        t2i.put(200L, 300L);
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 7L);
        setField(other, "partitionToTablets", p2t);
        setField(other, "tabletToIndexMetaId", t2i);
        setField(other, "commitVersionMap", commit);
        setField(other, "errMsg", "ohno");
        setField(other, "finishedTimeMs", 999L);

        runReplay(target, other, null, null);

        assertEquals(7L, target.getTransactionId().orElse(-1L));
        assertEquals(p2t, getField(target, "partitionToTablets"));
        assertEquals(t2i, getField(target, "tabletToIndexMetaId"));
        assertEquals(commit, getField(target, "commitVersionMap"));
        assertEquals("ohno", getField(target, "errMsg"));
    }

    private static Object getField(Object o, String name) throws Exception {
        Field f = findField(o.getClass(), name);
        f.setAccessible(true);
        return f.get(o);
    }

    /**
     * Returns a mock EditLog whose logAlterJob immediately invokes the supplied
     * WALApplier so persistStateChange's in-memory state flip happens.
     */
    private static EditLog mockEditLog() {
        EditLog edit = mock(EditLog.class);
        Mockito.doAnswer(inv -> {
            WALApplier applier = inv.getArgument(1);
            applier.apply(null);
            return null;
        }).when(edit).logAlterJob(any(), any(WALApplier.class));
        return edit;
    }

    // -------- copy constructor (subclass) --------

    @Test
    public void testCopyConstructorPreservesFields() throws Exception {
        LakeTableAddIndexJob original = newJob();
        setField(original, "watershedTxnId", 42L);
        Map<Long, List<Long>> p2t = new HashMap<>();
        p2t.put(1L, Collections.singletonList(2L));
        Map<Long, Long> t2i = new HashMap<>();
        t2i.put(2L, 3L);
        Map<Long, Long> commit = new HashMap<>();
        commit.put(1L, 5L);
        setField(original, "partitionToTablets", p2t);
        setField(original, "tabletToIndexMetaId", t2i);
        setField(original, "commitVersionMap", commit);

        LakeTableAddIndexJob copy = new LakeTableAddIndexJob(original);
        assertEquals(Optional.of(42L), copy.getTransactionId());
        assertEquals(p2t, getField(copy, "partitionToTablets"));
        assertEquals(t2i, getField(copy, "tabletToIndexMetaId"));
        assertEquals(commit, getField(copy, "commitVersionMap"));
        // Distinct map instances.
        assertNotSame(p2t, getField(copy, "partitionToTablets"));
        assertNotSame(t2i, getField(copy, "tabletToIndexMetaId"));
        assertNotSame(commit, getField(copy, "commitVersionMap"));
    }
}
