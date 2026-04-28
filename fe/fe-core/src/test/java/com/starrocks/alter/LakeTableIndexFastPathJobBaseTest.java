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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.Utils;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionIdGenerator;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LakeTableIndexFastPathJobBase} lifecycle methods.
 *
 * <p>Covers getInfo, getTransactionId, replay branches, cancelImpl, plus the
 * lifecycle hooks runPendingJob / runWaitingTxnJob / runRunningJob /
 * runFinishedRewritingJob, lakePublishVersion, and the dispatchAllTasks /
 * updateNextVersion internals (called via reflection).
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
        // Locker.lockTablesWithIntensiveDbLock dereferences
        // GlobalStateMgr.getCurrentState().getLockManager(); supply a real
        // LockManager so cancelImpl can take its locks.
        when(gsm.getLockManager()).thenReturn(new com.starrocks.common.util.concurrent.lock.LockManager());
        // Build the EditLog mock OUTSIDE the chained when(..) to avoid
        // Mockito's UnfinishedStubbing detection (mockEditLog itself does
        // doAnswer().when(edit) which Mockito interprets as nested
        // stubbing if invoked inside another when().thenReturn(...) chain).
        EditLog editLog = mockEditLog();
        when(gsm.getEditLog()).thenReturn(editLog);
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

    // -------- helpers for lifecycle tests --------

    private static void invokePrivate(Object target, String name) throws Exception {
        Method m = LakeTableIndexFastPathJobBase.class.getDeclaredMethod(name);
        m.setAccessible(true);
        try {
            m.invoke(target);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    private static GlobalTransactionMgr mockTxnMgrYielding(long nextTxnId) {
        GlobalTransactionMgr txnMgr = mock(GlobalTransactionMgr.class);
        TransactionIdGenerator gen = mock(TransactionIdGenerator.class);
        when(gen.getNextTransactionId()).thenReturn(nextTxnId);
        when(txnMgr.getTransactionIDGenerator()).thenReturn(gen);
        return txnMgr;
    }

    // -------- runPendingJob --------

    @Test
    @SuppressWarnings("unchecked")
    public void testRunPendingJob_DispatchedTabletsCollected() throws Exception {
        LakeTableAddIndexJob job = newJob();
        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getId()).thenReturn(100L);
        MaterializedIndex idx = mock(MaterializedIndex.class);
        when(idx.getId()).thenReturn(50L);
        Tablet tablet = mock(Tablet.class);
        when(tablet.getId()).thenReturn(1L);
        when(idx.getTablets()).thenReturn(Collections.singletonList(tablet));
        when(pp.getAllMaterializedIndices(any())).thenReturn(Collections.singletonList(idx));
        when(table.getAllPhysicalPartitions()).thenReturn(Collections.singletonList(pp));

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        GlobalTransactionMgr txnMgr1 = mockTxnMgrYielding(7777L);
        when(gsm.getGlobalTransactionMgr()).thenReturn(txnMgr1);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            invokePrivate(job, "runPendingJob");
        }
        assertEquals(7777L, getField(job, "watershedTxnId"));
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        Map<Long, List<Long>> p2t = (Map<Long, List<Long>>) getField(job, "partitionToTablets");
        assertEquals(Collections.singletonList(1L), p2t.get(100L));
        Map<Long, Long> t2i = (Map<Long, Long>) getField(job, "tabletToIndexMetaId");
        assertEquals(Long.valueOf(50L), t2i.get(1L));
    }

    @Test
    public void testRunPendingJob_DbMissingThrows() {
        LakeTableAddIndexJob job = newJob();
        GlobalStateMgr gsm = buildLockableGsm(null, null);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runPendingJob"));
        }
    }

    @Test
    public void testRunPendingJob_TableMissingThrows() {
        LakeTableAddIndexJob job = newJob();
        Database db = new Database(2L, "db");
        GlobalStateMgr gsm = buildLockableGsm(db, null);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runPendingJob"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRunPendingJob_NoTabletsSkipsPartition() throws Exception {
        LakeTableAddIndexJob job = newJob();
        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getId()).thenReturn(100L);
        when(pp.getAllMaterializedIndices(any())).thenReturn(new ArrayList<>());
        when(table.getAllPhysicalPartitions()).thenReturn(Collections.singletonList(pp));

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        GlobalTransactionMgr txnMgr2 = mockTxnMgrYielding(42L);
        when(gsm.getGlobalTransactionMgr()).thenReturn(txnMgr2);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            invokePrivate(job, "runPendingJob");
        }
        Map<Long, List<Long>> p2t = (Map<Long, List<Long>>) getField(job, "partitionToTablets");
        assertTrue(p2t.isEmpty());
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
    }

    // -------- runWaitingTxnJob --------

    @Test
    public void testRunWaitingTxnJob_PreviousTxnsNotFinishedReturns() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.WAITING_TXN);
        setField(job, "watershedTxnId", 5L);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        GlobalTransactionMgr txnMgr = mock(GlobalTransactionMgr.class);
        when(txnMgr.isPreviousTransactionsFinished(eq(5L), eq(2L), any())).thenReturn(false);
        when(gsm.getGlobalTransactionMgr()).thenReturn(txnMgr);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            invokePrivate(job, "runWaitingTxnJob");
        }
        assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
    }

    @Test
    public void testRunWaitingTxnJob_PreviousTxnsFinishedTransitionsToRunning() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.WAITING_TXN);
        setField(job, "watershedTxnId", 5L);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        GlobalTransactionMgr txnMgr = mock(GlobalTransactionMgr.class);
        when(txnMgr.isPreviousTransactionsFinished(eq(5L), eq(2L), any())).thenReturn(true);
        when(gsm.getGlobalTransactionMgr()).thenReturn(txnMgr);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            invokePrivate(job, "runWaitingTxnJob");
        }
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
    }

    @Test
    public void testRunWaitingTxnJob_TxnMgrThrowsWrappedAsAlterCancel() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.WAITING_TXN);
        setField(job, "watershedTxnId", 5L);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        GlobalTransactionMgr txnMgr = mock(GlobalTransactionMgr.class);
        when(txnMgr.isPreviousTransactionsFinished(anyLong(), anyLong(), any()))
                .thenThrow(new RuntimeException("boom"));
        when(gsm.getGlobalTransactionMgr()).thenReturn(txnMgr);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runWaitingTxnJob"));
        }
    }

    // -------- runRunningJob --------

    /** AgentTask whose isFinished() returns the given flag. */
    private static AgentTask mockTaskFinished(boolean finished, long backendId, long sig) {
        AgentTask t = mock(AgentTask.class);
        when(t.isFinished()).thenReturn(finished);
        when(t.getBackendId()).thenReturn(backendId);
        when(t.getSignature()).thenReturn(sig);
        return t;
    }

    @Test
    public void testRunRunningJob_BatchTaskInFlightReturns() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.RUNNING);
        AgentBatchTask batch = new AgentBatchTask();
        batch.addTask(mockTaskFinished(false, 1L, 1L));
        setField(job, "batchTask", batch);

        invokePrivate(job, "runRunningJob");
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
    }

    @Test
    public void testRunRunningJob_BatchTaskTimeoutThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.RUNNING);
        // Force the timeout window closed.
        setField(job, "createTimeMs", 0L);
        setField(job, "timeoutMs", 1L);
        AgentBatchTask batch = new AgentBatchTask();
        batch.addTask(mockTaskFinished(false, 1L, 1L));
        setField(job, "batchTask", batch);

        assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runRunningJob"));
    }

    @Test
    public void testRunRunningJob_PartialFailureThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.RUNNING);
        AgentBatchTask batch = new AgentBatchTask();
        batch.addTask(mockTaskFinished(true, 1L, 1L));
        // Force getFinishedTaskNum() < getTaskNum() to simulate the
        // inconsistency the production check guards against. Use
        // doReturn().when() because spy + when() invokes the real method.
        AgentBatchTask spied = Mockito.spy(batch);
        Mockito.doReturn(true).when(spied).isFinished();
        Mockito.doReturn(0).when(spied).getFinishedTaskNum();
        Mockito.doReturn(1).when(spied).getTaskNum();
        setField(job, "batchTask", spied);

        assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runRunningJob"));
    }

    @Test
    public void testRunRunningJob_HappyPathPersistsFinishedRewriting() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.RUNNING);
        Map<Long, List<Long>> p2t = new HashMap<>();
        p2t.put(100L, Collections.singletonList(1L));
        setField(job, "partitionToTablets", p2t);
        Map<Long, Long> t2i = new HashMap<>();
        t2i.put(1L, 50L);
        setField(job, "tabletToIndexMetaId", t2i);

        AgentBatchTask batch = new AgentBatchTask();
        batch.addTask(mockTaskFinished(true, 1L, 1L));
        setField(job, "batchTask", batch);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getNextVersion()).thenReturn(5L);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            invokePrivate(job, "runRunningJob");
        }
        @SuppressWarnings("unchecked")
        Map<Long, Long> commitMap = (Map<Long, Long>) getField(job, "commitVersionMap");
        assertEquals(Long.valueOf(5L), commitMap.get(100L));
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        // updateNextVersion runnable should have flipped the next version on the partition.
        verify(pp).setNextVersion(6L);
    }

    @Test
    public void testRunRunningJob_DbMissingThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.RUNNING);
        AgentBatchTask batch = new AgentBatchTask();
        batch.addTask(mockTaskFinished(true, 1L, 1L));
        setField(job, "batchTask", batch);

        GlobalStateMgr gsm = buildLockableGsm(null, null);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runRunningJob"));
        }
    }

    // -------- runFinishedRewritingJob / lakePublishVersion --------

    /** Pre-populate publishVersionFuture with a completed Boolean future to skip the executor. */
    private static void setPublishFuture(LakeTableAddIndexJob job, boolean value) throws Exception {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        f.complete(value);
        setField(job, "publishVersionFuture", f);
    }

    @Test
    public void testRunFinishedRewritingJob_PublishVersionPendingReturns() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.FINISHED_REWRITING);
        // Future not yet done -> publishVersion() returns false; method returns early.
        CompletableFuture<Boolean> pending = new CompletableFuture<>();
        setField(job, "publishVersionFuture", pending);

        invokePrivate(job, "runFinishedRewritingJob");
        assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
    }

    @Test
    public void testRunFinishedRewritingJob_PublishPendingTimeoutThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.FINISHED_REWRITING);
        setField(job, "createTimeMs", 0L);
        setField(job, "timeoutMs", 1L);
        CompletableFuture<Boolean> pending = new CompletableFuture<>();
        setField(job, "publishVersionFuture", pending);

        assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runFinishedRewritingJob"));
    }

    @Test
    public void testRunFinishedRewritingJob_HappyPath() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.FINISHED_REWRITING);
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 5L);
        setField(job, "commitVersionMap", commit);
        setPublishFuture(job, true);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            invokePrivate(job, "runFinishedRewritingJob");
        }
        verify(pp).setVisibleVersion(eq(5L), anyLong());
        verify(table).setState(OlapTable.OlapTableState.NORMAL);
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
    }

    @Test
    public void testRunFinishedRewritingJob_DbMissingThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "jobState", AlterJobV2.JobState.FINISHED_REWRITING);
        setPublishFuture(job, true);

        GlobalStateMgr gsm = buildLockableGsm(null, null);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "runFinishedRewritingJob"));
        }
    }

    // -------- lakePublishVersion --------

    @Test
    public void testLakePublishVersion_DbMissingReturnsFalse() {
        LakeTableAddIndexJob job = newJob();
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(anyLong())).thenReturn(null);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertFalse(job.lakePublishVersion());
        }
    }

    @Test
    public void testLakePublishVersion_TableMissingReturnsFalse() {
        LakeTableAddIndexJob job = newJob();
        Database db = new Database(2L, "db");
        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(anyLong())).thenReturn(db);
        when(lm.getTable(anyLong(), anyLong())).thenReturn(null);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertFalse(job.lakePublishVersion());
        }
    }

    @Test
    public void testLakePublishVersion_PartitionDisappearedReturnsFalse() throws Exception {
        LakeTableAddIndexJob job = newJob();
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 5L);
        setField(job, "commitVersionMap", commit);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        when(table.getPhysicalPartition(100L)).thenReturn(null);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(anyLong())).thenReturn(db);
        when(lm.getTable(anyLong(), anyLong())).thenReturn(table);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertFalse(job.lakePublishVersion());
        }
    }

    @Test
    public void testLakePublishVersion_HappyPath() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "watershedTxnId", 7L);
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 5L);
        setField(job, "commitVersionMap", commit);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        MaterializedIndex idx = mock(MaterializedIndex.class);
        when(idx.getTablets()).thenReturn(new ArrayList<>());
        when(pp.getLatestBaseIndex()).thenReturn(idx);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(anyLong())).thenReturn(db);
        when(lm.getTable(anyLong(), anyLong())).thenReturn(table);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<Utils> utilsStatic = Mockito.mockStatic(Utils.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            // Utils.publishVersion is a no-op overload accepting (List, TxnInfoPB, long, long, ComputeResource, boolean).
            utilsStatic.when(() -> Utils.publishVersion(any(), any(), anyLong(), anyLong(), any(), anyBoolean()))
                    .thenAnswer(inv -> null);
            assertTrue(job.lakePublishVersion());
            utilsStatic.verify(() -> Utils.publishVersion(any(), any(), eq(4L), eq(5L), any(), anyBoolean()),
                    times(1));
        }
    }

    @Test
    public void testLakePublishVersion_UtilsThrowsReturnsFalse() throws Exception {
        LakeTableAddIndexJob job = newJob();
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 5L);
        setField(job, "commitVersionMap", commit);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        MaterializedIndex idx = mock(MaterializedIndex.class);
        when(idx.getTablets()).thenReturn(new ArrayList<>());
        when(pp.getLatestBaseIndex()).thenReturn(idx);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        LocalMetastore lm = mock(LocalMetastore.class);
        when(lm.getDb(anyLong())).thenReturn(db);
        when(lm.getTable(anyLong(), anyLong())).thenReturn(table);
        when(gsm.getLocalMetastore()).thenReturn(lm);
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<Utils> utilsStatic = Mockito.mockStatic(Utils.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            utilsStatic.when(() -> Utils.publishVersion(any(), any(), anyLong(), anyLong(), any(), anyBoolean()))
                    .thenThrow(new RuntimeException("rpc fail"));
            assertFalse(job.lakePublishVersion());
        }
    }

    // -------- dispatchAllTasks / updateNextVersion --------

    @Test
    public void testDispatchAllTasks_HappyPath() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "batchTask", new AgentBatchTask());
        Map<Long, List<Long>> p2t = new HashMap<>();
        p2t.put(100L, Collections.singletonList(1L));
        setField(job, "partitionToTablets", p2t);
        Map<Long, Long> t2i = new HashMap<>();
        t2i.put(1L, 50L);
        setField(job, "tabletToIndexMetaId", t2i);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getVisibleVersion()).thenReturn(3L);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);
        when(table.getIndexMetaByMetaId(50L)).thenReturn(null);

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        WarehouseManager wm = mock(WarehouseManager.class);
        ComputeNode cn = mock(ComputeNode.class);
        when(cn.getId()).thenReturn(11L);
        when(wm.getComputeNodeAssignedToTablet(any(), eq(1L))).thenReturn(cn);
        when(gsm.getWarehouseMgr()).thenReturn(wm);

        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        when(schemaInfo.toTabletSchema()).thenReturn(new TTabletSchema());

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<SchemaInfo> siStatic = Mockito.mockStatic(SchemaInfo.class);
                MockedStatic<AgentTaskQueue> queueStatic = Mockito.mockStatic(AgentTaskQueue.class);
                MockedStatic<AgentTaskExecutor> execStatic = Mockito.mockStatic(AgentTaskExecutor.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            siStatic.when(() -> SchemaInfo.fromMaterializedIndex(eq(table), eq(50L), any()))
                    .thenReturn(schemaInfo);
            invokePrivate(job, "dispatchAllTasks");
            queueStatic.verify(() -> AgentTaskQueue.addBatchTask(any()), times(1));
            execStatic.verify(() -> AgentTaskExecutor.submit(any()), times(1));
        }
        AgentBatchTask batch = (AgentBatchTask) getField(job, "batchTask");
        assertEquals(1, batch.getTaskNum());
    }

    @Test
    public void testDispatchAllTasks_DbMissingThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "batchTask", new AgentBatchTask());
        GlobalStateMgr gsm = buildLockableGsm(null, null);
        when(gsm.getWarehouseMgr()).thenReturn(mock(WarehouseManager.class));
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "dispatchAllTasks"));
        }
    }

    @Test
    public void testDispatchAllTasks_TableMissingThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "batchTask", new AgentBatchTask());
        Database db = new Database(2L, "db");
        GlobalStateMgr gsm = buildLockableGsm(db, null);
        when(gsm.getWarehouseMgr()).thenReturn(mock(WarehouseManager.class));
        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "dispatchAllTasks"));
        }
    }

    @Test
    public void testDispatchAllTasks_PartitionDisappearedThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "batchTask", new AgentBatchTask());
        Map<Long, List<Long>> p2t = new HashMap<>();
        p2t.put(100L, Collections.singletonList(1L));
        setField(job, "partitionToTablets", p2t);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        when(table.getPhysicalPartition(100L)).thenReturn(null);
        GlobalStateMgr gsm = buildLockableGsm(db, table);
        when(gsm.getWarehouseMgr()).thenReturn(mock(WarehouseManager.class));

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "dispatchAllTasks"));
        }
    }

    @Test
    public void testDispatchAllTasks_NoComputeNodeThrows() throws Exception {
        LakeTableAddIndexJob job = newJob();
        setField(job, "batchTask", new AgentBatchTask());
        Map<Long, List<Long>> p2t = new HashMap<>();
        p2t.put(100L, Collections.singletonList(1L));
        setField(job, "partitionToTablets", p2t);
        Map<Long, Long> t2i = new HashMap<>();
        t2i.put(1L, 50L);
        setField(job, "tabletToIndexMetaId", t2i);

        Database db = new Database(2L, "db");
        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp = mock(PhysicalPartition.class);
        when(pp.getVisibleVersion()).thenReturn(3L);
        when(table.getPhysicalPartition(100L)).thenReturn(pp);

        GlobalStateMgr gsm = buildLockableGsm(db, table);
        WarehouseManager wm = mock(WarehouseManager.class);
        when(wm.getComputeNodeAssignedToTablet(any(), eq(1L))).thenReturn(null);
        when(gsm.getWarehouseMgr()).thenReturn(wm);

        try (MockedStatic<GlobalStateMgr> gsmStatic = Mockito.mockStatic(GlobalStateMgr.class)) {
            gsmStatic.when(GlobalStateMgr::getCurrentState).thenReturn(gsm);
            assertThrows(AlterCancelException.class, () -> invokePrivate(job, "dispatchAllTasks"));
        }
    }

    @Test
    public void testUpdateNextVersion_BumpsAllPartitions() throws Exception {
        LakeTableAddIndexJob job = newJob();
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 5L);
        commit.put(101L, 7L);
        setField(job, "commitVersionMap", commit);

        OlapTable table = mock(OlapTable.class);
        PhysicalPartition pp1 = mock(PhysicalPartition.class);
        PhysicalPartition pp2 = mock(PhysicalPartition.class);
        when(table.getPhysicalPartition(100L)).thenReturn(pp1);
        when(table.getPhysicalPartition(101L)).thenReturn(pp2);

        Method m = LakeTableIndexFastPathJobBase.class.getDeclaredMethod("updateNextVersion", OlapTable.class);
        m.setAccessible(true);
        m.invoke(job, table);

        verify(pp1).setNextVersion(6L);
        verify(pp2).setNextVersion(8L);
    }

    @Test
    public void testUpdateNextVersion_SkipsMissingPartition() throws Exception {
        LakeTableAddIndexJob job = newJob();
        Map<Long, Long> commit = new HashMap<>();
        commit.put(100L, 5L);
        setField(job, "commitVersionMap", commit);

        OlapTable table = mock(OlapTable.class);
        when(table.getPhysicalPartition(100L)).thenReturn(null);

        Method m = LakeTableIndexFastPathJobBase.class.getDeclaredMethod("updateNextVersion", OlapTable.class);
        m.setAccessible(true);
        m.invoke(job, table); // must not NPE
        // Nothing further to verify; the no-op must not throw.
        assertNotNull(table);
    }

    // -------- existing tests below --------

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
