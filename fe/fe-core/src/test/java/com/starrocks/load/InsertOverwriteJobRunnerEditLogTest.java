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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.journal.JournalWriteException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.common.DmlException;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class InsertOverwriteJobRunnerEditLogTest {
    private static final String DB_NAME = "test_insert_overwrite_editlog";
    private static final String TABLE_NAME = "t1";

    private static final long INDEX_ID = 41007L;
    // Backend id used by the single replica of every tablet built in this test. The
    // replicas exist so that markTabletForceDelete has backend ids to record; they are
    // deliberately NOT registered in the tablet inverted index.
    private static final long BACKEND_ID = 20001L;

    private Database db;
    private OlapTable table;
    private long oldPartitionRetentionSecs;
    private long dbId;
    private long tableId;
    private long sourcePartitionId;
    private long sourcePhysicalPartitionId;
    private long tempPartitionId;
    private long tempPhysicalPartitionId;
    private long sourceTabletId;
    private long tempTabletId;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        oldPartitionRetentionSecs = Config.partition_recycle_retention_period_secs;
        Config.partition_recycle_retention_period_secs = 0;
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        dbId = GlobalStateMgr.getCurrentState().getNextId();
        tableId = GlobalStateMgr.getCurrentState().getNextId();
        sourcePartitionId = GlobalStateMgr.getCurrentState().getNextId();
        sourcePhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        tempPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        tempPhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        sourceTabletId = GlobalStateMgr.getCurrentState().getNextId();
        tempTabletId = GlobalStateMgr.getCurrentState().getNextId();

        db = new Database(dbId, DB_NAME);
        metastore.unprotectCreateDb(db);
        table = createHashOlapTable(dbId, tableId, TABLE_NAME, sourcePartitionId, sourcePhysicalPartitionId,
                sourceTabletId);
        db.registerTableUnlocked(table);
        addTempPartition(dbId, table, sourcePartitionId, tempPartitionId, tempPhysicalPartitionId, tempTabletId,
                TABLE_NAME + "_tmp");
    }

    @AfterEach
    public void tearDown() {
        Config.partition_recycle_retention_period_secs = oldPartitionRetentionSecs;
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * A tablet reshard (a pre-split job, or the background split/merge daemon) holds
     * {@code TABLET_RESHARD} on the table until it finishes, and it cannot be aborted once it has left
     * PENDING. Failing the commit for that would throw away everything the overwrite already wrote,
     * so doCommit waits it out within the load's own remaining budget and then commits normally.
     */
    @Test
    public void testDoCommitWaitsOutTabletReshard() throws Exception {
        InsertOverwriteJob job = new InsertOverwriteJob(2101L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job, contextWithBudget(3600), null);

        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        Thread restorer = new Thread(() -> {
            try {
                Thread.sleep(1200);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            // Taking the table's WRITE lock here also proves doCommit does not hold it while waiting:
            // a real reshard job needs that lock to restore NORMAL, so holding it would deadlock.
            Locker locker = new Locker();
            Assertions.assertTrue(locker.lockTableAndCheckDbExist(db, table.getId(), LockType.WRITE));
            try {
                table.setState(OlapTable.OlapTableState.NORMAL);
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }
        }, "reshard-state-restorer");
        restorer.start();
        try {
            runner.doCommit();
        } finally {
            restorer.join();
            table.setState(OlapTable.OlapTableState.NORMAL);
        }

        Partition replaced = table.getPartition(TABLE_NAME);
        Assertions.assertNotNull(replaced);
        Assertions.assertEquals(tempPartitionId, replaced.getId());
        Assertions.assertFalse(table.existTempPartitions());
    }

    /**
     * Only {@code TABLET_RESHARD} is safe to wait out. An alter state can change the schema or index
     * metadata this job's temporary partition and already-built insert plan were derived from, so
     * committing after it finished could target a layout that no longer matches -- those keep failing
     * immediately, even when the load has budget left.
     */
    @Test
    public void testDoCommitFailsFastWhileTableIsAltering() {
        InsertOverwriteJob job = new InsertOverwriteJob(2102L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job, contextWithBudget(3600), null);

        for (OlapTable.OlapTableState altering : Lists.newArrayList(
                OlapTable.OlapTableState.SCHEMA_CHANGE, OlapTable.OlapTableState.ROLLUP)) {
            table.setState(altering);
            try {
                long startMs = System.currentTimeMillis();
                DmlException ex = Assertions.assertThrows(DmlException.class, runner::doCommit);
                long elapsedMs = System.currentTimeMillis() - startMs;
                assertTableStateFailure(ex);
                Assertions.assertTrue(elapsedMs < 2000,
                        "must not wait for state " + altering + ", waited " + elapsedMs + "ms");
            } finally {
                table.setState(OlapTable.OlapTableState.NORMAL);
            }
        }
        Assertions.assertTrue(table.existTempPartitions(), "a failed commit must not swap partitions");
    }

    /**
     * A reshard that outlasts the load's own budget still fails, with exactly the pre-existing error:
     * the wait only ever delays that failure, it never introduces a different one.
     */
    @Test
    public void testDoCommitGivesUpOnReshardingTableWhenBudgetIsExhausted() {
        InsertOverwriteJob job = new InsertOverwriteJob(2103L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));
        // Started two hours ago with a one-second budget: the deadline is long past.
        ConnectContext context = contextWithBudget(1);
        context.setStartTime(Instant.now().minus(Duration.ofHours(2)));
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job, context, null);

        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        try {
            long startMs = System.currentTimeMillis();
            DmlException ex = Assertions.assertThrows(DmlException.class, runner::doCommit);
            long elapsedMs = System.currentTimeMillis() - startMs;
            assertTableStateFailure(ex);
            Assertions.assertTrue(elapsedMs < 2000, "budget was exhausted, waited " + elapsedMs + "ms");
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertTrue(table.existTempPartitions(), "a failed commit must not swap partitions");
    }

    @Test
    public void testDoCommitGivesUpOnReshardingTableWhenTheStatementIsCancelled() {
        // A cancelled statement must not keep the request alive for the rest of its budget. By this
        // point the data is written and the coordinator has finished, so nothing else would interrupt
        // this thread -- only the explicit check does.
        InsertOverwriteJob job = new InsertOverwriteJob(2104L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));
        // Enough budget left that returning quickly can only be the cancellation check, but not so much
        // that losing the check would hang this test instead of failing it.
        ConnectContext context = contextWithBudget(60);
        // The real cancellation route, not setKilled(): kill(false, ...) is what KILL QUERY, a cancelled
        // MV TaskRun and a closed client all use, and it deliberately leaves isKilled false.
        context.setExecutor(new StmtExecutor(context, mock(InsertStmt.class)));
        context.kill(false, "kill TaskRun");
        Assertions.assertFalse(context.isKilled(), "kill(false, ...) must not set the connection flag");
        Assertions.assertTrue(context.isStatementCancelled(), "the statement must still read as cancelled");
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job, context, null);

        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        try {
            long startMs = System.currentTimeMillis();
            DmlException ex = Assertions.assertThrows(DmlException.class, runner::doCommit);
            long elapsedMs = System.currentTimeMillis() - startMs;
            assertTableStateFailure(ex);
            Assertions.assertTrue(elapsedMs < 2000,
                    "a cancelled statement must not wait out the reshard, waited " + elapsedMs + "ms");
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertTrue(table.existTempPartitions(), "a failed commit must not swap partitions");
    }

    /** A context whose remaining budget is {@code timeoutSeconds} from now. */
    private static ConnectContext contextWithBudget(int timeoutSeconds) {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.getSessionVariable().setQueryTimeoutS(timeoutSeconds);
        context.setStartTime(Instant.now());
        return context;
    }

    /**
     * doCommit wraps everything thrown inside its locked section into
     * {@code DmlException("replace partitions failed", cause)}, so the table-state message can be at
     * either level depending on where it was raised.
     */
    private static void assertTableStateFailure(DmlException thrown) {
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains("table state is")) {
                return;
            }
        }
        Assertions.fail("expected the unchanged table-state error, got: " + thrown.getMessage());
    }

    @Test
    public void testDoCommitAndReplayCommitWithEditLog() throws Exception {
        InsertOverwriteJob job = new InsertOverwriteJob(2002L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        runner.doCommit();

        Partition replaced = table.getPartition(TABLE_NAME);
        Assertions.assertNotNull(replaced);
        Assertions.assertEquals(tempPartitionId, replaced.getId());
        Assertions.assertFalse(table.existTempPartitions());

        resetTableForReplay();

        InsertOverwriteStateChangeInfo info = (InsertOverwriteStateChangeInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE);
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_SUCCESS, info.getToState());
        Assertions.assertEquals(Lists.newArrayList(sourcePartitionId), info.getSourcePartitionIds());
        Assertions.assertEquals(Lists.newArrayList(tempPartitionId), info.getTmpPartitionIds());

        runner.replayStateChange(info);

        Partition replayed = table.getPartition(TABLE_NAME);
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(tempPartitionId, replayed.getId());
        Assertions.assertFalse(table.existTempPartitions());
    }

    @Test
    public void testGcAndReplayGCWithEditLog() throws Exception {
        InsertOverwriteJob job = new InsertOverwriteJob(2004L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        runner.gc();
        Assertions.assertFalse(table.existTempPartitions());

        addTempPartition(dbId, table, sourcePartitionId, tempPartitionId, tempPhysicalPartitionId, tempTabletId,
                TABLE_NAME + "_tmp");

        InsertOverwriteStateChangeInfo info = (InsertOverwriteStateChangeInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE);
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, info.getToState());
        Assertions.assertEquals(Lists.newArrayList(tempPartitionId), info.getTmpPartitionIds());
        runner.replayStateChange(info);

        Assertions.assertFalse(table.existTempPartitions());
    }

    @Test
    public void testDoCommitMarksSourceTabletsForceDelete() {
        InsertOverwriteJob job = new InsertOverwriteJob(2007L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        runner.doCommit();

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        // the swapped-out source tablets must be marked so BE drops them directly
        // instead of moving them to trash
        Assertions.assertTrue(invertedIndex.tabletForceDelete(sourceTabletId, BACKEND_ID));
        // the tablets of the new (previously temp) partition are live and must never
        // be marked
        Assertions.assertFalse(invertedIndex.tabletForceDelete(tempTabletId, BACKEND_ID));
    }

    @Test
    public void testPostCommitFailureDoesNotFailCommittedJob() {
        InsertStmt insertStmt = mock(InsertStmt.class);
        when(insertStmt.getTargetPartitionIds()).thenReturn(Lists.newArrayList(sourcePartitionId));
        // doCommit's critical section never touches insertStmt for a non-dynamic job,
        // so this throws exactly inside postCommit's statistics collection
        when(insertStmt.getTxnId()).thenThrow(new RuntimeException("injected post-commit failure"));

        InsertOverwriteJob job = new InsertOverwriteJob(2008L, insertStmt, db.getId(), table.getId(),
                0L, false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job, null, null);
        // the swap is journaled before post-commit work runs; a post-commit failure
        // must not escape doCommit, otherwise the job would be routed to gc(), which
        // would drop the partitions that just became live
        Assertions.assertDoesNotThrow(runner::doCommit);

        Partition replaced = table.getPartition(TABLE_NAME);
        Assertions.assertNotNull(replaced);
        Assertions.assertEquals(tempPartitionId, replaced.getId());
        Assertions.assertFalse(table.existTempPartitions());
        // marking precedes the failing statistics collection, so it must have happened
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                .tabletForceDelete(sourceTabletId, BACKEND_ID));
    }

    @Test
    public void testGcAbortsDynamicTxnWhenTableDropped() throws Exception {
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                dbId, Lists.newArrayList(tableId), "test_dynamic_overwrite_gc_abort",
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                TransactionState.LoadJobSourceType.INSERT_STREAMING, 600);

        InsertOverwriteJob job = new InsertOverwriteJob(2009L, db.getId(), table.getId(), null, true);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_FAILED);
        job.setTxnId(txnId);

        // drop the target table so gc() hits the early table-not-exist exit
        db.dropTable(TABLE_NAME);

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        Assertions.assertThrows(DmlException.class, runner::gc);

        // the PREPARE transaction must not leak until the txn timeout checker reaps
        // it: gc() aborts it even though the table is gone
        TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionState(dbId, txnId);
        Assertions.assertNotNull(txnState);
        Assertions.assertEquals(TransactionStatus.ABORTED, txnState.getTransactionStatus());
    }

    @Test
    public void testDoCommitEditLogException() {
        InsertOverwriteJob job = new InsertOverwriteJob(2005L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInsertOverwriteStateChange(any(InsertOverwriteStateChangeInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
            DmlException exception = Assertions.assertThrows(DmlException.class, runner::doCommit);
            Assertions.assertTrue(exception.getMessage().contains("replace partitions failed"));

            Partition sourcePartition = table.getPartition(TABLE_NAME);
            Assertions.assertNotNull(sourcePartition);
            Assertions.assertEquals(sourcePartitionId, sourcePartition.getId());
            Assertions.assertTrue(table.existTempPartitions());

            // a failed commit must leave no force-delete marks: the source tablets are
            // still live, and a stale mark would make BE skip trash on a later
            // legitimate drop of these tablets
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
            Assertions.assertFalse(invertedIndex.tabletForceDelete(sourceTabletId, BACKEND_ID));
            Assertions.assertFalse(invertedIndex.tabletForceDelete(tempTabletId, BACKEND_ID));
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testDoCommitEditLogExceptionPreservesJournalFailureMessage() {
        InsertOverwriteJob job = new InsertOverwriteJob(2007L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_RUNNING);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));
        job.setSourcePartitionNames(Lists.newArrayList(TABLE_NAME));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(new EditLog(null));
        JournalWriteException journalException = new JournalWriteException(
                JournalWriteException.Reason.ADMISSION_CLOSED,
                "leader is demoting, submit log is not allowed");
        doThrow(new IllegalStateException(journalException))
                .when(spyEditLog).logInsertOverwriteStateChange(any(InsertOverwriteStateChangeInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
            DmlException exception = Assertions.assertThrows(DmlException.class, runner::doCommit);
            Assertions.assertTrue(exception.getMessage().contains("replace partitions failed"));
            Assertions.assertTrue(exception.getMessage().contains("leader is demoting"),
                    "error msg must expose journal failure reason, got: " + exception.getMessage());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testGcEditLogException() {
        InsertOverwriteJob job = new InsertOverwriteJob(2006L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);
        job.setJobState(InsertOverwriteJobState.OVERWRITE_FAILED);
        job.setTmpPartitionIds(Lists.newArrayList(tempPartitionId));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInsertOverwriteStateChange(any(InsertOverwriteStateChangeInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
            Assertions.assertDoesNotThrow(runner::gc);
            Assertions.assertTrue(table.existTempPartitions());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    /**
     * A runner rebuilt for replay has no plan behind it, so it must report no estimate at all. A
     * derived boundary source sizes its split off this number, and a leftover non-zero value would
     * make it carve tablets for data the replay is not loading.
     */
    @Test
    public void testReplayRunnerHasZeroOutputEstimates() {
        InsertOverwriteJob job = new InsertOverwriteJob(2010L, db.getId(), table.getId(),
                Lists.newArrayList(sourcePartitionId), false);

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);

        Assertions.assertEquals(Estimates.ZERO, runner.getOutputEstimates());
    }

    private static OlapTable createHashOlapTable(long dbId, long tableId, String tableName, long partitionId,
                                                 long physicalPartitionId, long tabletId) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("k1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("k2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(new Replica(tabletId + 10000L, BACKEND_ID, Replica.ReplicaState.NORMAL, 1L, 0), false);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, physicalPartitionId, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.setDefaultDistributionInfo(distributionInfo);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static void addTempPartition(long dbId, OlapTable table, long sourcePartitionId, long partitionId,
                                         long physicalPartitionId, long tabletId, String partitionName) {
        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(new Replica(tabletId + 10000L, BACKEND_ID, Replica.ReplicaState.NORMAL, 1L, 0), false);
        TabletMeta tabletMeta = new TabletMeta(dbId, table.getId(), partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Partition partition = new Partition(partitionId, physicalPartitionId, partitionName, baseIndex,
                distributionInfo);

        table.addTempPartition(partition);
        table.getPartitionInfo().addPartition(partitionId,
                table.getPartitionInfo().getDataProperty(sourcePartitionId),
                table.getPartitionInfo().getReplicationNum(sourcePartitionId));
    }

    private void resetTableForReplay() {
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(sourcePartitionId);
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(tempPartitionId);
        table.dropPartition(dbId, TABLE_NAME, true);
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(tempPartitionId);

        Partition sourcePartition = buildPartition(dbId, table, sourcePartitionId, sourcePhysicalPartitionId,
                sourceTabletId, TABLE_NAME);
        table.addPartition(sourcePartition);
        table.getPartitionInfo().addPartition(sourcePartitionId,
                com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY, (short) 1);

        addTempPartition(dbId, table, sourcePartitionId, tempPartitionId, tempPhysicalPartitionId, tempTabletId,
                TABLE_NAME + "_tmp");
    }

    private static Partition buildPartition(long dbId, OlapTable table, long partitionId, long physicalPartitionId,
                                            long tabletId, String partitionName) {
        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(new Replica(tabletId + 10000L, BACKEND_ID, Replica.ReplicaState.NORMAL, 1L, 0), false);
        TabletMeta tabletMeta = new TabletMeta(dbId, table.getId(), partitionId, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        return new Partition(partitionId, physicalPartitionId, partitionName, baseIndex, distributionInfo);
    }
}
