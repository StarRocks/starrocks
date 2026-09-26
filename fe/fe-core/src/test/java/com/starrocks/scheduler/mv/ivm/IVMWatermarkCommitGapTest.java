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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.load.loadv2.IVMInsertLoadTxnCallback;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * IVM applies each base-table window {@code (from, to]} exactly once, and the only thing standing
 * between "applied once" and "applied twice" is the bookmark. The MV data lands in the INSERT txn's
 * journal record; the bookmark advances in a separate one written afterwards by
 * {@link IVMInsertLoadTxnCallback#afterCommitted}, which can be lost to a WAL-gate close on leader
 * demotion or to a process death between the two writes.
 *
 * <p>These pin the recovery: the window rides the txn record, so a node replaying that record
 * promotes the bookmark itself and the next run does not re-consume what the last one applied.
 */
public class IVMWatermarkCommitGapTest extends MVIVMIcebergTestBase {
    private static final String BASE_TABLE = "iceberg0.unpartitioned_db.t0";
    private static final String MV_QUERY = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";

    @Test
    public void testConsumedWindowRidesTheTransactionRecord() throws Exception {
        TransactionState txnState = insertTxnState();
        commitFirstWindow(txnState);

        Map<BaseTableInfo, TvrVersionRange> window =
                attachmentOf(txnState).getIvmRefreshCommitInfo().getCommittedTvrMap();
        Assertions.assertNotNull(window, "beforeCommitted must record the window on the txn attachment");
        Assertions.assertEquals(TvrTableSnapshot.of(1L), window.get(getMv("test_mv1").getBaseTableInfos().get(0)),
                "the recorded window must end where run 1 consumed up to");
    }

    @Test
    public void testReplayPromotesBookmarkWhenBookmarkWriteIsLost() throws Exception {
        TransactionState txnState = insertTxnState();
        commitFirstWindow(txnState);
        // afterCommitted is deliberately not called: the MV holds (0, 1] but the bookmark write never lands.
        Assertions.assertEquals(TvrTableSnapshot.of(0L), committedBookmark(),
                "without the bookmark write the committed map is still at 0");

        replayingNode().replayOnCommitted(txnState);

        Assertions.assertEquals(TvrTableSnapshot.of(1L), committedBookmark(),
                "replaying the txn record must promote the bookmark the lost write would have");
        Assertions.assertEquals(Map.of("start", "1", "end", "2"), consumedWindow(secondRun()),
                "run 1 already applied (0, 1]; run 2 must start at 1, or the MV counts that window twice");
    }

    /**
     * The control for the test above: the same two runs, differing only in whether the bookmark's
     * journal record lands. It must stay green, otherwise the assertion above would be measuring
     * something other than the lost write.
     */
    @Test
    public void testBookmarkIsNotReconsumedWhenBookmarkWriteLands() throws Exception {
        TransactionState txnState = insertTxnState();
        commitFirstWindow(txnState).afterCommitted(txnState);

        Assertions.assertEquals(TvrTableSnapshot.of(1L), committedBookmark(),
                "afterCommitted should advance the bookmark to the end of the applied window");
        Assertions.assertEquals(Map.of("start", "1", "end", "2"), consumedWindow(secondRun()),
                "with the bookmark at 1, run 2 consumes only what run 1 did not");
    }

    /**
     * The replaying node reads the record back off the journal, so the window is only recoverable if it
     * survives serialization -- a complex map key and a polymorphic value, both of which gson drops or
     * mangles without the right builder settings.
     */
    @Test
    public void testConsumedWindowSurvivesTheJournal() throws Exception {
        TransactionState txnState = insertTxnState();
        commitFirstWindow(txnState);

        TransactionState fromJournal =
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(txnState), TransactionState.class);
        replayingNode().replayOnCommitted(fromJournal);

        Assertions.assertInstanceOf(TvrTableSnapshot.class, committedBookmark(),
                "the bookmark must come back as a TvrTableSnapshot; planning rejects any other subtype");
        Assertions.assertEquals(TvrTableSnapshot.of(1L), committedBookmark(),
                "the window must survive the round trip the replaying node reads it from");
    }

    /**
     * The commit path does not persist the state object the callback mutated, it persists a copy taken
     * right after. The window has to be on that copy or it never reaches the journal at all.
     */
    @Test
    public void testWindowSurvivesTheCopyTheCommitPathRecords() throws Exception {
        TransactionState txnState = insertTxnState();
        commitFirstWindow(txnState);

        replayingNode().replayOnCommitted(new TransactionState(txnState));

        Assertions.assertEquals(TvrTableSnapshot.of(1L), committedBookmark(),
                "the copy the commit path records must carry the window beforeCommitted attached");
    }

    @Test
    public void testCommitFailsWhenTheRecordCannotCarryTheWindow() throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(MV_QUERY, "incremental");
        seedTvrBaselineAtVersionZero(mv);
        advanceTableVersionTo(1L);
        mockListTableDeltaTraitsPerVersion(0L, 0L);
        refresh(mv);

        IVMInsertLoadTxnCallback callback =
                new IVMInsertLoadTxnCallback(mv.getMvId().getDbId(), mv.getId());
        Assertions.assertThrows(TransactionException.class,
                () -> callback.beforeCommitted(new TransactionState()),
                "a window with nowhere to be recorded must abort the commit rather than commit unrecorded");
    }

    /**
     * A demoting node advances its replay position past its own writes, so it is the one node replay
     * cannot reach; its memory has to converge here instead.
     */
    @Test
    public void testBookmarkConvergesInMemoryWhenTheSchemeWriteFails() throws Exception {
        TransactionState txnState = insertTxnState();
        IVMInsertLoadTxnCallback callback = commitFirstWindow(txnState);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("leader WAL gate is closed"))
                .when(spyEditLog).logMvChangeRefreshScheme(any(ChangeMaterializedViewRefreshSchemeLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        try {
            Assertions.assertThrows(RuntimeException.class, () -> callback.afterCommitted(txnState),
                    "the write failure must still reach the caller");
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }

        Assertions.assertEquals(TvrTableSnapshot.of(1L), committedBookmark(),
                "the bookmark is already durable on the txn record, so memory must converge to it");
    }

    /** An exception out of replay would stop the replaying node's journal, so a dropped MV must be silent. */
    @Test
    public void testReplayToleratesDroppedMaterializedView() throws Exception {
        TransactionState txnState = insertTxnState();
        commitFirstWindow(txnState);
        InsertLoadJob replayJob = replayingNode();

        starRocksAssert.dropMaterializedView("test_mv1");

        Assertions.assertDoesNotThrow(() -> replayJob.replayOnCommitted(txnState));
    }

    /**
     * The window is dead weight once its record is durable -- replay reads it off the journal, not off this
     * object -- and a committed attachment lingers in memory until its transaction state is evicted.
     */
    @Test
    public void testCommitInfoIsDroppedOnceItsRecordIsDurable() throws Exception {
        TransactionState txnState = insertTxnState();
        IVMInsertLoadTxnCallback callback = commitFirstWindow(txnState);
        Assertions.assertNotNull(attachmentOf(txnState).getIvmRefreshCommitInfo(),
                "the window should still be on the attachment the commit path is about to record");

        callback.afterCommitted(txnState);

        Assertions.assertNull(attachmentOf(txnState).getIvmRefreshCommitInfo(),
                "afterCommitted should let the window go once the COMMITTED record carries it");
    }

    /** A txn record written before this change, or by any non-IVM insert, carries no window. */
    @Test
    public void testReplayIgnoresTransactionRecordWithoutWindow() throws Exception {
        commitFirstWindow(insertTxnState());
        TransactionState plainInsert = insertTxnState();

        replayingNode().replayOnCommitted(plainInsert);

        Assertions.assertEquals(TvrTableSnapshot.of(0L), committedBookmark(),
                "a txn record with no window must leave the bookmark alone");
    }

    /**
     * Run 1 over base version 1, up to and including the INSERT txn commit. Returns the callback that
     * captured the window; a fresh one would have nothing to promote.
     */
    private IVMInsertLoadTxnCallback commitFirstWindow(TransactionState txnState) throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(MV_QUERY, "incremental");
        seedTvrBaselineAtVersionZero(mv);

        advanceTableVersionTo(1L);
        mockListTableDeltaTraitsPerVersion(0L, 0L);
        Assertions.assertEquals(Map.of("start", "0", "end", "1"), consumedWindow(refresh(mv)),
                "run 1 should consume (0, 1]");

        IVMInsertLoadTxnCallback callback =
                new IVMInsertLoadTxnCallback(mv.getMvId().getDbId(), mv.getId());
        callback.beforeCommitted(txnState);
        return callback;
    }

    /** The load job as a node other than the committing leader rebuilt it: no live callback, ids only. */
    private InsertLoadJob replayingNode() {
        MaterializedView mv = getMv("test_mv1");
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getLoadJobType()).thenReturn(TLoadJobType.INSERT_QUERY);
        return new InsertLoadJob("replay", mv.getDbId(), mv.getId(), 1L, "load_id", "test_user",
                System.currentTimeMillis(), 3600L, 0L, false, coordinator, null);
    }

    private static InsertTxnCommitAttachment attachmentOf(TransactionState txnState) {
        return (InsertTxnCommitAttachment) txnState.getTxnCommitAttachment();
    }

    private MVTaskRunProcessor secondRun() throws Exception {
        advanceTableVersionTo(2L);
        mockListTableDeltaTraitsPerVersion(0L, 0L);
        return refresh(getMv("test_mv1"));
    }

    private MVTaskRunProcessor refresh(MaterializedView mv) throws Exception {
        MVTaskRunProcessor processor = getMVTaskRunProcessor(mv);
        Assertions.assertInstanceOf(MVIVMRefreshProcessor.class, processor.getMVRefreshProcessor());
        return processor;
    }

    private static Map<String, String> consumedWindow(MVTaskRunProcessor processor) {
        MVTaskRunExtraMessage extraMessage =
                processor.getMvTaskRunContext().getStatus().getMvTaskRunExtraMessage();
        return extraMessage.getImvSourceVersionRange().get(BASE_TABLE);
    }

    private TvrVersionRange committedBookmark() {
        MaterializedView mv = getMv("test_mv1");
        Map<BaseTableInfo, TvrVersionRange> committed = mv.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap();
        return committed.get(mv.getBaseTableInfos().get(0));
    }
}
