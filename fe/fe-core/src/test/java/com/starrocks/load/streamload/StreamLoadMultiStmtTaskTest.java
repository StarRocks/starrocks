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

package com.starrocks.load.streamload;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.ExplicitTxnState;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TransactionStmtExecutor;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StreamLoadMultiStmtTaskTest {
    private Database db;
    private StreamLoadMultiStmtTask multiTask;

    @BeforeEach
    public void setUp() {
        db = new Database(1L, "test_db");
        multiTask = new StreamLoadMultiStmtTask(1L, db, "label_multi", "u", "127.0.0.1",
                1000L, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    public void testExecuteTaskNoSubTask() {
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.executeTask(0, "unknown", null, resp));
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testPrepareChannelNoSubTask() {
        TransactionResult resp = new TransactionResult();
        multiTask.prepareChannel(0, "unknown", null, resp);
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testCommitTxnEmpty() throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(null, resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
    }

    @Test
    public void testManualCancelTask() throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        Assertions.assertTrue(multiTask.endTimeMs() > 0);
    }

    @Test
    public void testBeginTxnSetsExecutionIdAndResource() throws Exception {
        TUniqueId expectedLoadId = (TUniqueId) Deencapsulation.getField(multiTask, "loadId");
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                Assertions.assertNotNull(ctx.getExecutionId());
                String label = DebugUtil.printId(ctx.getExecutionId());
                Assertions.assertFalse(label.isEmpty());
                Assertions.assertEquals(expectedLoadId.getHi(), ctx.getExecutionId().getHi());
                Assertions.assertEquals(expectedLoadId.getLo(), ctx.getExecutionId().getLo());
                Assertions.assertEquals(WarehouseManager.DEFAULT_RESOURCE,
                        ctx.getCurrentComputeResource());
                Assertions.assertEquals("label_multi", labelOverride);
                ctx.setTxnId(987654321L);
            }
        };
        TransactionResult resp = new TransactionResult();
        multiTask.beginTxn(resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals(987654321L, multiTask.getTxnId());
    }

    @Test
    public void testCheckNeedRemoveAndDurable() throws Exception {
        Assertions.assertFalse(multiTask.checkNeedRemove(System.currentTimeMillis(), false));
        StreamLoadTask sub = new StreamLoadTask(2L, db, new OlapTable(), "label_sub", "u",
                "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(sub, "state", StreamLoadTask.State.FINISHED);
        @SuppressWarnings("unchecked")
        java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask,
                        "taskMaps");
        map.put("tbl", sub);
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.FINISHED);
        Deencapsulation.setField(multiTask, "endTimeMs",
                System.currentTimeMillis()
                        - (Config.stream_load_task_keep_max_second * 1000L + 10));
        Assertions.assertTrue(multiTask.isFinalState());
        Assertions.assertTrue(multiTask.checkNeedRemove(System.currentTimeMillis(), false));
    }

    @Test
    public void testToThriftAndStreamLoadThriftEmpty() {
        Assertions.assertTrue(multiTask.toThrift().isEmpty());
        Assertions.assertTrue(multiTask.toStreamLoadThrift().isEmpty());
    }

    @Test
    public void testCallbackDelegations() throws Exception {
        StreamLoadTask sub1 = new StreamLoadTask(3L, db, new OlapTable(), "l1", "u",
                "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        StreamLoadTask sub2 = new StreamLoadTask(4L, db, new OlapTable(), "l2", "u",
                "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        @SuppressWarnings("unchecked")
        java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask,
                        "taskMaps");
        map.put("t1", sub1);
        map.put("t2", sub2);
        TransactionState txnState = new TransactionState();
        multiTask.beforePrepared(txnState);
        multiTask.afterPrepared(txnState, true);
        multiTask.replayOnPrepared(txnState);
        multiTask.beforeCommitted(txnState);
        multiTask.afterCommitted(txnState, true);
        multiTask.replayOnCommitted(txnState);
        multiTask.afterAborted(txnState, true, "reason");
        multiTask.replayOnAborted(txnState);
        multiTask.afterVisible(txnState, true);
        multiTask.replayOnVisible(txnState);
        List<List<String>> show = multiTask.getShowInfo();
        Assertions.assertEquals(2, show.size());
    }

    // ---- Cover beginTxn Double Begin (lines 281-289) ----
    @Test
    public void testBeginTxnDoubleBegin() throws Exception {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(12345L);
            }
        };
        TransactionResult resp1 = new TransactionResult();
        multiTask.beginTxn(resp1);
        Assertions.assertEquals(12345L, multiTask.getTxnId());

        TransactionResult resp2 = new TransactionResult();
        multiTask.beginTxn(resp2);
        Assertions.assertEquals(ActionStatus.LABEL_ALREADY_EXISTS, resp2.status);
    }

    // ---- Cover tryRollbackNow rollback exception + reconcile returns null (lines 190-197, 218-228) ----
    @Test
    public void testManualCancelWithTxnRollbackFailAndReconcileFail() throws Exception {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(100L);
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
                throw new RuntimeException("rollback failed");
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return null;
            }
        };

        multiTask.beginTxn(new TransactionResult());
        Assertions.assertEquals(100L, multiTask.getTxnId());

        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("ABORTING", multiTask.getStateName());
        Assertions.assertTrue(resp.stateOK());
    }

    private void setupRollbackFailWithReconcileMock(long txnId,
            ExplicitTxnState reconcileResult, boolean reconcileThrows) {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(txnId);
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
                throw new RuntimeException("rollback failed");
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalTransactionMgr getGlobalTransactionMgr() {
                return new GlobalTransactionMgr(null) {
                    @Override
                    public ExplicitTxnState getExplicitTxnState(long id) {
                        if (reconcileThrows) {
                            throw new RuntimeException("reconcile exception");
                        }
                        return reconcileResult;
                    }
                };
            }
        };
    }

    // ---- Cover reconcileWithTxnManager explicitState==null (lines 240-244) ----
    @Test
    public void testManualCancelWithReconcileExplicitStateNull() throws Exception {
        setupRollbackFailWithReconcileMock(200L, null, false);
        multiTask.beginTxn(new TransactionResult());
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- Cover reconcileWithTxnManager VISIBLE/COMMITTED (lines 246-264) ----
    @Test
    public void testManualCancelWithReconcileVisible() throws Exception {
        ExplicitTxnState explicitState = new ExplicitTxnState();
        TransactionState txnState = new TransactionState();
        txnState.setTransactionStatus(TransactionStatus.VISIBLE);
        explicitState.setTransactionState(txnState);

        setupRollbackFailWithReconcileMock(300L, explicitState, false);
        multiTask.beginTxn(new TransactionResult());
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
    }

    // ---- Cover reconcileWithTxnManager exception (lines 267-270) ----
    @Test
    public void testManualCancelWithReconcileException() throws Exception {
        setupRollbackFailWithReconcileMock(400L, null, true);
        multiTask.beginTxn(new TransactionResult());
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("ABORTING", multiTask.getStateName());
    }

    // ---- Cover retryAbortIfNeeded (lines 280-289) ----
    @Test
    public void testRetryAbortIfNeededNotAborting() {
        Assertions.assertTrue(multiTask.retryAbortIfNeeded(System.currentTimeMillis()));
    }

    @Test
    public void testRetryAbortIfNeededNotYetTime() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Deencapsulation.setField(multiTask, "nextAbortRetryTimeMs",
                System.currentTimeMillis() + 100000);
        Assertions.assertFalse(multiTask.retryAbortIfNeeded(System.currentTimeMillis()));
    }

    @Test
    public void testRetryAbortIfNeededTimeReached() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Deencapsulation.setField(multiTask, "nextAbortRetryTimeMs", 0L);
        boolean result = multiTask.retryAbortIfNeeded(System.currentTimeMillis());
        Assertions.assertTrue(result);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- Cover commitTxn state checks (lines 377-378, 393-394, 400-401, 407-433) ----
    @Test
    public void testCommitTxnWhenAlreadyCommitted() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITED);
        Deencapsulation.setField(multiTask, "txnId", 999L);
        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(null, resp);
        Assertions.assertTrue(resp.stateOK());
    }

    @Test
    public void testCommitTxnWhenCancelled() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.CANCELLED);
        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(null, resp);
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testCommitTxnWhenAborting() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Deencapsulation.setField(multiTask, "abortReason", "test");
        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(null, resp);
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testCommitTxnWhenCommiting() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITING);
        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(null, resp);
        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("already committing"));
    }

    // ---- Cover manualCancelTask all branches (lines 452-471) ----
    @Test
    public void testManualCancelTaskWhenAlreadyCommitted() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITED);
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testManualCancelTaskWhenAlreadyCancelled() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.CANCELLED);
        Deencapsulation.setField(multiTask, "txnId", 999L);
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertTrue(resp.stateOK());
    }

    @Test
    public void testManualCancelTaskWhenCommiting() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITING);
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("committing"));
    }

    @Test
    public void testManualCancelTaskWhenAborting() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Deencapsulation.setField(multiTask, "txnId", 999L);
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("abort is already in progress"));
    }

    // ---- Cover manualCancelTask rollback pending path (lines 469-471) ----
    @Test
    public void testManualCancelTaskRollbackPending() throws Exception {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(500L);
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
                throw new RuntimeException("rollback failed");
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return null;
            }
        };

        multiTask.beginTxn(new TransactionResult());
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("rollback pending"));
    }

    // ---- Cover checkNeedRemove endTimeMs==-1 branch (line 483) ----
    @Test
    public void testCheckNeedRemoveWhenAborting() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Assertions.assertFalse(multiTask.checkNeedRemove(System.currentTimeMillis(), true));
    }

    @Test
    public void testCheckNeedRemoveFinalStateEndTimeMissing() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.CANCELLED);
        Deencapsulation.setField(multiTask, "endTimeMs", -1L);
        boolean result = multiTask.checkNeedRemove(System.currentTimeMillis(), true);
        Assertions.assertTrue(result);
        Assertions.assertTrue(multiTask.endTimeMs() > 0);
    }

    // ---- Cover checkNeedRemove timeout path (line 515 = cancelOnTimeout) ----
    @Test
    public void testCheckNeedRemoveTriggersTimeout() {
        Deencapsulation.setField(multiTask, "createTimeMs",
                System.currentTimeMillis() - 2000);
        Deencapsulation.setField(multiTask, "timeoutMs", 1000L);
        boolean result = multiTask.checkNeedRemove(System.currentTimeMillis(), false);
        Assertions.assertFalse(result);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    @Test
    public void testCancelOnTimeoutEarlyReturn() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITING);
        multiTask.cancelOnTimeout();
        Assertions.assertEquals("COMMITING", multiTask.getStateName());
    }

    @Test
    public void testCancelOnExceptionWhenInFinalState() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.CANCELLED);
        multiTask.cancelOnException("ignored");
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- Cover executeTask with existing subtask (line 715) ----
    @Test
    public void testExecuteTaskWithExistingSubTask() {
        StreamLoadTask sub = new StreamLoadTask(2L, db, new OlapTable(), "l", "u",
                "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(sub, "tableName", "tbl1");
        @SuppressWarnings("unchecked")
        java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask,
                        "taskMaps");
        map.put("tbl1", sub);
        HttpHeaders headers = new DefaultHttpHeaders();
        TransactionResult resp = new TransactionResult();
        multiTask.executeTask(0, "tbl1", headers, resp);
        Assertions.assertTrue(resp.stateOK());
    }

    // ---- Cover executeTask when ABORTING (line 704) ----
    @Test
    public void testExecuteTaskWhenAborting() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Deencapsulation.setField(multiTask, "abortReason", "test");
        HttpHeaders headers = new DefaultHttpHeaders();
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.executeTask(0, "t", headers, resp));
        Assertions.assertFalse(resp.stateOK());
    }

    // ---- Cover tryLoad when CANCELLED/ABORTING/COMMITED (lines 700-710) ----
    @Test
    public void testTryLoadWhenCancelled() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.CANCELLED);
        Deencapsulation.setField(multiTask, "errorMsg", "test");
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.tryLoad(0, "t", resp));
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testTryLoadWhenAborting() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.ABORTING);
        Deencapsulation.setField(multiTask, "abortReason", "test");
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.tryLoad(0, "t", resp));
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testTryLoadWhenCommitted() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITED);
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.tryLoad(0, "t", resp));
        Assertions.assertFalse(resp.stateOK());
    }

    // ---- Cover prepareChannel with existing subtask (line 728) ----
    @Test
    public void testPrepareChannelWithSubTask() {
        StreamLoadTask sub = new StreamLoadTask(2L, db, new OlapTable(), "l", "u",
                "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(sub, "tableName", "tbl1");
        @SuppressWarnings("unchecked")
        java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask,
                        "taskMaps");
        map.put("tbl1", sub);
        HttpHeaders headers = new DefaultHttpHeaders();
        TransactionResult resp = new TransactionResult();
        multiTask.prepareChannel(0, "tbl1", headers, resp);
        Assertions.assertTrue(resp.stateOK());
    }

    // ---- Cover cancelAfterRestart with txnId (covers transitionToAborting + tryRollbackNow) ----
    @Test
    public void testCancelAfterRestartWithTxnId() throws Exception {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(999L);
            }
        };
        multiTask.beginTxn(new TransactionResult());
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.LOADING);
        multiTask.cancelAfterRestart();
        Assertions.assertTrue("CANCELLED".equals(multiTask.getStateName())
                || "ABORTING".equals(multiTask.getStateName()));
    }

    @Test
    public void testCancelAfterRestartWithNoTxnId() {
        multiTask.cancelAfterRestart();
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }
}
