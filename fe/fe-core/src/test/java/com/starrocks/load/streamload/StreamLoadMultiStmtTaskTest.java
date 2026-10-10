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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.ExplicitTxnState;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionAlreadyCommitException;
import com.starrocks.transaction.TransactionNotFoundException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
    public void testBeginTxnBindsContextToTaskWarehouseInSharedDataMode() throws Exception {
        ComputeResource resource = WarehouseComputeResource.of(4242L);
        StreamLoadMultiStmtTask task = new StreamLoadMultiStmtTask(2L, db, "label_wh", "u", "127.0.0.1",
                1000L, System.currentTimeMillis(), resource);
        List<Long> boundWarehouseIds = new ArrayList<>();
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        new MockUp<com.starrocks.qe.ConnectContext>() {
            @Mock
            public void setCurrentWarehouseId(long warehouseId) {
                boundWarehouseIds.add(warehouseId);
            }
        };
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                // The transaction takes its compute resource from the context: it must be the
                // task's, not one acquired from the context's default warehouse.
                Assertions.assertSame(resource, ctx.getCurrentComputeResourceNoAcquire());
                ctx.setTxnId(42L);
            }
        };
        TransactionResult resp = new TransactionResult();
        task.beginTxn(resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals(42L, task.getTxnId());
        Assertions.assertEquals(List.of(4242L), boundWarehouseIds);
    }

    @Test
    public void testBeginTxnUsesTaskTimeoutForTransaction() throws Exception {
        StreamLoadMultiStmtTask task = new StreamLoadMultiStmtTask(3L, db, "label_timeout", "u", "127.0.0.1",
                7_200_000L, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                // beginStmt sizes the transaction timeout from the context, so it must carry the
                // task's HTTP timeout rather than the session default query_timeout.
                Assertions.assertEquals(7200, ctx.getExecTimeout());
                ctx.setTxnId(43L);
            }
        };
        TransactionResult resp = new TransactionResult();
        task.beginTxn(resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals(43L, task.getTxnId());
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

    @Test
    public void testCancelAfterRestartOnDeserializedTask() {
        String json = GsonUtils.GSON.toJson(multiTask);
        StreamLoadMultiStmtTask deserialized = GsonUtils.GSON.fromJson(json, StreamLoadMultiStmtTask.class);
        Assertions.assertDoesNotThrow(deserialized::cancelAfterRestart);
        Assertions.assertEquals("CANCELLED", deserialized.getStateName());
    }

    private StreamLoadTask addSubTask(String tableName, StreamLoadTask.State state) {
        StreamLoadTask sub = new StreamLoadTask(2L, db, new OlapTable(), "label_multi", "u",
                "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(sub, "tableName", tableName);
        Deencapsulation.setField(sub, "state", state);
        @SuppressWarnings("unchecked")
        java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask,
                        "taskMaps");
        map.put(tableName, sub);
        return sub;
    }

    private void mockCommitTxnDependencies(long txnId) {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(txnId);
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
            }

            @Mock
            public void commitStmt(com.starrocks.qe.ConnectContext context,
                                   com.starrocks.sql.ast.txn.CommitStmt stmt) {
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }
        };
    }

    // ---- Commit success must propagate final state to sub-tasks so that
    // information_schema.loads / SHOW STREAM LOAD do not show PREPARING forever ----
    @Test
    public void testCommitTxnPropagatesFinishedToSubTasksWhenTxnVisible() throws Exception {
        mockCommitTxnDependencies(777L);
        TransactionState visibleTxn = new TransactionState();
        visibleTxn.setTransactionStatus(TransactionStatus.VISIBLE);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return visibleTxn;
            }
        };

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARED);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
        Assertions.assertEquals("FINISHED", sub.getStateName());
        Assertions.assertTrue(sub.endTimeMs() > 0);
        Assertions.assertEquals("FINISHED", multiTask.toThrift().get(0).getState());
    }

    @Test
    public void testCommitTxnPropagatesCommittedWhenTxnStatusUnknown() throws Exception {
        mockCommitTxnDependencies(778L);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return null;
            }
        };

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARED);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
        Assertions.assertEquals("COMMITED", sub.getStateName());
    }

    // Mocks a commit that reaches commitStmt: every sub-task dispatches its channel, its coordinator
    // finishes and its load is added to the transaction. commitStmt is replaced by commitStmtBehavior.
    private void mockCommitUpToCommitStmt(long txnId, Consumer<com.starrocks.qe.ConnectContext> commitStmtBehavior) {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(txnId);
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
            }

            @Mock
            public void commitStmt(com.starrocks.qe.ConnectContext context,
                                   com.starrocks.sql.ast.txn.CommitStmt stmt) {
                commitStmtBehavior.accept(context);
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public void prepareChannel(int channelId, String tableName, HttpHeaders headers,
                                       TransactionResult resp) {
            }

            @Mock
            public boolean checkNeedPrepareTxn() {
                return true;
            }

            @Mock
            public void waitCoordFinish(TransactionResult resp) {
            }

            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }
        };
    }

    // Like TransactionStmtExecutor.commitStmt when the commit fails: it reports the error through the
    // context state and resets the context's txnId (it also clears the explicit transaction state, which
    // these tests never create).
    private static Consumer<com.starrocks.qe.ConnectContext> commitStmtFailsWith(String error) {
        return ctx -> {
            ctx.getState().setError(error);
            ctx.setTxnId(0);
        };
    }

    // Fakes the transaction manager's view of the transaction: getTransactionState returns txnState, and
    // abortTransaction, after failing its first failedAborts calls, behaves like DatabaseTransactionMgr's:
    // it aborts a PREPARE or PREPARED transaction and throws for a committed or final one. Every call is
    // recorded in aborts as "txnId:reason", and the tablets it is passed in abortedTablets.
    private void fakeTxnManager(TransactionState txnState, int failedAborts, List<String> aborts,
                                List<TabletCommitInfo> abortedTablets) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }

            @Mock
            public void abortTransaction(long dbId, long transactionId, String reason,
                                         List<TabletCommitInfo> finishedTablets,
                                         List<TabletFailInfo> failedTablets,
                                         TxnCommitAttachment txnCommitAttachment) throws StarRocksException {
                aborts.add(transactionId + ":" + reason);
                abortedTablets.addAll(finishedTablets);
                if (aborts.size() <= failedAborts) {
                    throw new StarRocksException("abort failed for test");
                }
                if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                    throw new TransactionAlreadyCommitException("transaction " + transactionId + " is committed");
                }
                if (!txnState.isRunning()) {
                    throw new TransactionNotFoundException(transactionId);
                }
                txnState.setTransactionStatus(TransactionStatus.ABORTED);
            }
        };
    }

    private static TransactionState txnStateWithStatus(TransactionStatus status) {
        TransactionState txnState = new TransactionState();
        txnState.setTransactionStatus(status);
        return txnState;
    }

    // ---- commitStmt reports a failed commit through the context state instead of throwing. The
    // response must carry the failure: with an OK response the client treats the transaction as
    // committed although it is rolled back ----
    @Test
    public void testCommitTxnReportsCommitStmtError() throws Exception {
        mockCommitUpToCommitStmt(895L, commitStmtFailsWith("commit failed for test"));
        fakeTxnManager(txnStateWithStatus(TransactionStatus.PREPARED), 0, new ArrayList<>(), new ArrayList<>());

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("commit failed for test"), resp.msg);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        Assertions.assertEquals("CANCELLED", sub.getStateName());
    }

    // ---- Once commitStmt has cleared the explicit transaction state, rollbackStmt does nothing: the
    // failed commit must abort the transaction in the transaction manager, with the sub-tasks' tablets,
    // instead of leaving it PREPARED until the transaction timeout ----
    @Test
    public void testCommitTxnAbortsTxnInTxnManagerWhenCommitStmtFails() throws Exception {
        mockCommitUpToCommitStmt(896L, commitStmtFailsWith("commit rate exceeded for test"));
        TransactionState txnState = txnStateWithStatus(TransactionStatus.PREPARED);
        List<String> aborts = new ArrayList<>();
        List<TabletCommitInfo> abortedTablets = new ArrayList<>();
        fakeTxnManager(txnState, 0, aborts, abortedTablets);

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        List<TabletCommitInfo> tablets = List.of(new TabletCommitInfo(10L, 20L));
        sub.getTxnStateItem().setTabletCommitInfos(tablets);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertEquals(List.of("896:commit failed: commit rate exceeded for test"), aborts);
        Assertions.assertEquals(tablets, abortedTablets);
        Assertions.assertEquals(TransactionStatus.ABORTED, txnState.getTransactionStatus());
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- A commitStmt error does not always mean the commit failed: it may come after the transaction
    // was committed. The commit is then reported as successful and the task is COMMITED, not rolled back ----
    @Test
    public void testCommitTxnSucceedsWhenTxnCommittedDespiteCommitStmtError() throws Exception {
        mockCommitUpToCommitStmt(897L, commitStmtFailsWith("error after commit for test"));
        List<String> aborts = new ArrayList<>();
        fakeTxnManager(txnStateWithStatus(TransactionStatus.VISIBLE), 0, aborts, new ArrayList<>());

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertTrue(resp.stateOK(), resp.msg);
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
        Assertions.assertEquals("FINISHED", sub.getStateName());
        Assertions.assertTrue(aborts.isEmpty(), aborts.toString());
    }

    // ---- The same holds for an unchecked exception escaping commitStmt after the commit ----
    @Test
    public void testCommitTxnSucceedsWhenCommitStmtThrowsAfterCommit() throws Exception {
        mockCommitUpToCommitStmt(898L, ctx -> {
            ctx.setTxnId(0);
            throw new IllegalStateException("thrown after commit for test");
        });
        List<String> aborts = new ArrayList<>();
        fakeTxnManager(txnStateWithStatus(TransactionStatus.COMMITTED), 0, aborts, new ArrayList<>());

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        Assertions.assertDoesNotThrow(() -> multiTask.commitTxn(new DefaultHttpHeaders(), resp));

        Assertions.assertTrue(resp.stateOK(), resp.msg);
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
        Assertions.assertEquals("COMMITED", sub.getStateName());
        Assertions.assertTrue(aborts.isEmpty(), aborts.toString());
    }

    // ---- A commit that failed because the transaction was already aborted (e.g. by the transaction
    // timeout checker) ends CANCELLED: the abort finds no running transaction, and the reconcile reads
    // ABORTED from the transaction manager instead of relying on the cleared explicit state ----
    @Test
    public void testCommitTxnCancelledWhenTxnAlreadyAborted() throws Exception {
        mockCommitUpToCommitStmt(899L, commitStmtFailsWith("transaction already aborted for test"));
        fakeTxnManager(txnStateWithStatus(TransactionStatus.ABORTED), 0, new ArrayList<>(), new ArrayList<>());

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("transaction already aborted for test"), resp.msg);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- If that abort fails, the task stays ABORTING while the transaction is still PREPARED, and the
    // background retry aborts it in the transaction manager again ----
    @Test
    public void testRetryAbortAfterCommitStmtFailureAbortsTxnInTxnManager() throws Exception {
        mockCommitUpToCommitStmt(900L, commitStmtFailsWith("commit failed for test"));
        TransactionState txnState = txnStateWithStatus(TransactionStatus.PREPARED);
        List<String> aborts = new ArrayList<>();
        fakeTxnManager(txnState, 1, aborts, new ArrayList<>());

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertEquals(1, aborts.size());
        Assertions.assertEquals("ABORTING", multiTask.getStateName());

        Assertions.assertTrue(multiTask.retryAbortIfNeeded(Long.MAX_VALUE));
        Assertions.assertEquals(2, aborts.size());
        Assertions.assertEquals(TransactionStatus.ABORTED, txnState.getTransactionStatus());
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- The response follows the outcome the rollback settles: if the abort finds the transaction
    // committed after commitTxn read it as PREPARED, the task is COMMITED and the response stays OK ----
    @Test
    public void testCommitTxnStaysOkWhenAbortFindsTxnCommitted() throws Exception {
        mockCommitUpToCommitStmt(901L, commitStmtFailsWith("commit failed for test"));
        TransactionState txnState = txnStateWithStatus(TransactionStatus.PREPARED);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }

            @Mock
            public void abortTransaction(long dbId, long transactionId, String reason,
                                         List<TabletCommitInfo> finishedTablets,
                                         List<TabletFailInfo> failedTablets,
                                         TxnCommitAttachment txnCommitAttachment) throws StarRocksException {
                txnState.setTransactionStatus(TransactionStatus.COMMITTED);
                throw new TransactionAlreadyCommitException("transaction " + transactionId + " is committed");
            }
        };

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertTrue(resp.stateOK(), resp.msg);
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
        Assertions.assertEquals("COMMITED", sub.getStateName());
    }

    // Mocks a commit in which only failingTable's load fails, when its channel is prepared (failChannel) or
    // when its coordinator is waited for, with an error that does not name the table.
    private void mockCommitWithFailingTable(long txnId, String failingTable, boolean failChannel, String error) {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(txnId);
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public void prepareChannel(int channelId, String tableName, HttpHeaders headers,
                                       TransactionResult resp) {
                if (failChannel && failingTable.equals(tableName)) {
                    resp.setErrorMsg(error);
                }
            }

            @Mock
            public boolean checkNeedPrepareTxn() {
                return true;
            }

            @Mock
            public void waitCoordFinish(Invocation inv, TransactionResult resp) {
                StreamLoadTask self = inv.getInvokedInstance();
                if (!failChannel && failingTable.equals(self.getTableName())) {
                    resp.setErrorMsg(error);
                }
            }

            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }

            @Mock
            public void cancelCoordinatorOnly(String reason) {
            }
        };
    }

    // ---- All tables of a shared transaction have its label, so a commit that fails because of one
    // table's load must name that table: the client cannot tell otherwise ----
    @Test
    public void testCommitTxnErrorNamesTableWhoseLoadFailed() throws Exception {
        mockCommitWithFailingTable(902L, "tbl2", false, "abnormal data more than max filter rate");

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        addSubTask("tbl2", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("table tbl2"), resp.msg);
        Assertions.assertTrue(resp.msg.contains("abnormal data more than max filter rate"), resp.msg);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    @Test
    public void testCommitTxnErrorNamesTableWhoseChannelFailed() throws Exception {
        mockCommitWithFailingTable(903L, "tbl2", true, "channel failed for test");

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        addSubTask("tbl2", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("table tbl2"), resp.msg);
        Assertions.assertTrue(resp.msg.contains("channel failed for test"), resp.msg);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
    }

    // ---- Reconcile finding the txn committed must not leave sub-tasks CANCELLED ----
    @Test
    public void testReconcileCommittedPropagatesToSubTasksInsteadOfCancel() throws Exception {
        ExplicitTxnState explicitState = new ExplicitTxnState();
        TransactionState txnState = new TransactionState();
        txnState.setTransactionStatus(TransactionStatus.VISIBLE);
        explicitState.setTransactionState(txnState);
        setupRollbackFailWithReconcileMock(300L, explicitState, false);

        multiTask.beginTxn(new TransactionResult());
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
        Assertions.assertEquals("FINISHED", sub.getStateName());
    }

    // ---- New loads must be rejected while the commit is in progress ----
    @Test
    public void testTryLoadWhenCommiting() throws StarRocksException {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITING);
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.tryLoad(0, "t", resp));
        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("committing"));
    }

    @Test
    public void testExecuteTaskWhenCommiting() {
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITING);
        HttpHeaders headers = new DefaultHttpHeaders();
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.executeTask(0, "t", headers, resp));
        Assertions.assertFalse(resp.stateOK());
        Assertions.assertTrue(resp.msg.contains("committing"));
    }

    // ---- A COMMITED sub-task is upgraded to FINISHED once the txn becomes visible ----
    @Test
    public void testMarkCommittedByParentUpgradesCommittedToFinished() {
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        long now = System.currentTimeMillis();

        sub.markCommittedByParent(false, now);
        Assertions.assertEquals("COMMITED", sub.getStateName());

        // repeated non-visible propagation is a no-op
        sub.markCommittedByParent(false, now + 1);
        Assertions.assertEquals("COMMITED", sub.getStateName());

        sub.markCommittedByParent(true, now + 2);
        Assertions.assertEquals("FINISHED", sub.getStateName());
        Assertions.assertEquals(now + 2, sub.endTimeMs());
        // commitTimeMs from the first propagation is preserved
        Assertions.assertEquals(now, sub.commitTimeMs());

        // CANCELLED/FINISHED are never overridden
        sub.markCommittedByParent(false, now + 3);
        Assertions.assertEquals("FINISHED", sub.getStateName());
    }

    // ---- The cleaner thread converges COMMITED sub-tasks once the txn is visible ----
    @Test
    public void testCheckNeedRemoveUpgradesCommittedSubTasksWhenVisible() {
        TransactionState visibleTxn = new TransactionState();
        visibleTxn.setTransactionStatus(TransactionStatus.VISIBLE);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return visibleTxn;
            }
        };
        StreamLoadTask sub = addSubTask("tbl1", StreamLoadTask.State.COMMITED);
        Deencapsulation.setField(multiTask, "state", StreamLoadMultiStmtTask.State.COMMITED);
        Deencapsulation.setField(multiTask, "endTimeMs", System.currentTimeMillis());

        Assertions.assertFalse(multiTask.checkNeedRemove(System.currentTimeMillis(), false));
        Assertions.assertEquals("FINISHED", sub.getStateName());
    }

    // ---- Multi-table commit must dispatch every table's channel (prepareChannel)
    // before blocking on any coordinator (waitCoordFinish), so the per-table flushes
    // overlap on the BEs instead of running one-at-a-time ----
    @Test
    public void testCommitTxnFiresAllChannelsBeforeWaiting() throws Exception {
        List<String> callOrder = new ArrayList<>();
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(890L);
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
            }

            @Mock
            public void commitStmt(com.starrocks.qe.ConnectContext context,
                                   com.starrocks.sql.ast.txn.CommitStmt stmt) {
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public void prepareChannel(int channelId, String tableName, HttpHeaders headers,
                                       TransactionResult resp) {
                callOrder.add("prepare:" + tableName);
            }

            @Mock
            public boolean checkNeedPrepareTxn() {
                return true;
            }

            @Mock
            public void waitCoordFinish(TransactionResult resp) {
                callOrder.add("wait");
            }

            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }
        };
        TransactionState visibleTxn = new TransactionState();
        visibleTxn.setTransactionStatus(TransactionStatus.VISIBLE);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return visibleTxn;
            }
        };

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        addSubTask("tbl2", StreamLoadTask.State.PREPARING);
        addSubTask("tbl3", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals("COMMITED", multiTask.getStateName());

        long prepareCount = callOrder.stream().filter(s -> s.startsWith("prepare:")).count();
        long waitCount = callOrder.stream().filter(s -> s.equals("wait")).count();
        Assertions.assertEquals(3, prepareCount);
        Assertions.assertEquals(3, waitCount);

        // Every prepareChannel must appear before the first waitCoordFinish.
        int lastPrepareIdx = -1;
        int firstWaitIdx = Integer.MAX_VALUE;
        for (int i = 0; i < callOrder.size(); i++) {
            if (callOrder.get(i).startsWith("prepare:")) {
                lastPrepareIdx = i;
            } else if (callOrder.get(i).equals("wait") && firstWaitIdx == Integer.MAX_VALUE) {
                firstWaitIdx = i;
            }
        }
        Assertions.assertTrue(lastPrepareIdx < firstWaitIdx,
                "all prepareChannel calls must precede any waitCoordFinish, actual order: " + callOrder);
    }

    // ---- When a prepareChannel fails partway through the fire loop, the abort path must
    // cancel EVERY sub-task's coordinator, including the ones already dispatched (in-flight)
    // before the failure and the ones not yet reached ----
    @Test
    public void testCommitTxnCancelsAllSubTasksWhenPrepareChannelFails() throws Exception {
        List<String> cancelled = new ArrayList<>();
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(891L);
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
                // rollback succeeds, so tryRollbackNow proceeds to cancel sub-tasks
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
                // tables dispatched before the failing one are drained into the txn
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public void prepareChannel(int channelId, String tableName, HttpHeaders headers,
                                       TransactionResult resp) {
                // Fail exactly one table; leave the others OK. Whichever iteration order
                // ConcurrentHashMap picks, the abort path must still reach all sub-tasks.
                if ("tbl2".equals(tableName)) {
                    resp.setErrorMsg("prepareChannel failed for " + tableName);
                }
            }

            @Mock
            public boolean checkNeedPrepareTxn() {
                return true;
            }

            @Mock
            public void waitCoordFinish(TransactionResult resp) {
            }

            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }

            @Mock
            public void cancelCoordinatorOnly(Invocation inv, String reason) {
                StreamLoadTask self = inv.getInvokedInstance();
                cancelled.add(self.getTableName());
            }
        };

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        addSubTask("tbl2", StreamLoadTask.State.PREPARING);
        addSubTask("tbl3", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        // Every sub-task's coordinator must be cancelled, regardless of whether it was
        // dispatched before the failing table or never reached.
        Assertions.assertEquals(3, cancelled.size());
        Assertions.assertTrue(cancelled.contains("tbl1"));
        Assertions.assertTrue(cancelled.contains("tbl2"));
        Assertions.assertTrue(cancelled.contains("tbl3"));
    }

    // ---- A prepareChannel failure must not drop the loads already dispatched before it:
    // they must still be added to the transaction (loadData) so the rollback aborts them
    // with their tablet infos instead of taking rollbackStmt's empty-item branch ----
    @Test
    public void testCommitTxnLoadsDispatchedTablesWhenLaterPrepareFails() throws Exception {
        int[] prepareCalls = {0};
        int[] loadCalls = {0};
        boolean[] committed = {false};
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(893L);
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
                loadCalls[0]++;
            }

            @Mock
            public void commitStmt(com.starrocks.qe.ConnectContext context,
                                   com.starrocks.sql.ast.txn.CommitStmt stmt) {
                committed[0] = true;
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public void prepareChannel(int channelId, String tableName, HttpHeaders headers,
                                       TransactionResult resp) {
                // Fail the second channel dispatched; the first is already dispatched.
                prepareCalls[0]++;
                if (prepareCalls[0] == 2) {
                    resp.setErrorMsg("prepareChannel failed on the 2nd channel");
                }
            }

            @Mock
            public boolean checkNeedPrepareTxn() {
                return true;
            }

            @Mock
            public void waitCoordFinish(TransactionResult resp) {
            }

            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }

            @Mock
            public void cancelCoordinatorOnly(String reason) {
            }
        };

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        addSubTask("tbl2", StreamLoadTask.State.PREPARING);
        addSubTask("tbl3", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        // The one table dispatched before the failing 2nd prepare must still be loaded into
        // the transaction; the pre-fix code returned immediately and loaded zero tables.
        Assertions.assertEquals(1, loadCalls[0]);
        Assertions.assertFalse(committed[0]);
    }

    // ---- A waitCoordFinish failure in the drain loop must abort the commit: record the
    // error and STOP draining (break, not continue): a waitCoordFinish failure already
    // aborted the shared transaction, so no later table may be loaded, and commit must not run ----
    @Test
    public void testCommitTxnStopsDrainingWhenWaitCoordFinishFails() throws Exception {
        int[] waitCalls = {0};
        int[] loadCalls = {0};
        boolean[] committed = {false};
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                ctx.setTxnId(894L);
            }

            @Mock
            public void loadData(long dbId, long tableId,
                                 ExplicitTxnState.ExplicitTxnStateItem item,
                                 com.starrocks.qe.ConnectContext context) {
                loadCalls[0]++;
            }

            @Mock
            public void commitStmt(com.starrocks.qe.ConnectContext context,
                                   com.starrocks.sql.ast.txn.CommitStmt stmt) {
                committed[0] = true;
            }

            @Mock
            public void rollbackStmt(com.starrocks.qe.ConnectContext ctx,
                                     com.starrocks.sql.ast.txn.RollbackStmt stmt) {
            }
        };
        new MockUp<StreamLoadTask>() {
            @Mock
            public void prepareChannel(int channelId, String tableName, HttpHeaders headers,
                                       TransactionResult resp) {
                // all channels dispatch OK
            }

            @Mock
            public boolean checkNeedPrepareTxn() {
                return true;
            }

            @Mock
            public void waitCoordFinish(TransactionResult resp) {
                // First dispatched table succeeds (and is loaded), the second fails. The loop
                // must then break: a third wait must never run and its table must not be loaded.
                waitCalls[0]++;
                if (waitCalls[0] == 2) {
                    resp.setErrorMsg("coordinator join failed");
                }
            }

            @Mock
            public OlapTable getTable() {
                return new OlapTable();
            }

            @Mock
            public void cancelCoordinatorOnly(String reason) {
            }
        };

        multiTask.beginTxn(new TransactionResult());
        addSubTask("tbl1", StreamLoadTask.State.PREPARING);
        addSubTask("tbl2", StreamLoadTask.State.PREPARING);
        addSubTask("tbl3", StreamLoadTask.State.PREPARING);

        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(new DefaultHttpHeaders(), resp);

        Assertions.assertFalse(resp.stateOK());
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        // break (not continue): only 2 waits ran (stopped at the failing one) and only the
        // first table was loaded; the third table is never waited or loaded, and commit never runs.
        Assertions.assertEquals(2, waitCalls[0]);
        Assertions.assertEquals(1, loadCalls[0]);
        Assertions.assertFalse(committed[0]);
    }

    // ---- A file_bundling table joining the multi-statement txn turns on combined txn log ----
    @Test
    public void testFileBundlingTableEnablesCombinedTxnLog() {
        Deencapsulation.setField(multiTask, "txnId", 4242L);
        TransactionState txnState = new TransactionState();
        Assertions.assertFalse(txnState.isUseCombinedTxnLog());
        ExplicitTxnState explicitState = new ExplicitTxnState();
        explicitState.setTransactionState(txnState);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public ExplicitTxnState getExplicitTxnState(long id) {
                return explicitState;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public Boolean isFileBundling() {
                return true;
            }
        };

        Deencapsulation.invoke(multiTask, "decideCombinedTxnLogFromFirstTable", new OlapTable());
        Assertions.assertTrue(txnState.isUseCombinedTxnLog());
    }

    // ---- A non-file_bundling table leaves the combined txn log flag untouched ----
    @Test
    public void testNonFileBundlingTableDoesNotEnableCombinedTxnLog() {
        Deencapsulation.setField(multiTask, "txnId", 4243L);
        TransactionState txnState = new TransactionState();
        ExplicitTxnState explicitState = new ExplicitTxnState();
        explicitState.setTransactionState(txnState);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public ExplicitTxnState getExplicitTxnState(long id) {
                return explicitState;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public Boolean isFileBundling() {
                return false;
            }
        };

        Deencapsulation.invoke(multiTask, "decideCombinedTxnLogFromFirstTable", new OlapTable());
        Assertions.assertFalse(txnState.isUseCombinedTxnLog());
    }

    // ---- No transaction state yet: a file_bundling table must not blow up ----
    @Test
    public void testEnableCombinedTxnLogNoExplicitTxnStateIsNoop() {
        Deencapsulation.setField(multiTask, "txnId", 4244L);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public ExplicitTxnState getExplicitTxnState(long id) {
                return null;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public Boolean isFileBundling() {
                return true;
            }
        };

        Assertions.assertDoesNotThrow(() ->
                Deencapsulation.invoke(multiTask, "decideCombinedTxnLogFromFirstTable", new OlapTable()));
    }
}
