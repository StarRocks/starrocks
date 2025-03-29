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

package com.starrocks.load.batchwrite;

import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.proto.PUpdateTransactionStateRequest;
import com.starrocks.proto.PUpdateTransactionStateResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.PBackendService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionStateSnapshot;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.MockedBackend;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TxnStateDispatcherTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    private MockedPBackendService backendService;
    private SystemInfoService systemInfoService;

    @Before
    public void setUp() throws Exception {
        backendService = new MockedPBackendService();
        new MockUp<BrpcProxy>() {
            @Mock
            private synchronized PBackendService getBackendService(TNetworkAddress address) {
                return backendService;
            }
        };

        systemInfoService = new SystemInfoService();
        Backend node1 = new Backend(1, "10.0.0.1", 9050);
        node1.setAlive(true);
        node1.setBrpcPort(8060);
        systemInfoService.addBackend(node1);
        Backend node2 = new Backend(2, "10.0.0.2", 9050);
        node2.setAlive(false);
        node2.setBrpcPort(8060);
        systemInfoService.addBackend(node2);
    }

    @Test
    public void testSubmitTaskSuccess() throws Exception {
        MockExecutor executor = new MockExecutor();
        TxnStateDispatcher dispatcher = new TxnStateDispatcher(executor);

        // success without retry
        dispatcher.submitTask("db1", 1, 1);
        assertEquals(1, executor.getNumExecuteCalled());
        assertEquals(1, executor.numPendingTasks());
        TxnStateDispatcher.Task task1 = dispatcher.getPendingTask(1, 1);
        assertNotNull(task1);
        assertEquals("db1", task1.getDbName());
        assertEquals(1, task1.getTxnId());
        assertEquals(1, task1.getBackendId());
        assertEquals(1, dispatcher.getNumSubmittedTasks());

        // success with retry
        executor.setArtificialRejectNum(3);
        dispatcher.submitTask("db2", 2, 2);
        assertEquals(5, executor.getNumExecuteCalled());
        assertEquals(2, executor.numPendingTasks());
        assertEquals(0, executor.getArtificialRejectNum());
        TxnStateDispatcher.Task task2 = dispatcher.getPendingTask(2, 2);
        assertNotNull(task2);
        assertEquals("db2", task2.getDbName());
        assertEquals(2, task2.getTxnId());
        assertEquals(2, task2.getBackendId());
        assertEquals(2, dispatcher.getNumSubmittedTasks());
    }

    @Test
    public void testSubmitTaskFail() throws Exception {
        MockExecutor executor = new MockExecutor();
        TxnStateDispatcher dispatcher = new TxnStateDispatcher(executor);

        executor.setArtificialRejectNum(4);
        try {
            dispatcher.submitTask("db1", 1, 2);
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof RejectedExecutionException);
        }
        assertEquals(4, executor.getNumExecuteCalled());
        assertEquals(0, executor.getArtificialRejectNum());
        assertEquals(0, executor.numPendingTasks());
        assertNull(dispatcher.getPendingTask(1, 2));
        assertEquals(0, dispatcher.getNumSubmittedTasks());
    }

    @Test
    public void testDispatchSuccess() throws Exception {
        Database db1 = new Database(1, "db1");
        TransactionStateSnapshot txnState1 = new TransactionStateSnapshot(TransactionStatus.VISIBLE, "");
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                globalStateMgr.getLocalMetastore().getDb("db1");
                result = db1;
                globalStateMgr.getGlobalTransactionMgr().getTxnState(db1, 1L);
                result = txnState1;
            }
        };
        PUpdateTransactionStateResponse response = new PUpdateTransactionStateResponse();
        StatusPB statusPB = new StatusPB();
        statusPB.setStatusCode(0);
        response.setResults(Collections.singletonList(statusPB));
        CompletableFuture<PUpdateTransactionStateResponse> future = new CompletableFuture<>();
        future.complete(response);
        backendService.addResponseFuture(future);
        TxnStateDispatcher.DispatchResult expected = TxnStateDispatcher.DispatchResult.success(txnState1);
        testDispatchBase("db1", 1, 1, expected, 0);
    }

    @Test
    public void testDispatchRetrySuccess() throws Exception {
        Database db1 = new Database(1, "db1");
        TransactionStateSnapshot txnState1 = new TransactionStateSnapshot(TransactionStatus.VISIBLE, "");
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                globalStateMgr.getLocalMetastore().getDb("db1");
                result = db1;
                globalStateMgr.getGlobalTransactionMgr().getTxnState(db1, 1L);
                result = txnState1;
            }
        };
        CompletableFuture<PUpdateTransactionStateResponse> failureFuture = new CompletableFuture<>();
        failureFuture.completeExceptionally(new Exception("artificial failure"));
        for (int i = 0; i < Config.merge_commit_txn_state_dispatch_retry_times; i++) {
            backendService.addResponseFuture(failureFuture);
        }
        PUpdateTransactionStateResponse response = new PUpdateTransactionStateResponse();
        StatusPB statusPB = new StatusPB();
        statusPB.setStatusCode(0);
        response.setResults(Collections.singletonList(statusPB));
        CompletableFuture<PUpdateTransactionStateResponse> successFuture = new CompletableFuture<>();
        successFuture.complete(response);
        backendService.addResponseFuture(successFuture);
        TxnStateDispatcher.DispatchResult expected = TxnStateDispatcher.DispatchResult.success(txnState1);
        testDispatchBase("db1", 1, 1, expected,
                Config.merge_commit_txn_state_dispatch_retry_times);
    }

    @Test
    public void testBackendNotExist() throws Exception {
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
            }
        };
        TxnStateDispatcher.DispatchResult expected = TxnStateDispatcher.DispatchResult.fail(
                TxnStateDispatcher.DispatchStatus.ABORT, "can't find backend");
        testDispatchBase("db1", 1, 3, expected, 0);
    }

    @Test
    public void testDbNotExist() throws Exception {
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                globalStateMgr.getLocalMetastore().getDb("db1");
                result = null;
            }
        };
        TransactionStateSnapshot txnState = new TransactionStateSnapshot(TransactionStatus.UNKNOWN, "can't find database db1");
        TxnStateDispatcher.DispatchResult expected = TxnStateDispatcher.DispatchResult.success(txnState);
        PUpdateTransactionStateResponse response = new PUpdateTransactionStateResponse();
        StatusPB statusPB = new StatusPB();
        statusPB.setStatusCode(0);
        response.setResults(Collections.singletonList(statusPB));
        CompletableFuture<PUpdateTransactionStateResponse> future = new CompletableFuture<>();
        future.complete(response);
        backendService.addResponseFuture(future);
        testDispatchBase("db1", 1, 1, expected, 0);
    }

    @Test
    public void testGetTxnStateFail() throws Exception {
        Database db1 = new Database(1, "db1");
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                globalStateMgr.getLocalMetastore().getDb("db1");
                result = db1;
                globalStateMgr.getGlobalTransactionMgr().getTxnState(db1, 1L);
                result = new Exception("artificial failure");
            }
        };
        TransactionStateSnapshot txnState = new TransactionStateSnapshot(TransactionStatus.UNKNOWN,
                "can't get txn state, exception: artificial failure");
        TxnStateDispatcher.DispatchResult expected = TxnStateDispatcher.DispatchResult.success(txnState);
        PUpdateTransactionStateResponse response = new PUpdateTransactionStateResponse();
        StatusPB statusPB = new StatusPB();
        statusPB.setStatusCode(0);
        response.setResults(Collections.singletonList(statusPB));
        CompletableFuture<PUpdateTransactionStateResponse> future = new CompletableFuture<>();
        future.complete(response);
        backendService.addResponseFuture(future);
        testDispatchBase("db1", 1, 1, expected, 0);
    }

    @Test
    public void testReachMaxRetry() throws Exception {
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
            }
        };
        TxnStateDispatcher.DispatchResult expected = TxnStateDispatcher.DispatchResult.fail(
                TxnStateDispatcher.DispatchStatus.RETRYABLE, "backend [10.0.0.2] does not alive");
        testDispatchBase("db1", 1, 2, expected,
                Config.merge_commit_txn_state_dispatch_retry_times);
    }

    private void testDispatchBase(String dbName, long txnId, long backendId,
                                  TxnStateDispatcher.DispatchResult expected, int numRetry) throws Exception {
        MockExecutor executor = new MockExecutor();
        TxnStateDispatcher dispatcher = new TxnStateDispatcher(executor);
        TaskExecuteListener taskExecuteListener = new TaskExecuteListener();
        dispatcher.setTaskExecuteListener(taskExecuteListener);
        dispatcher.submitTask(dbName, txnId, backendId);
        executor.runOneTask();
        assertNull(dispatcher.getPendingTask(txnId, backendId));
        TaskExecuteResult result = taskExecuteListener.getResult(txnId, backendId);
        assertNotNull(result);
        assertTask(dbName, txnId, backendId, result.task);
        assertEquals(expected.getStatus(), result.dispatchResult.getStatus());
        assertTxnState(expected.getState(), result.dispatchResult.getState());
        assertEquals(expected.getFailReason(), result.dispatchResult.getFailReason());
        assertEquals(numRetry, result.numRetry);
    }

    private static void assertTask(String dbName, long txnId, long backendId, TxnStateDispatcher.Task actual) {
        assertEquals(dbName, actual.getDbName());
        assertEquals(txnId, actual.getTxnId());
        assertEquals(backendId, actual.getBackendId());
    }

    private static void assertTxnState(TransactionStateSnapshot expected, TransactionStateSnapshot actual) {
        if (expected == null) {
            assertNull(actual);
            return;
        }
        assertNotNull(actual);
        assertEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected.getReason(), actual.getReason());
    }

    private static class MockExecutor implements Executor {

        private int numExecuteCalled = 0;
        private int artificialRejectNum = 0;
        private final LinkedList<Runnable> pendingTasks = new LinkedList<>();

        @Override
        public void execute(Runnable command) {
            numExecuteCalled += 1;
            if (artificialRejectNum > 0) {
                artificialRejectNum -= 1;
                throw new RejectedExecutionException("reject submit");
            }
            pendingTasks.add(command);
        }

        public void runOneTask() {
            if (pendingTasks.isEmpty()) {
                return;
            }
            Runnable task = pendingTasks.pollFirst();
            task.run();
        }

        public void setArtificialRejectNum(int artificialRejectNum) {
            this.artificialRejectNum = artificialRejectNum;
        }

        public int getArtificialRejectNum() {
            return artificialRejectNum;
        }

        public int getNumExecuteCalled() {
            return numExecuteCalled;
        }

        public int numPendingTasks() {
            return pendingTasks.size();
        }
    }

    private static class MockedPBackendService extends MockedBackend.MockPBackendService {

        private LinkedList<Future<PUpdateTransactionStateResponse>> responseList = new LinkedList<>();

        @Override
        public Future<PUpdateTransactionStateResponse> updateTransactionState(PUpdateTransactionStateRequest request) {
            if (responseList.isEmpty()) {
                CompletableFuture<PUpdateTransactionStateResponse> future = new CompletableFuture<>();
                future.completeExceptionally(new Exception("reponse not available"));
                return future;
            }
            return responseList.pollFirst();
        }

        public void addResponseFuture(Future<PUpdateTransactionStateResponse> responseFuture) {
            this.responseList.add(responseFuture);
        }
    }

    private static class TaskExecuteResult {
        TxnStateDispatcher.Task task;
        TxnStateDispatcher.DispatchResult dispatchResult;
        int numRetry;

        public TaskExecuteResult(
                TxnStateDispatcher.Task task, TxnStateDispatcher.DispatchResult dispatchResult, int numRetry) {
            this.task = task;
            this.dispatchResult = dispatchResult;
            this.numRetry = numRetry;
        }
    }

    private static class TaskExecuteListener implements TxnStateDispatcher.TaskExecuteListener {

        private final Map<TxnStateDispatcher.TaskId, TaskExecuteResult> results = new HashMap<>();

        @Override
        public void onFinish(TxnStateDispatcher.Task task, TxnStateDispatcher.DispatchResult result, int numRetry) {
            results.put(task.taskId, new TaskExecuteResult(task, result, numRetry));
        }

        public TaskExecuteResult getResult(long txnId, long backendId) {
            return results.get(new TxnStateDispatcher.TaskId(txnId, backendId));
        }
    }
}
