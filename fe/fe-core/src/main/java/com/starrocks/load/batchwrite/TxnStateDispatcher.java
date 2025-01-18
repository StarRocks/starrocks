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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.proto.PUpdateTransactionStateRequest;
import com.starrocks.proto.PUpdateTransactionStateResponse;
import com.starrocks.proto.TransactionStatePB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.PBackendService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionStateSnapshot;
import com.starrocks.transaction.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Dispatch transaction state to backends after the load finished.
 * TODO send txn states on the same backend in batch
 */
public class TxnStateDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(TxnStateDispatcher.class);

    private final Executor rpcExecutor;
    private final AtomicLong numSubmittedTasks;
    private final ConcurrentMap<TaskId, Task> pendingTasks;

    // For testing
    private TaskExecuteListener taskExecuteListener;

    public TxnStateDispatcher(Executor rpcExecutor) {
        this.rpcExecutor = rpcExecutor;
        this.numSubmittedTasks = new AtomicLong(0);
        this.pendingTasks = new ConcurrentHashMap<>();
    }

    public void submitTask(String dbName, long txnId, long backendId) throws Exception {
        TaskId taskId = new TaskId(txnId, backendId);
        Task task = new Task(dbName, taskId);
        Task oldTask = pendingTasks.putIfAbsent(taskId, task);
        if (oldTask != null) {
            return;
        }
        int numRetry = 0;
        boolean success = false;
        Throwable lastException = null;
        while (true) {
            try {
                rpcExecutor.execute(() -> runTask(task));
                success = true;
                break;
            } catch (Throwable e) {
                LOG.warn("Failed to submit txn state update task, db: {}, txn_id: {}, backend_id: {}, num retry: {}",
                        dbName, txnId, backendId, numRetry, e);
                lastException = e;
            }
            if (numRetry >= 3) {
                break;
            }
            numRetry += 1;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }
        if (success) {
            numSubmittedTasks.incrementAndGet();
            LOG.debug("Success to submit txn state update task, db: {}, txn_id: {}, backend_id: {}, num retry: {}",
                    dbName, txnId, backendId, numRetry);
        } else {
            pendingTasks.remove(taskId);
            throw new Exception(String.format("Failed to submit txn state update task, db: %s, txn_id: %s, " +
                            "backend_id: %s, num retry: %s", dbName, txnId, backendId, numRetry), lastException);
        }
    }

    private void runTask(Task task) {
        int maxRetries = Config.merge_commit_txn_state_dispatch_retry_times;
        int numRetry = 0;
        DispatchResult result;
        while (true) {
            result = dispatchTxnState(task.dbName, task.taskId.txnId, task.taskId.backendId);
            if (result.getStatus() != DispatchStatus.RETRYABLE) {
                break;
            }
            LOG.warn("Dispatch txn state failed, db: {}, txn_id: {}, backend_id: {}, num retry: {}, fail reason: {}," +
                            " txn state: {}", task.dbName, task.taskId.txnId, task.taskId.backendId, numRetry,
                                    result.getFailReason(), result.getState());
            if (numRetry >= maxRetries) {
                break;
            }
            numRetry += 1;
            try {
                Thread.sleep(Config.merge_commit_txn_state_dispatch_retry_interval_ms);
            } catch (InterruptedException e) {
                break;
            }
        }
        if (result.getStatus() == DispatchStatus.SUCCESS) {
            LOG.debug("Success to dispatch txn state, db: {}, txn_id: {}, backend_id: {}, num retry: {}, txn state: {}",
                    task.dbName, task.taskId.txnId, task.taskId.backendId, numRetry, result.getState());
        } else {
            LOG.warn("Failed to dispatch txn state, db: {}, txn_id: {}, backend_id: {}, fail reason: {}, txn state: {}",
                    task.dbName, task.taskId.txnId, task.taskId.backendId, result.getFailReason(), result.getState());
        }
        pendingTasks.remove(task.taskId);
        if (taskExecuteListener != null) {
            taskExecuteListener.onFinish(task, result, numRetry);
        }
    }

    private DispatchResult dispatchTxnState(String dbName, long txnId, long backendId) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ComputeNode computeNode = globalStateMgr.getNodeMgr().getClusterInfo().getBackendOrComputeNode(backendId);
        if (computeNode == null) {
            return DispatchResult.fail(DispatchStatus.ABORT, "can't find backend");
        }
        if (!computeNode.isAlive()) {
            return DispatchResult.fail(DispatchStatus.RETRYABLE,
                    String.format("backend [%s] does not alive", computeNode.getHost()));
        }
        TNetworkAddress address = new TNetworkAddress(computeNode.getHost(), computeNode.getBrpcPort());
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        TransactionStateSnapshot state;
        if (db == null) {
            state = new TransactionStateSnapshot(TransactionStatus.UNKNOWN, "can't find database " + dbName);
        } else {
            try {
                state = globalStateMgr.getGlobalTransactionMgr().getTxnState(db, txnId);
            } catch (Throwable e) {
                state = new TransactionStateSnapshot(TransactionStatus.UNKNOWN,
                        "can't get txn state, exception: " + e.getMessage());
            }
        }
        try {
            TransactionStatePB statePB = new TransactionStatePB();
            statePB.setTxnId(txnId);
            statePB.setStatus(state.getStatus().toProto());
            statePB.setReason(state.getReason());
            PUpdateTransactionStateRequest request = new PUpdateTransactionStateRequest();
            request.setStates(Collections.singletonList(statePB));
            PBackendService service = BrpcProxy.getBackendService(address);
            Future<PUpdateTransactionStateResponse> future = service.updateTransactionState(request);
            future.get();
            return DispatchResult.success(state);
        } catch (Throwable e) {
            return DispatchResult.fail(DispatchStatus.RETRYABLE, state,
                    "failed to update txn state, exception: " + e.getMessage());
        }
    }

    public long getNumSubmittedTasks() {
        return numSubmittedTasks.get();
    }

    Task getPendingTask(long txnId, long backendId) {
        return pendingTasks.get(new TaskId(txnId, backendId));
    }

    void setTaskExecuteListener(TaskExecuteListener taskExecuteListener) {
        this.taskExecuteListener = taskExecuteListener;
    }

    static class TaskId {
        private final long txnId;
        private final long backendId;

        public TaskId(long txnId, long backendId) {
            this.txnId = txnId;
            this.backendId = backendId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TaskId taskId = (TaskId) o;
            return txnId == taskId.txnId && backendId == taskId.backendId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(txnId, backendId);
        }
    }

    static class Task {
        final String dbName;
        final TaskId taskId;

        public Task(String dbName, TaskId taskId) {
            this.dbName = dbName;
            this.taskId = taskId;
        }

        public String getDbName() {
            return dbName;
        }

        public long getTxnId() {
            return taskId.txnId;
        }

        public long getBackendId() {
            return taskId.backendId;
        }
    }

    enum DispatchStatus {
        // Dispatch transaction state to backend successfully
        SUCCESS,
        // Dispatch transaction state to backend failed, and can't retry
        ABORT,
        // Dispatch transaction state to backend failed, but can retry
        RETRYABLE
    }

    static class DispatchResult {
        private DispatchStatus status;
        // Maybe null if status is not SUCCESS
        private @Nullable TransactionStateSnapshot state;
        // Maybe null if status is SUCCESS
        private @Nullable String failReason;

        public DispatchStatus getStatus() {
            return status;
        }

        public TransactionStateSnapshot getState() {
            return state;
        }

        public String getFailReason() {
            return failReason;
        }

        public static DispatchResult success(TransactionStateSnapshot stateSnapshot) {
            Preconditions.checkNotNull(stateSnapshot);
            DispatchResult result = new DispatchResult();
            result.status = DispatchStatus.SUCCESS;
            result.state = stateSnapshot;
            return result;
        }

        public static DispatchResult fail(DispatchStatus type, String reason) {
            Preconditions.checkNotNull(type);
            Preconditions.checkNotNull(reason);
            DispatchResult result = new DispatchResult();
            result.status = type;
            result.failReason = reason;
            return result;
        }

        public static DispatchResult fail(DispatchStatus type, TransactionStateSnapshot stateSnapshot, String reason) {
            Preconditions.checkNotNull(type);
            Preconditions.checkNotNull(stateSnapshot);
            Preconditions.checkNotNull(reason);
            DispatchResult result = new DispatchResult();
            result.status = type;
            result.state = stateSnapshot;
            result.failReason = reason;
            return result;
        }
    }

    // For testing
    interface TaskExecuteListener {
        void onFinish(Task task, DispatchResult result, int numRetry);
    }
}
