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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.ExplicitTxnState;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.warehouse.cngroup.ComputeResource;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Multi-statement stream load task state transition diagram:
 *
 * <pre>
 *   BEGIN
 *     |
 *     +--[commitTxn]-------------------------> COMMITING
 *     |                                            |
 *     |                                            +--[success]---------> COMMITED
 *     |                                            +--[fail]------------> ABORTING
 *     |
 *     +--[manualCancel / timeout / exception]----> ABORTING
 *     +--[cancelAfterRestart, txnId!=0]---------> ABORTING
 *     +--[cancelAfterRestart, txnId==0]---------> CANCELLED
 *
 *   ABORTING
 *     |
 *     +--[tryRollbackNow / reconcile success]----> CANCELLED
 *     +--[reconcile finds txn committed]---------> COMMITED
 * </pre>
 *
 * Final states: CANCELLED, COMMITED, FINISHED.
 * COMMITING and ABORTING are intermediate; cancel is rejected in COMMITING.
 * BEFORE_LOAD, LOADING, PREPARING, PREPARED are defined for compatibility but not used in this class.
 */
public class StreamLoadMultiStmtTask extends AbstractStreamLoadTask {
    private static final Logger LOG = LogManager.getLogger(StreamLoadMultiStmtTask.class);

    /** Initial retry interval (ms) for stream load abort rollback. */
    private static final long ABORT_RETRY_INTERVAL_MS = 5000;
    /** Max retry interval (ms) for stream load abort rollback backoff. */
    private static final long ABORT_RETRY_MAX_INTERVAL_MS = 60000;

    public enum State {
        BEGIN,
        BEFORE_LOAD,
        LOADING,
        PREPARING,
        PREPARED,
        COMMITING,  // Intermediate state: commit in progress, reject cancel during this state
        ABORTING,   // Intermediate state: rollback in progress or pending retry, non-final
        COMMITED,
        CANCELLED,
        FINISHED
    }

    @SerializedName(value = "id")
    private long id;
    private TUniqueId loadId;
    @SerializedName("loadIdHi")
    private long loadIdHi;
    @SerializedName("loadIdLo")
    private long loadIdLo;
    @SerializedName(value = "label")
    private String label;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "txnId")
    private long txnId;
    @SerializedName(value = "createTimeMs")
    private long createTimeMs;
    @SerializedName(value = "state")
    private State state;
    @SerializedName(value = "warehouseId")
    private long warehouseId;
    @SerializedName(value = "computeResource")
    private ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;

    @SerializedName(value = "type")
    private StreamLoadTask.Type type = StreamLoadTask.Type.MULTI_STATEMENT_STREAM_LOAD;

    private ConnectContext context = new ConnectContext();
    @SerializedName(value = "taskMaps")
    private Map<String, StreamLoadTask> taskMaps = new ConcurrentHashMap<>();
    private ReentrantReadWriteLock lock;
    @SerializedName(value = "timeoutMs")
    private long timeoutMs;
    @SerializedName(value = "user")
    private String user = "";
    @SerializedName(value = "clientIp")
    private String clientIp = "";
    @SerializedName(value = "endTimeMs")
    private long endTimeMs = -1;

    private String errorMsg = "";
    private int channelNum = 0;
    private List<Object> channels = Lists.newArrayList();

    @SerializedName(value = "abortRetryCount")
    private int abortRetryCount = 0;
    @SerializedName(value = "nextAbortRetryTimeMs")
    private long nextAbortRetryTimeMs = 0;
    @SerializedName(value = "abortReason")
    private String abortReason = "";
    private String lastAbortErrorMsg = "";

    public StreamLoadMultiStmtTask(long id, Database db, String label, String user, String clientIp,
                          long timeoutMs, long createTimeMs, ComputeResource computeResource) {
        this.id = id;
        this.dbId = db.getId();
        this.dbName = db.getFullName();
        this.label = label;
        this.user = user;
        this.clientIp = clientIp;
        this.timeoutMs = timeoutMs;
        this.createTimeMs = createTimeMs;
        this.computeResource = computeResource;
        this.warehouseId = computeResource.getWarehouseId();
        this.state = State.BEGIN;
        this.type = StreamLoadTask.Type.MULTI_STATEMENT_STREAM_LOAD;
        this.loadId = UUIDUtil.genTUniqueId();
        init();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void init() {
        this.lock = new ReentrantReadWriteLock(true);
    }

    /**
     * Transition state to ABORTING and record the reason.
     * Caller MUST hold writeLock.
     */
    private void transitionToAborting(String reason) {
        this.state = State.ABORTING;
        this.abortReason = reason;
        this.abortRetryCount = 0;
        this.nextAbortRetryTimeMs = 0;
        this.lastAbortErrorMsg = "";
    }

    /**
     * Attempt to rollback the transaction right now.
     * If rollback succeeds, transitions state from ABORTING to CANCELLED.
     * If rollback fails, stays in ABORTING with retry info updated.
     * Also cancels sub-task coordinators on success.
     *
     * @return true if transaction has reached final state (CANCELLED or COMMITED)
     */
    private boolean tryRollbackNow() {
        if (txnId == 0 || context == null) {
            writeLock();
            try {
                this.state = State.CANCELLED;
                this.endTimeMs = System.currentTimeMillis();
            } finally {
                writeUnlock();
            }
            return true;
        }

        boolean rollbackSuccess = false;
        try {
            TransactionStmtExecutor.rollbackStmt(context, new RollbackStmt(NodePosition.ZERO));
            rollbackSuccess = true;
        } catch (Exception e) {
            this.lastAbortErrorMsg = e.getMessage();
            LOG.warn("Failed to rollback transaction, label: {}, txnId: {}, reason: {}, " +
                    "retryCount: {}", label, txnId, abortReason, abortRetryCount, e);
        }

        if (!rollbackSuccess) {
            rollbackSuccess = reconcileWithTxnManager();
        }

        if (rollbackSuccess) {
            for (StreamLoadTask task : taskMaps.values()) {
                task.cancelCoordinatorOnly(abortReason);
            }
            writeLock();
            try {
                if (this.state != State.COMMITED) {
                    this.state = State.CANCELLED;
                }
                this.endTimeMs = System.currentTimeMillis();
            } finally {
                writeUnlock();
            }
            LOG.info("Transaction rollback succeeded, label: {}, txnId: {}, reason: {}, " +
                    "retryCount: {}", label, txnId, abortReason, abortRetryCount);
            return true;
        }

        writeLock();
        try {
            this.abortRetryCount++;
            long backoffMs = Math.min(
                    ABORT_RETRY_INTERVAL_MS * (1L << Math.min(abortRetryCount, 10)),
                    ABORT_RETRY_MAX_INTERVAL_MS);
            this.nextAbortRetryTimeMs = System.currentTimeMillis() + backoffMs;
        } finally {
            writeUnlock();
        }
        return false;
    }

    /**
     * Check the actual transaction status in the transaction manager to handle cases
     * where rollbackStmt fails but the transaction has already reached a final state.
     *
     * @return true if the transaction is confirmed to be in a final state
     */
    private boolean reconcileWithTxnManager() {
        try {
            GlobalTransactionMgr txnMgr =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            ExplicitTxnState explicitState = txnMgr.getExplicitTxnState(txnId);
            if (explicitState == null) {
                LOG.info("ExplicitTxnState already cleared for txnId: {}, label: {}, " +
                        "treating as aborted", txnId, label);
                return true;
            }
            TransactionState txnState = explicitState.getTransactionState();
            if (txnState != null) {
                TransactionStatus status = txnState.getTransactionStatus();
                if (status == TransactionStatus.ABORTED || status == TransactionStatus.VISIBLE
                        || status == TransactionStatus.COMMITTED) {
                    LOG.info("Transaction already in final status {}, label: {}, txnId: {}",
                            status, label, txnId);
                    if (status == TransactionStatus.VISIBLE
                            || status == TransactionStatus.COMMITTED) {
                        writeLock();
                        try {
                            this.state = State.COMMITED;
                            this.endTimeMs = System.currentTimeMillis();
                        } finally {
                            writeUnlock();
                        }
                    }
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to reconcile transaction status, label: {}, txnId: {}",
                    label, txnId, e);
        }
        return false;
    }

    /**
     * Called by background cleanup thread to retry abort for tasks stuck in ABORTING state.
     *
     * @param currentMs current time in milliseconds
     * @return true if task is no longer in ABORTING state (either CANCELLED or reconciled)
     */
    public boolean retryAbortIfNeeded(long currentMs) {
        if (this.state != State.ABORTING) {
            return true;
        }
        if (currentMs < this.nextAbortRetryTimeMs) {
            return false;
        }
        LOG.info("Background retry abort for label: {}, txnId: {}, retryCount: {}, reason: {}",
                label, txnId, abortRetryCount, abortReason);
        return tryRollbackNow();
    }

    @Override
    public void beginTxnFromFrontend(TransactionResult resp) {
        beginTxn(resp);
    }

    @Override
    public long getCurrentWarehouseId() {
        return warehouseId;
    }

    @Override
    public void beginTxnFromFrontend(int channelId, int channelNum, TransactionResult resp) {
        beginTxn(resp);
    }

    @Override
    public void beginTxnFromBackend(TUniqueId requestId, String clientIp, long backendId, TransactionResult resp) {
    }

    public void beginTxn(TransactionResult resp) {
        // Check if task already committed or finished - return LABEL_ALREADY_EXISTS like basic transaction
        if (this.state == State.COMMITED || this.state == State.FINISHED) {
            resp.status = ActionStatus.LABEL_ALREADY_EXISTS;
            resp.msg = String.format("Label [%s] has already been used.", label);
            resp.addResultEntry("ExistingJobStatus", state.name());
            return;
        }

        // Check if transaction already started (Double Begin) - return LABEL_ALREADY_EXISTS like basic transaction
        if (this.txnId != 0) {
            resp.status = ActionStatus.LABEL_ALREADY_EXISTS;
            resp.msg = String.format("Transaction already started with label [%s], txnId: %d", label, txnId);
            resp.addResultEntry("ExistingJobStatus", state.name());
            resp.addResultEntry("TxnId", txnId);
            return;
        }

        // Ensure a non-empty label is generated in TransactionStmtExecutor.beginStmt
        // by providing a valid executionId. Use the pre-generated loadId as executionId.
        if (context.getExecutionId() == null) {
            context.setExecutionId(loadId);
        }
        // Also propagate compute resource so the txn carries the same resource context.
        if (context.getCurrentComputeResource() == null) {
            context.setCurrentComputeResource(computeResource);
        }

        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO),
                TransactionState.LoadJobSourceType.MULTI_STATEMENT_STREAMING, label);
        this.txnId = context.getTxnId();
        LOG.info("start transaction id {}", txnId);
    }

    @Override
    public void waitCoordFinishAndPrepareTxn(long preparedTimeoutMs, TransactionResult resp) {
        // Prepare all sub-tasks
        for (StreamLoadTask task : taskMaps.values()) {
            task.waitCoordFinishAndPrepareTxn(preparedTimeoutMs, resp);
            if (!resp.stateOK()) {
                return;
            }
        }
    }

    @Override
    public void commitTxn(HttpHeaders headers, TransactionResult resp) throws StarRocksException {
        // Phase 1: Check state and mark as COMMITING (with lock)
        writeLock();
        try {
            if (this.state == State.COMMITED || this.state == State.FINISHED) {
                resp.setOKMsg(String.format("Transaction %d has already committed, label is %s", txnId, label));
                resp.addResultEntry("Label", label);
                resp.addResultEntry("TxnId", txnId);
                return;
            }
            if (this.state == State.CANCELLED) {
                resp.setErrorMsg(String.format("Can not commit CANCELLED transaction %d, label is %s", txnId, label));
                return;
            }
            if (this.state == State.ABORTING) {
                resp.setErrorMsg(String.format("Transaction %d is aborting (reason: %s), label is %s",
                        txnId, abortReason, label));
                return;
            }
            if (this.state == State.COMMITING) {
                resp.setErrorMsg(String.format("Transaction %d is already committing, label is %s", txnId, label));
                return;
            }
            this.state = State.COMMITING;
        } finally {
            writeUnlock();
        }

        // Phase 2: Execute time-consuming operations (without lock)
        boolean success = false;
        String commitErrorMsg = null;
        try {
            LOG.info("commit {} sub tasks", taskMaps.size());
            for (StreamLoadTask task : taskMaps.values()) {
                task.prepareChannel(0, task.getTableName(), headers, resp);
                if (!resp.stateOK()) {
                    commitErrorMsg = "prepareChannel failed";
                    return;
                }
                if (task.checkNeedPrepareTxn()) {
                    task.waitCoordFinish(resp);
                }
                if (!resp.stateOK()) {
                    commitErrorMsg = "waitCoordFinish failed";
                    return;
                }
                TransactionStmtExecutor.loadData(dbId, task.getTable().getId(), task.getTxnStateItem(), context);
            }
            TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));
            if (context.getState().isError()) {
                commitErrorMsg = context.getState().getErrorMessage();
            } else {
                success = true;
            }
        } catch (Exception e) {
            commitErrorMsg = e.getMessage();
            throw e;
        } finally {
            if (success) {
                writeLock();
                try {
                    this.state = State.COMMITED;
                    this.endTimeMs = System.currentTimeMillis();
                } finally {
                    writeUnlock();
                }
            } else {
                this.errorMsg = commitErrorMsg != null ? commitErrorMsg : "commit failed";
                LOG.warn("Commit failed for transaction {}, label: {}, error: {}",
                        txnId, label, this.errorMsg);
                writeLock();
                try {
                    transitionToAborting("commit failed: " + this.errorMsg);
                } finally {
                    writeUnlock();
                }
                tryRollbackNow();
            }
        }
    }

    @Override
    public void manualCancelTask(TransactionResult resp) throws StarRocksException {
        writeLock();
        try {
            if (this.state == State.COMMITED || this.state == State.FINISHED) {
                resp.setErrorMsg(String.format("Can not abort VISIBLE transaction %d, label is %s", txnId, label));
                return;
            }
            if (this.state == State.CANCELLED) {
                resp.setOKMsg(String.format("Transaction %d has already aborted, label is %s", txnId, label));
                resp.addResultEntry("Label", label);
                resp.addResultEntry("TxnId", txnId);
                return;
            }
            if (this.state == State.COMMITING) {
                resp.setErrorMsg(String.format("Can not abort transaction %d during committing, label is %s",
                        txnId, label));
                return;
            }
            if (this.state == State.ABORTING) {
                resp.setOKMsg(String.format("Transaction %d abort is already in progress, label is %s. " +
                        "Background retry will continue.", txnId, label));
                return;
            }
            transitionToAborting("manual abort");
        } finally {
            writeUnlock();
        }

        if (tryRollbackNow()) {
            resp.setOKMsg("stream load " + label + " abort");
        } else {
            resp.setOKMsg(String.format("stream load %s abort initiated, rollback pending " +
                    "(background retry will continue), label is %s, txnId is %d", label, label, txnId));
        }
    }

    @Override
    public boolean checkNeedRemove(long currentMs, boolean isForce) {
        if (this.state == State.ABORTING) {
            return false;
        }

        if (isFinalState()) {
            if (endTimeMs == -1) {
                endTimeMs = currentMs;
            }
            return isForce || ((currentMs - endTimeMs) > Config.stream_load_task_keep_max_second * 1000L);
        }

        if (isTimeout(currentMs)) {
            cancelOnTimeout();
            return false;
        }

        return false;
    }

    public boolean isAborting() {
        return this.state == State.ABORTING;
    }

    /**
     * Check if the task has exceeded its timeout period.
     * Timeout is calculated from createTimeMs (when BEGIN was called).
     */
    public boolean isTimeout(long currentMs) {
        if (isFinalState() || this.state == State.ABORTING) {
            return false;
        }
        return currentMs - createTimeMs > timeoutMs;
    }

    public void cancelOnTimeout() {
        writeLock();
        try {
            if (isFinalState() || this.state == State.COMMITING || this.state == State.ABORTING) {
                return;
            }
            LOG.info("Cancel multi-statement stream load task due to timeout, label: {}, txnId: {}, " +
                    "createTimeMs: {}, timeoutMs: {}", label, txnId, createTimeMs, timeoutMs);
            this.errorMsg = "transaction timeout after " + timeoutMs + "ms";
            transitionToAborting("timeout");
        } finally {
            writeUnlock();
        }

        tryRollbackNow();
    }

    public void cancelOnException(String errorMessage) {
        writeLock();
        try {
            if (isFinalState() || this.state == State.COMMITING || this.state == State.ABORTING) {
                return;
            }
            LOG.warn("Multi-statement stream load task cancelled due to exception, label: {}, txnId: {}, " +
                    "error: {}. Transaction auto-aborted (consistent with basic transaction behavior).",
                    label, txnId, errorMessage);
            this.errorMsg = errorMessage;
            transitionToAborting("LOAD exception: " + errorMessage);
        } finally {
            writeUnlock();
        }

        tryRollbackNow();
    }

    @Override
    public boolean checkNeedPrepareTxn() {
        return taskMaps.values().stream().allMatch(task -> task.checkNeedPrepareTxn());
    }

    @Override
    public boolean isDurableLoadState() {
        return state == State.PREPARED || state == State.CANCELLED || state == State.COMMITED
                || state == State.FINISHED || state == State.ABORTING;
    }

    @Override
    public long endTimeMs() {
        return endTimeMs;
    }

    @Override
    public long createTimeMs() {
        return createTimeMs;
    }

    @Override
    public List<List<String>> getShowInfo() {
        List<List<String>> result = Lists.newArrayList();
        for (StreamLoadTask task : taskMaps.values()) {
            result.addAll(task.getShowInfo());
        }
        return result;
    }

    @Override
    public List<List<String>> getShowBriefInfo() {
        List<List<String>> result = Lists.newArrayList();
        for (StreamLoadTask task : taskMaps.values()) {
            result.addAll(task.getShowBriefInfo());
        }
        return result;
    }

    // Implement abstract methods from parent class
    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public long getDBId() {
        return dbId;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public Long getLoadStartTimeMs() {
        return null;
    }

    @Override
    public String getDBName() {
        return dbName;
    }

    @Override
    public long getTxnId() {
        return txnId;
    }

    @Override
    public String getStateName() {
        return state.name();
    }

    @Override
    public boolean isFinal() {
        return isFinalState();
    }

    @Override
    public boolean isFinalState() {
        if (this.state == State.CANCELLED || this.state == State.COMMITED || this.state == State.FINISHED) {
            return true;
        }
        // ABORTING is NOT a final state - rollback is pending
        return false;
    }

    @Override
    public List<TLoadInfo> toThrift() {
        // loop taskMaps
        List<TLoadInfo> result = Lists.newArrayList();
        for (StreamLoadTask task : taskMaps.values()) {
            result.addAll(task.toThrift());
        }
        return result;
    }

    @Override
    public List<TStreamLoadInfo> toStreamLoadThrift() {
        // loop taskMaps
        List<TStreamLoadInfo> result = Lists.newArrayList();
        for (StreamLoadTask task : taskMaps.values()) {
            result.addAll(task.toStreamLoadThrift());
        }
        return result;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        loadId = new TUniqueId(loadIdHi, loadIdLo);
        init();
    }

    @Override
    public void gsonPreProcess() throws IOException {
        loadIdHi = loadId.getHi();
        loadIdLo = loadId.getLo();
    }

    @Override
    public TNetworkAddress tryLoad(int channelId, String tableName, TransactionResult resp) throws StarRocksException {
        if (this.state == State.CANCELLED) {
            resp.setErrorMsg(String.format("stream load task %s has already been cancelled: %s", label, errorMsg));
            return null;
        }
        if (this.state == State.ABORTING) {
            resp.setErrorMsg(String.format("stream load task %s is aborting: %s", label, abortReason));
            return null;
        }
        if (this.state == State.COMMITED || this.state == State.FINISHED) {
            resp.setErrorMsg(String.format("stream load task %s has already been %s", label, state.name().toLowerCase()));
            return null;
        }

        // For multi-statement tasks, delegate to specific table task if exists
        StreamLoadTask task = taskMaps.get(tableName);
        if (task != null) {
            return task.tryLoad(0, tableName, resp);
        } else {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                throw new MetaNotFoundException("Database " + dbId + "has been deleted");
            }
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, tableName);
            if (table == null) {
                throw new MetaNotFoundException("Failed to find table " + tableName + " in db " + dbName);
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            task = new StreamLoadTask(id, db, table, label, user, clientIp, timeoutMs, 1, 0, createTimeMs, computeResource);
            taskMaps.put(table.getName(), task);
            LOG.info("Add stream load task {}", task.getShowInfo());
            task.tryBegin(0, 1, txnId);
            return task.tryLoad(0, tableName, resp);
        }
    }

    @Override
    public TNetworkAddress executeTask(int channelId, String tableName, HttpHeaders headers, TransactionResult resp) {
        if (this.state == State.CANCELLED) {
            resp.setErrorMsg(String.format("stream load task %s has already been cancelled: %s", label, errorMsg));
            return null;
        }
        if (this.state == State.ABORTING) {
            resp.setErrorMsg(String.format("stream load task %s is aborting: %s", label, abortReason));
            return null;
        }
        if (this.state == State.COMMITED || this.state == State.FINISHED) {
            resp.setErrorMsg(String.format("stream load task %s has already been %s", label, state.name().toLowerCase()));
            return null;
        }

        // For multi-statement tasks, delegate to specific table task if exists
        StreamLoadTask task = taskMaps.get(tableName);
        if (task != null) {
            return task.executeTask(0, tableName, headers, resp);
        }
        // For multi-statement tasks, we don't strictly validate table names
        // as they can handle multiple tables dynamically
        resp.setErrorMsg("Table " + tableName + " not found in multi-statement task or not initialized yet");
        return null;
    }

    @Override
    public void prepareChannel(int channelId, String tableName, HttpHeaders headers, TransactionResult resp) {
        // For multi-statement tasks, delegate to specific table task if exists
        StreamLoadTask task = taskMaps.get(tableName);
        if (task != null) {
            task.prepareChannel(0, tableName, headers, resp);
        } else {
            // For multi-statement tasks, we don't strictly validate table names
            // as they can handle multiple tables dynamically
            resp.setErrorMsg("Table " + tableName + " not found in multi-statement task or not initialized yet");
        }
    }

    @Override
    public void cancelAfterRestart() {
        for (StreamLoadTask task : taskMaps.values()) {
            task.cancelAfterRestart();
        }
        this.errorMsg = "cancelled after FE restart";
        if (txnId != 0) {
            transitionToAborting("recover after FE restart");
            tryRollbackNow();
        } else {
            this.state = State.CANCELLED;
            this.endTimeMs = System.currentTimeMillis();
        }
    }

    @Override
    public long getFinishTimestampMs() {
        // loop taskMaps get max finish timestamp
        return taskMaps.values().stream()
                .mapToLong(StreamLoadTask::getFinishTimestampMs)
                .max()
                .orElse(-1L);
    }

    @Override
    public String getStringByType() {
        return "MULTI_STATEMENT_STREAM_LOAD";
    }

    @Override
    public String getTableName() {
        // loop taskMaps and get table names
        return String.join(",", taskMaps.keySet());
    }

    @Override
    public void beforePrepared(TransactionState txnState) throws TransactionException {
        for (StreamLoadTask task : taskMaps.values()) {
            task.beforePrepared(txnState);
        }
    }

    @Override
    public void afterPrepared(TransactionState txnState, boolean txnOperated) throws StarRocksException {
        for (StreamLoadTask task : taskMaps.values()) {
            task.afterPrepared(txnState, txnOperated);
        }
    }

    @Override
    public void replayOnPrepared(TransactionState txnState) {
        for (StreamLoadTask task : taskMaps.values()) {
            task.replayOnPrepared(txnState);
        }
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        for (StreamLoadTask task : taskMaps.values()) {
            task.beforeCommitted(txnState);
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws StarRocksException {
        for (StreamLoadTask task : taskMaps.values()) {
            task.afterCommitted(txnState, txnOperated);
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        for (StreamLoadTask task : taskMaps.values()) {
            task.replayOnCommitted(txnState);
        }
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws StarRocksException {
        for (StreamLoadTask task : taskMaps.values()) {
            task.afterAborted(txnState, txnOperated, txnStatusChangeReason);
        }
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        for (StreamLoadTask task : taskMaps.values()) {
            task.replayOnAborted(txnState);
        }
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        for (StreamLoadTask task : taskMaps.values()) {
            task.afterVisible(txnState, txnOperated);
        }
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        for (StreamLoadTask task : taskMaps.values()) {
            task.replayOnVisible(txnState);
        }
    }

    public Collection<StreamLoadTask> getTasks() {
        return taskMaps.values();
    }
}
