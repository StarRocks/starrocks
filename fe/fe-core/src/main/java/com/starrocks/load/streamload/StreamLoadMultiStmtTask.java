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
import com.starrocks.common.io.Text;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.warehouse.cngroup.ComputeResource;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamLoadMultiStmtTask extends AbstractStreamLoadTask {
    private static final Logger LOG = LogManager.getLogger(StreamLoadMultiStmtTask.class);

    public enum State {
        BEGIN,
        BEFORE_LOAD,
        LOADING,
        PREPARING,
        PREPARED,
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

    // Add missing fields
    private String errorMsg = "";
    private int channelNum = 0;
    private List<Object> channels = Lists.newArrayList();

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
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));
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
        // Commit all sub-tasks
        LOG.info("commit {} sub tasks", taskMaps.size());
        for (StreamLoadTask task : taskMaps.values()) {
            task.prepareChannel(0, task.getTableName(), headers, resp);
            if (!resp.stateOK()) {
                return;
            }
            if (task.checkNeedPrepareTxn()) {
                task.waitCoordFinish(resp);
            }
            if (!resp.stateOK()) {
                return;
            }
            TransactionStmtExecutor.loadData(dbId, task.getTable().getId(), task.getTxnStateItem(), context);
        }
        TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));
        this.state = State.COMMITED;
    }

    @Override
    public void manualCancelTask(TransactionResult resp) throws StarRocksException {
        // Cancel all sub-tasks
        for (StreamLoadTask task : taskMaps.values()) {
            task.manualCancelTask(resp);
        }
        this.state = State.CANCELLED;
        this.endTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean checkNeedRemove(long currentMs, boolean isForce) {
        if (!isFinalState()) {
            return false;
        }
        if (endTimeMs == -1) {
            return false;
        }
        return isForce || ((currentMs - endTimeMs) > Config.stream_load_task_keep_max_second * 1000L);
    }

    @Override
    public boolean checkNeedPrepareTxn() {
        return taskMaps.values().stream().allMatch(task -> task.checkNeedPrepareTxn());
    }

    @Override
    public boolean isDurableLoadState() {
        return state == State.PREPARED || state == State.CANCELLED || state == State.COMMITED || state == State.FINISHED;
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
        for (StreamLoadTask task : taskMaps.values()) {
            if (!task.isFinalState()) {
                return false;
            }
        }
        return true;
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

    // Implement remaining abstract methods from parent classes
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        loadId = new TUniqueId(loadIdHi, loadIdLo);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        loadIdHi = loadId.getHi();
        loadIdLo = loadId.getLo();
    }

    @Override
    public TNetworkAddress tryLoad(int channelId, String tableName, TransactionResult resp) throws StarRocksException {
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
        // loop taskMaps and execute cancelAfterRestart
        for (StreamLoadTask task : taskMaps.values()) {
            task.cancelAfterRestart();
        }
        state = State.CANCELLED;
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
}
