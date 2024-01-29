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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.Version;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.LoadJobWithWarehouse;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStreamLoadChannel;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionCommitFailedException;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamLoadTask extends AbstractTxnStateChangeCallback
        implements Writable, GsonPostProcessable, GsonPreProcessable, LoadJobWithWarehouse {
    private static final Logger LOG = LogManager.getLogger(StreamLoadTask.class);

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

    public enum Type {
        STREAM_LOAD,
        ROUTINE_LOAD,
        PARALLEL_STREAM_LOAD     // default
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
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "errorMsg")
    private String errorMsg;
    @SerializedName(value = "trackingUrl")
    private String trackingUrl;
    @SerializedName(value = "timeoutMs")
    private long timeoutMs;
    @SerializedName(value = "createTimeMs")
    private long createTimeMs;
    @SerializedName(value = "state")
    private State state;
    @SerializedName(value = "beforeLoadTimeMs")
    private long beforeLoadTimeMs;
    @SerializedName(value = "startLoadingTimeMs")
    private long startLoadingTimeMs;
    @SerializedName(value = "startPreparingTimeMs")
    private long startPreparingTimeMs;
    @SerializedName(value = "finishPreparingTimeMs")
    private long finishPreparingTimeMs;
    @SerializedName(value = "endTimeMs")
    private long endTimeMs;
    @SerializedName(value = "txnId")
    private long txnId;
    @SerializedName(value = "channelNum")
    private int channelNum;
    @SerializedName(value = "preparedChannelNum")
    private int preparedChannelNum;
    @SerializedName(value = "numRowsNormal")
    private long numRowsNormal;
    @SerializedName(value = "numRowsAbnormal")
    private long numRowsAbnormal;
    @SerializedName(value = "numRowsUnselected")
    private long numRowsUnselected;
    @SerializedName(value = "numLoadBytesTotal")
    private long numLoadBytesTotal;

    // used for sync stream load and routine load
    private boolean isSyncStreamLoad = false;

    private Type type = Type.PARALLEL_STREAM_LOAD;

    private List<State> channels;
    private StreamLoadParam streamLoadParam;
    private StreamLoadInfo streamLoadInfo;
    private Coordinator coord;
    private Map<Integer, TNetworkAddress> channelIdToBEHTTPAddress;
    private Map<Integer, TNetworkAddress> channelIdToBEHTTPPort;
    private OlapTable table;
    private long taskDeadlineMs;
    private boolean isCommitting;

    private ReentrantReadWriteLock lock;

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public StreamLoadTask(long id, Database db, OlapTable table, String label,
                          long timeoutMs, long createTimeMs, boolean isRoutineLoad) {
        this(id, db, table, label, timeoutMs, 1, 0, createTimeMs);
        isSyncStreamLoad = true;
        if (isRoutineLoad) {
            type = Type.ROUTINE_LOAD;
        } else {
            type = Type.STREAM_LOAD;
        }
    }
    public StreamLoadTask(long id, Database db, OlapTable table, String label,
            long timeoutMs, int channelNum, int channelId, long createTimeMs) {
        this.id = id;
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.dbId = db.getId();
        this.dbName = db.getFullName();
        this.tableId = table.getId();
        this.tableName = table.getName();
        this.table = table;
        this.label = label;
        this.timeoutMs = timeoutMs;
        this.channelNum = channelNum;
        this.createTimeMs = createTimeMs;
        this.state = State.BEGIN;
        this.preparedChannelNum = 0;
        this.numRowsNormal = -1;
        this.numRowsAbnormal = -1;
        this.numRowsUnselected = -1;
        this.numLoadBytesTotal = -1;
        this.trackingUrl = "";
        this.endTimeMs = -1;
        this.txnId = -1;
        this.errorMsg = null;

        init();
    }

    public void init() {
        this.lock = new ReentrantReadWriteLock(true);
        this.taskDeadlineMs = this.createTimeMs + this.timeoutMs;
        this.channels = Lists.newArrayListWithCapacity(this.channelNum);
        for (int i = 0; i < this.channelNum; i++) {
            this.channels.add(this.state);
        }
        this.channelIdToBEHTTPAddress = null;
        this.channelIdToBEHTTPPort = null;
        this.coord = null;
        this.streamLoadParam = null;
        this.streamLoadInfo = null;
        this.isCommitting = false;
    }

    @Override
    public String getCurrentWarehouse() {
        // TODO(lzh): pass the current warehouse.
        return WarehouseManager.DEFAULT_WAREHOUSE_NAME;
    }

    @Override
    public boolean isFinal() {
        return isFinalState();
    }

    @Override
    public long getFinishTimestampMs() {
        return endTimeMs();
    }

    public void beginTxn(int channelId, int channelNum, TransactionResult resp) {
        long startTimeMs = System.currentTimeMillis();
        boolean exception = false;
        writeLock();
        try {
            if (channelNum != this.channelNum) {
                throw new Exception("channel num " + String.valueOf(channelNum) + " does not equal to original channel num "
                    + String.valueOf(this.channelNum));
            }
            if (channelId >= this.channelNum || channelId < 0) {
                throw new Exception("channel id should be between [0, " + String.valueOf(this.channelNum - 1) + "].");
            }

            switch (this.state) {
                case BEGIN: {
                    unprotectedBeginTxn(false);
                    this.state = State.BEFORE_LOAD;
                    this.channels.set(channelId, State.BEFORE_LOAD);
                    this.beforeLoadTimeMs = System.currentTimeMillis();
                    resp.addResultEntry("Label", this.label);
                    resp.addResultEntry("TxnId", this.txnId);
                    resp.addResultEntry("BeginChannel", channelNum);
                    resp.addResultEntry("BeginTxnTimeMs", this.beforeLoadTimeMs - this.createTimeMs);
                    LOG.info("stream load {} channel_id {} begin. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    break;
                }
                case BEFORE_LOAD:
                case LOADING:
                case PREPARING: {
                    if (this.channels.get(channelId) != State.BEGIN) {
                        resp.setOKMsg("stream load " + label + " channel " + String.valueOf(channelId)
                                + " has already begun");
                        break;
                    }
                    this.channels.set(channelId, State.BEFORE_LOAD);
                    resp.addResultEntry("BeginChannel", channelNum);
                    LOG.info("stream load {} channel_id {} begin. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    break;
                }
                case PREPARED: {
                    resp.setOKMsg("stream load task " + label + " has already been prepared");
                    break;
                }
                case COMMITED: {
                    resp.setOKMsg("stream load task " + label + " has already been committed");
                    break;
                }
                case CANCELLED: {
                    resp.setOKMsg("stream load task " + label + " has already been cancelled: "
                            + this.errorMsg);
                    break;
                }
                case FINISHED: {
                    resp.setOKMsg("stream load task " + label + " has already been finished");
                    break;
                }
                default: {
                    resp.setOKMsg("stream load task is in unexpected state " + this.state);
                    break;
                }
            }
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        } finally {
            writeUnlock();
        }

        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
        }
    }

    public TNetworkAddress tryLoad(int channelId, TransactionResult resp) {
        long startTimeMs = System.currentTimeMillis();
        boolean needUnLock = true;
        boolean exception = false;
        readLock();
        try {
            if (channelId >= this.channelNum || channelId < 0) {
                throw new Exception("channel id should be between [0, " + String.valueOf(this.channelNum - 1) + "].");
            }
            switch (this.state) {
                case BEFORE_LOAD: {
                    // do nothing, we will change state to loading in executeTask
                    break;
                }
                case LOADING:
                case PREPARING: {
                    if (this.channels.get(channelId) != State.BEGIN
                            && this.channels.get(channelId) != State.BEFORE_LOAD
                            && this.channels.get(channelId) != State.LOADING) {
                        readUnlock();
                        needUnLock = false;
                        throw new Exception(
                                "channel state should be BEGIN | BEFORE_LOAD | LOADING when channel is loading");
                    }
                    this.channels.set(channelId, State.LOADING);
                    TNetworkAddress redirectAddr = channelIdToBEHTTPAddress.get(channelId);
                    if (redirectAddr == null) {
                        throw new Exception(
                            "can not find redirect address for stream load label " + label + ", channel id " + channelId);
                    }
                    return redirectAddr;
                }
                case PREPARED: {
                    resp.setOKMsg("stream load task " + label + " has already been prepared");
                    break;
                }
                case COMMITED: {
                    resp.setOKMsg("stream load task " + label + " has already been committed");
                    break;
                }
                case CANCELLED: {
                    resp.setOKMsg("stream load task " + label + " has already been cancelled: "
                            + this.errorMsg);
                    break;
                }
                case FINISHED: {
                    resp.setOKMsg("stream load task " + label + " has already been finished");
                    break;
                }
                default: {
                    resp.setOKMsg("stream load task is in unexpected state " + this.state);
                    break;
                }
            }
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        } finally {
            if (needUnLock) {
                readUnlock();
            }
        }
        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
        }
        return null;
    }

    public TNetworkAddress executeTask(int channelId, HttpHeaders headers, TransactionResult resp) {
        long startTimeMs = System.currentTimeMillis();
        boolean exception = false;
        writeLock();
        try {
            if (channelId >= this.channelNum || channelId < 0) {
                throw new Exception("channel id should be between [0, " + String.valueOf(this.channelNum - 1) + "].");
            }
            switch (this.state) {
                case BEFORE_LOAD: {
                    if (this.channels.get(channelId) != State.BEGIN
                            && this.channels.get(channelId) != State.BEFORE_LOAD) {
                        throw new Exception(
                                "channel state should be BEGIN | BEFORE_LOAD when task is going to execute");
                    }
                    unprotectedExecute(headers);
                    this.state = State.LOADING;
                    this.startLoadingTimeMs = System.currentTimeMillis();
                    this.channels.set(channelId, State.LOADING);
                    LOG.info("stream load {} channel_id {} start loading. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    return channelIdToBEHTTPAddress.get(channelId);
                }
                case LOADING:
                case PREPARING: {
                    if (this.channels.get(channelId) != State.BEGIN
                            && this.channels.get(channelId) != State.BEFORE_LOAD
                            && this.channels.get(channelId) != State.LOADING) {
                        throw new Exception(
                                "channel state should be BEGIN | BEFORE_LOAD | LOADING when channel is loading");
                    }
                    LOG.info("stream load {} channel_id {} start loading. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    this.channels.set(channelId, State.LOADING);
                    TNetworkAddress redirectAddr = channelIdToBEHTTPAddress.get(channelId);
                    if (redirectAddr == null) {
                        throw new Exception(
                            "can not find redirect address for stream load label " + label + ", channel id " + channelId);
                    }
                    return redirectAddr;
                }
                case PREPARED: {
                    resp.setOKMsg("stream load task " + label + " has already been prepared");
                    break;
                }
                case COMMITED: {
                    resp.setOKMsg("stream load task " + label + " has already been committed");
                    break;
                }
                case CANCELLED: {
                    resp.setOKMsg("stream load task " + label + " has already been cancelled: "
                            + this.errorMsg);
                    break;
                }
                case FINISHED: {
                    resp.setOKMsg("stream load task " + label + " has already been finished");
                    break;
                }
                default: {
                    resp.setOKMsg("stream load task is in unexpected state " + this.state);
                    break;
                }
            }
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        } finally {
            writeUnlock();
        }

        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
        }
        return null;
    }

    public void prepareChannel(int channelId,  HttpHeaders headers, TransactionResult resp) {
        long startTimeMs = System.currentTimeMillis();
        boolean needUnLock = true;
        boolean exception = false;
        writeLock();
        try {
            if (channelId >= this.channelNum || channelId < 0) {
                throw new Exception("channel id should be between [0, " + String.valueOf(this.channelNum - 1) + "].");
            }
            switch (this.state) {
                case BEFORE_LOAD: {
                    if (this.channels.get(channelId) != State.BEGIN
                            && this.channels.get(channelId) != State.BEFORE_LOAD) {
                        throw new Exception(
                                "channel state should be BEGIN | BEFORE_LOAD when task is going to execute");
                    }
                    unprotectedExecute(headers);
                    this.state = State.PREPARING;
                    this.channels.set(channelId, State.PREPARING);
                    this.startPreparingTimeMs = System.currentTimeMillis();
                    this.preparedChannelNum += 1;
                    LOG.info("stream load {} channel_id {} start preparing. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    
                    resp.addResultEntry("Label", this.label);
                    resp.addResultEntry("TxnId", this.txnId);
                    resp.addResultEntry("ChannelId", channelId);
                    resp.addResultEntry("Prepared Channel Num", this.preparedChannelNum);
                    writeUnlock();
                    needUnLock = false;
                    unprotectedFinishStreamLoadChannel(channelId);
                    return;
                }
                case LOADING: {
                    if (this.channels.get(channelId) != State.BEGIN
                            && this.channels.get(channelId) != State.BEFORE_LOAD
                            && this.channels.get(channelId) != State.LOADING) {
                        throw new Exception(
                                "channel state should be BEGIN | BEFORE_LOAD | LOADING when task is going to prepare, " +
                                " cur state is " + this.state);
                    }
                    this.channels.set(channelId, State.PREPARING);
                    this.state = State.PREPARING;
                    this.startPreparingTimeMs = System.currentTimeMillis();
                    this.preparedChannelNum += 1;

                    LOG.info("stream load {} channel_id {} start preparing. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    resp.addResultEntry("Label", this.label);
                    resp.addResultEntry("TxnId", this.txnId);
                    resp.addResultEntry("ChannelId", channelId);
                    resp.addResultEntry("Prepared Channel Num", this.preparedChannelNum);
                    writeUnlock();
                    needUnLock = false;
                    unprotectedFinishStreamLoadChannel(channelId);
                    return; 
                }
                case PREPARING: {
                    if (this.channels.get(channelId) != State.BEGIN
                            && this.channels.get(channelId) != State.BEFORE_LOAD
                            && this.channels.get(channelId) != State.LOADING) {
                        throw new Exception(
                                "channel state should be BEGIN | BEFORE_LOAD | LOADING when channel is ready for prepare, " +
                                "cur state is " + this.state);
                    }
                    this.channels.set(channelId, State.PREPARING);
                    this.state = State.PREPARING;
                    this.preparedChannelNum += 1;
                    LOG.info("stream load {} channel_id {} start preparing. db: {}, tbl: {}, txn_id: {}",
                            label, channelId, dbName, tableName, txnId);
                    resp.addResultEntry("Label", this.label);
                    resp.addResultEntry("TxnId", this.txnId);
                    resp.addResultEntry("ChannelId", channelId);
                    resp.addResultEntry("Prepared Channel Num", this.preparedChannelNum);
                    writeUnlock();
                    needUnLock = false;
                    unprotectedFinishStreamLoadChannel(channelId);        
                    return; 
                }
                case PREPARED: {
                    resp.setOKMsg("stream load task " + label + " has already been prepared");
                    break;
                }
                case COMMITED: {
                    resp.setOKMsg("stream load task " + label + " has already been committed");
                    break;
                }
                case CANCELLED: {
                    resp.setOKMsg("stream load task " + label + " has already been cancelled: "
                            + this.errorMsg);
                    break;
                }
                case FINISHED: {
                    resp.setOKMsg("stream load task " + label + " has already been finished");
                    break;
                }
                default: {
                    resp.setOKMsg("stream load task is in unexpected state " + this.state);
                    break;
                }
            }
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        } finally {
            if (needUnLock) {
                writeUnlock();
            }
        }

        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
        }
        return;
    }

    public void waitCoordFinishAndPrepareTxn(TransactionResult resp) {
        long startTimeMs = System.currentTimeMillis();
        boolean exception = false;
        writeLock();
        try {
            if (isFinalState()) {
                if (state == State.CANCELLED) {
                    resp.setOKMsg("txn could not be prepared because task state is: " + state
                            + ", error_msg: " + errorMsg);

                } else {
                    resp.setOKMsg("txn could not be prepared because task state is: " + state);
                }
                return;
            }
            if (this.state == State.PREPARED) {
                resp.setOKMsg("stream load task " + this.label + " has already been prepared");
                return;
            }
            if (this.state != State.PREPARING) {
                throw new UserException("stream load task " + this.label
                         + " s state (" + this.state + ") is not preparing, can not prepare txn");
            }
            unprotectedWaitCoordFinish();
            if (!checkDataQuality()) {
                throw new UserException("abnormal data more than max filter rate, tracking_url: " + 
                    this.trackingUrl);
            }
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        } finally {
            writeUnlock();
        }

        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
            return;
        }
        
        try {
            unprotectedPrepareTxn();
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        }

        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
            return;
        }
            

        resp.addResultEntry("NumberTotalRows", numRowsNormal + numRowsAbnormal + numRowsUnselected);
        resp.addResultEntry("NumberLoadedRows", numRowsNormal);
        resp.addResultEntry("NumberFilteredRows", numRowsAbnormal);
        resp.addResultEntry("NumberUnselectedRows", numRowsUnselected);
        resp.addResultEntry("LoadBytes", numLoadBytesTotal);
        resp.addResultEntry("TrackingURL", trackingUrl);
        resp.addResultEntry("Prepared Time", finishPreparingTimeMs - this.startPreparingTimeMs);
        resp.setOKMsg("stream load " + label + " finish preparing");
        LOG.info("stream load {} finish preparing. db: {}, tbl: {}, txn_id: {}",
                label, dbName, tableName, txnId);
    }

    public void commitTxn(TransactionResult resp) throws UserException {
        long startTimeMs = System.currentTimeMillis();
        boolean exception = false;
        readLock();
        try {
            if (isFinalState()) {
                if (state == State.CANCELLED) {
                    resp.setOKMsg("txn could not be committed because task state is: " + state
                            + ", error_msg: " + errorMsg);

                } else {
                    resp.setOKMsg("txn could not be committed because task state is: " + state);
                }
                return;
            }
            if (this.state != State.PREPARED) {
                resp.setOKMsg("stream load task " + this.label + " state ("
                        + this.state + ") is not prepared, can not commit");
                return;
            }
        } finally {
            readUnlock();
        }

        try {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().commitPreparedTransaction(dbId, txnId, timeoutMs);
        } catch (Exception e) {
            this.errorMsg = new LogBuilder(LogKey.STREAM_LOAD_TASK, id, ':').add("label", label)
                    .add("error_msg", "cancel stream task for exception: " + e.getMessage()).build_http_log();
            exception = true;
        }

        if (exception && this.errorMsg != null) {
            LOG.warn(errorMsg);
            cancelTask();
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
            resp.setErrorMsg(this.errorMsg);
            return;
        }

        LOG.info("stream load {} finish commiting. db: {}, tbl: {}, txn_id: {}",
                    label, dbName, tableName, txnId);
        resp.addResultEntry("NumberTotalRows", numRowsNormal + numRowsAbnormal + numRowsUnselected);
        resp.addResultEntry("NumberLoadedRows", numRowsNormal);
        resp.addResultEntry("NumberFilteredRows", numRowsAbnormal);
        resp.addResultEntry("NumberUnselectedRows", numRowsUnselected);
        resp.addResultEntry("LoadBytes", numLoadBytesTotal);
        resp.addResultEntry("TrackingURL", trackingUrl);
        resp.addResultEntry("Committed time", System.currentTimeMillis() - startTimeMs);
        resp.setOKMsg("stream load " + label + " commit");
    }

    public void manualCancelTask(TransactionResult resp) throws UserException {
        long startTimeMs = System.currentTimeMillis();
        readLock();
        try {
            if (isCommitting) {
                resp.setOKMsg("txn can not be cancelled because task state is committing");
                return;
            } 
        } finally {
            readUnlock();
        }
        
        String errorMsg = cancelTask("manual abort");
        if (errorMsg != null) {
            resp.setOKMsg("stream load " + label + " abort fail");
            resp.addResultEntry("Abort fail reason", errorMsg);
        } else {
            resp.setOKMsg("stream load " + label + " abort");
            resp.addResultEntry("Cancelled time", endTimeMs - startTimeMs);
        }
    }

    public void unprotectedExecute(HttpHeaders headers) throws UserException {
        streamLoadParam = StreamLoadParam.parseHttpHeader(headers);
        streamLoadInfo = StreamLoadInfo.fromStreamLoadContext(loadId, txnId, (int) timeoutMs / 1000, streamLoadParam);
        if (table == null) {
            getTable();
        }
        LoadPlanner loadPlanner = new LoadPlanner(id, loadId, txnId, dbId, dbName, table,
                streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(), streamLoadInfo.isPartialUpdate(),
                null, null, streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(), 
                streamLoadInfo.getNegative(), channelNum, streamLoadInfo.getColumnExprDescs(), streamLoadInfo, label,
                streamLoadInfo.getTimeout());
        
        loadPlanner.plan();

        coord = new Coordinator(loadPlanner);
        coord.setLoadJobType(TLoadJobType.STREAM_LOAD);

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, coord);

            int waitSecond = (int) (getLeftTimeMs() / 1000);
            if (waitSecond <= 0) {
                throw new LoadException("Load timeout. Increase the timeout and retry");
            }

            coord.exec();
            this.channelIdToBEHTTPAddress = coord.getChannelIdToBEHTTPMap();
            this.channelIdToBEHTTPPort = coord.getChannelIdToBEPortMap();
        } catch (Exception e) {
            throw new UserException(e.getMessage());
        } 
    }

    private void unprotectedFinishStreamLoadChannel(int channelId) throws UserException {
        TNetworkAddress address = channelIdToBEHTTPPort.get(channelId);

        boolean ok = false;
        BackendService.Client client = null;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TStreamLoadChannel streamLoadChannel = new TStreamLoadChannel();
            streamLoadChannel.setLabel(label);
            streamLoadChannel.setChannel_id(channelId);
            TStatus tStatus = client.finish_stream_load_channel(streamLoadChannel);
            ok = true;

            if (tStatus.getStatus_code() != TStatusCode.OK) {
                // ignore fail status
            }
            LOG.info("finish stream load channel label: {} channel id {}", label, channelId);
        } catch (Exception e) {
            throw new UserException("failed to send finish stream load channel: " + e.getMessage(), e);
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    private void unprotectedWaitCoordFinish() throws UserException {
        try {
            int waitSecond = (int) (getLeftTimeMs() / 1000);
            if (waitSecond <= 0) {
                throw new LoadException("Load timeout. Increase the timeout and retry");
            }
            if (coord.join(waitSecond)) {
                Status status = coord.getExecStatus();
                Map<String, String> loadCounters = coord.getLoadCounters();
                if (loadCounters == null || loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL) == null) {
                    throw new LoadException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
                }
                this.numRowsNormal = Long.parseLong(loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL));
                this.numRowsAbnormal = Long.parseLong(loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL));
                this.numRowsUnselected = Long.parseLong(loadCounters.get(LoadJob.UNSELECTED_ROWS));
                this.numLoadBytesTotal = Long.parseLong(loadCounters.get(LoadJob.LOADED_BYTES));

                if (numRowsNormal == 0) {
                    throw new LoadException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
                }

                if (coord.isEnableLoadProfile()) {
                    collectProfile();
                }

                this.trackingUrl = coord.getTrackingUrl();
                if (!status.ok()) {
                    throw new LoadException(status.getErrorMsg());
                }
            } else {
                throw new LoadException("coordinator could not finished before job timeout");
            }
        } catch (Exception e) {
            throw new UserException(e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }


    public void cancelTask() {
        cancelTask(null);
    }

    public String cancelTask(String msg) {
        String reason;
        if (msg != null && errorMsg == null) {
            reason = msg;
        } else {
            reason = errorMsg;
        }
        readLock();
        try {
            if (isFinalState()) {
                if (state == State.CANCELLED) {
                    return "cur task state is: " + state 
                            + ", error_msg: " + errorMsg;
                } else {
                    return "cur task state is: " + state;
                }
            } 
        } finally {
            readUnlock();
        }
        try {
            if (txnId != -1L) {
<<<<<<< HEAD
                GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(
                        dbId, txnId, reason);
=======
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                        dbId, txnId, reason, Coordinator.getCommitInfos(coord), Coordinator.getFailInfos(coord), null);
>>>>>>> 203e9d07d6 ([Enhancement] Aborting transaction supports carrying finished tablets info to help clean dirty data for shared-data mode (#39834))
            } else {
                writeLock();
                for (int i = 0; i < channelNum; i++) {
                    this.channels.set(i, State.CANCELLED);
                }
                this.state = State.CANCELLED;
                writeUnlock();
            }
        } catch (Exception e) {
            LOG.warn("stream load " + label + " abort txn fail, errmsg: " + e.getMessage());
            return e.getMessage();
        }
        LOG.info("stream load {} cancel. db: {}, tbl: {}, txn_id: {}",
                    label, dbName, tableName, txnId);
        return null;
    }

    public void unprotectedBeginTxn(boolean replay) throws UserException {
        this.txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(
                dbId, Lists.newArrayList(tableId), label, null,
                new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING, id,
                timeoutMs / 1000);
    } 

    public void unprotectedPrepareTxn() throws UserException {
        List<TabletCommitInfo> commitInfos = TabletCommitInfo.fromThrift(coord.getCommitInfos());
        List<TabletFailInfo> failInfos = TabletFailInfo.fromThrift(coord.getFailInfos());
        finishPreparingTimeMs = System.currentTimeMillis();
        StreamLoadTxnCommitAttachment txnCommitAttachment = new StreamLoadTxnCommitAttachment(
                beforeLoadTimeMs, startLoadingTimeMs, startPreparingTimeMs, finishPreparingTimeMs,
                endTimeMs, numRowsNormal, numRowsAbnormal, numRowsUnselected, numLoadBytesTotal,
                trackingUrl);
        GlobalStateMgr.getCurrentGlobalTransactionMgr().prepareTransaction(dbId, 
                txnId, commitInfos, failInfos, txnCommitAttachment);
    }

    public boolean checkNeedRemove(long currentMs) {
        readLock();
        try {
            if (!isFinalState()) {
                return false;
            }
        } finally {
            readUnlock();
        }
        Preconditions.checkState(endTimeMs != -1, endTimeMs);
        if ((currentMs - endTimeMs) > Config.label_keep_max_second * 1000) {
            return true;
        }
        return false;
    }

    protected boolean checkDataQuality() {
        if (numRowsNormal == -1 || numRowsAbnormal == -1) {
            return true;
        }

        if (numRowsAbnormal > (numRowsAbnormal + numRowsNormal) * streamLoadParam.maxFilterRatio) {
            return false;
        }

        return true;
    }

    public boolean checkNeedPrepareTxn() {
        return this.preparedChannelNum == this.channelNum;
    }

    @Override
    public void beforePrepared(TransactionState txnState) throws TransactionException {
        writeLock();
        try {
            if (isFinalState()) {
                throw new TransactionException("txn could not be prepared because task state is: " + state);
            }
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterPrepared(TransactionState txnState, boolean txnOperated) throws UserException {
        if (!txnOperated) {
            return;
        }
        writeLock();
        try {
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.PREPARED);
            }
            this.state = State.PREPARED;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnPrepared(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            txnId = txnState.getTransactionId();
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.PREPARED);
            }
            state = State.PREPARED;
            this.preparedChannelNum = this.channelNum;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        writeLock();
        try {
            if (isFinalState()) {
                throw new TransactionException("txn could not be commited because task state is: " + state);
            }
            isCommitting = true;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        if (!txnOperated) {
            return;
        }

        // sync stream load collect profile
        if (isSyncStreamLoad() && coord.isEnableLoadProfile()) {
            collectProfile();
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }

        writeLock();
        try {
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.COMMITED);
            }
            this.state = State.COMMITED;
            isCommitting = false;
        } finally {
            writeUnlock();
        }
    }

    public void collectProfile() {
        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - createTimeMs;

        // For the usage scenarios of flink cdc or routine load,
        // the frequency of stream load maybe very high, resulting in many profiles,
        // but we may only care about the long-duration stream load profile.
        if (totalTimeMs < Config.stream_load_profile_collect_second * 1000) {
            LOG.info(String.format("Load %s, totalTimeMs %d < Config.stream_load_profile_collect_second %d)",
                    label, totalTimeMs, Config.stream_load_profile_collect_second));
            return;
        }

        RuntimeProfile profile = new RuntimeProfile("Load");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(loadId));
        summaryProfile.addInfoString(ProfileManager.START_TIME,
                TimeUtils.longToTimeString(createTimeMs));

        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(System.currentTimeMillis()));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString("StarRocks Version",
                String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, dbName);

        Map<String, String> loadCounters = coord.getLoadCounters();
        if (loadCounters != null && loadCounters.size() != 0) {
            summaryProfile.addInfoString("NumRowsNormal", loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL));
            summaryProfile.addInfoString("NumLoadBytesTotal", loadCounters.get(LoadJob.LOADED_BYTES));
            summaryProfile.addInfoString("NumRowsAbnormal", loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL));
            summaryProfile.addInfoString("numRowsUnselected", loadCounters.get(LoadJob.UNSELECTED_ROWS));
        }
        ConnectContext session = ConnectContext.get();
        if (session != null) {
            SessionVariable variables = session.getSessionVariable();
            if (variables != null) {
                summaryProfile.addInfoString("NonDefaultSessionVariables", variables.getNonDefaultVariablesJson());
            }
        }

        profile.addChild(summaryProfile);
        if (coord.getQueryProfile() != null) {
            if (!isSyncStreamLoad()) {
                coord.collectProfileSync();
                profile.addChild(coord.buildQueryProfile(session == null || session.needMergeProfile()));
            } else {
                profile.addChild(coord.getQueryProfile());
            }
        }

        ProfileManager.getInstance().pushLoadProfile(profile);
    }

    public void setLoadState(long loadBytes, long loadRows, long filteredRows, long unselectedRows,
                             String errorLogUrl, String errorMsg) {
        this.numRowsNormal = loadRows;
        this.numRowsAbnormal = filteredRows;
        this.numRowsUnselected = unselectedRows;
        this.numLoadBytesTotal = loadBytes;
        this.trackingUrl = errorLogUrl;
        this.errorMsg = errorMsg;
    }


    @Override
    public void replayOnCommitted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            txnId = txnState.getTransactionId();
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.COMMITED);
            }
            this.state = State.COMMITED;
            this.preparedChannelNum = this.channelNum;
        } finally {
            writeUnlock();
        }
    } 

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        if (!txnOperated) {
            return;
        }

        if (isSyncStreamLoad && coord.isEnableLoadProfile()) {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }

        writeLock();
        try {
            if (isFinalState()) {
                return;
            }
            if (coord != null && !isSyncStreamLoad) {
                coord.cancel();
                QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
            }
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.CANCELLED);
            }
            endTimeMs = System.currentTimeMillis();
            state = State.CANCELLED;
            errorMsg = txnState.getReason();
            gcObject();
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            txnId = txnState.getTransactionId();
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.CANCELLED);
            }
            errorMsg = txnState.getReason();
            state = State.CANCELLED;
            endTimeMs = txnState.getFinishTime();
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            return;
        }
        writeLock();
        try {
            for (int i = 0; i < channelNum; i++) {
                channels.set(i, State.FINISHED);
            }
            state = State.FINISHED;
            endTimeMs = System.currentTimeMillis();
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
            gcObject();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            for (int i = 0; i < channelNum; i++) {
                this.channels.set(i, State.FINISHED);
            }
            this.preparedChannelNum = this.channelNum;
            state = State.FINISHED;
            endTimeMs = txnState.getFinishTime();
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    public void gcObject() {
        coord = null;
        channelIdToBEHTTPAddress = null;
        channelIdToBEHTTPPort = null;
        table = null;
        streamLoadParam = null;
        streamLoadInfo = null;
    }

    public void cancelAfterRestart() {
        errorMsg = "task not in durable state (PREPARED|CANCELLED|COMMITTED|FINISHED) will be cancelled after fe restart";
        for (int i = 0; i < channelNum; i++) {
            this.channels.set(i, State.CANCELLED);
        }
        endTimeMs = System.currentTimeMillis();
        state = State.CANCELLED;
    }

    private void replayTxnAttachment(TransactionState txnState) {
        if (txnState.getTxnCommitAttachment() == null) {
            return;
        }
        StreamLoadTxnCommitAttachment attachment = (StreamLoadTxnCommitAttachment) txnState.getTxnCommitAttachment();
        this.trackingUrl = attachment.getTrackingURL();
        this.beforeLoadTimeMs = attachment.getBeforeLoadTimeMs();
        this.startLoadingTimeMs = attachment.getStartLoadingTimeMs();
        this.startPreparingTimeMs = attachment.getStartPreparingTimeMs();
        this.finishPreparingTimeMs = attachment.getFinishPreparingTimeMs();
        this.endTimeMs = attachment.getEndTimeMs();
        this.numRowsNormal = attachment.getNumRowsNormal();
        this.numRowsAbnormal = attachment.getNumRowsAbnormal();
        this.numRowsUnselected = attachment.getNumRowsUnselected();
        this.numLoadBytesTotal = attachment.getNumLoadBytesTotal();
    }


    public OlapTable getTable() throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        database.readLock();
        try {
            OlapTable table = (OlapTable) database.getTable(tableId);
            if (table == null) {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            }
            return table;
        } finally {
            database.readUnlock();
        }
    }

    private long getLeftTimeMs() {
        return taskDeadlineMs - System.currentTimeMillis();
    }

    private long getLeftTimeMsWithCurMs(long currentMs) {
        return taskDeadlineMs - currentMs;
    }

    public long createTimeMs() {
        return createTimeMs;
    }

    public long endTimeMs() {
        return endTimeMs;
    }

    public boolean isFinalState() {
        return state == State.CANCELLED || state == State.COMMITED || state == State.FINISHED;
    }

    public boolean isDurableLoadState() {
        return state == State.PREPARED || state == State.CANCELLED || state == State.COMMITED || state == State.FINISHED;
    }

    public String getDBName() {
        return dbName;
    }

    public long getDBId() {
        return dbId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public long getId() {
        return id;
    }

    public String getStateName() {
        return state.name();
    }
    
    public void setTUniqueId(TUniqueId loadId) {
        this.loadId = loadId;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isSyncStreamLoad() {
        return isSyncStreamLoad;
    }

    public boolean isRoutineLoadTask() {
        return type == Type.ROUTINE_LOAD;
    }
    // for sync stream load
    public void setCoordinator(Coordinator coord) {
        this.coord = coord;
    }

    public String getStringByType() {
        switch (this.type) {
            case ROUTINE_LOAD:
                return "ROUTINE_LOAD";
            case STREAM_LOAD:
                return "STREAM_LOAD";
            case PARALLEL_STREAM_LOAD:
                return "PARALLEL_STREAM_LOAD";
            default:
                return "UNKNOWN";
        }
    }

    public List<String> getShowInfo() {
        readLock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(label);
            row.add(String.valueOf(id));
            row.add(loadId.toString());
            row.add(String.valueOf(txnId));
            row.add(dbName);
            row.add(tableName);
            row.add(state.name());
            row.add(errorMsg);
            row.add(trackingUrl);
            row.add(String.valueOf(channelNum));
            row.add(String.valueOf(preparedChannelNum));
            
            row.add(String.valueOf(numRowsNormal));
            row.add(String.valueOf(numRowsAbnormal));
            row.add(String.valueOf(numRowsUnselected));
            row.add(String.valueOf(numLoadBytesTotal));

            row.add(String.valueOf(timeoutMs / 1000));
            row.add(TimeUtils.longToTimeString(createTimeMs));
            row.add(TimeUtils.longToTimeString(beforeLoadTimeMs));
            row.add(TimeUtils.longToTimeString(startLoadingTimeMs));
            row.add(TimeUtils.longToTimeString(startPreparingTimeMs));
            row.add(TimeUtils.longToTimeString(finishPreparingTimeMs));
            row.add(TimeUtils.longToTimeString(endTimeMs));

            StringBuilder channelStateBuilder = new StringBuilder();
            for (int i = 0; i < channels.size(); i++) {
                if (i > 0) {
                    channelStateBuilder.append(" | ");
                }
                channelStateBuilder.append(channels.get(i).name());
            }
            row.add(channelStateBuilder.toString());
            row.add(getStringByType());
            return row;
        } finally {
            readUnlock();
        }
    }

    public List<String> getShowBriefInfo() {
        readLock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(label);
            row.add(String.valueOf(id));
            row.add(dbName);
            row.add(tableName);
            row.add(state.name());
            return row;
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
        out.writeLong(loadId.getHi());
        out.writeLong(loadId.getLo());
    }

    public static StreamLoadTask read(DataInput in) throws IOException {
        String json = Text.readString(in);
        StreamLoadTask task = GsonUtils.GSON.fromJson(json, StreamLoadTask.class);
        long hi = in.readLong();
        long lo = in.readLong();
        TUniqueId loadId = new TUniqueId(hi, lo);
        task.init();
        task.setTUniqueId(loadId);
        // Only task which type is PARALLEL will be persisted
        // just set type to PARALLEL
        task.setType(Type.PARALLEL_STREAM_LOAD);
        return task;
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
}
