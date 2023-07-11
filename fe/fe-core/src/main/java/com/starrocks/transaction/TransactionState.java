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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/TransactionState.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.TraceManager;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TUniqueId;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public class TransactionState implements Writable {
    private static final Logger LOG = LogManager.getLogger(TransactionState.class);

    // compare the TransactionState by txn id, desc
    public static class TxnStateComparator implements Comparator<TransactionState> {
        @Override
        public int compare(TransactionState t1, TransactionState t2) {
            return Long.compare(t2.getTransactionId(), t1.getTransactionId());
        }
    }

    public static final TxnStateComparator TXN_ID_COMPARATOR = new TxnStateComparator();

    public enum LoadJobSourceType {
        FRONTEND(1),                    // old dpp load, mini load, insert stmt(not streaming type) use this type
        BACKEND_STREAMING(2),           // streaming load use this type
        INSERT_STREAMING(3),            // insert stmt (streaming type) use this type
        ROUTINE_LOAD_TASK(4),           // routine load task use this type
        BATCH_LOAD_JOB(5),              // load job v2 for broker load
        DELETE(6),                     // synchronization delete job use this type
        LAKE_COMPACTION(7),            // compaction of LakeTable
        FRONTEND_STREAMING(8),          // FE streaming load use this type
        MV_REFRESH(9);                  // Refresh MV

        private final int flag;

        LoadJobSourceType(int flag) {
            this.flag = flag;
        }

        public int value() {
            return flag;
        }

        public static LoadJobSourceType valueOf(int flag) {
            switch (flag) {
                case 1:
                    return FRONTEND;
                case 2:
                    return BACKEND_STREAMING;
                case 3:
                    return INSERT_STREAMING;
                case 4:
                    return ROUTINE_LOAD_TASK;
                case 5:
                    return BATCH_LOAD_JOB;
                case 6:
                    return DELETE;
                case 7:
                    return LAKE_COMPACTION;
                case 8:
                    return FRONTEND_STREAMING;
                case 9:
                    return MV_REFRESH;
                default:
                    return null;
            }
        }
    }

    public enum TxnStatusChangeReason {
        DB_DROPPED,
        TIMEOUT,
        OFFSET_OUT_OF_RANGE,
        PAUSE,
        NO_PARTITIONS,
        FILTERED_ROWS;

        public static TxnStatusChangeReason fromString(String reasonString) {
            if (Strings.isNullOrEmpty(reasonString)) {
                return null;
            }

            for (TxnStatusChangeReason txnStatusChangeReason : TxnStatusChangeReason.values()) {
                if (reasonString.contains(txnStatusChangeReason.toString())) {
                    return txnStatusChangeReason;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            switch (this) {
                case OFFSET_OUT_OF_RANGE:
                    return "Offset out of range";
                case NO_PARTITIONS:
                    return "all partitions have no load data";
                case FILTERED_ROWS:
                    return "too many filtered rows";
                default:
                    return this.name();
            }
        }
    }

    public enum TxnSourceType {
        FE(1),
        BE(2);

        public int value() {
            return flag;
        }

        private final int flag;

        TxnSourceType(int flag) {
            this.flag = flag;
        }

        public static TxnSourceType valueOf(int flag) {
            switch (flag) {
                case 1:
                    return FE;
                case 2:
                    return BE;
                default:
                    return null;
            }
        }
    }

    public static class TxnCoordinator {
        @SerializedName("st")
        public TxnSourceType sourceType;
        @SerializedName("ip")
        public String ip;

        public TxnCoordinator() {
        }

        public TxnCoordinator(TxnSourceType sourceType, String ip) {
            this.sourceType = sourceType;
            this.ip = ip;
        }

        public static TxnCoordinator fromThisFE() {
            return new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                    FrontendOptions.getLocalHostAddress());
        }

        @Override
        public String toString() {
            return sourceType.toString() + ": " + ip;
        }
    }

    @SerializedName("dd")
    private long dbId;
    @SerializedName("tl")
    private List<Long> tableIdList;
    @SerializedName("tx")
    private long transactionId;
    @SerializedName("lb")
    private String label;
    // requestId is used to judge whether a begin request is an internal retry request.
    // no need to persist it.
    private TUniqueId requestId;
    @SerializedName("ci")
    private final Map<Long, TableCommitInfo> idToTableCommitInfos;
    // coordinator is show who begin this txn (FE, or one of BE, etc...)
    @SerializedName("tc")
    private TxnCoordinator txnCoordinator;
    @SerializedName("ts")
    private TransactionStatus transactionStatus;
    @SerializedName("st")
    private LoadJobSourceType sourceType;
    @SerializedName("pt")
    private long prepareTime;
    @SerializedName("ct")
    private long commitTime;
    @SerializedName("ft")
    private long finishTime;
    @SerializedName("rs")
    private String reason = "";

    // whether this txn is finished using new mechanism
    // this field needs to be persisted, so we shared the serialization field with `reason`.
    // `reason` is only used when txn is aborted, so it's ok to reuse the space for visible txns.
    @SerializedName("nf")
    private boolean newFinish = false;
    @SerializedName("fs")
    private TxnFinishState finishState;

    // error replica ids
    @SerializedName("er")
    private Set<Long> errorReplicas;
    private final CountDownLatch latch;

    // these states need not be serialized
    private final Map<Long, PublishVersionTask> publishVersionTasks; // Only for OlapTable
    private boolean hasSendTask;
    private long publishVersionTime = -1;
    private long publishVersionFinishTime = -1;

    @SerializedName("cb")
    private long callbackId = -1;
    @SerializedName("to")
    private long timeoutMs = Config.stream_load_default_timeout_second * 1000L;

    // optional
    @SerializedName("ta")
    private TxnCommitAttachment txnCommitAttachment;

    // this map should be set when load execution begin, so that when the txn commit, it will know
    // which tables and rollups it loaded.
    // tbl id -> (index ids)
    private final Map<Long, Set<Long>> loadedTblIndexes = Maps.newHashMap();

    private String errorLogUrl = null;

    // record some error msgs during the transaction operation.
    // this msg will be shown in show proc "/transactions/dbId/";
    // no need to persist.
    private String errMsg = "";

    private long lastErrTimeMs = 0;

    // used for PublishDaemon to check whether this txn can be published
    // not persisted, so need to rebuilt if FE restarts
    private volatile TransactionChecker finishChecker = null;
    private long checkerCreationTime = 0;
    private Span txnSpan = null;
    private String traceParent = null;
    private Set<TabletCommitInfo> tabletCommitInfos = null;

    public TransactionState() {
        this.dbId = -1;
        this.tableIdList = Lists.newArrayList();
        this.transactionId = -1;
        this.label = "";
        this.idToTableCommitInfos = Maps.newHashMap();
        this.txnCoordinator = new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"); // mocked, to avoid NPE
        this.transactionStatus = TransactionStatus.PREPARE;
        this.sourceType = LoadJobSourceType.FRONTEND;
        this.prepareTime = -1;
        this.commitTime = -1;
        this.finishTime = -1;
        this.reason = "";
        this.errorReplicas = Sets.newHashSet();
        this.publishVersionTasks = Maps.newHashMap();
        this.hasSendTask = false;
        this.latch = new CountDownLatch(1);
        this.txnSpan = TraceManager.startNoopSpan();
        this.traceParent = TraceManager.toTraceParent(txnSpan.getSpanContext());
    }

    public TransactionState(long dbId, List<Long> tableIdList, long transactionId, String label, TUniqueId requestId,
                            LoadJobSourceType sourceType, TxnCoordinator txnCoordinator, long callbackId,
                            long timeoutMs) {
        this.dbId = dbId;
        this.tableIdList = (tableIdList == null ? Lists.newArrayList() : tableIdList);
        this.transactionId = transactionId;
        this.label = label;
        this.requestId = requestId;
        this.idToTableCommitInfos = Maps.newHashMap();
        this.txnCoordinator = txnCoordinator;
        this.transactionStatus = TransactionStatus.PREPARE;
        this.sourceType = sourceType;
        this.prepareTime = -1;
        this.commitTime = -1;
        this.finishTime = -1;
        this.reason = "";
        this.errorReplicas = Sets.newHashSet();
        this.publishVersionTasks = Maps.newHashMap();
        this.hasSendTask = false;
        this.latch = new CountDownLatch(1);
        this.callbackId = callbackId;
        this.timeoutMs = timeoutMs;
        this.txnSpan = TraceManager.startSpan("txn");
        txnSpan.setAttribute("txn_id", transactionId);
        txnSpan.setAttribute("label", label);
        this.traceParent = TraceManager.toTraceParent(txnSpan.getSpanContext());
    }

    public void setErrorReplicas(Set<Long> newErrorReplicas) {
        this.errorReplicas = newErrorReplicas;
    }

    public boolean isRunning() {
        return transactionStatus == TransactionStatus.PREPARE || transactionStatus == TransactionStatus.COMMITTED;
    }

    public void setTabletCommitInfos(List<TabletCommitInfo> infos) {
        this.tabletCommitInfos = Sets.newHashSet();
        this.tabletCommitInfos.addAll(infos);
    }

    public boolean tabletCommitInfosContainsReplica(long tabletId, long backendId) {
        TabletCommitInfo info = new TabletCommitInfo(tabletId, backendId);
        if (this.tabletCommitInfos == null || this.tabletCommitInfos.contains(info)) {
            // if tabletCommitInfos is null, skip this check and return true
            return true;
        } else {
            return false;
        }
    }

    // Only for OlapTable
    public void addPublishVersionTask(Long backendId, PublishVersionTask task) {
        this.publishVersionTasks.put(backendId, task);
    }

    public void setHasSendTask(boolean hasSendTask) {
        this.hasSendTask = hasSendTask;
        this.publishVersionTime = System.currentTimeMillis();
    }

    public void updateSendTaskTime() {
        this.publishVersionTime = System.currentTimeMillis();
    }

    public void updatePublishTaskFinishTime() {
        this.publishVersionFinishTime = System.currentTimeMillis();
    }

    public long getPublishVersionTime() {
        return this.publishVersionTime;
    }

    public boolean hasSendTask() {
        return this.hasSendTask;
    }

    public TUniqueId getRequestId() {
        return requestId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public String getLabel() {
        return this.label;
    }

    public TxnCoordinator getCoordinator() {
        return txnCoordinator;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public long getPrepareTime() {
        return prepareTime;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public String getReason() {
        return reason;
    }

    public TxnCommitAttachment getTxnCommitAttachment() {
        return txnCommitAttachment;
    }

    public long getCallbackId() {
        return callbackId;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setErrorLogUrl(String errorLogUrl) {
        this.errorLogUrl = errorLogUrl;
    }

    public String getErrorLogUrl() {
        return errorLogUrl;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        // status changed
        this.transactionStatus = transactionStatus;

        // after status changed
        if (transactionStatus == TransactionStatus.VISIBLE) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_SUCCESS.increase(1L);
            }
            txnSpan.addEvent("set_visible");
            txnSpan.end();
        } else if (transactionStatus == TransactionStatus.ABORTED) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_FAILED.increase(1L);
            }
            txnSpan.setAttribute("state", "aborted");
            txnSpan.end();
        } else if (transactionStatus == TransactionStatus.COMMITTED) {
            txnSpan.addEvent("set_committed");
        }
    }

    public void notifyVisible() {
        // To avoid the method not having to be called repeatedly or in advance,
        // the following trigger conditions have been added
        // 1. the transactionStatus status must be VISIBLE
        // 2. this.latch.countDown(); has not been called before
        // 3. this.latch can not be null
        if (transactionStatus == TransactionStatus.VISIBLE && this.latch != null && this.latch.getCount() != 0) {
            this.latch.countDown();
        }
    }

    public TxnStateChangeCallback beforeStateTransform(TransactionStatus transactionStatus)
            throws TransactionException {
        // callback will pass to afterStateTransform since it may be deleted from
        // GlobalTransactionMgr between beforeStateTransform and afterStateTransform
        TxnStateChangeCallback callback = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .getCallbackFactory().getCallback(callbackId);
        // before status changed
        if (callback != null) {
            switch (transactionStatus) {
                case ABORTED:
                    callback.beforeAborted(this);
                    break;
                case COMMITTED:
                    callback.beforeCommitted(this);
                    break;
                case PREPARED:
                    callback.beforePrepared(this);
                    break;
                default:
                    break;
            }
        } else if (callbackId > 0) {
            if (Objects.requireNonNull(transactionStatus) == TransactionStatus.COMMITTED) {
                // Maybe listener has been deleted. The txn need to be aborted later.
                throw new TransactionException(
                        "Failed to commit txn when callback " + callbackId + "could not be found");
            }
        }

        return callback;
    }

    public void afterStateTransform(TransactionStatus transactionStatus, boolean txnOperated) throws UserException {
        // after status changed
        TxnStateChangeCallback callback = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .getCallbackFactory().getCallback(callbackId);
        if (callback != null) {
            if (Objects.requireNonNull(transactionStatus) == TransactionStatus.VISIBLE) {
                callback.afterVisible(this, txnOperated);
            }
        }
    }

    public void afterStateTransform(TransactionStatus transactionStatus, boolean txnOperated,
                                    TxnStateChangeCallback callback,
                                    String txnStatusChangeReason)
            throws UserException {
        // after status changed
        if (callback != null) {
            switch (transactionStatus) {
                case ABORTED:
                    callback.afterAborted(this, txnOperated, txnStatusChangeReason);
                    break;
                case COMMITTED:
                    callback.afterCommitted(this, txnOperated);
                    break;
                case PREPARED:
                    callback.afterPrepared(this, txnOperated);
                    break;
                default:
                    break;
            }
        }
    }

    public void replaySetTransactionStatus() {
        TxnStateChangeCallback callback =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().getCallback(
                        callbackId);
        if (callback != null) {
            if (transactionStatus == TransactionStatus.ABORTED) {
                callback.replayOnAborted(this);
            } else if (transactionStatus == TransactionStatus.COMMITTED) {
                callback.replayOnCommitted(this);
            } else if (transactionStatus == TransactionStatus.VISIBLE) {
                callback.replayOnVisible(this);
            } else if (transactionStatus == TransactionStatus.PREPARED) {
                callback.replayOnPrepared(this);
            }
        }
    }

    public void waitTransactionVisible() throws InterruptedException {
        this.latch.await();
    }

    public boolean waitTransactionVisible(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        return this.latch.await(timeout, unit);
    }

    public void setPrepareTime(long prepareTime) {
        this.prepareTime = prepareTime;
    }

    public void setCommitTime(long commitTime) {
        this.commitTime = commitTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public void setReason(String reason) {
        this.reason = Strings.nullToEmpty(reason);
    }

    public Set<Long> getErrorReplicas() {
        return this.errorReplicas;
    }

    public long getDbId() {
        return dbId;
    }

    public List<Long> getTableIdList() {
        if (tableIdList.isEmpty()) {
            // Old version sometimes forgot to set the tabletIdList, collect tabletIdList
            // from idToTableCommitInfos.
            // NOTE: this works only when the state is COMMITTED or VISIBLE
            tableIdList = Lists.newArrayList(idToTableCommitInfos.keySet());
        }
        return tableIdList;
    }

    public void setTableIdList(List<Long> tableIdList) {
        this.tableIdList = tableIdList;
    }

    public Map<Long, TableCommitInfo> getIdToTableCommitInfos() {
        return idToTableCommitInfos;
    }

    public void putIdToTableCommitInfo(long tableId, TableCommitInfo tableCommitInfo) {
        idToTableCommitInfos.put(tableId, tableCommitInfo);
    }

    public TableCommitInfo getTableCommitInfo(long tableId) {
        return this.idToTableCommitInfos.get(tableId);
    }

    public void removeTable(long tableId) {
        this.idToTableCommitInfos.remove(tableId);
    }

    public void setTxnCommitAttachment(TxnCommitAttachment txnCommitAttachment) {
        this.txnCommitAttachment = txnCommitAttachment;
    }

    // return true if txn is in final status and label is expired
    public boolean isExpired(long currentMillis) {
        return transactionStatus.isFinalStatus() && (currentMillis - finishTime) / 1000 > Config.label_keep_max_second;
    }

    // return true if txn is running but timeout
    public boolean isTimeout(long currentMillis) {
        return (transactionStatus == TransactionStatus.PREPARE && currentMillis - prepareTime > timeoutMs)
                || (transactionStatus == TransactionStatus.PREPARED && (currentMillis - commitTime)
                / 1000 > Config.prepared_transaction_default_timeout_second);
    }

    /*
     * Add related table indexes to the transaction.
     * If function should always be called before adding this transaction state to transaction manager,
     * No other thread will access this state. So no need to lock
     */
    public void addTableIndexes(OlapTable table) {
        Set<Long> indexIds = loadedTblIndexes.computeIfAbsent(table.getId(), k -> Sets.newHashSet());
        // always equal the index ids
        indexIds.clear();
        indexIds.addAll(table.getIndexIdToMeta().keySet());
    }

    public List<MaterializedIndex> getPartitionLoadedTblIndexes(long tableId, Partition partition) {
        List<MaterializedIndex> loadedIndex;
        if (loadedTblIndexes.isEmpty()) {
            loadedIndex = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        } else {
            loadedIndex = Lists.newArrayList();
            for (long indexId : loadedTblIndexes.get(tableId)) {
                MaterializedIndex index = partition.getIndex(indexId);
                if (index != null) {
                    loadedIndex.add(index);
                }
            }
        }
        return loadedIndex;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TransactionState. ");
        sb.append("txn_id: ").append(transactionId);
        sb.append(", label: ").append(label);
        sb.append(", db id: ").append(dbId);
        sb.append(", table id list: ").append(StringUtils.join(tableIdList, ","));
        sb.append(", callback id: ").append(callbackId);
        sb.append(", coordinator: ").append(txnCoordinator.toString());
        sb.append(", transaction status: ").append(transactionStatus);
        sb.append(", error replicas num: ").append(errorReplicas.size());
        sb.append(", replica ids: ").append(Joiner.on(",").join(errorReplicas.stream().limit(5).toArray()));
        sb.append(", prepare time: ").append(prepareTime);
        sb.append(", commit time: ").append(commitTime);
        sb.append(", finish time: ").append(finishTime);
        if (commitTime > prepareTime) {
            sb.append(", write cost: ").append(commitTime - prepareTime).append("ms");
        }
        if (publishVersionTime != -1 && publishVersionFinishTime != -1) {
            if (publishVersionTime > commitTime) {
                sb.append(", wait for publish cost: ").append(publishVersionTime - commitTime).append("ms");
            }
            if (publishVersionFinishTime > publishVersionTime) {
                sb.append(", publish rpc cost: ").append(publishVersionFinishTime - publishVersionTime).append("ms");
            }
            if (finishTime > publishVersionFinishTime) {
                sb.append(", finish txn cost: ").append(finishTime - publishVersionFinishTime).append("ms");
            }
        }
        if (finishTime > commitTime && commitTime > 0) {
            sb.append(", publish total cost: ").append(finishTime - commitTime).append("ms");
        }
        if (finishTime > prepareTime) {
            sb.append(", total cost: ").append(finishTime - prepareTime).append("ms");
        }
        sb.append(", reason: ").append(reason);
        if (newFinish) {
            sb.append(", newFinish");
        }
        if (txnCommitAttachment != null) {
            sb.append(" attachment: ").append(txnCommitAttachment);
        }
        return sb.toString();
    }

    public LoadJobSourceType getSourceType() {
        return sourceType;
    }

    public Map<Long, PublishVersionTask> getPublishVersionTasks() {
        return publishVersionTasks;
    }

    public void clearAfterPublished() {
        publishVersionTasks.clear();
        finishChecker = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(transactionId);
        Text.writeString(out, label);
        out.writeLong(dbId);
        out.writeInt(idToTableCommitInfos.size());
        for (TableCommitInfo info : idToTableCommitInfos.values()) {
            info.write(out);
        }
        out.writeInt(txnCoordinator.sourceType.value());
        Text.writeString(out, txnCoordinator.ip);
        out.writeInt(transactionStatus.value());
        out.writeInt(sourceType.value());
        out.writeLong(prepareTime);
        out.writeLong(commitTime);
        out.writeLong(finishTime);
        // if txn use new publish mechanism, store state in the space originally used by `reason`
        // if new publish with TxnFinishState, write json {"normal":[xx],"abnormal":[xxx,xxx]}
        // else write `reason` field
        // they are both String, so they are compatible
        if (transactionStatus == TransactionStatus.VISIBLE) {
            if (newFinish) {
                Preconditions.checkNotNull(finishState);
                Text.writeString(out, GsonUtils.GSON.toJson(finishState));
            } else {
                Text.writeString(out, "");
            }
        } else {
            Text.writeString(out, reason);
        }
        out.writeInt(errorReplicas.size());
        for (long errorReplicaId : errorReplicas) {
            out.writeLong(errorReplicaId);
        }

        if (txnCommitAttachment == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            txnCommitAttachment.write(out);
        }
        out.writeLong(callbackId);
        out.writeLong(timeoutMs);
        out.writeInt(tableIdList.size());
        for (Long tableId : tableIdList) {
            out.writeLong(tableId);
        }
    }

    public void readFields(DataInput in) throws IOException {
        transactionId = in.readLong();
        label = Text.readString(in);
        dbId = in.readLong();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableCommitInfo info = new TableCommitInfo();
            info.readFields(in);
            idToTableCommitInfos.put(info.getTableId(), info);
        }
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_83) {
            TxnSourceType sourceType = TxnSourceType.valueOf(in.readInt());
            String ip = Text.readString(in);
            txnCoordinator = new TxnCoordinator(sourceType, ip);
        } else {
            // to compatible old version, the old txn coordinator looks like: "BE: 192.186.1.1"
            String coordStr = Text.readString(in);
            String[] parts = coordStr.split(":");
            if (parts.length != 2) {
                // should not happen, just create a mocked TxnCoordinator
                txnCoordinator = new TxnCoordinator(TxnSourceType.FE, "127.0.0.1");
            } else {
                if (parts[0].trim().equalsIgnoreCase("FE")) {
                    txnCoordinator = new TxnCoordinator(TxnSourceType.FE, parts[1].trim());
                } else if (parts[0].trim().equalsIgnoreCase("BE")) {
                    txnCoordinator = new TxnCoordinator(TxnSourceType.BE, parts[1].trim());
                } else {
                    // unknown format, should not happen, just create a mocked TxnCoordinator
                    txnCoordinator = new TxnCoordinator(TxnSourceType.FE, "127.0.0.1");
                }
            }
        }
        transactionStatus = TransactionStatus.valueOf(in.readInt());
        sourceType = LoadJobSourceType.valueOf(in.readInt());
        prepareTime = in.readLong();
        commitTime = in.readLong();
        finishTime = in.readLong();
        if (transactionStatus == TransactionStatus.VISIBLE) {
            int len = in.readInt();
            if (len == 0) {
                newFinish = false;
            } else {
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                try {
                    // originally, TxnFinishState is serialized using protobuf(baidu jprotobuf),
                    // but looks like the ser/deser is not compatible with java native serialization
                    // causing FE error when upgrading, so changed to json serialization, but keep
                    // the old deserialization code here for compatibility
                    // see: https://github.com/StarRocks/starrocks/issues/15248
                    byte version = bytes[0];
                    if (version == 1) {
                        if (finishState == null) {
                            finishState = new TxnFinishState();
                        }
                        byte[] pbBytes = new byte[len - 1];
                        System.arraycopy(bytes, 1, pbBytes, 0, len - 1);
                        finishState.fromBytes(pbBytes);
                        newFinish = true;
                    } else {
                        String content = Text.decode(bytes);
                        if (content.length() > 2 && content.charAt(0) == '{') {
                            finishState = GsonUtils.GSON.fromJson(content, TxnFinishState.class);
                            newFinish = true;
                        } else {
                            // old reason
                            reason = content;
                            newFinish = false;
                        }
                    }
                } catch (IOException e) {
                    LOG.warn("failed to deserialize TxnFinishState data: " + Hex.encodeHexString(bytes));
                }
            }
        } else {
            reason = Text.readString(in);
        }
        int errorReplicaNum = in.readInt();
        for (int i = 0; i < errorReplicaNum; ++i) {
            errorReplicas.add(in.readLong());
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_49) {
            if (in.readBoolean()) {
                txnCommitAttachment = TxnCommitAttachment.read(in);
            }
            callbackId = in.readLong();
            timeoutMs = in.readLong();
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_79) {
            tableIdList = Lists.newArrayList();
            int tableListSize = in.readInt();
            for (int i = 0; i < tableListSize; i++) {
                tableIdList.add(in.readLong());
            }
        }

        txnSpan.setAttribute("txn_id", transactionId);
        txnSpan.setAttribute("label", label);
        if (transactionStatus == TransactionStatus.COMMITTED) {
            txnSpan.addEvent("set_committed");
        } else if (transactionStatus == TransactionStatus.ABORTED) {
            txnSpan.setAttribute("state", "aborted");
            txnSpan.setStatus(StatusCode.ERROR, reason);
            txnSpan.end();
        } else if (transactionStatus == TransactionStatus.VISIBLE) {
            txnSpan.addEvent("set_visible");
            txnSpan.end();
        }
    }

    public void setErrorMsg(String errMsg) {
        this.errMsg = errMsg;
        lastErrTimeMs = System.nanoTime() / 1000000;
    }

    public void clearErrorMsg() {
        this.errMsg = "";
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public long getLastErrTimeMs() {
        return lastErrTimeMs;
    }

    // create publish version task for OlapTable transaction
    public List<PublishVersionTask> createPublishVersionTask() {
        List<PublishVersionTask> tasks = new ArrayList<>();
        if (this.hasSendTask()) {
            return tasks;
        }

        Set<Long> publishBackends = this.getPublishVersionTasks().keySet();
        // public version tasks are not persisted in globalStateMgr, so publishBackends may be empty.
        // We have to send publish version task to all backends
        if (publishBackends.isEmpty()) {
            // note: tasks are sent to all backends including dead ones, or else
            // transaction manager will treat it as success
            List<Long> allBackends = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false);
            if (!allBackends.isEmpty()) {
                publishBackends = Sets.newHashSet();
                publishBackends.addAll(allBackends);
            } else {
                // all backends may be dropped, no need to create task
                LOG.warn("transaction {} want to publish, but no backend exists", this.getTransactionId());
                return tasks;
            }
        }

        List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
        for (TableCommitInfo tableCommitInfo : this.getIdToTableCommitInfos().values()) {
            partitionCommitInfos.addAll(tableCommitInfo.getIdToPartitionCommitInfo().values());
        }

        List<TPartitionVersionInfo> partitionVersions = new ArrayList<>(partitionCommitInfos.size());
        for (PartitionCommitInfo commitInfo : partitionCommitInfos) {
            TPartitionVersionInfo version = new TPartitionVersionInfo(commitInfo.getPartitionId(),
                    commitInfo.getVersion(), 0);
            partitionVersions.add(version);
        }

        long createTime = System.currentTimeMillis();
        for (long backendId : publishBackends) {
            PublishVersionTask task = new PublishVersionTask(backendId,
                    this.getTransactionId(),
                    this.getDbId(),
                    commitTime,
                    partitionVersions,
                    traceParent,
                    txnSpan,
                    createTime,
                    this);
            this.addPublishVersionTask(backendId, task);
            tasks.add(task);
        }
        return tasks;
    }

    public boolean allPublishTasksFinishedOrQuorumWaitTimeout(Set<Long> publishErrorReplicas) {
        boolean timeout = System.currentTimeMillis() - getCommitTime() > Config.quorom_publish_wait_time_ms;
        for (PublishVersionTask publishVersionTask : getPublishVersionTasks().values()) {
            if (publishVersionTask.isFinished()) {
                publishErrorReplicas.addAll(publishVersionTask.getErrorReplicas());
            } else if (!timeout) {
                return false;
            }
        }
        return true;
    }

    // Note: caller should hold db lock
    public void prepareFinishChecker(Database db) {
        synchronized (this) {
            finishChecker = TransactionChecker.create(this, db);
            checkerCreationTime = System.nanoTime();
        }
    }

    public boolean checkCanFinish() {
        // finishChecker may be null if FE restarts
        // finishChecker may require refresh if table/partition is dropped, or index is changed caused by Alter job
        if (finishChecker == null || System.nanoTime() - checkerCreationTime > 10000000000L) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                // consider txn finished if db is dropped
                return true;
            }
            db.readLock();
            try {
                prepareFinishChecker(db);
            } finally {
                db.readUnlock();
            }
        }
        if (finishState == null) {
            finishState = new TxnFinishState();
        }
        boolean ret = finishChecker.finished(finishState);
        if (ret) {
            txnSpan.addEvent("check_ok");
        }
        return ret;
    }

    public String getPublishTimeoutDebugInfo() {
        if (!hasSendTask()) {
            return "txn has not sent publish tasks yet, maybe waiting previous txns on the same table(s) to finish, tableIds: " +
                    Joiner.on(",").join(getTableIdList());
        } else if (finishChecker != null) {
            return finishChecker.debugInfo();
        } else {
            return getErrMsg();
        }
    }

    public void setFinishState(TxnFinishState finishState) {
        this.finishState = finishState;
    }

    public TxnFinishState getFinishState() {
        return finishState;
    }

    public void setNewFinish() {
        newFinish = true;
    }

    public boolean isNewFinish() {
        return newFinish;
    }

    public Span getTxnSpan() {
        return txnSpan;
    }

    public String getTraceParent() {
        return traceParent;
    }
}