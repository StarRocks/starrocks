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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/InsertLoadJob.java

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

package com.starrocks.load.loadv2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.load.FailMsg.CancelType;
import com.starrocks.planner.FileScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The class is performed to record the finished info of insert load job.
 * It is created after txn is visible which belongs to insert load job.
 * The state of insert load job is always finished, so it will never be scheduled by JobScheduler.
 */
public class InsertLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    @SerializedName("tid")
    private long tableId;
    // Which DML statement created the job: INSERT_SELECT, INSERT_VALUES, INSERT_FILES, UPDATE, DELETE or MERGE.
    @SerializedName("sk")
    private String statementKind;
    @SerializedName("cip")
    private String clientIp;
    private long estimateScanRow = 0;
    private TLoadJobType loadType;
    private Coordinator coordinator;
    private InsertLoadTxnCallback txnCallback;
    // The two fields below only matter on the FE that runs the statement and are not persisted.
    // Whether the statement registers a profile for this job after it recorded the final state.
    private volatile boolean profileExpected = false;
    // The loads_history row is handed over once, by the statement or by the visible callback.
    private final AtomicBoolean historyRecorded = new AtomicBoolean(false);

    // only for log replay
    public InsertLoadJob() {
        super();
        this.jobType = EtlJobType.INSERT;
    }

    public InsertLoadJob(String label, long dbId, long tableId, long txnId,
                         String loadId, String user, long createTimestamp,
                         long timeout, long warehouseId, Coordinator coordinator,
                         InsertLoadTxnCallback insertLoadTxnCallback,
                         String statementKind, String clientIp) {
        super(dbId, label);
        this.tableId = tableId;
        this.statementKind = statementKind;
        this.clientIp = clientIp;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.state = JobState.LOADING;
        this.jobType = EtlJobType.INSERT;
        this.loadType = coordinator.getLoadJobType();
        this.timeoutSecond = timeout;
        this.warehouseId = warehouseId;
        this.coordinator = coordinator;
        this.loadIds.add(loadId);
        this.transactionId = txnId;
        this.user = user;
        this.txnCallback = insertLoadTxnCallback;
    }

    // only used for test
    public InsertLoadJob(String label, long dbId, long tableId, long createTimestamp, String failMsg,
                         String trackingUrl, Coordinator coordinator) throws MetaNotFoundException {
        super(dbId, label);
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.finishTimestamp = System.currentTimeMillis();
        if (Strings.isNullOrEmpty(failMsg)) {
            this.state = JobState.FINISHED;
            this.progress = 100;
        } else {
            this.state = JobState.CANCELLED;
            this.failMsg = new FailMsg(CancelType.LOAD_RUN_FAIL, failMsg);
            this.progress = 0;
        }
        this.jobType = EtlJobType.INSERT;
        this.timeoutSecond = Config.insert_load_default_timeout_second;
        this.loadingStatus.setTrackingUrl(trackingUrl);
        this.loadType = TLoadJobType.INSERT_QUERY;
        this.coordinator = coordinator;
    }

    // Holds on every FE and after a restart: the commit timestamp is set on replay as well, and it is persisted.
    boolean isTxnCommitted() {
        return loadCommittedTimestamp != -1;
    }

    // Whether a statement on this FE runs this job. That statement clears the coordinator when it records the final
    // state, and a job restored from the journal or an image has no coordinator.
    boolean hasRunningStatement() {
        // The statement clears the coordinator before it logs the final state, so a reader without the lock could
        // see a job in between as one that no statement runs.
        readLock();
        try {
            return coordinator != null;
        } finally {
            readUnlock();
        }
    }

    public void setLoadFinishOrCancel(String failMsg, String trackingUrl) {
        boolean publishPending = false;
        writeLock();
        try {
            this.finishTimestamp = System.currentTimeMillis();
            JobState finalState;
            if (Strings.isNullOrEmpty(failMsg)) {
                finalState = JobState.FINISHED;
                this.progress = 100;
            } else {
                finalState = JobState.CANCELLED;
                this.failMsg = new FailMsg(CancelType.LOAD_RUN_FAIL, failMsg);
                this.progress = 0;
            }
            this.loadingStatus.setTrackingUrl(trackingUrl);
            this.profileExpected = isProfileExpected(this.coordinator);
            this.coordinator = null;
            // persistent
            GlobalStateMgr.getCurrentState().getEditLog().logEndLoadJob(
                    new LoadJobFinalOperation(this.id, this.loadingStatus, this.progress,
                            this.loadStartTimestamp, this.finishTimestamp, finalState, this.failMsg),
                    wal -> {
                        this.state = finalState;
                    });
        } finally {
            writeUnlock();
            // A statement whose publish wait timed out finishes the job while the transaction is
            // only committed. The callback then stays registered to learn the visible time.
            publishPending = isPublishPending();
            if (!publishPending) {
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(this.id);
            }
        }
        if (publishPending) {
            // load_jobs shows the final state at once. The history row is written once the visible
            // time is known: the writer holds it until then, and gives up after a while. A row it
            // cannot hold is recorded as it is.
            offerRows(() -> LoadJobsTable.offer(this::toHistoryRow));
            if (LoadsHistoryWriter.holdUntilVisible(this.id, this::toHistoryRow)) {
                historyRecorded.set(true);
            } else {
                recordHistory();
            }
        } else {
            recordHistory();
        }
    }

    // The statement registers its profile only after it recorded the final state of the job, so
    // toThrift() does not see the profile yet when the history row is taken.
    private static boolean isProfileExpected(Coordinator coordinator) {
        try {
            ConnectContext context = ConnectContext.get();
            if (context == null || coordinator == null || coordinator.getQueryProfile() == null) {
                return false;
            }
            return context.isProfileEnabled() || LoadErrorUtils.enableProfileAfterError(coordinator);
        } catch (Exception e) {
            return false;
        }
    }

    // Whether the job finished while its transaction is committed but not visible yet.
    @VisibleForTesting
    boolean isPublishPending() {
        if (state != JobState.FINISHED || visibleTimestamp > 0 || transactionId <= 0) {
            return false;
        }
        try {
            TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTransactionState(dbId, transactionId);
            return txnState != null && txnState.getTransactionStatus() == TransactionStatus.COMMITTED;
        } catch (Exception e) {
            return false;
        }
    }

    /** Hand the final row to the loads_history writer, once, and to the load_jobs writer. */
    private void recordHistory() {
        if (historyRecorded.compareAndSet(false, true)) {
            offerRows(() -> {
                Supplier<TLoadInfo> row = Suppliers.memoize(this::toHistoryRow);
                LoadsHistoryWriter.offer(row);
                LoadJobsTable.offer(row);
            });
        }
    }

    private TLoadInfo toHistoryRow() {
        TLoadInfo info = toThrift();
        if (profileExpected && !info.isSetProfile_id() && info.isSetLoad_id()) {
            // Same value toThrift() reports once the profile is registered.
            info.setProfile_id(info.getLoad_id());
        }
        return info;
    }

    @Override
    public void updateProgress(TReportExecStatusParams params) {
        writeLock();
        try {
            super.updateProgress(params);
            if (!loadingStatus.getLoadStatistic().getLoadFinish()) {
                if (this.loadType == TLoadJobType.INSERT_QUERY) {
                    if (loadingStatus.getLoadStatistic().totalFileSizeB != 0) {
                        // progress of file scan
                        progress = (int) ((double) loadingStatus.getLoadStatistic().sourceScanBytes() /
                                loadingStatus.getLoadStatistic().totalFileSize() * 100);
                    } else {
                        // progress of table scan. Slightly smaller than actual
                        progress = (int) ((double) loadingStatus.getLoadStatistic().totalSourceLoadRows()
                                / (estimateScanRow + 1) * 100);
                    }
                } else {
                    progress = (int) ((double) loadingStatus.getLoadStatistic().totalSinkLoadRows() 
                        / (estimateScanRow + 1) * 100);
                }

                if (progress >= 100) {
                    progress = 99;
                }
            }
        } finally {
            writeUnlock();
        }
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            return Sets.newHashSet(String.valueOf(tableId));
        }
        // The database will not be locked in here.
        // The getTable is a thread-safe method called without read lock of database
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        if (table == null) {
            return Sets.newHashSet(String.valueOf(tableId));
        }
        return Sets.newHashSet(table.getName());
    }

    @Override
    public Set<Long> getTableIds() {
        return Sets.newHashSet(tableId);
    }

    @Override
    public Set<String> getTableNames(boolean noThrow) throws MetaNotFoundException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        if (table == null) {
            if (noThrow) {
                return Sets.newHashSet();
            } else {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            }
        }
        return Sets.newHashSet(table.getName());
    }

    @Override
    public boolean hasTxn() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            return true;
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        if (table == null) {
            return true;
        }

        if (table instanceof SystemTable
                || table instanceof IcebergTable
                || table instanceof HiveTable
                || table instanceof ExternalOlapTable) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    protected List<TabletCommitInfo> getTabletCommitInfos() {
        return Coordinator.getCommitInfos(coordinator);
    }

    @Override
    protected List<TabletFailInfo> getTabletFailInfos() {
        return Coordinator.getFailInfos(coordinator);
    }

    public void setEstimateScanRow(long rows) {
        this.estimateScanRow = rows;
    }

    public void updateLoadingStatus(Map<String, String> counters) {
        this.loadingStatus.getCounters().putAll(counters);
    }

    public void setTransactionId(long txnId) {
        this.transactionId = txnId;
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        if (txnCallback != null) {
            txnCallback.beforeCommitted(txnState);
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState) throws StarRocksException {
        loadCommittedTimestamp = txnState.getCommitTime();
        captureCommitThrottle(txnState);
        if (txnCallback != null) {
            txnCallback.afterCommitted(txnState);
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        loadCommittedTimestamp = txnState.getCommitTime();
        // The live txnCallback is not persisted, so the replaying node asks the factory instead.
        InsertLoadTxnCallback replayCallback = InsertLoadTxnCallbackFactory.ofReplay(dbId, tableId, txnState);
        if (replayCallback != null) {
            replayCallback.replayOnCommitted(txnState);
        }
    }

    @Override
    public void afterAborted(TransactionState txnState, String txnStatusChangeReason) {
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
    }

    @Override
    public void afterVisible(TransactionState txnState) {
        visibleTimestamp = txnState.getFinishTime() > 0 ? txnState.getFinishTime() : System.currentTimeMillis();
        boolean finished;
        readLock();
        try {
            finished = state == JobState.FINISHED;
        } finally {
            readUnlock();
        }
        if (finished) {
            // The statement finished the job before the publish did and left the history row
            // with the writer, which sees the visible time from now on.
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(this.id);
            // load_jobs already has the final row; write it again with the visible time.
            offerRows(() -> LoadJobsTable.offer(this::toHistoryRow));
        }
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        visibleTimestamp = txnState.getFinishTime();
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    protected String getStatementKind() {
        return statementKind;
    }

    @Override
    protected String getClientIp() {
        return clientIp;
    }

    /**
     * Classify the DML statement that produced an insert load job. INSERT is split by
     * its source: literal VALUES, FILES() (planned as a FileScanNode), or any other query.
     */
    public static String statementKindOf(DmlStmt stmt, ExecPlan execPlan) {
        if (stmt instanceof UpdateStmt) {
            return "UPDATE";
        }
        if (stmt instanceof DeleteStmt) {
            return "DELETE";
        }
        if (stmt instanceof MergeIntoStmt) {
            return "MERGE";
        }
        if (stmt instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) stmt;
            if (insertStmt.getQueryStatement() != null
                    && insertStmt.getQueryStatement().getQueryRelation() instanceof ValuesRelation) {
                return "INSERT_VALUES";
            }
            if (execPlan != null && execPlan.getScanNodes().stream().anyMatch(node -> node instanceof FileScanNode)) {
                return "INSERT_FILES";
            }
            return "INSERT_SELECT";
        }
        return null;
    }
}
