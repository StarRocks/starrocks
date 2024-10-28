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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for executing a load.
 */
public class LoadExecutor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LoadExecutor.class);

    // Initialized in constructor ==================================
    private final TableId tableId;
    private final String label;
    private final TUniqueId loadId;
    private final StreamLoadInfo streamLoadInfo;
    private final ImmutableMap<String, String> loadParameters;
    private final ConnectContext connectContext;
    private final Set<Long> coordinatorBackendIds;
    private final int batchWriteIntervalMs;
    private final Coordinator.Factory coordinatorFactory;
    private final LoadExecuteCallback loadExecuteCallback;
    private final TimeTrace timeTrace;
    private final AtomicReference<Throwable> failure;

    // Initialized in beginTxn() ==================================
    private long txnId = -1;

    // Initialized in executeLoad() ==================================
    private Coordinator coordinator;
    private List<TabletCommitInfo> tabletCommitInfo;
    private List<TabletFailInfo> tabletFailInfo;

    public LoadExecutor(
            TableId tableId,
            String label,
            TUniqueId loadId,
            StreamLoadInfo streamLoadInfo,
            int batchWriteIntervalMs,
            ImmutableMap<String, String> loadParameters,
            ConnectContext connectContext,
            Set<Long> coordinatorBackendIds,
            Coordinator.Factory coordinatorFactory,
            LoadExecuteCallback loadExecuteCallback) {
        this.tableId = tableId;
        this.label = label;
        this.loadId = loadId;
        this.streamLoadInfo = streamLoadInfo;
        this.batchWriteIntervalMs = batchWriteIntervalMs;
        this.loadParameters = loadParameters;
        this.connectContext = connectContext;
        this.coordinatorBackendIds = coordinatorBackendIds;
        this.coordinatorFactory = coordinatorFactory;
        this.loadExecuteCallback = loadExecuteCallback;
        this.timeTrace = new TimeTrace();
        this.failure = new AtomicReference<>();
    }

    @Override
    public void run() {
        timeTrace.startRunTsMs.set(System.currentTimeMillis());
        try {
            beginTxn();
            executeLoad();
            commitAndPublishTxn();
        } catch (Exception e) {
            failure.set(e);
            abortTxn(e);
            LOG.error("Failed to execute load, label: {}, load id: {}, txn id: {}",
                    label, DebugUtil.printId(loadId), txnId, e);
        } finally {
            loadExecuteCallback.finishLoad(label);
            timeTrace.finishTimeMs.set(System.currentTimeMillis());
        }
    }

    public String getLabel() {
        return label;
    }

    /**
     * Checks if the given backend id is contained in the coordinator backend IDs.
     */
    public boolean containCoordinatorBackend(long backendId) {
        return coordinatorBackendIds.contains(backendId);
    }

    /**
     * Checks if this batch is active and can accept new load requests.
     */
    public boolean isActive() {
        if (failure.get() != null) {
            return false;
        }
        long joinPlanTimeMs = timeTrace.joinPlanTimeMs.get();
        return joinPlanTimeMs <= 0 || (System.currentTimeMillis() - joinPlanTimeMs < batchWriteIntervalMs);
    }

    public Throwable getFailure() {
        return failure.get();
    }

    private void beginTxn() throws Exception {
        timeTrace.beginTxnTsMs.set(System.currentTimeMillis());
        Pair<Database, OlapTable> pair = getDbAndTable();
        txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                pair.first.getId(), Lists.newArrayList(pair.second.getId()), label,
                TransactionState.TxnCoordinator.fromThisFE(),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING,
                streamLoadInfo.getTimeout(), streamLoadInfo.getWarehouseId());
    }

    private void commitAndPublishTxn() throws Exception {
        timeTrace.commitTxnTimeMs.set(System.currentTimeMillis());
        Pair<Database, OlapTable> pair = getDbAndTable();
        long publishTimeoutMs =
                streamLoadInfo.getTimeout() * 1000L - (timeTrace.commitTxnTimeMs.get() - timeTrace.createTimeMs.get());
        boolean publishSuccess = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().commitAndPublishTransaction(
                pair.first, txnId, tabletCommitInfo, tabletFailInfo, publishTimeoutMs, null);
        if (!publishSuccess) {
            throw new LoadException(String.format(
                    "Publish timeout, total timeout time: %s ms, publish timeout time: %s ms",
                        streamLoadInfo.getTimeout() * 1000, publishTimeoutMs));
        }
    }

    private void abortTxn(Throwable reason) {
        if (txnId == -1) {
            return;
        }
        try {
            Pair<Database, OlapTable> pair = getDbAndTable();
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                    pair.first.getId(), txnId, reason == null ? "" : reason.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to abort transaction {}", txnId, e);
        }
    }

    private void executeLoad() throws Exception {
        timeTrace.executePlanTimeMs.set(System.currentTimeMillis());
        try {
            Pair<Database, OlapTable> pair = getDbAndTable();
            timeTrace.buildPlanTimeMs.set(System.currentTimeMillis());
            LoadPlanner loadPlanner = new LoadPlanner(-1, loadId, txnId, pair.first.getId(),
                    tableId.getDbName(), pair.second, streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(),
                    streamLoadInfo.isPartialUpdate(), connectContext, null,
                    streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(),
                    streamLoadInfo.getNegative(), coordinatorBackendIds.size(), streamLoadInfo.getColumnExprDescs(),
                    streamLoadInfo, label, streamLoadInfo.getTimeout());
            loadPlanner.setWarehouseId(streamLoadInfo.getWarehouseId());
            loadPlanner.setBatchWrite(batchWriteIntervalMs, loadParameters, coordinatorBackendIds);
            loadPlanner.plan();

            coordinator = coordinatorFactory.createStreamLoadScheduler(loadPlanner);
            QeProcessorImpl.INSTANCE.registerQuery(loadId, coordinator);
            timeTrace.executePlanTimeMs.set(System.currentTimeMillis());
            coordinator.exec();

            timeTrace.joinPlanTimeMs.set(System.currentTimeMillis());
            int waitSecond = streamLoadInfo.getTimeout() -
                    (int) (System.currentTimeMillis() - timeTrace.createTimeMs.get()) / 1000;
            if (coordinator.join(waitSecond)) {
                Status status = coordinator.getExecStatus();
                if (!status.ok()) {
                    throw new LoadException(
                            String.format("Failed to execute load, status code: %s, error message: %s",
                                    status.getErrorCodeString(), status.getErrorMsg()));
                }
                tabletCommitInfo = TabletCommitInfo.fromThrift(coordinator.getCommitInfos());
                tabletFailInfo = TabletFailInfo.fromThrift(coordinator.getFailInfos());
            } else {
                throw new LoadException(
                        String.format("Timeout to execute load after waiting for %s seconds", waitSecond));
            }
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }

    private Pair<Database, OlapTable> getDbAndTable() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new LoadException(String.format("Database %s does not exist", tableId.getDbName()));
        }

        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            table = GlobalStateMgr.getCurrentState()
                    .getLocalMetastore().getTable(db.getFullName(), tableId.getTableName());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        if (table == null) {
            throw new LoadException(String.format(
                    "Table [%s.%s] does not exist", tableId.getDbName(), tableId.getTableName()));
        }
        return Pair.create(db, (OlapTable) table);
    }

    @VisibleForTesting
    Set<Long> getCoordinatorBackendIds() {
        return coordinatorBackendIds;
    }

    @VisibleForTesting
    Coordinator getCoordinator() {
        return coordinator;
    }

    @VisibleForTesting
    TimeTrace getTimeTrace() {
        return timeTrace;
    }

    // Trace the timing of various stages of the load operation.
    static class TimeTrace {
        AtomicLong createTimeMs;
        AtomicLong startRunTsMs = new AtomicLong(-1);
        AtomicLong beginTxnTsMs = new AtomicLong(-1);
        AtomicLong buildPlanTimeMs = new AtomicLong(-1);
        AtomicLong executePlanTimeMs = new AtomicLong(-1);
        AtomicLong joinPlanTimeMs = new AtomicLong(-1);
        AtomicLong commitTxnTimeMs = new AtomicLong(-1);
        AtomicLong finishTimeMs = new AtomicLong(-1);

        public TimeTrace() {
            this.createTimeMs = new AtomicLong(System.currentTimeMillis());
        }
    }
}
