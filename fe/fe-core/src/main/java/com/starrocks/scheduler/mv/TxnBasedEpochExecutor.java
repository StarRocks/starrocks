// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.UserException;
import com.starrocks.qe.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO(murphy) abstract it to support other kinds of EpochExecutor
 */
class TxnBasedEpochExecutor {
    private static final Logger LOG = LogManager.getLogger(TxnBasedEpochExecutor.class);

    // TODO(murphy) make it configurable
    private static final long JOB_TIMEOUT = 120;
    private static final long TXN_VISIBLE_TIMEOUT_MILLIS = 10_1000;

    private final MVMaintenanceJob mvMaintenanceJob;
    private final MVEpoch epoch;

    public TxnBasedEpochExecutor(MVMaintenanceJob mvMaintenanceJob) {
        this.mvMaintenanceJob = mvMaintenanceJob;
        this.epoch = new MVEpoch();
    }

    public void start() {
        this.epoch.onReady();
    }

    public void run() {
        if (this.epoch.onSchedule()) {
            try {
                beginEpoch();
                commitEpoch();
                this.epoch.reset();
            } catch (Throwable e) {
                abortEpoch();
            }
        } else {
            LOG.warn("Already running");
        }
    }

    private void beginEpoch() {
        MaterializedView view = mvMaintenanceJob.getView();
        MvId mvId = view.getMvId();
        long dbId = view.getDbId();
        List<Long> tableIdList = new ArrayList<>(view.getBaseTableIds());
        String label = "mv_refresh_" + mvId;
        TUniqueId requestId = new TUniqueId();
        TransactionState.TxnCoordinator txnCoordinator = TransactionState.TxnCoordinator.fromThisFE();
        TransactionState.LoadJobSourceType loadSource = TransactionState.LoadJobSourceType.BATCH_LOAD_JOB;

        try {
            long txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                    .beginTransaction(dbId, tableIdList, label, txnCoordinator, loadSource, JOB_TIMEOUT);
            this.epoch.transactionId = txnId;
        } catch (Exception e) {
            this.epoch.onFailed();
            LOG.warn("Failed to begin transaction for epoch {}", this.epoch);
            throw new RuntimeException(e);
        }
    }

    private void commitEpoch() {
        long dbId = mvMaintenanceJob.getView().getDbId();
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        Coordinator queryCoordinator = mvMaintenanceJob.getQueryCoordinator();
        // TODO(murphy) collect the commit info
        List<TabletCommitInfo> commitInfo = TabletCommitInfo.fromThrift(queryCoordinator.getCommitInfos());
        List<TabletFailInfo> failedInfo = TabletFailInfo.fromThrift(queryCoordinator.getFailInfos());

        try {
            this.epoch.onCommitting();
            GlobalStateMgr.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(database,
                    this.epoch.transactionId, commitInfo, failedInfo, TXN_VISIBLE_TIMEOUT_MILLIS);
            this.epoch.onCommitted();
        } catch (UserException e) {
            this.epoch.onFailed();
            // TODO(murphy) handle error
        }
    }

    private void abortEpoch() {
        long dbId = mvMaintenanceJob.getView().getDbId();
        long txnId = this.epoch.transactionId;
        String failReason = "";
        try {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(dbId, txnId, failReason);
            this.epoch.onFailed();
        } catch (UserException e) {
            LOG.warn("Abort transaction failed: {}", txnId);
        }

    }
}
