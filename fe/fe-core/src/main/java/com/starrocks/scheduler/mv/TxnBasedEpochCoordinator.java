// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.UserException;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.qe.Coordinator;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TMVStartEpochTask;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * EpochCoordinator based on transaction:
 * 1. Epoch start: begin the transaction, and allocate a transaction id
 * 2. Epoch execution: notify all executors to execute job within transaction
 * 3. Epoch commit: after execution, commit the transaction
 */
class TxnBasedEpochCoordinator implements EpochCoordinator {
    private static final Logger LOG = LogManager.getLogger(TxnBasedEpochCoordinator.class);

    // TODO(murphy) make it configurable
    private static final long JOB_TIMEOUT = 120;
    private static final long TXN_VISIBLE_TIMEOUT_MILLIS = 100_1000;

    private final MVMaintenanceJob mvMaintenanceJob;

    public TxnBasedEpochCoordinator(MVMaintenanceJob mvMaintenanceJob) {
        this.mvMaintenanceJob = mvMaintenanceJob;
    }

    /**
     * TODO(murphy) make it event-driven instead of blocking here
     */
    public void runEpoch(MVEpoch epoch) {
        beginEpoch(epoch);
        boolean success = waitEpochFinish(epoch);
        if (success) {
            commitEpoch(epoch);
        } else {
            epoch.onFailed();
            abortEpoch(epoch);
        }
    }

    private void beginEpoch(MVEpoch epoch) {
        MaterializedView view = mvMaintenanceJob.getView();
        MvId mvId = view.getMvId();
        long dbId = view.getDbId();
        List<Long> tableIdList = new ArrayList<>(view.getBaseTableIds());
        String label = "mv_refresh_" + mvId;
        TUniqueId requestId = new TUniqueId();
        TransactionState.TxnCoordinator txnCoordinator = TransactionState.TxnCoordinator.fromThisFE();
        TransactionState.LoadJobSourceType loadSource = TransactionState.LoadJobSourceType.MV_REFRESH;

        try {
            long txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                    .beginTransaction(dbId, tableIdList, label, txnCoordinator, loadSource, JOB_TIMEOUT);
            epoch.setTxnId(txnId);
            execute(epoch);
        } catch (Exception e) {
            epoch.onFailed();
            LOG.warn("Failed to begin transaction for epoch {}", epoch);
            throw new RuntimeException(e);
        }
    }

    private boolean waitEpochFinish(MVEpoch epoch) {
        // TODO(murphy) wait all task finish
        return true;
    }

    private void commitEpoch(MVEpoch epoch) {
        long dbId = mvMaintenanceJob.getView().getDbId();
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        Coordinator queryCoordinator = mvMaintenanceJob.getQueryCoordinator();
        List<TabletCommitInfo> commitInfo = TabletCommitInfo.fromThrift(queryCoordinator.getCommitInfos());
        List<TabletFailInfo> failedInfo = TabletFailInfo.fromThrift(queryCoordinator.getFailInfos());

        try {
            epoch.onCommitting();

            boolean published = GlobalStateMgr.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(database,
                    epoch.getTxnId(), commitInfo, failedInfo, TXN_VISIBLE_TIMEOUT_MILLIS);
            Preconditions.checkState(published, "must be published");

            // TODO(murphy) collect binlog consumption state from execution
            BinlogConsumeStateVO binlogState = new BinlogConsumeStateVO();

            // TODO(murphy) atomic persist the epoch update and transaction commit
            GlobalStateMgr.getCurrentState().getEditLog().logMVEpochChange(epoch);

            epoch.onCommitted(binlogState);
        } catch (UserException e) {
            epoch.onFailed();
            // TODO(murphy) handle error
        }
    }

    private void abortEpoch(MVEpoch epoch) {
        long dbId = mvMaintenanceJob.getView().getDbId();
        long txnId = epoch.getTxnId();
        String failReason = "";
        try {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(dbId, txnId, failReason);
            epoch.onFailed();
        } catch (UserException e) {
            LOG.warn("Abort transaction failed: {}", txnId);
        }
    }

    // TODO(murphy) notify all executors, and wait for finishing
    private void execute(MVEpoch epoch) throws Exception {
        List<MVMaintenanceTask> tasks = mvMaintenanceJob.getTasks();
        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();
        String dbName = GlobalStateMgr.getCurrentState().getDb(mvMaintenanceJob.getView().getDbId()).getFullName();

        for (MVMaintenanceTask task : tasks) {
            long beId = task.getBeId();
            long taskId = task.getTaskId();
            Backend backend = Preconditions.checkNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackend(beId));
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

            // Request information
            MaterializedView view = mvMaintenanceJob.getView();

            // Build request
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setTask_type(MVTaskType.START_EPOCH);
            TMVStartEpochTask taskMsg = new TMVStartEpochTask();
            request.start_epoch = taskMsg;
            taskMsg.setEpoch(epoch.toThrift());
            try {
                results.add(BackendServiceClient.getInstance().submitMVMaintenanceTaskAsync(address, request));
            } catch (Exception e) {
                epoch.onFailed();
                LOG.warn("deploy job of MV {} failed: ", view.getName());
                throw new RuntimeException(e);
            }
        }

        // Wait for all RPC
        Exception ex = null;
        for (Future<PMVMaintenanceTaskResult> future : results) {
            try {
                future.wait();
            } catch (InterruptedException e) {
                if (ex == null) {
                    ex = e;
                }
                LOG.error("deploy MV task failed", e);
            }
        }
        if (ex != null) {
            throw ex;
        }
    }
}
