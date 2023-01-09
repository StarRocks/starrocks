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

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.UserException;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TMVStartEpochTask;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * EpochCoordinator based on transaction:
 * 1. Epoch start: begin the transaction, and allocate a transaction id
 * 2. Epoch execution: notify all executors to execute job within transaction
 * 3. Epoch commit: after execution, commit the transaction
 */
class TxnBasedEpochCoordinator implements EpochCoordinator {
    private static final Logger LOG = LogManager.getLogger(TxnBasedEpochCoordinator.class);

    // TODO(murphy) make it configurable
    private static final long MAX_EXEC_MILLIS = 1000;
    private static final long MAX_SCAN_ROWS = 100_000;
    private static final long JOB_TIMEOUT = 120;
    private static final long TXN_VISIBLE_TIMEOUT_MILLIS = 100_000;

    private final MVMaintenanceJob mvMaintenanceJob;

    public TxnBasedEpochCoordinator(MVMaintenanceJob mvMaintenanceJob) {
        this.mvMaintenanceJob = mvMaintenanceJob;
    }

    /**
     * TODO(murphy) make it event-driven instead of blocking here
     */
    public void runEpoch(MVEpoch epoch) {
        LOG.info("runEpoch: {}", epoch);
        try {
            beginEpoch(epoch);
            boolean success = waitEpochFinish(epoch);
            if (success) {
                commitEpoch(epoch);
            } else {
                epoch.onFailed();
                abortEpoch(epoch);
            }
        } catch (Exception e) {
            LOG.warn("runEpoch failed: {}", e);
            throw e;
        } finally {
            // TODO: When to reset epoch?
            epoch.reset();
        }
    }

    private void beginEpoch(MVEpoch epoch) {
        LOG.info("beginEpoch: {}", epoch);

        MaterializedView view = mvMaintenanceJob.getView();
        long dbId = view.getDbId();
        List<Long> tableIdList =
                view.getBaseTableInfos().stream().map(t -> t.getTableId()).collect(Collectors.toList());
        long currentTs = System.currentTimeMillis();
        String label = String.format("MV_REFRESH_%d-%d-%d", dbId, view.getId(), currentTs);
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
        LOG.info("commitEpoch: {}", epoch);
        long dbId = mvMaintenanceJob.getView().getDbId();
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        List<TabletCommitInfo> commitInfo = null;
        List<TabletFailInfo> failedInfo = null;

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
        LOG.info("abortEpoch: {}", epoch);
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
        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();

        for (MVMaintenanceTask task : mvMaintenanceJob.getTasks().values()) {
            long taskId = task.getTaskId();
            TNetworkAddress address = SystemInfoService.toBrpcHost(task.getBeHost());

            // Request information
            MaterializedView view = mvMaintenanceJob.getView();

            // Build request
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setQuery_id(mvMaintenanceJob.getQueryId());
            request.setTask_type(MVTaskType.START_EPOCH);
            request.setTask_id(taskId);
            request.setJob_id(mvMaintenanceJob.getJobId());
            request.setMv_name(mvMaintenanceJob.getView().getName());
            TMVStartEpochTask taskMsg = new TMVStartEpochTask();
            taskMsg.setEpoch(epoch.toThrift());
            // set binlog offset here
            taskMsg.setPer_node_scan_ranges(task.getBinlogConsumeState());

            taskMsg.setMax_exec_millis(MAX_EXEC_MILLIS);
            taskMsg.setMax_scan_rows(MAX_SCAN_ROWS);

            request.start_epoch = taskMsg;
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
                future.get();
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
