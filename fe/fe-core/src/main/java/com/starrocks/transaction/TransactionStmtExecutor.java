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

package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.LoadException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.DmlType;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TLoadJobType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.starrocks.common.ErrorCode.ERR_TXN_NOT_EXIST;

public class TransactionStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(TransactionStmtExecutor.class);

    public static void beginStmt(ConnectContext context, BeginStmt stmt) {
        if (context.getExplicitTxnState() != null) {
            //Repeated begin does not create a new transaction
            ExplicitTxnState explicitTxnState = context.getExplicitTxnState();
            String label = explicitTxnState.getTransactionState().getLabel();
            long transactionId = explicitTxnState.getTransactionState().getTransactionId();
            context.getState().setOk(0, 0, buildMessage(label, TransactionStatus.PREPARE, transactionId, -1));
            return;
        }

        long transactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionIDGenerator().getNextTransactionId();
        String label = DebugUtil.printId(context.getExecutionId());
        TransactionState transactionState = new TransactionState(transactionId, label, null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);

        transactionState.setPrepareTime(System.currentTimeMillis());
        transactionState.setWarehouseId(context.getCurrentWarehouseId());
        boolean combinedTxnLog = LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING);
        transactionState.setUseCombinedTxnLog(combinedTxnLog);

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);
        context.setExplicitTxnState(explicitTxnState);

        context.getState().setOk(0, 0, buildMessage(label, TransactionStatus.PREPARE, transactionId, -1));
    }

    public static void loadData(Database database,
                                Table targetTable,
                                ExecPlan execPlan,
                                DmlStmt dmlStmt,
                                OriginStatement originStmt,
                                ConnectContext context) {
        Coordinator coordinator = new DefaultCoordinator.Factory().createInsertScheduler(
                context, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());
        ExplicitTxnState explicitTxnState = context.getExplicitTxnState();
        TransactionState transactionState = explicitTxnState.getTransactionState();

        try {
            if (transactionState.getDbId() == 0) {
                transactionState.setDbId(database.getId());
                DatabaseTransactionMgr databaseTransactionMgr =
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                .getDatabaseTransactionMgr(database.getId());
                databaseTransactionMgr.upsertTransactionState(transactionState);
            }

            if (database.getId() != transactionState.getDbId()) {
                throw ErrorReportException.report(ErrorCode.ERR_TXN_FORBID_CROSS_DB);
            }

            if (transactionState.getTableIdList().contains(targetTable.getId())) {
                throw ErrorReportException.report(ErrorCode.ERR_TXN_IMPORT_SAME_TABLE);
            }
            transactionState.addTableIdList(targetTable.getId());

            ExplicitTxnState.ExplicitTxnStateItem item =
                    load(database, targetTable, execPlan, dmlStmt, originStmt, context, coordinator);
            explicitTxnState.addTransactionItem(item);

            context.getState().setOk(item.getLoadedRows(), Ints.saturatedCast(item.getFilteredRows()),
                    buildMessage(transactionState.getLabel(), TransactionStatus.PREPARE,
                            transactionState.getTransactionId(), database.getId()));
        } catch (StarRocksException | RpcException | InterruptedException e) {
            context.getState().setError(e.getMessage());
        }
    }

    public static void commitStmt(ConnectContext context, CommitStmt stmt) {
        ExplicitTxnState explicitTxnState = context.getExplicitTxnState();
        if (explicitTxnState == null) {
            //commit statement not in after begin, do nothing
            return;
        }

        if (explicitTxnState.getTransactionStateItems().isEmpty()) {
            TransactionState transactionState = explicitTxnState.getTransactionState();
            context.setExplicitTxnState(null);
            context.getState().setOk(0, 0, buildMessage(transactionState.getLabel(),
                    TransactionStatus.VISIBLE, transactionState.getTransactionId(), -1));
            return;
        }

        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        long transactionId = context.getExplicitTxnState().getTransactionState().getTransactionId();
        TransactionState transactionState = context.getExplicitTxnState().getTransactionState();
        long databaseId = transactionState.getDbId();
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseId);

        try {
            int timeout = context.getSessionVariable().getQueryTimeoutS();
            long jobDeadLineMs = System.currentTimeMillis() + timeout * 1000L;

            List<TabletCommitInfo> commitInfos = Lists.newArrayList();
            List<TabletFailInfo> failInfos = Lists.newArrayList();
            long loadedRows = 0;
            for (ExplicitTxnState.ExplicitTxnStateItem item : explicitTxnState.getTransactionStateItems()) {
                commitInfos.addAll(item.getTabletCommitInfos());
                failInfos.addAll(item.getTabletFailInfos());
                loadedRows += item.getLoadedRows();
            }

            TxnCommitAttachment txnCommitAttachment = new InsertTxnCommitAttachment(loadedRows);
            VisibleStateWaiter visibleWaiter = transactionMgr.retryCommitOnRateLimitExceeded(
                    database,
                    transactionId,
                    commitInfos,
                    failInfos,
                    txnCommitAttachment,
                    timeout);

            long publishWaitMs = Config.enable_sync_publish ? jobDeadLineMs - System.currentTimeMillis() :
                    context.getSessionVariable().getTransactionVisibleWaitTimeout() * 1000;

            TransactionStatus txnStatus;
            if (visibleWaiter.await(publishWaitMs, TimeUnit.MILLISECONDS)) {
                txnStatus = TransactionStatus.VISIBLE;
            } else {
                txnStatus = TransactionStatus.COMMITTED;
            }

            List<ExplicitTxnState.ExplicitTxnStateItem> explicitTxnStateItems
                    = explicitTxnState.getTransactionStateItems();
            List<Long> callbackIds = transactionState.getCallbackId();
            Preconditions.checkArgument(explicitTxnStateItems.size() == callbackIds.size());

            for (int i = 0; i < explicitTxnStateItems.size(); i++) {
                ExplicitTxnState.ExplicitTxnStateItem item = explicitTxnStateItems.get(i);

                DmlStmt dmlStmt = item.getDmlStmt();
                Table targetTable = GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(database.getFullName(), dmlStmt.getTableName().getTbl());

                MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                // collect table-level metrics
                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(targetTable.getId());
                entity.counterInsertLoadFinishedTotal.increase(1L);
                entity.counterInsertLoadRowsTotal.increase(item.getLoadedRows());
                entity.counterInsertLoadBytesTotal.increase(item.getLoadedBytes());

                GlobalStateMgr.getCurrentState().getOperationListenerBus()
                        .onDMLStmtJobTransactionFinish(transactionState, database, targetTable, DmlType.fromStmt(dmlStmt));

                context.getGlobalStateMgr().getLoadMgr().recordFinishedOrCancelledLoadJob(
                        callbackIds.get(i),
                        EtlJobType.INSERT,
                        "",
                        "");
            }

            context.getState().setOk(0, 0,
                    buildMessage(transactionState.getLabel(), txnStatus, transactionId, database.getId()));
        } catch (StarRocksException | LockTimeoutException e) {
            LOG.warn("errors when abort txn", e);
            context.getState().setError(e.getMessage());
        } finally {
            //clean global explicit transaction state
            context.setExplicitTxnState(null);
        }
    }

    public static void rollbackStmt(ConnectContext context, RollbackStmt stmt) {
        ExplicitTxnState explicitTxnState = context.getExplicitTxnState();
        if (explicitTxnState == null) {
            //rollback statement not in after begin, do nothing
            return;
        }

        if (explicitTxnState.getTransactionStateItems().isEmpty()) {
            TransactionState transactionState = explicitTxnState.getTransactionState();
            context.setExplicitTxnState(null);
            context.getState().setOk(0, 0, buildMessage(transactionState.getLabel(),
                    TransactionStatus.ABORTED, transactionState.getTransactionId(), -1));
            return;
        }

        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        long transactionId = context.getExplicitTxnState().getTransactionState().getTransactionId();
        try {
            TransactionState transactionState = context.getExplicitTxnState().getTransactionState();
            long databaseId = transactionState.getDbId();

            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            List<TabletCommitInfo> commitInfos = Lists.newArrayList();
            List<TabletFailInfo> failInfos = Lists.newArrayList();

            List<ExplicitTxnState.ExplicitTxnStateItem> explicitTxnStateItems
                    = explicitTxnState.getTransactionStateItems();
            List<Long> callbackIds = transactionState.getCallbackId();
            Preconditions.checkArgument(explicitTxnStateItems.size() == callbackIds.size());

            for (int i = 0; i < explicitTxnStateItems.size(); i++) {
                ExplicitTxnState.ExplicitTxnStateItem item = explicitTxnStateItems.get(i);
                commitInfos.addAll(item.getTabletCommitInfos());
                failInfos.addAll(item.getTabletFailInfos());
                loadMgr.recordFinishedOrCancelledLoadJob(callbackIds.get(i), EtlJobType.INSERT, "", "");
            }

            transactionMgr.abortTransaction(
                    databaseId,
                    transactionId,
                    "rollback transaction by user",
                    commitInfos,
                    failInfos,
                    null);

            context.getState().setOk(0, 0,
                    buildMessage(transactionState.getLabel(), TransactionStatus.ABORTED, transactionId, -1));
        } catch (StarRocksException e) {
            // just print a log if abort txn failed. This failure do not need to pass to user.
            // user only concern abort how txn failed.
            LOG.warn("errors when abort txn", e);
            context.getState().setError(e.getMessage());
        } finally {
            //clean global explicit transaction state
            context.setExplicitTxnState(null);
        }
    }

    public static String buildMessage(String label, TransactionStatus txnStatus, long transactionId, long databaseId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("'label':'").append(label).append("', ");
        sb.append("'status':'").append(txnStatus.name()).append("', ");
        sb.append("'txnId':'").append(transactionId).append("'");

        if (txnStatus == TransactionStatus.COMMITTED) {
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            String timeoutInfo = transactionMgr.getTxnPublishTimeoutDebugInfo(databaseId, transactionId);
            LOG.warn("txn {} publish timeout {}", transactionId, timeoutInfo);
            if (timeoutInfo.length() > 240) {
                timeoutInfo = timeoutInfo.substring(0, 240) + "...";
            }
            String errMsg = "Publish timeout " + timeoutInfo;

            sb.append(", 'err':'").append(errMsg).append("'");
        }

        sb.append("}");

        return sb.toString();
    }

    public static ExplicitTxnState.ExplicitTxnStateItem load(
            Database database,
            Table targetTable,
            ExecPlan execPlan,
            DmlStmt dmlStmt,
            OriginStatement originStmt,
            ConnectContext context,
            Coordinator coord) throws StarRocksException, InterruptedException, RpcException {
        try {
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

            MetricRepo.COUNTER_LOAD_ADD.increase(1L);

            // Every time set no send flag and clean all data in buffer
            if (context.getMysqlChannel() != null) {
                context.getMysqlChannel().reset();
            }

            long transactionId = dmlStmt.getTxnId();
            TransactionState txnState = transactionMgr.getTransactionState(database.getId(), transactionId);
            if (txnState == null) {
                throw ErrorReportException.report(ERR_TXN_NOT_EXIST, transactionId);
            }
            if (!txnState.getTableIdList().contains(targetTable.getId())) {
                txnState.getTableIdList().add(targetTable.getId());
                txnState.addTableIndexes((OlapTable) targetTable);
            }

            String label = txnState.getLabel();
            if (execPlan.getScanNodes().stream()
                    .anyMatch(scanNode -> scanNode instanceof OlapScanNode || scanNode instanceof FileScanNode)) {
                coord.setLoadJobType(TLoadJobType.INSERT_QUERY);
            } else {
                coord.setLoadJobType(TLoadJobType.INSERT_VALUES);
            }

            InsertLoadJob loadJob = context.getGlobalStateMgr().getLoadMgr().registerInsertLoadJob(
                    label,
                    database.getFullName(),
                    targetTable.getId(),
                    transactionId,
                    DebugUtil.printId(context.getExecutionId()),
                    context.getQualifiedUser(),
                    EtlJobType.INSERT,
                    System.currentTimeMillis(),
                    estimate(execPlan),
                    context.getSessionVariable().getQueryTimeoutS(),
                    context.getCurrentWarehouseId(),
                    coord);
            loadJob.setJobProperties(dmlStmt.getProperties());
            long jobId = loadJob.getId();
            txnState.addCallbackId(jobId);
            coord.setLoadJobId(jobId);

            QeProcessorImpl.QueryInfo queryInfo =
                    new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord);
            QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), queryInfo);

            coord.exec();
            coord.setExecPlan(execPlan);

            int timeout = context.getSessionVariable().getQueryTimeoutS();
            coord.join(timeout);
            if (!coord.isDone()) {
                /*
                 * In this case, There are two factors that lead query cancelled:
                 * 1: TIMEOUT
                 * 2: BE EXCEPTION
                 * So we should distinguish these two factors.
                 */
                if (!coord.checkBackendState()) {
                    // When enable_collect_query_detail_info is set to true, the plan will be recorded in the query detail,
                    // and hence there is no need to log it here.
                    if (Config.log_plan_cancelled_by_crash_be && context.getQueryDetail() == null) {
                        LOG.warn("Query cancelled by crash of backends [QueryId={}] [SQL={}] [Plan={}]",
                                DebugUtil.printId(context.getExecutionId()),
                                originStmt == null ? "" : originStmt.originStmt,
                                execPlan.getExplainString(TExplainLevel.COSTS));
                    }

                    coord.cancel(ErrorCode.ERR_QUERY_CANCELLED_BY_CRASH.formatErrorMsg());
                    throw new NoAliveBackendException();
                } else {
                    coord.cancel(ErrorCode.ERR_TIMEOUT.formatErrorMsg(getExecType(dmlStmt), timeout, ""));
                    if (coord.isThriftServerHighLoad()) {
                        throw new TimeoutException(getExecType(dmlStmt),
                                timeout,
                                "Please check the thrift-server-pool metrics, " +
                                        "if the pool size reaches thrift_server_max_worker_threads(default is 4096), " +
                                        "you can set the config to a higher value in fe.conf, " +
                                        "or set parallel_fragment_exec_instance_num to a lower value in session variable");
                    } else {
                        throw new TimeoutException(getExecType(dmlStmt), timeout,
                                String.format("please increase the '%s' session variable and retry",
                                        SessionVariable.INSERT_TIMEOUT));
                    }
                }
            }

            if (!coord.getExecStatus().ok()) {
                throw new LoadException(ErrorCode.ERR_FAILED_WHEN_INSERT, coord.getExecStatus().getErrorMsg().isEmpty() ?
                        coord.getExecStatus().getErrorCodeString() : coord.getExecStatus().getErrorMsg());
            }

            LOG.debug("delta files is {}", coord.getDeltaUrls());

            loadJob.updateLoadingStatus(coord.getLoadCounters());

            long loadedRows = coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null ?
                    Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL)) : 0;

            // filteredRows is stored in int64_t in the backend, so use long here.
            long filteredRows = coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null ?
                    Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL)) : 0;

            // insert will fail if 'filtered rows / total rows' exceeds max_filter_ratio
            // for native table and external catalog table(without insert load job)
            if (filteredRows > (filteredRows + loadedRows) * dmlStmt.getMaxFilterRatio()) {
                String trackingSql = "select tracking_log from information_schema.load_tracking_logs where job_id=" + jobId;
                throw new LoadException(ErrorCode.ERR_LOAD_HAS_FILTERED_DATA,
                        "txn_id = " + transactionId + ", tracking sql = " + trackingSql);
            }

            long loadedBytes = coord.getLoadCounters().get(LoadJob.LOADED_BYTES) != null ?
                    Long.parseLong(coord.getLoadCounters().get(LoadJob.LOADED_BYTES)) : 0;

            ExplicitTxnState.ExplicitTxnStateItem item = new ExplicitTxnState.ExplicitTxnStateItem();
            item.setDmlStmt(dmlStmt);
            item.setTabletCommitInfos(TabletCommitInfo.fromThrift(coord.getCommitInfos()));
            item.setTabletFailInfos(TabletFailInfo.fromThrift(coord.getFailInfos()));
            item.addLoadedRows(loadedRows);
            item.addFilteredRows(filteredRows);
            item.addLoadedBytes(loadedBytes);
            return item;
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
        }
    }

    public static LoadMgr.EstimateStats estimate(ExecPlan execPlan) {
        long estimateScanRows = -1;
        int estimateFileNum = 0;
        long estimateScanFileSize = 0;

        boolean needQuery = false;
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            if (scanNode instanceof OlapScanNode) {
                estimateScanRows += ((OlapScanNode) scanNode).getActualRows();
                needQuery = true;
            }
            if (scanNode instanceof FileScanNode) {
                estimateFileNum += ((FileScanNode) scanNode).getFileNum();
                estimateScanFileSize += ((FileScanNode) scanNode).getFileTotalSize();
                needQuery = true;
            }
        }

        if (needQuery) {
            estimateScanRows = execPlan.getFragments().get(0).getPlanRoot().getCardinality();
        }

        return new LoadMgr.EstimateStats(estimateScanRows, estimateFileNum, estimateScanFileSize);
    }

    public static String getExecType(StatementBase stmt) {
        if (stmt instanceof InsertStmt || stmt instanceof CreateTableAsSelectStmt) {
            return "Insert";
        } else if (stmt instanceof UpdateStmt) {
            return "Update";
        } else if (stmt instanceof DeleteStmt) {
            return "Delete";
        } else {
            return "Query";
        }
    }
}
