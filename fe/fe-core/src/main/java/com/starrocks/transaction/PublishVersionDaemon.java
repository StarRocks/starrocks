// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/PublishVersionDaemon.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.Utils;
import com.starrocks.rpc.RpcException;
import com.starrocks.scheduler.Constants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class PublishVersionDaemon extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);

    private static final long RETRY_INTERVAL_MS = 1000;

    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
            List<TransactionState> readyTransactionStates =
                    globalTransactionMgr.getReadyToPublishTransactions(Config.enable_new_publish_mechanism);
            if (readyTransactionStates == null || readyTransactionStates.isEmpty()) {
                return;
            }
            List<Long> allBackends = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false);
            if (allBackends.isEmpty()) {
                LOG.warn("some transaction state need to publish, but no backend exists");
                return;
            }

            if (!Config.use_staros) {
                publishVersionForOlapTable(readyTransactionStates);
                return;
            }

            List<TransactionState> olapTransactions = new ArrayList<>();
            List<TransactionState> lakeTransactions = new ArrayList<>();
            for (TransactionState txnState : readyTransactionStates) {
                if (isLakeTableTransaction(txnState)) {
                    lakeTransactions.add(txnState);
                } else {
                    olapTransactions.add(txnState);
                }
            }

            if (!olapTransactions.isEmpty()) {
                publishVersionForOlapTable(olapTransactions);
            }
            if (!lakeTransactions.isEmpty()) {
                publishVersionForLakeTable(lakeTransactions);
            }
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends", t);
        }
    }

    private void publishVersionForOlapTable(List<TransactionState> readyTransactionStates) throws UserException {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();

        // every backend-transaction identified a single task
        AgentBatchTask batchTask = new AgentBatchTask();
        // traverse all ready transactions and dispatch the version publish task to all backends
        for (TransactionState transactionState : readyTransactionStates) {
            List<PublishVersionTask> tasks = transactionState.createPublishVersionTask();
            for (PublishVersionTask task : tasks) {
                AgentTaskQueue.addTask(task);
                batchTask.addTask(task);
            }
            if (!tasks.isEmpty()) {
                transactionState.setHasSendTask(true);
                LOG.info("send publish tasks for txn_id: {}", transactionState.getTransactionId());
            }
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }

        if (Config.enable_new_publish_mechanism) {
            publishVersionNew(globalTransactionMgr, readyTransactionStates);
            return;
        }

        // try to finish the transaction, if failed just retry in next loop
        for (TransactionState transactionState : readyTransactionStates) {
            Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks();
            Set<Long> publishErrorReplicaIds = Sets.newHashSet();
            Set<Long> unfinishedBackends = Sets.newHashSet();
            boolean allTaskFinished = true;
            for (PublishVersionTask publishVersionTask : transTasks.values()) {
                if (publishVersionTask.isFinished()) {
                    // sometimes backend finish publish version task, but it maybe failed to change
                    // transaction id to version for some tablets,
                    // and it will upload the failed tablet info to fe and fe will deal with them
                    Set<Long> errReplicas = publishVersionTask.getErrorReplicas();
                    if (!errReplicas.isEmpty()) {
                        publishErrorReplicaIds.addAll(errReplicas);
                    }
                } else {
                    allTaskFinished = false;
                    // Publish version task may succeed and finish in quorum replicas
                    // but not finish in one replica.
                    // here collect the backendId that do not finish publish version
                    unfinishedBackends.add(publishVersionTask.getBackendId());
                }
            }
            boolean shouldFinishTxn = true;
            if (!allTaskFinished) {
                shouldFinishTxn = globalTransactionMgr.canTxnFinished(transactionState,
                        publishErrorReplicaIds, unfinishedBackends);
            }

            if (shouldFinishTxn) {
                globalTransactionMgr.finishTransaction(transactionState.getDbId(), transactionState.getTransactionId(),
                        publishErrorReplicaIds);
                if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                    transactionState.updateSendTaskTime();
                    LOG.debug("publish version for transaction {} failed, has {} error replicas during publish",
                            transactionState, publishErrorReplicaIds.size());
                } else {
                    for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                    }
                    // clear publish version tasks to reduce memory usage when state changed to visible.
                    transactionState.clearAfterPublished();

                    // Refresh materialized view when base table update transaction has been visible if necessary
                    refreshMvIfNecessary(transactionState);
                }
            }
        } // end for readyTransactionStates
    }

    private void publishVersionNew(GlobalTransactionMgr globalTransactionMgr, List<TransactionState> txns) {
        for (TransactionState transactionState : txns) {
            Set<Long> publishErrorReplicas = Sets.newHashSet();
            if (!transactionState.allPublishTasksFinishedOrQuorumWaitTimeout(publishErrorReplicas)) {
                continue;
            }
            try {
                if (transactionState.checkCanFinish()) {
                    globalTransactionMgr.finishTransactionNew(transactionState, publishErrorReplicas);
                }
                if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                    transactionState.updateSendTaskTime();
                    LOG.debug("publish version for transaction {} failed, has {} error replicas during publish",
                            transactionState, transactionState.getErrorReplicas().size());
                } else {
                    for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                    }
                    // clear publish version tasks to reduce memory usage when state changed to visible.
                    transactionState.clearAfterPublished();
                    // Refresh materialized view when base table update transaction has been visible if necessary
                    refreshMvIfNecessary(transactionState);
                }
            } catch (UserException e) {
                LOG.error("errors while publish version to all backends", e);
            }
        }
    }

    // TODO: support mix OlapTable with LakeTable
    boolean isLakeTableTransaction(TransactionState transactionState) {
        if (transactionState.getTableIdList().isEmpty()) {
            return false;
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(transactionState.getDbId());
        if (db == null) {
            return false;
        }
        db.readLock();
        try {
            for (long tableId : transactionState.getTableIdList()) {
                Table table = db.getTable(tableId);
                if (table != null) {
                    return table.isLakeTable();
                }
            }
        } finally {
            db.readUnlock();
        }
        return false;
    }

    // todo: refine performance
    void publishVersionForLakeTable(List<TransactionState> readyTransactionStates) throws UserException {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();

        for (TransactionState txnState : readyTransactionStates) {
            long txnId = txnState.getTransactionId();
            Database db = GlobalStateMgr.getCurrentState().getDb(txnState.getDbId());
            if (db == null) {
                LOG.info("the database of transaction {} has been deleted", txnId);
                globalTransactionMgr.finishTransaction(txnState.getDbId(), txnId, Sets.newHashSet());
                continue;
            }
            boolean finished = true;
            for (TableCommitInfo tableCommitInfo : txnState.getIdToTableCommitInfos().values()) {
                if (!publishTable(db, txnState, tableCommitInfo)) {
                    finished = false;
                }
            }
            if (finished) {
                globalTransactionMgr.finishTransaction(db.getId(), txnId, null);
            }
        }
    }

    private boolean publishTable(Database db, TransactionState txnState, TableCommitInfo tableCommitInfo) {
        boolean finished = true;
        long txnId = txnState.getTransactionId();
        for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {

            long currentTime = System.currentTimeMillis();
            long versionTime = partitionCommitInfo.getVersionTime();
            if (versionTime > 0) {
                continue;
            }
            if (versionTime < 0 && currentTime < Math.abs(versionTime) + RETRY_INTERVAL_MS) {
                finished = false;
                continue;
            }

            boolean ok = false;
            try {
                ok = publishPartition(db, tableCommitInfo, partitionCommitInfo, txnState);
            } catch (Throwable e) {
                LOG.error("Fail to publish partition {} of txn {}: {}", partitionCommitInfo.getPartitionId(),
                        txnId, e.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
            }
            if (ok) {
                partitionCommitInfo.setVersionTime(System.currentTimeMillis());
            } else {
                partitionCommitInfo.setVersionTime(-System.currentTimeMillis());
                finished = false;
            }
        }
        return finished;
    }

    private boolean publishPartition(@NotNull Database db, @NotNull TableCommitInfo tableCommitInfo,
                                     @NotNull PartitionCommitInfo partitionCommitInfo,
                                     @NotNull TransactionState txnState)
            throws RpcException, NoAliveBackendException {
        long tableId = tableCommitInfo.getTableId();
        long txnVersion = partitionCommitInfo.getVersion();
        long txnId = txnState.getTransactionId();
        String txnLabel = txnState.getLabel();
        List<Tablet> normalTablets = null;
        List<Tablet> shadowTablets = null;

        db.readLock();
        try {
            LakeTable table = (LakeTable) db.getTable(tableId);
            if (table == null) {
                txnState.removeTable(tableCommitInfo.getTableId());
                LOG.info("Removed non-exist table {} from transaction {}. txn_id={}", tableId, txnLabel, txnId);
                return true;
            }
            long partitionId = partitionCommitInfo.getPartitionId();
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                LOG.info("Ignore non-exist partition {} of table {} in txn {}", partitionId, table.getName(), txnLabel);
                return true;
            }
            if (partition.getVisibleVersion() + 1 != txnVersion) {
                LOG.info("Previous transaction has not finished. txn_id={} partition_version={}, txn_version={}",
                        txnId, partition.getVisibleVersion(), txnVersion);
                return false;
            }
            List<MaterializedIndex> indexes = txnState.getPartitionLoadedTblIndexes(table.getId(), partition);
            for (MaterializedIndex index : indexes) {
                if (!index.visibleForTransaction(txnId)) {
                    LOG.info("Ignored index {} for transaction {}", table.getIndexNameById(index.getId()), txnId);
                    continue;
                }
                if (index.getState() == MaterializedIndex.IndexState.SHADOW) {
                    shadowTablets = (shadowTablets == null) ? Lists.newArrayList() : shadowTablets;
                    shadowTablets.addAll(index.getTablets());
                } else {
                    normalTablets = (normalTablets == null) ? Lists.newArrayList() : normalTablets;
                    normalTablets.addAll(index.getTablets());
                }
            }
        } finally {
            db.readUnlock();
        }

        return publishNormalTablets(normalTablets, txnId, txnVersion) && publishShadowTablets(shadowTablets, txnId, txnVersion);
    }

    private boolean publishNormalTablets(@Nullable List<Tablet> tablets, long txnId, long version)
            throws RpcException, NoAliveBackendException {
        if (tablets == null || tablets.isEmpty()) {
            return true;
        }
        Utils.publishVersion(tablets, txnId, version - 1, version);
        return true;
    }

    private boolean publishShadowTablets(@Nullable List<Tablet> tablets, long txnId, long version)
            throws RpcException, NoAliveBackendException {
        if (tablets == null || tablets.isEmpty()) {
            return true;
        }
        Utils.publishLogVersion(tablets, txnId, version);
        return true;
    }

    /**
     * Refresh the materialized view if it should be triggered after base table was loaded.
     *
     * @param transactionState
     * @throws DdlException
     * @throws MetaNotFoundException
     */
    private void refreshMvIfNecessary(TransactionState transactionState)
            throws DdlException, MetaNotFoundException {
        // Refresh materialized view when base table update transaction has been visible
        long dbId = transactionState.getDbId();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        for (long tableId : transactionState.getTableIdList()) {
            Table table;
            db.readLock();
            try {
                table = db.getTable(tableId);
            } finally {
                db.readUnlock();
            }
            if (table == null) {
                LOG.warn("failed to get transaction tableId {} when pending refresh.", tableId);
                return;
            }
            Set<MvId> relatedMvs = table.getRelatedMaterializedViews();
            Iterator<MvId> mvIdIterator = relatedMvs.iterator();
            while (mvIdIterator.hasNext()) {
                MvId mvId = mvIdIterator.next();
                Database mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
                mvDb.readLock();
                try {
                    MaterializedView materializedView = (MaterializedView) mvDb.getTable(mvId.getId());
                    if (materializedView == null) {
                        LOG.warn("materialized view {} does not exists.", mvId.getId());
                        mvIdIterator.remove();
                        continue;
                    }
                    if (materializedView.shouldTriggeredRefreshBy(db.getFullName(), table.getName())) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().refreshMaterializedView(
                                mvDb.getFullName(), mvDb.getTable(mvId.getId()).getName(), false, null,
                                Constants.TaskRunPriority.NORMAL.value(), true, false);
                    }
                } finally {
                    mvDb.readUnlock();
                }
            }
        }
    }
}
