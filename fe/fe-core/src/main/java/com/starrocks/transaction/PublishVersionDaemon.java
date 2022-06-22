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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.lake.LakeTable;
import com.starrocks.catalog.lake.LakeTablet;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.lake.proto.PublishVersionRequest;
import com.starrocks.lake.proto.PublishVersionResponse;
import com.starrocks.rpc.BackendServiceProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PublishVersionDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);

    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
            List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions();
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
        // traverse all ready transactions and dispatch the publish version task to all backends
        for (TransactionState transactionState : readyTransactionStates) {
            List<PublishVersionTask> tasks = createPublishVersionTaskForOlapTable(transactionState);
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

        // try to finish the transaction, if failed just retry in next loop
        for (TransactionState transactionState : readyTransactionStates) {
            Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks();
            Set<Long> publishErrorReplicaIds = Sets.newHashSet();
            Set<Long> unfinishedBackends = Sets.newHashSet();
            boolean allTaskFinished = true;
            for (PublishVersionTask publishVersionTask : transTasks.values()) {
                if (publishVersionTask.isFinished()) {
                    // sometimes backend finish publish version task, but it maybe failed to change transactionid to version for some tablets
                    // and it will upload the failed tabletinfo to fe and fe will deal with them
                    Set<Long> errReplicas = publishVersionTask.collectErrorReplicas();
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
                    LOG.debug("publish version for transation {} failed, has {} error replicas during publish",
                            transactionState, publishErrorReplicaIds.size());
                } else {
                    for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
                        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
                    }
                    // clear publish version tasks to reduce memory usage when state changed to visible.
                    transactionState.clearPublishVersionTasks();
                }
            }
        } // end for readyTransactionStates
    }

    boolean isLakeTableTransaction(TransactionState transactionState) {
        Database db = GlobalStateMgr.getCurrentState().getDb(transactionState.getDbId());
        if (db == null) {
            return false;
        }
        // TODO: support mix OlapTable with LakeTable
        Table table = db.getTable(transactionState.getTableIdList().get(0));
        return table.isLakeTable();
    }

    public List<PublishVersionTask> createPublishVersionTaskForOlapTable(TransactionState transactionState) {
        List<PublishVersionTask> tasks = new ArrayList<>();
        if (transactionState.hasSendTask()) {
            return tasks;
        }

        Set<Long> publishBackends = transactionState.getPublishVersionTasks().keySet();
        // public version tasks are not persisted in globalStateMgr, so publishBackends may be empty.
        // We have to send publish version task to all backends
        if (publishBackends.isEmpty()) {
            // note: tasks are sended to all backends including dead ones, or else
            // transaction manager will treat it as success
            List<Long> allBackends = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false);
            if (!allBackends.isEmpty()) {
                publishBackends = Sets.newHashSet();
                publishBackends.addAll(allBackends);
            } else {
                // all backends may be dropped, no need to create task
                LOG.warn("transaction {} want to publish, but no backend exists", transactionState.getTransactionId());
                return tasks;
            }
        }

        List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
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
                    transactionState.getTransactionId(),
                    transactionState.getDbId(),
                    transactionState.getCommitTime(),
                    partitionVersions,
                    transactionState.getTraceParent(),
                    createTime);
            transactionState.addPublishVersionTask(backendId, task);
            tasks.add(task);
        }
        return tasks;
    }

    // todo: refine performance
    void publishVersionForLakeTable(List<TransactionState> readyTransactionStates) throws UserException {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();

        for (TransactionState txnState : readyTransactionStates) {
            long txnId = txnState.getTransactionId();
            Database db = GlobalStateMgr.getCurrentState().getDb(txnState.getDbId());
            if (db == null) {
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

    boolean publishTable(Database db, TransactionState txnState, TableCommitInfo tableCommitInfo) throws UserException {
        long txnId = txnState.getTransactionId();
        long tableId = tableCommitInfo.getTableId();
        LakeTable table = (LakeTable) db.getTable(tableId);
        if (table == null) {
            txnState.removeTable(tableCommitInfo.getTableId());
            LOG.info("Removed table {} from transaction {}", tableId, txnId);
            return true;
        }
        boolean finished = true;
        Preconditions.checkState(table.isLakeTable());
        for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPartitionId();
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                tableCommitInfo.removePartition(partitionId);
                LOG.info("Removed partition {} from transaction {}", partitionId, txnId);
                continue;
            }
            if (!publishPartition(txnState, table, partition, partitionCommitInfo)) {
                finished = false;
            }
        }
        return finished;
    }

    boolean publishPartition(TransactionState txnState, LakeTable table, Partition partition,
                             PartitionCommitInfo partitionCommitInfo) throws UserException {
        if (partition.getVisibleVersion() + 1 != partitionCommitInfo.getVersion()) {
            return false;
        }
        boolean finished = true;
        long txnId = txnState.getTransactionId();
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        List<MaterializedIndex> indexes = txnState.getPartitionLoadedTblIndexes(table.getId(), partition);
        for (MaterializedIndex index : indexes) {
            for (Tablet tablet : index.getTablets()) {
                long beId = ((LakeTablet) tablet).getPrimaryBackendId();
                beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
            }
        }
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            Backend backend = systemInfoService.getBackend(entry.getKey());
            // todo: handle backend == null
            TNetworkAddress address = new TNetworkAddress();
            address.setHostname(backend.getHost());
            address.setPort(backend.getBrpcPort());
            LakeService lakeService = BackendServiceProxy.getInstance().getLakeService(address);

            // todo: send tasks in parallel
            PublishVersionRequest request = new PublishVersionRequest();
            request.baseVersion = partitionCommitInfo.getVersion() - 1;
            request.newVersion = partitionCommitInfo.getVersion();
            request.tabletIds = entry.getValue();
            request.txnIds.add(txnId);

            Future<PublishVersionResponse> responseFuture = lakeService.publishVersion(request);
            try {
                PublishVersionResponse response = responseFuture.get();
                if (!response.failedTablets.isEmpty()) {
                    finished = false;
                }
            } catch (ExecutionException | InterruptedException e) {
                LOG.error(e);
                finished = false;
            }
        }
        return finished;
    }
}
