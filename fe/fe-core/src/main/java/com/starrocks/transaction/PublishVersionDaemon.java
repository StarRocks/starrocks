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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.PartitionPublishVersionData;
import com.starrocks.lake.TxnInfoHelper;
import com.starrocks.lake.Utils;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.proto.DeleteTxnLogRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TTaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class PublishVersionDaemon extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);

    private static final long RETRY_INTERVAL_MS = 1000;
    private static final int LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE = 512;
    public static final int LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE = 4096;
    // about 16 (2 * LAKE_PUBLISH_MAX_QUEUE_SIZE/LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE ) tasks pending for
    // each thread under the default configurations
    private static final int LAKE_PUBLISH_MAX_QUEUE_SIZE = 4096;

    private ThreadPoolExecutor lakeTaskExecutor;
    private ThreadPoolExecutor deleteTxnLogExecutor;
    private Set<Long> publishingLakeTransactions;

    @VisibleForTesting
    protected Set<Long> publishingLakeTransactionsBatchTableId;

    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            if (Config.lake_enable_batch_publish_version && RunMode.isSharedDataMode()) {
                // batch publish
                List<TransactionStateBatch> readyTransactionStatesBatch = globalTransactionMgr.
                        getReadyPublishTransactionsBatch();
                if (readyTransactionStatesBatch.size() != 0) {
                    publishVersionForLakeTableBatch(readyTransactionStatesBatch);
                }
                return;

            }

            List<TransactionState> readyTransactionStates =
                    globalTransactionMgr.getReadyToPublishTransactions(Config.enable_new_publish_mechanism);
            if (readyTransactionStates == null || readyTransactionStates.isEmpty()) {
                return;
            }

            // TODO: need to refactor after be split into cn + dn
            List<Long> allBackends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(false);
            if (RunMode.isSharedDataMode()) {
                allBackends.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodeIds(false));
            }

            if (allBackends.isEmpty()) {
                LOG.warn("some transaction state need to publish, but no backend exists");
                return;
            }

            if (RunMode.isSharedNothingMode()) { // share_nothing mode
                publishVersionForOlapTable(readyTransactionStates);
            } else { // share_data mode
                publishVersionForLakeTable(readyTransactionStates);
            }
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends", t);
        }
    }

    private int getOrFixLakeTaskExecutorThreadPoolMaxSizeConfig() {
        String configVarName = "lake_publish_version_max_threads";
        int maxSize = Config.lake_publish_version_max_threads;
        if (maxSize <= 0) {
            LOG.warn("Invalid configuration value '{}' for {}, force set to default value:{}",
                    maxSize, configVarName, LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE);
            maxSize = LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE;
            Config.lake_publish_version_max_threads = maxSize;
        } else if (maxSize > LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE) {
            LOG.warn(
                    "Configuration value for item {} exceeds the preset hard limit. Config value:{}," +
                            " preset hard limit:{}. Force set to default value:{}.",
                    configVarName, maxSize, LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE,
                    LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE);
            maxSize = LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE;
            Config.lake_publish_version_max_threads = maxSize;
        }
        return maxSize;
    }

    private void adjustLakeTaskExecutor() {
        if (lakeTaskExecutor == null) {
            return;
        }

        // only do update with valid setting
        int newNumThreads = Config.lake_publish_version_max_threads;
        if (newNumThreads > LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE || newNumThreads <= 0) {
            // DON'T LOG, otherwise the log line will repeat everytime the listener refreshes
            return;
        }
        ThreadPoolManager.setFixedThreadPoolSize(lakeTaskExecutor, newNumThreads);
    }

    /**
     * Create a thread pool executor for LakeTable synchronizing publish.
     * The thread pool size can be configured by `Config.lake_publish_version_max_threads` and is affected by the
     * following constant variables
     * - LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE
     * - LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
     * - LAKE_PUBLISH_MAX_QUEUE_SIZE
     * <p>
     * The valid range for the configuration item `Config.lake_publish_version_max_threads` is
     * (0, LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE], if the initial value is out of range,
     * the LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE will be used. During the runtime update, if the new value provided
     * is out of range, the value will be just ignored silently.
     * <p>
     * The thread pool is created with the corePoolSize and maxPoolSize equals to
     * `Config.lake_publish_version_max_threads`, or set to LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE in case exceeded.
     * core threads are also allowed to timeout when idle.
     * <p>
     * Threads in the thread pool will be created in the following way:
     * 1) a new thread will be created for a new added task when the total number of core threads is less than `corePoolSize`,
     * 2) new tasks will be entered the queue once the number of running core threads reaches `corePoolSize` and the
     * queue is not full yet,
     * 3) the new task will be rejected once the total number of threads reaches `corePoolSize` and the queue is also full.
     * <p>
     * core threads will be idle and timed out if no more tasks for a while (60 seconds by default).
     *
     * @return the thread pool executor
     */
    private @NotNull ThreadPoolExecutor getLakeTaskExecutor() {
        if (lakeTaskExecutor == null) {
            int numThreads = getOrFixLakeTaskExecutorThreadPoolMaxSizeConfig();
            lakeTaskExecutor =
                    ThreadPoolManager.newDaemonFixedThreadPool(numThreads, LAKE_PUBLISH_MAX_QUEUE_SIZE,
                            "lake-publish-task",
                            true);
            // allow core thread timeout as well
            lakeTaskExecutor.allowCoreThreadTimeOut(true);

            // register ThreadPool config change listener
            GlobalStateMgr.getCurrentState().getConfigRefreshDaemon()
                    .registerListener(() -> this.adjustLakeTaskExecutor());
        }
        return lakeTaskExecutor;
    }

    private @NotNull ThreadPoolExecutor getDeleteTxnLogExecutor() {
        if (deleteTxnLogExecutor == null) {
            // Create a new thread for every task if there is no idle threads available.
            // Idle threads will be cleaned after `KEEP_ALIVE_TIME` seconds, which is 60 seconds by default.
            int numThreads = Math.max(Config.lake_publish_delete_txnlog_max_threads, 1);
            deleteTxnLogExecutor = ThreadPoolManager.newDaemonCacheThreadPool(numThreads,
                    "lake-publish-delete-txnLog", true);

            // register ThreadPool config change listener
            GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(() -> {
                int newMaxThreads = Config.lake_publish_delete_txnlog_max_threads;
                if (deleteTxnLogExecutor != null && newMaxThreads > 0
                        && deleteTxnLogExecutor.getMaximumPoolSize() != newMaxThreads) {
                    deleteTxnLogExecutor.setMaximumPoolSize(Config.lake_publish_delete_txnlog_max_threads);
                }
            });
        }
        return deleteTxnLogExecutor;
    }

    private @NotNull Set<Long> getPublishingLakeTransactions() {
        if (publishingLakeTransactions == null) {
            publishingLakeTransactions = Sets.newConcurrentHashSet();
        }
        return publishingLakeTransactions;
    }

    // Only one table in all transactionStates in transactionStateBatch
    // so we can judge whether the transactionStateBatch is publishing by tableId
    // we can not judge whether one transactionBatch is publishing by the transactionStateBatch itself,
    // for maybe there are only 3 transactions in transactionGraph at first,sush as
    // 1->2->3, so the transactionStateBatch contains the three transactions.
    // But if another transaction is submitted at this time, then the next batch will contain the the new transaction,
    // such as 1->2->3->4-5,
    // the transactons will be published repeatedlly.
    private @NotNull Set<Long> getPublishingLakeTransactionsBatchTableId() {
        if (publishingLakeTransactionsBatchTableId == null) {
            publishingLakeTransactionsBatchTableId = Sets.newConcurrentHashSet();
        }
        return publishingLakeTransactionsBatchTableId;
    }

    private void publishVersionForOlapTable(List<TransactionState> readyTransactionStates) throws UserException {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        // every backend-transaction identified a single task
        AgentBatchTask batchTask = new AgentBatchTask();
        List<Long> transactionIds = new ArrayList<>();
        // traverse all ready transactions and dispatch the version publish task to all backends
        for (TransactionState transactionState : readyTransactionStates) {
            List<PublishVersionTask> tasks = transactionState.createPublishVersionTask();
            for (PublishVersionTask task : tasks) {
                AgentTaskQueue.addTask(task);
                batchTask.addTask(task);
            }
            if (!tasks.isEmpty()) {
                transactionState.setHasSendTask(true);
                transactionIds.add(transactionState.getTransactionId());
            }
        }
        LOG.debug("send publish tasks for transactions: {}", transactionIds);
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }

        // FIXME(murphy) refresh the mv in new publish mechanism
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
                }
            } catch (UserException e) {
                LOG.error("errors while publish version to all backends", e);
            }
        }
    }

    void publishVersionForLakeTable(List<TransactionState> readyTransactionStates) {
        Set<Long> publishingTransactions = getPublishingLakeTransactions();
        for (TransactionState txnState : readyTransactionStates) {
            long txnId = txnState.getTransactionId();
            if (!publishingTransactions.contains(txnId)) { // the set did not already contain the specified element
                Set<Long> publishingLakeTransactionsBatchTableId = getPublishingLakeTransactionsBatchTableId();
                // When the `enable_lake_batch_publish_version` switch is just set to false,
                // it is possible that the result of publish task has not been returned,
                // we need to wait for the result to return if the same table is involved.
                if (!txnState.getTableIdList().stream().allMatch(id -> !publishingLakeTransactionsBatchTableId.contains(id))) {
                    LOG.info("maybe enable_lake_batch_publish_version is set to false just now, txn {} will be published later",
                            txnState.getTransactionId());
                    continue;
                }
                publishingTransactions.add(txnId);
                CompletableFuture<Void> future = publishLakeTransactionAsync(txnState);
                future.thenRun(() -> publishingTransactions.remove(txnId));
            }
        }
    }

    void publishVersionForLakeTableBatch(List<TransactionStateBatch> readyTransactionStatesBatch) {
        Set<Long> publishingLakeTransactionsBatchTableId = getPublishingLakeTransactionsBatchTableId();
        for (TransactionStateBatch txnStateBatch : readyTransactionStatesBatch) {
            if (txnStateBatch.size() == 1) {
                // there are two situations:
                // 1. the transactionState in txnStateBatch is with multi-tables
                // 2. only one transactionState with the table committed in the interval of publish.
                TransactionState state = txnStateBatch.transactionStates.get(0);
                List<Long> tableIdList = state.getTableIdList();
                if (publishingLakeTransactionsBatchTableId.addAll(tableIdList)) {
                    CompletableFuture<Void> future = publishLakeTransactionAsync(state);
                    future.thenRun(() -> publishingLakeTransactionsBatchTableId.removeAll(tableIdList));
                }
            } else {
                long tableId = txnStateBatch.getTableId();
                if (publishingLakeTransactionsBatchTableId.add(tableId)) {
                    CompletableFuture<Void> future = publishLakeTransactionBatchAsync(txnStateBatch);
                    future.thenRun(() -> publishingLakeTransactionsBatchTableId.remove(tableId));
                }
            }
        }
    }

    private CompletableFuture<Void> publishLakeTransactionAsync(TransactionState txnState) {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long txnId = txnState.getTransactionId();
        long dbId = txnState.getDbId();
        LOG.info("start publish lake db:{} table:{} txn:{}", dbId, StringUtils.join(txnState.getTableIdList(), ","), txnId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            LOG.info("the database of transaction {} has been deleted", txnId);
            try {
                globalTransactionMgr.finishTransaction(txnState.getDbId(), txnId, Sets.newHashSet());
            } catch (UserException ex) {
                LOG.warn("Fail to finish txn " + txnId, ex);
            }
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Boolean> publishFuture;
        Collection<TableCommitInfo> tableCommitInfos = txnState.getIdToTableCommitInfos().values();
        if (tableCommitInfos.size() == 1) {
            TableCommitInfo tableCommitInfo = tableCommitInfos.iterator().next();
            publishFuture = publishLakeTableAsync(db, txnState, tableCommitInfo);
        } else {
            List<CompletableFuture<Boolean>> futureList = new ArrayList<>(tableCommitInfos.size());
            for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
                CompletableFuture<Boolean> future = publishLakeTableAsync(db, txnState, tableCommitInfo);
                futureList.add(future);
            }
            publishFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).thenApply(
                    v -> futureList.stream().allMatch(CompletableFuture::join));
        }

        return publishFuture.thenAccept(success -> {
            if (success) {
                try {
                    globalTransactionMgr.finishTransaction(dbId, txnId, null);
                } catch (UserException e) {
                    throw new RuntimeException(e);
                }
            }
        }).exceptionally(ex -> {
            LOG.error("Fail to finish transaction " + txnId, ex);
            return null;
        });
    }

    public boolean publishPartitionBatch(Database db, long tableId, PartitionPublishVersionData publishVersionData,
                                         TransactionStateBatch stateBatch) {
        final Long partitionId = publishVersionData.getPartitionId();
        final List<TransactionState> transactionStates = publishVersionData.getTransactionStates();
        final List<Long> versions = publishVersionData.getCommitVersions();
        final List<TxnInfoPB> txnInfos = publishVersionData.getTxnInfos();

        Map<Long, Set<Tablet>> shadowTabletsMap = new HashMap<>();
        Set<Tablet> normalTablets = null;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        // version -> shadowTablets
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (table == null) {
                // table has been dropped
                return true;
            }

            PhysicalPartition partition = table.getPhysicalPartition(publishVersionData.getPartitionId());
            if (partition == null) {
                LOG.info("partition is null in publish partition batch");
                return true;
            }
            if (partition.getVisibleVersion() + 1 != versions.get(0)) {
                LOG.error("publish partition batch partition.getVisibleVersion() + 1 != version.get(0)" + " "
                        + partition.getId() + " " + partition.getVisibleVersion() + " " + versions.get(0));
                return false;
            }

            for (int i = 0; i < transactionStates.size(); i++) {
                TransactionState txnState = transactionStates.get(i);
                warehouseId = txnState.getWarehouseId();
                List<MaterializedIndex> indexes = txnState.getPartitionLoadedTblIndexes(table.getId(), partition);
                for (MaterializedIndex index : indexes) {
                    if (!index.visibleForTransaction(txnState.getTransactionId())) {
                        LOG.info("Ignored index {} for transaction {}", table.getIndexNameById(index.getId()),
                                txnState.getTransactionId());
                        continue;
                    }
                    if (index.getState() == MaterializedIndex.IndexState.SHADOW) {
                        if (shadowTabletsMap.containsKey(versions.get(i))) {
                            shadowTabletsMap.get(versions.get(i)).addAll(index.getTablets());
                        } else {
                            Set<Tablet> tabletsNew = new HashSet<>(index.getTablets());
                            shadowTabletsMap.put(versions.get(i), tabletsNew);
                        }
                    } else {
                        normalTablets = (normalTablets == null) ? Sets.newHashSet() : normalTablets;
                        normalTablets.addAll(index.getTablets());
                    }
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        }

        long startVersion = versions.get(0);
        long endVersion = versions.get(versions.size() - 1);

        try {
            for (Map.Entry<Long, Set<Tablet>> item : shadowTabletsMap.entrySet()) {
                int index = versions.indexOf(item.getKey());
                List<Tablet> publishShadowTablets = new ArrayList<>(item.getValue());
                Utils.publishLogVersionBatch(publishShadowTablets,
                        txnInfos.subList(index, txnInfos.size()),
                        versions.subList(index, versions.size()),
                        warehouseId);
            }
            if (CollectionUtils.isNotEmpty(normalTablets)) {
                Map<Long, Double> compactionScores = new HashMap<>();
                List<Tablet> publishTablets = new ArrayList<>();
                publishTablets.addAll(normalTablets);

                // used to delete txnLog when publish success
                Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
                Utils.publishVersionBatch(publishTablets, txnInfos,
                        startVersion - 1, endVersion, compactionScores, nodeToTablets,
                        warehouseId);

                Quantiles quantiles = Quantiles.compute(compactionScores.values());
                stateBatch.setCompactionScore(tableId, partitionId, quantiles);
                stateBatch.putBeTablets(partitionId, nodeToTablets);
            }
        } catch (Exception e) {
            LOG.error("Fail to publish partition {} of txnIds {}:", partitionId,
                    txnInfos.stream().map(i -> i.txnId).collect(Collectors.toList()), e);
            return false;
        }

        return true;
    }

    private void deleteTxnLogIgnoreError(ComputeNode node, DeleteTxnLogRequest request) {
        try {
            LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
            // just ignore the response, for we don't care the result of delete txn log
            // and vacuum will clan the txn log finally if it failed.
            lakeService.deleteTxnLog(request);
        } catch (Exception e) {
            LOG.warn("delete txn log error: " + e.getMessage());
        }
    }

    private Runnable getDeleteTxnLogTask(TransactionStateBatch batch, List<Long> txnIds) {
        return () -> {
            // For each partition, send a delete request to all the tablets in the partition
            for (Map.Entry<Long, Map<ComputeNode, Set<Long>>> entry : batch.getPartitionToTablets().entrySet()) {
                Map<ComputeNode, Set<Long>> nodeToTablets = entry.getValue();
                for (Map.Entry<ComputeNode, Set<Long>> entryItem : nodeToTablets.entrySet()) {
                    ComputeNode node = entryItem.getKey();
                    DeleteTxnLogRequest request = new DeleteTxnLogRequest();
                    request.tabletIds = new ArrayList<>(entryItem.getValue());
                    request.txnIds = txnIds;
                    deleteTxnLogIgnoreError(node, request);
                }
            }
        };
    }

    private Runnable getDeleteCombinedTxnLogTask(TransactionStateBatch batch, List<Long> txnIds) {
        return () -> {
            List<TxnInfoPB> txnInfoPBList = new ArrayList<>();
            for (Long txnId : txnIds) {
                TxnInfoPB txnInfoPB = new TxnInfoPB();
                txnInfoPB.txnId = txnId;
                txnInfoPB.combinedTxnLog = true;
                txnInfoPBList.add(txnInfoPB);
            }
            // For each partition, send a delete request to any tablet in the partition
            for (Map.Entry<Long, Map<ComputeNode, Set<Long>>> entry : batch.getPartitionToTablets().entrySet()) {
                Map.Entry<ComputeNode, Set<Long>> entryItem = entry.getValue().entrySet().iterator().next();
                ComputeNode node = entryItem.getKey();
                DeleteTxnLogRequest request = new DeleteTxnLogRequest();
                request.tabletIds = new ArrayList<>(entryItem.getValue());
                request.txnInfos = txnInfoPBList;
                deleteTxnLogIgnoreError(node, request);
            }
        };
    }

    private void submitDeleteTxnLogJob(TransactionStateBatch txnStateBatch) {
        try {
            List<Long> txnIdsWithNormalTxnLog = new ArrayList<>();
            List<Long> txnIdsWithCombinedTxnLog = new ArrayList<>();
            for (TransactionState state : txnStateBatch.getTransactionStates()) {
                if (state.isUseCombinedTxnLog()) {
                    txnIdsWithCombinedTxnLog.add(state.getTransactionId());
                } else {
                    txnIdsWithNormalTxnLog.add(state.getTransactionId());
                }
            }
            if (!txnIdsWithNormalTxnLog.isEmpty()) {
                getDeleteTxnLogExecutor().submit(getDeleteTxnLogTask(txnStateBatch, txnIdsWithNormalTxnLog));
            }
            if (!txnIdsWithCombinedTxnLog.isEmpty()) {
                getDeleteTxnLogExecutor().submit(getDeleteCombinedTxnLogTask(txnStateBatch, txnIdsWithCombinedTxnLog));
            }
        } catch (Exception e) {
            LOG.warn("delete txn log error: " + e.getMessage());
        }
    }

    private CompletableFuture<Void> publishLakeTransactionBatchAsync(TransactionStateBatch txnStateBatch) {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        assert txnStateBatch.size() > 1;
        // pick up all tableCommitInfo
        // only one table,if batch has multi transactionState for now,
        // the batch only has one transactionState for multi table.
        long dbId = txnStateBatch.getDbId();
        long tableId = txnStateBatch.getTableId();
        List<TransactionState> states = txnStateBatch.getTransactionStates();
        Map<Long, PartitionPublishVersionData> publishVersionDataMap = new HashMap<>();

        for (TransactionState state : states) {
            TableCommitInfo tableCommitInfo = Objects.requireNonNull(state.getTableCommitInfo(tableId));
            Map<Long, PartitionCommitInfo> partitionCommitInfoMap = tableCommitInfo.getIdToPartitionCommitInfo();
            for (Long partitionId : partitionCommitInfoMap.keySet()) {
                if (!publishVersionDataMap.containsKey(partitionId)) {
                    publishVersionDataMap.put(partitionId, new PartitionPublishVersionData(tableId, partitionId));
                }
                PartitionPublishVersionData publishVersionData = publishVersionDataMap.get(partitionId);
                publishVersionData.addTransaction(state);
            }
        }
        LOG.info("start publish lake batch db:{} table:{} txns:{}", dbId, tableId,
                StringUtils.join(states.stream().map(TransactionState::getTransactionId).toArray(), ","));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);

        if (db == null) {
            LOG.info("the database of transaction batch {} has been deleted", txnStateBatch);
            try {
                for (TransactionState state : txnStateBatch.getTransactionStates()) {
                    globalTransactionMgr.finishTransaction(state.getDbId(), state.getTransactionId(),
                            Sets.newHashSet());
                }
            } catch (UserException ex) {
                LOG.warn("Fail to finish txn Batch " + txnStateBatch, ex);
            }
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Boolean>> futureList = new ArrayList<>();

        for (PartitionPublishVersionData publishVersionData : publishVersionDataMap.values()) {
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                boolean success = publishPartitionBatch(db, tableId, publishVersionData, txnStateBatch);
                long versionTime = success ? System.currentTimeMillis() : -System.currentTimeMillis();
                for (PartitionCommitInfo commitInfo : publishVersionData.getPartitionCommitInfos()) {
                    commitInfo.setVersionTime(versionTime);
                }
                return success;
            }, getLakeTaskExecutor()).exceptionally(ex -> {
                LOG.error("Fail to publish txn batch", ex);
                long versionTime = -System.currentTimeMillis();
                for (PartitionCommitInfo commitInfo : publishVersionData.getPartitionCommitInfos()) {
                    commitInfo.setVersionTime(versionTime);
                }
                return false;
            });
            futureList.add(future);
        }

        CompletableFuture<Boolean> publishFuture = CompletableFuture.allOf(
                        futureList.toArray(new CompletableFuture[0])).
                thenApply(v -> futureList.stream().allMatch(CompletableFuture::join));

        return publishFuture.thenAccept(success -> {
            if (success) {
                try {
                    globalTransactionMgr.finishTransactionBatch(dbId, txnStateBatch, null);
                    // here create the job to drop txnLog, for the visibleVersion has been updated
                    submitDeleteTxnLogJob(txnStateBatch);
                } catch (UserException e) {
                    throw new RuntimeException(e);
                }
            }
        }).exceptionally(ex -> {
            LOG.error("Fail to finish transaction batch");
            return null;
        });
    }

    private CompletableFuture<Boolean> publishLakeTableAsync(Database db, TransactionState txnState,
                                                             TableCommitInfo tableCommitInfo) {
        Collection<PartitionCommitInfo> partitionCommitInfos = tableCommitInfo.getIdToPartitionCommitInfo().values();
        if (partitionCommitInfos.size() == 1) {
            PartitionCommitInfo partitionCommitInfo = partitionCommitInfos.iterator().next();
            return publishLakePartitionAsync(db, tableCommitInfo, partitionCommitInfo, txnState);
        } else {
            List<CompletableFuture<Boolean>> futureList = new ArrayList<>();
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                CompletableFuture<Boolean> future =
                        publishLakePartitionAsync(db, tableCommitInfo, partitionCommitInfo, txnState);
                futureList.add(future);
            }
            return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futureList.stream().allMatch(CompletableFuture::join));
        }
    }

    private CompletableFuture<Boolean> publishLakePartitionAsync(@NotNull Database db,
                                                                 @NotNull TableCommitInfo tableCommitInfo,
                                                                 @NotNull PartitionCommitInfo partitionCommitInfo,
                                                                 @NotNull TransactionState txnState) {
        long versionTime = partitionCommitInfo.getVersionTime();
        if (versionTime > 0) {
            return CompletableFuture.completedFuture(true);
        }
        if (versionTime < 0 && System.currentTimeMillis() < Math.abs(versionTime) + RETRY_INTERVAL_MS) {
            return CompletableFuture.completedFuture(false);
        }

        return CompletableFuture.supplyAsync(() -> {
            boolean success = publishPartition(db, tableCommitInfo, partitionCommitInfo, txnState);
            partitionCommitInfo.setVersionTime(success ? System.currentTimeMillis() : -System.currentTimeMillis());
            return success;
        }, getLakeTaskExecutor()).exceptionally(ex -> {
            LOG.error("Fail to publish txn " + txnState.getTransactionId(), ex);
            partitionCommitInfo.setVersionTime(-System.currentTimeMillis());
            return false;
        });
    }

    private boolean publishPartition(@NotNull Database db, @NotNull TableCommitInfo tableCommitInfo,
                                     @NotNull PartitionCommitInfo partitionCommitInfo,
                                     @NotNull TransactionState txnState) {
        long tableId = tableCommitInfo.getTableId();
        long baseVersion = 0;
        long txnVersion = partitionCommitInfo.getVersion();
        long txnId = txnState.getTransactionId();
        long commitTime = txnState.getCommitTime();
        String txnLabel = txnState.getLabel();
        long warehouseId = txnState.getWarehouseId();
        List<Tablet> normalTablets = null;
        List<Tablet> shadowTablets = null;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (table == null) {
                txnState.removeTable(tableCommitInfo.getTableId());
                LOG.info("Removed non-exist table {} from transaction {}. txn_id={}", tableId, txnLabel, txnId);
                return true;
            }
            long partitionId = partitionCommitInfo.getPhysicalPartitionId();
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            if (partition == null) {
                LOG.info("Ignore non-exist partition {} of table {} in txn {}", partitionId, table.getName(), txnLabel);
                return true;
            }
            if (txnState.getSourceType() != TransactionState.LoadJobSourceType.REPLICATION &&
                    partition.getVisibleVersion() + 1 != txnVersion) {
                return false;
            }
            baseVersion = partition.getVisibleVersion();
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
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        }

        TxnInfoPB txnInfo = TxnInfoHelper.fromTransactionState(txnState);
        try {
            if (CollectionUtils.isNotEmpty(shadowTablets)) {
                Utils.publishLogVersion(shadowTablets, txnInfo, txnVersion, warehouseId);
            }
            if (CollectionUtils.isNotEmpty(normalTablets)) {
                Map<Long, Double> compactionScores = new HashMap<>();
                Utils.publishVersion(normalTablets, txnInfo, baseVersion, txnVersion, compactionScores,
                        warehouseId);

                Quantiles quantiles = Quantiles.compute(compactionScores.values());
                partitionCommitInfo.setCompactionScore(quantiles);
            }
            return true;
        } catch (Throwable e) {
            // prevent excessive logging
            if (partitionCommitInfo.getVersionTime() < 0 && 
                    Math.abs(partitionCommitInfo.getVersionTime()) + 10000 < System.currentTimeMillis()) {
                return false;
            }
            LOG.error("Fail to publish partition {} of txn {}: {}", partitionCommitInfo.getPhysicalPartitionId(),
                    txnId, e.getMessage());
            return false;
        }
    }
}
