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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.lake.Utils;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.scheduler.Constants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TTaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
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
    private Set<Long> publishingLakeTransactions;

    private Set<Long> publishingLakeTransactionsBatchTableId;


    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
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
            List<Long> allBackends = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false);
            if (RunMode.isSharedDataMode()) {
                allBackends.addAll(GlobalStateMgr.getCurrentSystemInfo().getComputeNodeIds(false));
            }

            if (allBackends.isEmpty()) {
                LOG.warn("some transaction state need to publish, but no backend exists");
                return;
            }

            if (!RunMode.allowCreateLakeTable()) {
                publishVersionForOlapTable(readyTransactionStates);
                return;
            }

            if (!RunMode.allowCreateOlapTable()) {
                publishVersionForLakeTable(readyTransactionStates);
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

        int oldNumThreads = lakeTaskExecutor.getMaximumPoolSize();
        if (oldNumThreads == newNumThreads) {
            return;
        }

        if (newNumThreads < oldNumThreads) { // scale in
            lakeTaskExecutor.setCorePoolSize(newNumThreads);
            lakeTaskExecutor.setMaximumPoolSize(newNumThreads);
        } else { // scale out
            lakeTaskExecutor.setMaximumPoolSize(newNumThreads);
            lakeTaskExecutor.setCorePoolSize(newNumThreads);
        }
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
                    return table.isCloudNativeTableOrMaterializedView();
                }
            }
        } finally {
            db.readUnlock();
        }
        return false;
    }

    void publishVersionForLakeTable(List<TransactionState> readyTransactionStates) {
        Set<Long> publishingTransactions = getPublishingLakeTransactions();
        for (TransactionState txnState : readyTransactionStates) {
            long txnId = txnState.getTransactionId();
            if (publishingTransactions.add(txnId)) { // the set did not already contain the specified element
                Set<Long> publishingLakeTransactionsBatchTableId = getPublishingLakeTransactionsBatchTableId();
                // When the `enable_lake_batch_publish_version` switch is just set to false,
                // it is possible that the result of publish task has not been returned,
                // we need to wait for the result to return if the same table is involved.
                if (!txnState.getTableIdList().stream().allMatch(id -> !publishingLakeTransactionsBatchTableId.contains(id))) {
                    LOG.info("maybe enable_lake_batch_publish_version is set to false just now, txn {} will be published later",
                            txnState.getTransactionId());
                    continue;
                }
                CompletableFuture<Void> future = publishLakeTransactionAsync(txnState);
                future.thenRun(() -> publishingTransactions.remove(txnId));
            }
        }
    }

    void publishVersionForLakeTableBatch(List<TransactionStateBatch> readyTransactionStatesBatch) {
        Set<Long> publishingLakeTransactionsBatchTableId = getPublishingLakeTransactionsBatchTableId();
        for (TransactionStateBatch txnStateBatch : readyTransactionStatesBatch) {
            long tableId = txnStateBatch.getTableId();
            if (publishingLakeTransactionsBatchTableId.add(tableId)) {
                CompletableFuture<Void> future = publishLakeTransactionBatchAsync(txnStateBatch);
                future.thenRun(() -> publishingLakeTransactionsBatchTableId.remove(tableId));
            }
        }
    }

    private CompletableFuture<Void> publishLakeTransactionAsync(TransactionState txnState) {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        long txnId = txnState.getTransactionId();
        long dbId = txnState.getDbId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
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
                    refreshMvIfNecessary(txnState);
                } catch (UserException e) {
                    throw new RuntimeException(e);
                }
            }
        }).exceptionally(ex -> {
            LOG.error("Fail to finish transaction " + txnId, ex);
            return null;
        });
    }


    public boolean publishPartitionBatch(Database db, long tableId, long partitionId, List<Long> txnIds,
                                         List<Long> versions, List<TransactionState> transactionStates,
                                         TransactionStateBatch stateBatch) {
        db.readLock();
        // version -> shadowTablets
        Map<Long, Set<Tablet>> shadowTabletsMap = new HashMap<>();
        Set<Tablet> normalTablets = null;
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                // table has been dropped
                return true;
            }

            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                LOG.info("partition is null in publish partition batch");
                return true;
            }
            if (partition.getVisibleVersion() + 1 != versions.get(0)) {
                LOG.info("publish partition batch partition.getVisibleVersion() + 1 != version.get(0)" + " "
                        + partition.getId() + " " + partition.getVisibleVersion() + " " + versions.get(0));
                return false;
            }

            for (int i = 0; i < transactionStates.size(); i++) {
                TransactionState txnState = transactionStates.get(i);
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
            db.readUnlock();
        }

        long startVersion = versions.get(0);
        long endVersion = versions.get(versions.size() - 1);

        try {
            for (Map.Entry<Long, Set<Tablet>> item : shadowTabletsMap.entrySet()) {
                int index = versions.indexOf(item.getKey());
                List<Tablet> publishShdowTablets = new ArrayList<>(item.getValue());
                Utils.publishLogVersionBatch(publishShdowTablets, txnIds.subList(index, txnIds.size()),
                        versions.subList(index, versions.size()));
            }
            if (CollectionUtils.isNotEmpty(normalTablets)) {
                Map<Long, Double> compactionScores = new HashMap<>();
                List<Tablet> publishTablets = new ArrayList<>();
                publishTablets.addAll(normalTablets);

                // commit time of last transactionState as commitTime
                long commitTime = transactionStates.get(transactionStates.size() - 1).getCommitTime();
                Utils.publishVersionBatch(publishTablets, txnIds,
                        startVersion - 1, endVersion, commitTime, compactionScores);

                Quantiles quantiles = Quantiles.compute(compactionScores.values());
                stateBatch.setCompactionScore(tableId, partitionId, quantiles);
            }
        } catch (Throwable e) {
            LOG.error("Fail to publish partition {} of txnIds {}: {}", partitionId,
                    txnIds, e.getMessage());
            return false;
        }

        return true;
    }

    private CompletableFuture<Void> publishLakeTransactionBatchAsync(TransactionStateBatch txnStateBatch) {
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        if (txnStateBatch.size() > 1) {
            // pick up all tableCommitInfo
            // only one table,if batch has multi transactionState for now,
            // the batch only has one transactionState for multi table.
            long dbId = txnStateBatch.getDbId();
            long tableId = txnStateBatch.getTableId();
            List<TransactionState> states = txnStateBatch.getTransactionStates();
            // partitionId -> txnIdList
            Map<Long, List<Long>> dirtyPartitons = new HashMap<>();
            // partitionId -> versionList
            Map<Long, List<Long>> partitionVersions = new HashMap<>();
            // partitionId -> transactionState
            Map<Long, List<TransactionState>> partitionStates = new HashMap<>();


            for (TransactionState state : states) {
                Map<Long, PartitionCommitInfo> partitionCommitInfoMap = state.getTableCommitInfo(tableId)
                        .getIdToPartitionCommitInfo();
                for (Map.Entry<Long, PartitionCommitInfo> item : partitionCommitInfoMap.entrySet()) {

                    if (!dirtyPartitons.containsKey(item.getKey())) {
                        dirtyPartitons.put(item.getKey(), new ArrayList<>());
                    }
                    List<Long> partitionCommitInfo = dirtyPartitons.get(item.getKey());
                    partitionCommitInfo.add(state.getTransactionId());

                    if (!partitionVersions.containsKey(item.getKey())) {
                        partitionVersions.put(item.getKey(), new ArrayList<>());
                    }
                    List<Long> versions = partitionVersions.get(item.getKey());
                    versions.add(item.getValue().getVersion());

                    if (!partitionStates.containsKey(item.getKey())) {
                        partitionStates.put(item.getKey(), new ArrayList<>());
                    }
                    List<TransactionState> partitionState = partitionStates.get(item.getKey());
                    partitionState.add(state);
                }
            }

            // TODO
            // make sure the txnIdList is correspond to versions
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);

            if (db == null) {
                LOG.info("the database of transaction batch {} has been deleted", txnStateBatch);
                try {
                    for (TransactionState state : txnStateBatch.getTransactionStates()) {
                        globalTransactionMgr.finishTransaction(state.getDbId(), state.getTransactionId(), Sets.newHashSet());
                    }
                } catch (UserException ex) {
                    LOG.warn("Fail to finish txn Batch " + txnStateBatch, ex);
                }
                return CompletableFuture.completedFuture(null);
            }

            List<CompletableFuture<Boolean>> futureList = new ArrayList<>();

            for (Map.Entry<Long, List<Long>> item : dirtyPartitons.entrySet()) {
                Long partitionId = item.getKey();

                CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                    boolean success = publishPartitionBatch(db, tableId, partitionId, item.getValue(),
                            partitionVersions.get(partitionId), partitionStates.get(partitionId), txnStateBatch);
                    partitionStates.get(partitionId).stream().forEach(state -> state.getTableCommitInfo(tableId).
                            getIdToPartitionCommitInfo().get(partitionId).setVersionTime(
                                    success ? System.currentTimeMillis() : -System.currentTimeMillis()));
                    return success;
                }, getLakeTaskExecutor()).exceptionally(ex -> {
                    LOG.error("Fail to publish txn batch ");
                    partitionStates.get(partitionId).stream().forEach(state -> state.getTableCommitInfo(tableId).
                            getIdToPartitionCommitInfo().get(partitionId).setVersionTime(-System.currentTimeMillis()));
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
                        //
                        for (TransactionState state : txnStateBatch.getTransactionStates()) {
                            refreshMvIfNecessary(state);
                        }

                    } catch (UserException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).exceptionally(ex -> {
                LOG.error("Fail to finish transaction batch");
                return null;
            });

        }

        // batch size == 1
        // degenerate into normal mode
        TransactionState txnState = null;
        try {
            txnState = txnStateBatch.index(0);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

        return publishLakeTransactionAsync(txnState);
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
        long txnVersion = partitionCommitInfo.getVersion();
        long txnId = txnState.getTransactionId();
        long commitTime = txnState.getCommitTime();
        String txnLabel = txnState.getLabel();
        List<Tablet> normalTablets = null;
        List<Tablet> shadowTablets = null;

        db.readLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                txnState.removeTable(tableCommitInfo.getTableId());
                LOG.info("Removed non-exist table {} from transaction {}. txn_id={}", tableId, txnLabel, txnId);
                return true;
            }
            long partitionId = partitionCommitInfo.getPartitionId();
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            if (partition == null) {
                LOG.info("Ignore non-exist partition {} of table {} in txn {}", partitionId, table.getName(), txnLabel);
                return true;
            }
            if (partition.getVisibleVersion() + 1 != txnVersion) {
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

        try {
            if (CollectionUtils.isNotEmpty(shadowTablets)) {
                Utils.publishLogVersion(shadowTablets, txnId, txnVersion);
            }
            if (CollectionUtils.isNotEmpty(normalTablets)) {
                Map<Long, Double> compactionScores = new HashMap<>();
                Utils.publishVersion(normalTablets, txnId, txnVersion - 1, txnVersion, commitTime / 1000,
                        compactionScores);

                Quantiles quantiles = Quantiles.compute(compactionScores.values());
                partitionCommitInfo.setCompactionScore(quantiles);
            }
            return true;
        } catch (Throwable e) {
            LOG.error("Fail to publish partition {} of txn {}: {}", partitionCommitInfo.getPartitionId(),
                    txnId, e.getMessage());
            return false;
        }
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
