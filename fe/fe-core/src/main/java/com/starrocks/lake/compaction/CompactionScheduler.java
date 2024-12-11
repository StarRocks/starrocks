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

package com.starrocks.lake.compaction;

<<<<<<< HEAD
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
=======
import com.google.common.annotations.VisibleForTesting;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
<<<<<<< HEAD
import com.starrocks.catalog.Partition;
=======
import com.starrocks.catalog.PhysicalPartition;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
<<<<<<< HEAD
import com.starrocks.common.UserException;
import com.starrocks.common.util.Daemon;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
=======
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.proto.CompactRequest;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.VisibleStateWaiter;
=======
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.VisibleStateWaiter;
import com.starrocks.warehouse.Warehouse;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
<<<<<<< HEAD
import java.util.HashMap;
=======
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
<<<<<<< HEAD
=======
import java.util.Set;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

public class CompactionScheduler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(CompactionScheduler.class);
    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();
<<<<<<< HEAD
    private static final long LOOP_INTERVAL_MS = 200L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS = LOOP_INTERVAL_MS * 2;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_FAILURE = LOOP_INTERVAL_MS * 10;
=======
    private static final long LOOP_INTERVAL_MS = 1000L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS = LOOP_INTERVAL_MS * 10;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_FAILURE = LOOP_INTERVAL_MS * 60;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    private static final long PARTITION_CLEAN_INTERVAL_SECOND = 30;
    private final CompactionMgr compactionManager;
    private final SystemInfoService systemInfoService;
    private final GlobalTransactionMgr transactionMgr;
    private final GlobalStateMgr stateMgr;
    private final ConcurrentHashMap<PartitionIdentifier, CompactionJob> runningCompactions;
    private final SynchronizedCircularQueue<CompactionRecord> history;
<<<<<<< HEAD
    private final SynchronizedCircularQueue<CompactionRecord> failHistory;
    private boolean finishedWaiting = false;
    private long waitTxnId = -1;
    private long lastPartitionCleanTime;

    CompactionScheduler(@NotNull CompactionMgr compactionManager, @NotNull SystemInfoService systemInfoService,
                        @NotNull GlobalTransactionMgr transactionMgr, @NotNull GlobalStateMgr stateMgr) {
=======
    private boolean finishedWaiting = false;
    private long waitTxnId = -1;
    private long lastPartitionCleanTime;
    private Set<Long> disabledTables; // copy-on-write

    CompactionScheduler(@NotNull CompactionMgr compactionManager, @NotNull SystemInfoService systemInfoService,
                        @NotNull GlobalTransactionMgr transactionMgr, @NotNull GlobalStateMgr stateMgr,
                        @NotNull String disableTablesStr) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        super("COMPACTION_DISPATCH", LOOP_INTERVAL_MS);
        this.compactionManager = compactionManager;
        this.systemInfoService = systemInfoService;
        this.transactionMgr = transactionMgr;
        this.stateMgr = stateMgr;
        this.runningCompactions = new ConcurrentHashMap<>();
        this.lastPartitionCleanTime = System.currentTimeMillis();
        this.history = new SynchronizedCircularQueue<>(Config.lake_compaction_history_size);
<<<<<<< HEAD
        this.failHistory = new SynchronizedCircularQueue<>(Config.lake_compaction_fail_history_size);
=======
        this.disabledTables = Collections.unmodifiableSet(new HashSet<>());

        disableTables(disableTablesStr);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    protected void runOneCycle() {
        cleanPartition();

        // Schedule compaction tasks only when this is a leader FE and all edit logs have finished replay.
        // In order to ensure that the input rowsets of compaction still exists when doing publishing version, it is
        // necessary to ensure that the compaction task of the same partition is executed serially, that is, the next
        // compaction task can be executed only after the status of the previous compaction task changes to visible or
        // canceled.
        if (stateMgr.isLeader() && stateMgr.isReady() && allCommittedCompactionsBeforeRestartHaveFinished()) {
            schedule();
            history.changeMaxSize(Config.lake_compaction_history_size);
<<<<<<< HEAD
            failHistory.changeMaxSize(Config.lake_compaction_fail_history_size);
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    // Returns true if all compaction transactions committed before this restart have finished(i.e., of VISIBLE state).
    private boolean allCommittedCompactionsBeforeRestartHaveFinished() {
        if (finishedWaiting) {
            return true;
        }
        // Note: must call getMinActiveCompactionTxnId() before getNextTransactionId(), otherwise if there are
        // no running transactions waitTxnId <= minActiveTxnId will always be false.
        long minActiveTxnId = transactionMgr.getMinActiveCompactionTxnId();
        if (waitTxnId < 0) {
            waitTxnId = transactionMgr.getTransactionIDGenerator().getNextTransactionId();
        }
        finishedWaiting = waitTxnId <= minActiveTxnId;
        return finishedWaiting;
    }

    private void schedule() {
        // Check whether there are completed compaction jobs.
        for (Iterator<Map.Entry<PartitionIdentifier, CompactionJob>> iterator = runningCompactions.entrySet().iterator();
                iterator.hasNext(); ) {
            Map.Entry<PartitionIdentifier, CompactionJob> entry = iterator.next();
            PartitionIdentifier partition = entry.getKey();
<<<<<<< HEAD
            CompactionJob job = entry.getValue();

            if (!job.transactionHasCommitted()) {
                String errorMsg = null;

                if (job.isCompleted()) {
                    job.getPartition().setMinRetainVersion(0);
                    try {
                        commitCompaction(partition, job);
=======

            // Make sure all running compactions' priority is reset
            PartitionStatistics statistics = compactionManager.getStatistics(partition);
            if (statistics != null && statistics.getPriority() != PartitionStatistics.CompactionPriority.DEFAULT) {
                statistics.resetPriority();
            }

            CompactionJob job = entry.getValue();
            if (!job.transactionHasCommitted()) {
                String errorMsg = null;

                CompactionTask.TaskResult taskResult = job.getResult();
                if (taskResult == CompactionTask.TaskResult.ALL_SUCCESS ||
                        (Config.lake_compaction_allow_partial_success &&
                        job.getAllowPartialSuccess() &&
                        taskResult == CompactionTask.TaskResult.PARTIAL_SUCCESS)) {
                    job.getPartition().setMinRetainVersion(0);
                    try {
                        commitCompaction(partition, job,
                                taskResult == CompactionTask.TaskResult.PARTIAL_SUCCESS /* forceCommit */);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                        assert job.transactionHasCommitted();
                    } catch (Exception e) {
                        LOG.error("Fail to commit compaction. {} error={}", job.getDebugString(), e.getMessage());
                        errorMsg = "fail to commit transaction: " + e.getMessage();
                    }
<<<<<<< HEAD
                } else if (job.isFailed()) {
=======
                } else if (taskResult == CompactionTask.TaskResult.PARTIAL_SUCCESS ||
                           taskResult == CompactionTask.TaskResult.NONE_SUCCESS) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    job.getPartition().setMinRetainVersion(0);
                    errorMsg = Objects.requireNonNull(job.getFailMessage(), "getFailMessage() is null");
                    LOG.error("Compaction job {} failed: {}", job.getDebugString(), errorMsg);
                    job.abort(); // Abort any executing task, if present.
<<<<<<< HEAD
=======
                } else if (taskResult != CompactionTask.TaskResult.NOT_FINISHED) {
                    errorMsg = String.format("Unexpected compaction result: %s, %s", taskResult.name(), job.getDebugString());
                    LOG.error(errorMsg);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                }

                if (errorMsg != null) {
                    iterator.remove();
                    job.finish();
<<<<<<< HEAD
                    failHistory.offer(CompactionRecord.build(job, errorMsg));
=======
                    history.offer(CompactionRecord.build(job, errorMsg));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                    abortTransactionIgnoreException(job, errorMsg);
                    continue;
                }
            }
<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (job.transactionHasCommitted() && job.waitTransactionVisible(50, TimeUnit.MILLISECONDS)) {
                iterator.remove();
                job.finish();
                history.offer(CompactionRecord.build(job));
                long cost = job.getFinishTs() - job.getStartTs();
                if (cost >= /*60 minutes=*/3600000) {
                    LOG.info("Removed published compaction. {} cost={}s running={}", job.getDebugString(),
                            cost / 1000, runningCompactions.size());
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("Removed published compaction. {} cost={}s running={}", job.getDebugString(),
                            cost / 1000, runningCompactions.size());
                }
<<<<<<< HEAD
                compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS);
=======
                int factor = (statistics != null) ? statistics.getPunishFactor() : 1;
                compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS * factor);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }

        // Create new compaction tasks.
<<<<<<< HEAD
        int index = 0;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        int compactionLimit = compactionTaskLimit();
        int numRunningTasks = runningCompactions.values().stream().mapToInt(CompactionJob::getNumTabletCompactionTasks).sum();
        if (numRunningTasks >= compactionLimit) {
            return;
        }

<<<<<<< HEAD
        List<PartitionIdentifier> partitions = compactionManager.choosePartitionsToCompact(runningCompactions.keySet());
        while (numRunningTasks < compactionLimit && index < partitions.size()) {
            PartitionIdentifier partition = partitions.get(index++);
            CompactionJob job = startCompaction(partition);
=======
        List<PartitionStatisticsSnapshot> partitions = compactionManager.choosePartitionsToCompact(
                runningCompactions.keySet(), disabledTables);
        int index = 0;
        while (numRunningTasks < compactionLimit && index < partitions.size()) {
            PartitionStatisticsSnapshot partitionStatisticsSnapshot = partitions.get(index++);
            CompactionJob job = startCompaction(partitionStatisticsSnapshot);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (job == null) {
                continue;
            }
            numRunningTasks += job.getNumTabletCompactionTasks();
<<<<<<< HEAD
            runningCompactions.put(partition, job);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created new compaction job. partition={} txnId={}", partition, job.getTxnId());
=======
            runningCompactions.put(partitionStatisticsSnapshot.getPartition(), job);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created new compaction job. {}, txnId={}",
                        partitionStatisticsSnapshot.toString(), job.getTxnId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
    }

    private void abortTransactionIgnoreException(CompactionJob job, String reason) {
        try {
            List<TabletCommitInfo> finishedTablets = job.buildTabletCommitInfo();
            transactionMgr.abortTransaction(job.getDb().getId(), job.getTxnId(), reason, finishedTablets,
                    Collections.emptyList(), null);
<<<<<<< HEAD
        } catch (UserException ex) {
=======
        } catch (StarRocksException ex) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            LOG.error("Fail to abort txn " + job.getTxnId(), ex);
        }
    }

<<<<<<< HEAD
    private int compactionTaskLimit() {
        if (Config.lake_compaction_max_tasks >= 0) {
            return Config.lake_compaction_max_tasks;
        }
        return (systemInfoService.getAliveBackendNumber() +
                systemInfoService.getAliveComputeNodeNumber()) * 16;
=======
    @VisibleForTesting
    protected int compactionTaskLimit() {
        if (Config.lake_compaction_max_tasks >= 0) {
            return Config.lake_compaction_max_tasks;
        }
        WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = manager.getCompactionWarehouse();
        List<ComputeNode> aliveComputeNodes = manager.getAliveComputeNodes(warehouse.getId());
        return aliveComputeNodes.size() * 16;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    private void cleanPartition() {
        long now = System.currentTimeMillis();
        if (now - lastPartitionCleanTime >= PARTITION_CLEAN_INTERVAL_SECOND * 1000L) {
            compactionManager.getAllPartitions()
                    .stream()
<<<<<<< HEAD
                    .filter(p -> !isPartitionExist(p))
=======
                    .filter(p -> !MetaUtils.isPartitionExist(stateMgr, p.getDbId(), p.getTableId(), p.getPartitionId()))
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    .filter(p -> !runningCompactions.containsKey(p)) // Ignore those partitions in runningCompactions
                    .forEach(compactionManager::removePartition);
            lastPartitionCleanTime = now;
        }
    }

<<<<<<< HEAD
    private boolean isPartitionExist(PartitionIdentifier partition) {
        Database db = stateMgr.getDb(partition.getDbId());
        if (db == null) {
            return false;
        }
        db.readLock();
        try {
            // lake table or lake materialized view
            OlapTable table = (OlapTable) db.getTable(partition.getTableId());
            return table != null && table.getPartition(partition.getPartitionId()) != null;
        } finally {
            db.readUnlock();
        }
    }

    private CompactionJob startCompaction(PartitionIdentifier partitionIdentifier) {
        Database db = stateMgr.getDb(partitionIdentifier.getDbId());
=======
    private CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot) {
        PartitionIdentifier partitionIdentifier = partitionStatisticsSnapshot.getPartition();
        Database db = stateMgr.getLocalMetastore().getDb(partitionIdentifier.getDbId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (db == null) {
            compactionManager.removePartition(partitionIdentifier);
            return null;
        }

        long txnId;
        long currentVersion;
        OlapTable table;
<<<<<<< HEAD
        Partition partition;
        Map<Long, List<Long>> beToTablets;

        db.readLock();

        try {
            // lake table or lake materialized view
            table = (OlapTable) db.getTable(partitionIdentifier.getTableId());
=======
        PhysicalPartition partition;
        Map<Long, List<Long>> beToTablets;

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);

        try {
            // lake table or lake materialized view
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), partitionIdentifier.getTableId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            // Compact a table of SCHEMA_CHANGE state does not make much sense, because the compacted data
            // will not be used after the schema change job finished.
            if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                compactionManager.enableCompactionAfter(partitionIdentifier, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                return null;
            }
<<<<<<< HEAD
            partition = (table != null) ? table.getPartition(partitionIdentifier.getPartitionId()) : null;
=======
            partition = (table != null) ? table.getPhysicalPartition(partitionIdentifier.getPartitionId()) : null;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (partition == null) {
                compactionManager.removePartition(partitionIdentifier);
                return null;
            }

            currentVersion = partition.getVisibleVersion();

            beToTablets = collectPartitionTablets(partition);
            if (beToTablets.isEmpty()) {
                compactionManager.enableCompactionAfter(partitionIdentifier, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                return null;
            }

            // Note: call `beginTransaction()` in the scope of database reader lock to make sure no shadow index will
            // be added to this table(i.e., no schema change) before calling `beginTransaction()`.
            txnId = beginTransaction(partitionIdentifier);

            partition.setMinRetainVersion(currentVersion);

<<<<<<< HEAD
        } catch (BeginTransactionException | AnalysisException | LabelAlreadyUsedException | DuplicatedRequestException e) {
=======
        } catch (RunningTxnExceedException | AnalysisException | LabelAlreadyUsedException | DuplicatedRequestException e) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            LOG.error("Fail to create transaction for compaction job. {}", e.getMessage());
            return null;
        } catch (Throwable e) {
            LOG.error("Unknown error: {}", e.getMessage());
            return null;
        } finally {
<<<<<<< HEAD
            db.readUnlock();
        }

        long nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS;
        CompactionJob job = new CompactionJob(db, table, partition, txnId);
        try {
            List<CompactionTask> tasks = createCompactionTasks(currentVersion, beToTablets, txnId);
=======
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        long nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS;
        CompactionJob job = new CompactionJob(db, table, partition, txnId, Config.lake_compaction_allow_partial_success);
        try {
            List<CompactionTask> tasks = createCompactionTasks(currentVersion, beToTablets, txnId,
                    job.getAllowPartialSuccess(), partitionStatisticsSnapshot.getPriority());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            for (CompactionTask task : tasks) {
                task.sendRequest();
            }
            job.setTasks(tasks);
            return job;
        } catch (Exception e) {
<<<<<<< HEAD
            LOG.error(e);
=======
            LOG.error(e.getMessage(), e);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            partition.setMinRetainVersion(0);
            nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_FAILURE;
            abortTransactionIgnoreError(job, e.getMessage());
            job.finish();
<<<<<<< HEAD
            failHistory.offer(CompactionRecord.build(job, e.getMessage()));
=======
            history.offer(CompactionRecord.build(job, e.getMessage()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return null;
        } finally {
            compactionManager.enableCompactionAfter(partitionIdentifier, nextCompactionInterval);
        }
    }

    @NotNull
<<<<<<< HEAD
    private List<CompactionTask> createCompactionTasks(long currentVersion, Map<Long, List<Long>> beToTablets, long txnId)
            throws UserException, RpcException {
=======
    private List<CompactionTask> createCompactionTasks(long currentVersion, Map<Long, List<Long>> beToTablets, long txnId,
            boolean allowPartialSuccess, PartitionStatistics.CompactionPriority priority)
            throws StarRocksException, RpcException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        List<CompactionTask> tasks = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            ComputeNode node = systemInfoService.getBackendOrComputeNode(entry.getKey());
            if (node == null) {
<<<<<<< HEAD
                throw new UserException("Node " + entry.getKey() + " has been dropped");
=======
                throw new StarRocksException("Node " + entry.getKey() + " has been dropped");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }

            LakeService service = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());

            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = currentVersion;
            request.timeoutMs = LakeService.TIMEOUT_COMPACT;
<<<<<<< HEAD
=======
            request.allowPartialSuccess = allowPartialSuccess;
            request.encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();
            request.forceBaseCompaction = (priority == PartitionStatistics.CompactionPriority.MANUAL_COMPACT);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

            CompactionTask task = new CompactionTask(node.getId(), service, request);
            tasks.add(task);
        }
        return tasks;
    }

    @NotNull
<<<<<<< HEAD
    private Map<Long, List<Long>> collectPartitionTablets(Partition partition) {
=======
    private Map<Long, List<Long>> collectPartitionTablets(PhysicalPartition partition) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        List<MaterializedIndex> visibleIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        for (MaterializedIndex index : visibleIndexes) {
            for (Tablet tablet : index.getTablets()) {
<<<<<<< HEAD
                Long beId = Utils.chooseBackend((LakeTablet) tablet);
                if (beId == null) {
                    beToTablets.clear();
                    return beToTablets;
                }
                beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
=======
                WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Warehouse warehouse = manager.getCompactionWarehouse();
                ComputeNode computeNode = manager.getComputeNodeAssignedToTablet(warehouse.getName(), (LakeTablet) tablet);
                if (computeNode == null) {
                    beToTablets.clear();
                    return beToTablets;
                }

                beToTablets.computeIfAbsent(computeNode.getId(), k -> Lists.newArrayList()).add(tablet.getId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
        return beToTablets;
    }

    // REQUIRE: has acquired the exclusive lock of Database.
    protected long beginTransaction(PartitionIdentifier partition)
<<<<<<< HEAD
            throws BeginTransactionException, AnalysisException, LabelAlreadyUsedException, DuplicatedRequestException {
=======
            throws RunningTxnExceedException, AnalysisException, LabelAlreadyUsedException, DuplicatedRequestException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        long dbId = partition.getDbId();
        long tableId = partition.getTableId();
        long partitionId = partition.getPartitionId();
        long currentTs = System.currentTimeMillis();
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.LAKE_COMPACTION;
        TransactionState.TxnSourceType txnSourceType = TransactionState.TxnSourceType.FE;
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(txnSourceType, HOST_NAME);
        String label = String.format("COMPACTION_%d-%d-%d-%d", dbId, tableId, partitionId, currentTs);
<<<<<<< HEAD
        return transactionMgr.beginTransaction(dbId, Lists.newArrayList(tableId), label, coordinator,
                loadJobSourceType, Config.lake_compaction_default_timeout_second);
    }

    private void commitCompaction(PartitionIdentifier partition, CompactionJob job)
            throws UserException {
        Preconditions.checkState(job.isCompleted());

        List<TabletCommitInfo> commitInfoList = job.buildTabletCommitInfo();

        Database db = stateMgr.getDb(partition.getDbId());
=======

        WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = manager.getCompactionWarehouse();
        return transactionMgr.beginTransaction(dbId, Lists.newArrayList(tableId), label, coordinator,
                loadJobSourceType, Config.lake_compaction_default_timeout_second, warehouse.getId());
    }

    private void commitCompaction(PartitionIdentifier partition, CompactionJob job, boolean forceCommit)
            throws StarRocksException {
        List<TabletCommitInfo> commitInfoList = job.buildTabletCommitInfo();

        Database db = stateMgr.getLocalMetastore().getDb(partition.getDbId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (db == null) {
            throw new MetaNotFoundException("database not exist");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing compaction transaction. partition={} txnId={}", partition, job.getTxnId());
        }

        VisibleStateWaiter waiter;
<<<<<<< HEAD
        db.writeLock();
        try {
            waiter = transactionMgr.commitTransaction(db.getId(), job.getTxnId(), commitInfoList, Lists.newArrayList());
        } finally {
            db.writeUnlock();
=======

        TransactionState transactionState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionState(db.getId(), job.getTxnId());
        List<Long> tableIdList = transactionState.getTableIdList();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), tableIdList, LockType.WRITE);
        try {
            CompactionTxnCommitAttachment attachment = null;
            if (forceCommit) { // do not write extra info if no need to force commit
                attachment = new CompactionTxnCommitAttachment(true /* forceCommit */);
            }
            waiter = transactionMgr.commitTransaction(db.getId(), job.getTxnId(), commitInfoList,
                    Collections.emptyList(), attachment);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), tableIdList, LockType.WRITE);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        job.setVisibleStateWaiter(waiter);
        job.setCommitTs(System.currentTimeMillis());
    }

    private void abortTransactionIgnoreError(CompactionJob job, String reason) {
        try {
            List<TabletCommitInfo> finishedTablets = job.buildTabletCommitInfo();
            transactionMgr.abortTransaction(job.getDb().getId(), job.getTxnId(), reason, finishedTablets,
                    Collections.emptyList(), null);
<<<<<<< HEAD
        } catch (UserException ex) {
=======
        } catch (StarRocksException ex) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            LOG.error(ex);
        }
    }

<<<<<<< HEAD
    @NotNull
    List<CompactionRecord> getHistory() {
        ImmutableList.Builder<CompactionRecord> builder = ImmutableList.builder();
        history.forEach(builder::add);
        failHistory.forEach(builder::add);
        for (CompactionJob job : runningCompactions.values()) {
            builder.add(CompactionRecord.build(job));
        }
        return builder.build();
=======
    // get running compaction and history compaction, sorted by start time
    @NotNull
    List<CompactionRecord> getHistory() {
        List<CompactionRecord> list = new ArrayList<>();
        history.forEach(list::add);
        for (CompactionJob job : getRunningCompactions().values()) {
            list.add(CompactionRecord.build(job));
        }
        Collections.sort(list, new Comparator<CompactionRecord>() {
            @Override
            public int compare(CompactionRecord l, CompactionRecord r) {
                return l.getStartTs() > r.getStartTs() ? -1 : (l.getStartTs() < r.getStartTs()) ? 1 : 0;
            }
        });
        return list;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @NotNull
    public void cancelCompaction(long txnId) {
        for (Iterator<Map.Entry<PartitionIdentifier, CompactionJob>> iterator = runningCompactions.entrySet().iterator();
                iterator.hasNext(); ) {
            Map.Entry<PartitionIdentifier, CompactionJob> entry = iterator.next();
            CompactionJob job = entry.getValue();

            if (job.getTxnId() == txnId) {
                // just abort compaction task here, the background thread can abort transaction automatically
                job.abort();
                break;
            }
        }
    }

<<<<<<< HEAD
=======
    protected ConcurrentHashMap<PartitionIdentifier, CompactionJob> getRunningCompactions() {
        return runningCompactions;
    }

    public boolean existCompaction(long txnId) {
        for (Iterator<Map.Entry<PartitionIdentifier, CompactionJob>> iterator = getRunningCompactions().entrySet().iterator();
                iterator.hasNext(); ) {
            Map.Entry<PartitionIdentifier, CompactionJob> entry = iterator.next();
            CompactionJob job = entry.getValue();
            if (job.getTxnId() == txnId) {
                return true;
            }
        }
        return false;
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    private static class SynchronizedCircularQueue<E> {
        private CircularFifoQueue<E> q;

        SynchronizedCircularQueue(int size) {
            q = new CircularFifoQueue<>(size);
        }

        synchronized void changeMaxSize(int newMaxSize) {
            if (newMaxSize == q.maxSize()) {
                return;
            }
            CircularFifoQueue<E> newQ = new CircularFifoQueue<>(newMaxSize);
            for (E e : q) {
                newQ.offer(e);
            }
            q = newQ;
        }

        synchronized void offer(E e) {
            q.offer(e);
        }

        synchronized void forEach(Consumer<? super E> consumer) {
            q.forEach(consumer);
        }
    }
<<<<<<< HEAD
=======

    public boolean isTableDisabled(Long tableId) {
        return disabledTables.contains(tableId);
    }

    public void disableTables(String disableTablesStr) {
        Set<Long> newDisabledTables = new HashSet<>();
        if (!disableTablesStr.isEmpty()) {
            String[] arr = disableTablesStr.split(";");
            for (String a : arr) {
                try {
                    long l = Long.parseLong(a);
                    newDisabledTables.add(l);
                } catch (NumberFormatException e) {
                    LOG.warn("Bad format of disable tables string: {}, now is {}, should be like \"tableId1;tableId2\"",
                            e, disableTablesStr);
                    return;
                }
            }
        }
        disabledTables = Collections.unmodifiableSet(newDisabledTables);
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}
