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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.CompactRequest;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.VisibleStateWaiter;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

public class CompactionScheduler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(CompactionScheduler.class);
    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();
    private static final long LOOP_INTERVAL_MS = 200L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS = LOOP_INTERVAL_MS * 2;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_FAILURE = LOOP_INTERVAL_MS * 10;
    private static final long PARTITION_CLEAN_INTERVAL_SECOND = 30;
    private final CompactionMgr compactionManager;
    private final SystemInfoService systemInfoService;
    private final GlobalTransactionMgr transactionMgr;
    private final GlobalStateMgr stateMgr;
    private final ConcurrentHashMap<PartitionIdentifier, CompactionJob> runningCompactions;
    private final SynchronizedCircularQueue<CompactionRecord> history;
    private final SynchronizedCircularQueue<CompactionRecord> failHistory;
    private boolean finishedWaiting = false;
    private long waitTxnId = -1;
    private long lastPartitionCleanTime;

    CompactionScheduler(@NotNull CompactionMgr compactionManager, @NotNull SystemInfoService systemInfoService,
                        @NotNull GlobalTransactionMgr transactionMgr, @NotNull GlobalStateMgr stateMgr) {
        super("COMPACTION_DISPATCH", LOOP_INTERVAL_MS);
        this.compactionManager = compactionManager;
        this.systemInfoService = systemInfoService;
        this.transactionMgr = transactionMgr;
        this.stateMgr = stateMgr;
        this.runningCompactions = new ConcurrentHashMap<>();
        this.lastPartitionCleanTime = System.currentTimeMillis();
        this.history = new SynchronizedCircularQueue<>(Config.lake_compaction_history_size);
        this.failHistory = new SynchronizedCircularQueue<>(Config.lake_compaction_fail_history_size);
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
            failHistory.changeMaxSize(Config.lake_compaction_fail_history_size);
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
            CompactionJob job = entry.getValue();

            if (!job.transactionHasCommitted()) {
                String errorMsg = null;

                if (job.isCompleted()) {
                    job.getPartition().setMinRetainVersion(0);
                    try {
                        commitCompaction(partition, job);
                        assert job.transactionHasCommitted();
                    } catch (Exception e) {
                        LOG.error("Fail to commit compaction. {} error={}", job.getDebugString(), e.getMessage());
                        errorMsg = "fail to commit transaction: " + e.getMessage();
                    }
                } else if (job.isFailed()) {
                    job.getPartition().setMinRetainVersion(0);
                    errorMsg = Objects.requireNonNull(job.getFailMessage(), "getFailMessage() is null");
                    LOG.error("Compaction job {} failed: {}", job.getDebugString(), errorMsg);
                    job.abort(); // Abort any executing task, if present.
                }

                if (errorMsg != null) {
                    iterator.remove();
                    job.finish();
                    failHistory.offer(CompactionRecord.build(job, errorMsg));
                    compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                    abortTransactionIgnoreException(job, errorMsg);
                    continue;
                }
            }

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
                compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS);
            }
        }

        // Create new compaction tasks.
        int index = 0;
        int compactionLimit = compactionTaskLimit();
        int numRunningTasks = runningCompactions.values().stream().mapToInt(CompactionJob::getNumTabletCompactionTasks).sum();
        if (numRunningTasks >= compactionLimit) {
            return;
        }

        List<PartitionIdentifier> partitions = compactionManager.choosePartitionsToCompact(runningCompactions.keySet());
        while (numRunningTasks < compactionLimit && index < partitions.size()) {
            PartitionIdentifier partition = partitions.get(index++);
            CompactionJob job = startCompaction(partition);
            if (job == null) {
                continue;
            }
            numRunningTasks += job.getNumTabletCompactionTasks();
            runningCompactions.put(partition, job);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created new compaction job. partition={} txnId={}", partition, job.getTxnId());
            }
        }
    }

    private void abortTransactionIgnoreException(CompactionJob job, String reason) {
        try {
            List<TabletCommitInfo> finishedTablets = job.buildTabletCommitInfo();
            transactionMgr.abortTransaction(job.getDb().getId(), job.getTxnId(), reason, finishedTablets,
                    Collections.emptyList(), null);
        } catch (UserException ex) {
            LOG.error("Fail to abort txn " + job.getTxnId(), ex);
        }
    }

    private int compactionTaskLimit() {
        if (Config.lake_compaction_max_tasks >= 0) {
            return Config.lake_compaction_max_tasks;
        }
        return (systemInfoService.getAliveBackendNumber() +
                systemInfoService.getAliveComputeNodeNumber()) * 16;
    }

    private void cleanPartition() {
        long now = System.currentTimeMillis();
        if (now - lastPartitionCleanTime >= PARTITION_CLEAN_INTERVAL_SECOND * 1000L) {
            compactionManager.getAllPartitions()
                    .stream()
                    .filter(p -> !isPartitionExist(p))
                    .filter(p -> !runningCompactions.containsKey(p)) // Ignore those partitions in runningCompactions
                    .forEach(compactionManager::removePartition);
            lastPartitionCleanTime = now;
        }
    }

    private boolean isPartitionExist(PartitionIdentifier partition) {
        Database db = stateMgr.getDb(partition.getDbId());
        if (db == null) {
            return false;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            // lake table or lake materialized view
            OlapTable table = (OlapTable) db.getTable(partition.getTableId());
            return table != null && table.getPhysicalPartition(partition.getPartitionId()) != null;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    private CompactionJob startCompaction(PartitionIdentifier partitionIdentifier) {
        Database db = stateMgr.getDb(partitionIdentifier.getDbId());
        if (db == null) {
            compactionManager.removePartition(partitionIdentifier);
            return null;
        }

        long txnId;
        long currentVersion;
        OlapTable table;
        PhysicalPartition partition;
        Map<Long, List<Long>> beToTablets;

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);

        try {
            // lake table or lake materialized view
            table = (OlapTable) db.getTable(partitionIdentifier.getTableId());
            // Compact a table of SCHEMA_CHANGE state does not make much sense, because the compacted data
            // will not be used after the schema change job finished.
            if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                compactionManager.enableCompactionAfter(partitionIdentifier, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                return null;
            }
            partition = (table != null) ? table.getPhysicalPartition(partitionIdentifier.getPartitionId()) : null;
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

        } catch (RunningTxnExceedException | AnalysisException | LabelAlreadyUsedException | DuplicatedRequestException e) {
            LOG.error("Fail to create transaction for compaction job. {}", e.getMessage());
            return null;
        } catch (Throwable e) {
            LOG.error("Unknown error: {}", e.getMessage());
            return null;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        long nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS;
        CompactionJob job = new CompactionJob(db, table, partition, txnId);
        try {
            List<CompactionTask> tasks = createCompactionTasks(currentVersion, beToTablets, txnId);
            for (CompactionTask task : tasks) {
                task.sendRequest();
            }
            job.setTasks(tasks);
            return job;
        } catch (Exception e) {
            LOG.error(e);
            partition.setMinRetainVersion(0);
            nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_FAILURE;
            abortTransactionIgnoreError(job, e.getMessage());
            job.finish();
            failHistory.offer(CompactionRecord.build(job, e.getMessage()));
            return null;
        } finally {
            compactionManager.enableCompactionAfter(partitionIdentifier, nextCompactionInterval);
        }
    }

    @NotNull
    private List<CompactionTask> createCompactionTasks(long currentVersion, Map<Long, List<Long>> beToTablets, long txnId)
            throws UserException, RpcException {
        List<CompactionTask> tasks = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            ComputeNode node = systemInfoService.getBackendOrComputeNode(entry.getKey());
            if (node == null) {
                throw new UserException("Node " + entry.getKey() + " has been dropped");
            }

            LakeService service = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());

            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = currentVersion;
            request.timeoutMs = LakeService.TIMEOUT_COMPACT;

            CompactionTask task = new CompactionTask(node.getId(), service, request);
            tasks.add(task);
        }
        return tasks;
    }

    @NotNull
    private Map<Long, List<Long>> collectPartitionTablets(PhysicalPartition partition) {
        List<MaterializedIndex> visibleIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        for (MaterializedIndex index : visibleIndexes) {
            for (Tablet tablet : index.getTablets()) {
                Long beId = Utils.chooseBackend((LakeTablet) tablet);
                if (beId == null) {
                    beToTablets.clear();
                    return beToTablets;
                }
                beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
            }
        }
        return beToTablets;
    }

    // REQUIRE: has acquired the exclusive lock of Database.
    protected long beginTransaction(PartitionIdentifier partition)
            throws RunningTxnExceedException, AnalysisException, LabelAlreadyUsedException, DuplicatedRequestException {
        long dbId = partition.getDbId();
        long tableId = partition.getTableId();
        long partitionId = partition.getPartitionId();
        long currentTs = System.currentTimeMillis();
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.LAKE_COMPACTION;
        TransactionState.TxnSourceType txnSourceType = TransactionState.TxnSourceType.FE;
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(txnSourceType, HOST_NAME);
        String label = String.format("COMPACTION_%d-%d-%d-%d", dbId, tableId, partitionId, currentTs);
        return transactionMgr.beginTransaction(dbId, Lists.newArrayList(tableId), label, coordinator,
                loadJobSourceType, Config.lake_compaction_default_timeout_second);
    }

    private void commitCompaction(PartitionIdentifier partition, CompactionJob job)
            throws UserException {
        Preconditions.checkState(job.isCompleted());

        List<TabletCommitInfo> commitInfoList = job.buildTabletCommitInfo();

        Database db = stateMgr.getDb(partition.getDbId());
        if (db == null) {
            throw new MetaNotFoundException("database not exist");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing compaction transaction. partition={} txnId={}", partition, job.getTxnId());
        }

        VisibleStateWaiter waiter;

        TransactionState transactionState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionState(db.getId(), job.getTxnId());
        List<Long> tableIdList = transactionState.getTableIdList();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, tableIdList, LockType.WRITE);
        try {
            waiter = transactionMgr.commitTransaction(db.getId(), job.getTxnId(), commitInfoList,
                    Collections.emptyList(), null);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, tableIdList, LockType.WRITE);
        }
        job.setVisibleStateWaiter(waiter);
        job.setCommitTs(System.currentTimeMillis());
    }

    private void abortTransactionIgnoreError(CompactionJob job, String reason) {
        try {
            List<TabletCommitInfo> finishedTablets = job.buildTabletCommitInfo();
            transactionMgr.abortTransaction(job.getDb().getId(), job.getTxnId(), reason, finishedTablets,
                    Collections.emptyList(), null);
        } catch (UserException ex) {
            LOG.error(ex);
        }
    }

    @NotNull
    List<CompactionRecord> getHistory() {
        ImmutableList.Builder<CompactionRecord> builder = ImmutableList.builder();
        history.forEach(builder::add);
        failHistory.forEach(builder::add);
        for (CompactionJob job : runningCompactions.values()) {
            builder.add(CompactionRecord.build(job));
        }
        return builder.build();
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
}
