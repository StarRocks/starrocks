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

import com.google.common.annotations.VisibleForTesting;
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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.CompactRequest;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
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
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

public class CompactionScheduler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(CompactionScheduler.class);
    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();
    private static final long LOOP_INTERVAL_MS = 1000L;
    private static final long PARTITION_CLEAN_INTERVAL_SECOND = 30;
    private final CompactionMgr compactionManager;
    private final SystemInfoService systemInfoService;
    private final GlobalTransactionMgr transactionMgr;
    private final GlobalStateMgr stateMgr;
    private final ConcurrentHashMap<PartitionIdentifier, CompactionJob> runningCompactions;
    private final SynchronizedCircularQueue<CompactionRecord> history;
    private long lastPartitionCleanTime;
    private Set<Long> disabledTables; // copy-on-write

    CompactionScheduler(@NotNull CompactionMgr compactionManager, @NotNull SystemInfoService systemInfoService,
                        @NotNull GlobalTransactionMgr transactionMgr, @NotNull GlobalStateMgr stateMgr,
                        @NotNull String disableTablesStr) {
        super("COMPACTION_DISPATCH", LOOP_INTERVAL_MS);
        this.compactionManager = compactionManager;
        this.systemInfoService = systemInfoService;
        this.transactionMgr = transactionMgr;
        this.stateMgr = stateMgr;
        this.runningCompactions = new ConcurrentHashMap<>();
        this.lastPartitionCleanTime = System.currentTimeMillis();
        this.history = new SynchronizedCircularQueue<>(Config.lake_compaction_history_size);
        this.disabledTables = Collections.unmodifiableSet(new HashSet<>());

        disableTables(disableTablesStr);
    }

    @Override
    protected void runOneCycle() {
        cleanPhysicalPartition();

        // Schedule compaction tasks only when this is a leader FE and all edit logs have finished replay.
        if (stateMgr.isLeader() && stateMgr.isReady()) {
            schedule();
            history.changeMaxSize(Config.lake_compaction_history_size);
        }
    }

    private void schedule() {
        // Check whether there are completed compaction jobs.
        for (Iterator<Map.Entry<PartitionIdentifier, CompactionJob>> iterator = runningCompactions.entrySet().iterator();
                iterator.hasNext(); ) {
            Map.Entry<PartitionIdentifier, CompactionJob> entry = iterator.next();
            PartitionIdentifier partition = entry.getKey();

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
                        assert job.transactionHasCommitted();
                    } catch (Exception e) {
                        LOG.error("Fail to commit compaction. {} error={}", job.getDebugString(), e.getMessage());
                        errorMsg = "fail to commit transaction: " + e.getMessage();
                    }
                } else if (taskResult == CompactionTask.TaskResult.PARTIAL_SUCCESS ||
                           taskResult == CompactionTask.TaskResult.NONE_SUCCESS) {
                    job.getPartition().setMinRetainVersion(0);
                    errorMsg = Objects.requireNonNull(job.getFailMessage(), "getFailMessage() is null");
                    LOG.error("Compaction job {} failed: {}", job.getDebugString(), errorMsg);
                    job.abort(); // Abort any executing task, if present.
                } else if (taskResult != CompactionTask.TaskResult.NOT_FINISHED) {
                    errorMsg = String.format("Unexpected compaction result: %s, %s", taskResult.name(), job.getDebugString());
                    LOG.error(errorMsg);
                }

                if (errorMsg != null) {
                    iterator.remove();
                    job.finish();
                    history.offer(CompactionRecord.build(job, errorMsg));
                    compactionManager.enableCompactionAfter(partition, Config.lake_min_compaction_interval_ms_on_failure);
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
                int factor = (statistics != null) ? statistics.getPunishFactor() : 1;
                compactionManager.enableCompactionAfter(partition, Config.lake_min_compaction_interval_ms_on_success * factor);
            }
        }

        // Create new compaction tasks.
        int compactionLimit = compactionTaskLimit();
        int numRunningTasks = runningCompactions.values().stream().mapToInt(CompactionJob::getNumTabletCompactionTasks).sum();
        if (numRunningTasks >= compactionLimit) {
            return;
        }

        List<PartitionStatisticsSnapshot> partitions = compactionManager.choosePartitionsToCompact(
                runningCompactions.keySet(), disabledTables);
        int index = 0;
        while (numRunningTasks < compactionLimit && index < partitions.size()) {
            PartitionStatisticsSnapshot partitionStatisticsSnapshot = partitions.get(index++);
            CompactionJob job = startCompaction(partitionStatisticsSnapshot);
            if (job == null) {
                continue;
            }
            numRunningTasks += job.getNumTabletCompactionTasks();
            runningCompactions.put(partitionStatisticsSnapshot.getPartition(), job);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created new compaction job. {}, txnId={}",
                        partitionStatisticsSnapshot.toString(), job.getTxnId());
            }
        }
    }

    private void abortTransactionIgnoreException(CompactionJob job, String reason) {
        try {
            List<TabletCommitInfo> finishedTablets = job.buildTabletCommitInfo();
            transactionMgr.abortTransaction(job.getDb().getId(), job.getTxnId(), reason, finishedTablets,
                    Collections.emptyList(), null);
        } catch (StarRocksException ex) {
            LOG.error("Fail to abort txn " + job.getTxnId(), ex);
        }
    }

    @VisibleForTesting
    protected int compactionTaskLimit() {
        if (Config.lake_compaction_max_tasks >= 0) {
            return Config.lake_compaction_max_tasks;
        }
        WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = manager.getCompactionWarehouse();
        List<ComputeNode> aliveComputeNodes = manager.getAliveComputeNodes(warehouse.getId());
        return aliveComputeNodes.size() * 16;
    }

    private void cleanPhysicalPartition() {
        long now = System.currentTimeMillis();
        if (now - lastPartitionCleanTime >= PARTITION_CLEAN_INTERVAL_SECOND * 1000L) {
            compactionManager.getAllPartitions()
                    .stream()
                    .filter(p -> !MetaUtils.isPhysicalPartitionExist(stateMgr, p.getDbId(), p.getTableId(), p.getPartitionId()))
                    .filter(p -> !runningCompactions.containsKey(p)) // Ignore those partitions in runningCompactions
                    .forEach(compactionManager::removePartition);
            lastPartitionCleanTime = now;
        }
    }

    private CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot) {
        PartitionIdentifier partitionIdentifier = partitionStatisticsSnapshot.getPartition();
        Database db = stateMgr.getLocalMetastore().getDb(partitionIdentifier.getDbId());
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
        locker.lockDatabase(db.getId(), LockType.READ);

        try {
            // lake table or lake materialized view
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), partitionIdentifier.getTableId());
            // Compact a table of SCHEMA_CHANGE state does not make much sense, because the compacted data
            // will not be used after the schema change job finished.
            if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                compactionManager.enableCompactionAfter(partitionIdentifier, Config.lake_min_compaction_interval_ms_on_failure);
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
                compactionManager.enableCompactionAfter(partitionIdentifier, Config.lake_min_compaction_interval_ms_on_failure);
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
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        long nextCompactionInterval = Config.lake_min_compaction_interval_ms_on_success;
        CompactionJob job = new CompactionJob(db, table, partition, txnId, Config.lake_compaction_allow_partial_success);
        try {
            List<CompactionTask> tasks = createCompactionTasks(currentVersion, beToTablets, txnId,
                    job.getAllowPartialSuccess(), partitionStatisticsSnapshot.getPriority());
            for (CompactionTask task : tasks) {
                task.sendRequest();
            }
            job.setTasks(tasks);
            return job;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            partition.setMinRetainVersion(0);
            nextCompactionInterval = Config.lake_min_compaction_interval_ms_on_failure;
            abortTransactionIgnoreError(job, e.getMessage());
            job.finish();
            history.offer(CompactionRecord.build(job, e.getMessage()));
            return null;
        } finally {
            compactionManager.enableCompactionAfter(partitionIdentifier, nextCompactionInterval);
        }
    }

    @NotNull
    private List<CompactionTask> createCompactionTasks(long currentVersion, Map<Long, List<Long>> beToTablets, long txnId,
            boolean allowPartialSuccess, PartitionStatistics.CompactionPriority priority)
            throws StarRocksException, RpcException {
        List<CompactionTask> tasks = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            ComputeNode node = systemInfoService.getBackendOrComputeNode(entry.getKey());
            if (node == null) {
                throw new StarRocksException("Node " + entry.getKey() + " has been dropped");
            }

            LakeService service = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());

            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = currentVersion;
            request.timeoutMs = LakeService.TIMEOUT_COMPACT;
            request.allowPartialSuccess = allowPartialSuccess;
            request.encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();
            request.forceBaseCompaction = (priority == PartitionStatistics.CompactionPriority.MANUAL_COMPACT);

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
                WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Warehouse warehouse = manager.getCompactionWarehouse();
                ComputeNode computeNode = manager.getComputeNodeAssignedToTablet(warehouse.getName(), (LakeTablet) tablet);
                if (computeNode == null) {
                    beToTablets.clear();
                    return beToTablets;
                }

                beToTablets.computeIfAbsent(computeNode.getId(), k -> Lists.newArrayList()).add(tablet.getId());
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

        WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = manager.getCompactionWarehouse();
        return transactionMgr.beginTransaction(dbId, Lists.newArrayList(tableId), label, coordinator,
                loadJobSourceType, Config.lake_compaction_default_timeout_second, warehouse.getId());
    }

    private void commitCompaction(PartitionIdentifier partition, CompactionJob job, boolean forceCommit)
            throws StarRocksException {
        List<TabletCommitInfo> commitInfoList = job.buildTabletCommitInfo();

        Database db = stateMgr.getLocalMetastore().getDb(partition.getDbId());
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
        }
        job.setVisibleStateWaiter(waiter);
        job.setCommitTs(System.currentTimeMillis());
    }

    private void abortTransactionIgnoreError(CompactionJob job, String reason) {
        try {
            List<TabletCommitInfo> finishedTablets = job.buildTabletCommitInfo();
            transactionMgr.abortTransaction(job.getDb().getId(), job.getTxnId(), reason, finishedTablets,
                    Collections.emptyList(), null);
        } catch (StarRocksException ex) {
            LOG.error(ex);
        }
    }

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
}
