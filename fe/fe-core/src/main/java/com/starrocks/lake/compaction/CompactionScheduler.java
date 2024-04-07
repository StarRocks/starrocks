// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.Daemon;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.VisibleStateWaiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public class CompactionScheduler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(CompactionScheduler.class);
    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();
    private static final long LOOP_INTERVAL_MS = 500L;
    private static final long TXN_TIMEOUT_SECOND = 86400L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS = 3000L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_FAILURE = 6000L;
    private static final long PARTITION_CLEAN_INTERVAL_SECOND = 30;

    private boolean finishedWaiting = false;
    private long waitTxnId = -1;
    private final CompactionManager compactionManager;
    private final SystemInfoService systemInfoService;
    private final GlobalTransactionMgr transactionMgr;
    private final GlobalStateMgr stateMgr;
    private final Map<PartitionIdentifier, CompactionContext> runningCompactions;
    private long lastPartitionCleanTime;

    CompactionScheduler(@NotNull CompactionManager compactionManager, @NotNull SystemInfoService systemInfoService,
                        @NotNull GlobalTransactionMgr transactionMgr, @NotNull GlobalStateMgr stateMgr) {
        super("COMPACTION_DISPATCH", LOOP_INTERVAL_MS);
        this.compactionManager = compactionManager;
        this.systemInfoService = systemInfoService;
        this.transactionMgr = transactionMgr;
        this.stateMgr = stateMgr;
        this.runningCompactions = Maps.newHashMap();
        this.lastPartitionCleanTime = System.currentTimeMillis();
    }

    @Override
    protected void runOneCycle() {
        cleanPartition();

        // Schedule compaction tasks only when this is a leader FE and all edit logs have finished replay.
        // In order to ensure that the input rowsets of compaction still exists when doing publishing version, it is
        // necessary to ensure that the compaction task of the same partition is executed serially, that is, the next
        // compaction task can be executed only after the status of the previous compaction task changes to visible or canceled.
        if (stateMgr.isLeader() && stateMgr.isReady() && allCommittedTransactionsBeforeRestartHaveFinished()) {
            schedule();
        }
    }

    // Returns true if all transactions committed before this restart have finished(i.e., of VISIBLE state).
    // Technically, we only need to wait for compaction transactions finished, but I don't wanna to check the
    // type of each transaction.
    private boolean allCommittedTransactionsBeforeRestartHaveFinished() {
        if (finishedWaiting) {
            return true;
        }
        // Note: must call getMinActiveTxnId() before getNextTransactionId(), otherwise if there are no running transactions
        // waitTxnId <= minActiveTxnId will always be false.
        long minActiveTxnId = transactionMgr.getMinActiveTxnId();
        if (waitTxnId < 0) {
            waitTxnId = transactionMgr.getTransactionIDGenerator().getNextTransactionId();
        }
        finishedWaiting = waitTxnId <= minActiveTxnId;
        return finishedWaiting;
    }

    private void schedule() {
        // Check whether there are completed compaction jobs.
        for (Iterator<Map.Entry<PartitionIdentifier, CompactionContext>> iterator = runningCompactions.entrySet().iterator();
                iterator.hasNext(); ) {
            Map.Entry<PartitionIdentifier, CompactionContext> entry = iterator.next();
            PartitionIdentifier partition = entry.getKey();
            CompactionContext context = entry.getValue();

            if (context.compactionFinishedOnBE() && !context.transactionHasCommitted()) {
                try {
                    commitCompaction(partition, context);
                } catch (Exception e) {
                    iterator.remove();
                    compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                    LOG.error("Fail to commit compaction. {} error={}", context.getDebugString(), e.getMessage());
                    continue;
                }
            }

            if (context.transactionHasCommitted() && context.waitTransactionVisible(100, TimeUnit.MILLISECONDS)) {
                context.setCommitTs(System.currentTimeMillis());
                iterator.remove();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Removed published compaction. {} cost={}ms running={}", context.getDebugString(),
                            (context.getCommitTs() - context.getStartTs()), runningCompactions.size());
                }
                compactionManager.enableCompactionAfter(partition, MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS);
            }
        }

        // Create new compaction tasks.
        int index = 0;
        int compactionLimit = compactionTaskLimit();
        int numRunningTasks = runningCompactions.values().stream().mapToInt(CompactionContext::getNumCompactionTasks).sum();
        if (numRunningTasks >= compactionLimit) {
            return;
        }

        List<PartitionIdentifier> partitions = compactionManager.choosePartitionsToCompact(runningCompactions.keySet());
        while (numRunningTasks < compactionLimit && index < partitions.size()) {
            PartitionIdentifier partition = partitions.get(index++);
            CompactionContext context = startCompaction(partition);
            if (context == null) {
                continue;
            }
            numRunningTasks += context.getNumCompactionTasks();
            runningCompactions.put(partition, context);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created new compaction job. partition={} txnId={}", partition, context.getTxnId());
            }
        }
    }

    private int compactionTaskLimit() {
        if (Config.lake_compaction_max_tasks >= 0) {
            return Config.lake_compaction_max_tasks;
        }
        return systemInfoService.getAliveBackendNumber() * 16;
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
        db.readLock();
        try {
            LakeTable table = (LakeTable) db.getTable(partition.getTableId());
            return table != null && table.getPartition(partition.getPartitionId()) != null;
        } finally {
            db.readUnlock();
        }
    }

    private CompactionContext startCompaction(PartitionIdentifier partitionIdentifier) {
        Database db = stateMgr.getDb(partitionIdentifier.getDbId());
        if (db == null) {
            compactionManager.removePartition(partitionIdentifier);
            return null;
        }

        if (!db.tryReadLock(50, TimeUnit.MILLISECONDS)) {
            LOG.info("Skipped partition compaction due to get database lock timeout");
            compactionManager.enableCompactionAfter(partitionIdentifier, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
            return null;
        }

        long txnId;
        long currentVersion;
        LakeTable table;
        Partition partition;
        Map<Long, List<Long>> beToTablets;

        try {
            table = (LakeTable) db.getTable(partitionIdentifier.getTableId());
            // Compact a table of SCHEMA_CHANGE state does not make much sense, because the compacted data
            // will not be used after the schema change job finished.
            if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                compactionManager.enableCompactionAfter(partitionIdentifier, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
                return null;
            }
            partition = (table != null) ? table.getPartition(partitionIdentifier.getPartitionId()) : null;
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
        } catch (BeginTransactionException | AnalysisException | LabelAlreadyUsedException | DuplicatedRequestException e) {
            LOG.error("Fail to create transaction for compaction job. {}", e.getMessage());
            return null;
        } catch (Throwable e) {
            LOG.error("Unknown error: {}", e.getMessage());
            return null;
        } finally {
            db.readUnlock();
        }

        CompactionContext context = new CompactionContext();
        context.setTxnId(txnId);
        context.setBeToTablets(beToTablets);
        context.setStartTs(System.currentTimeMillis());
        context.setPartitionName(String.format("%s.%s.%s", db.getFullName(), table.getName(), partition.getName()));

        long nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS;
        try {
            List<Future<CompactResponse>> futures = compactTablets(currentVersion, beToTablets, txnId);
            context.setResponseList(futures);
            return context;
        } catch (Exception e) {
            LOG.error(e);
            nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_FAILURE;
            abortTransactionIgnoreError(db.getId(), txnId, e.getMessage());
            return null;
        } finally {
            compactionManager.enableCompactionAfter(partitionIdentifier, nextCompactionInterval);
        }
    }

    @NotNull
    private List<Future<CompactResponse>> compactTablets(long currentVersion, Map<Long, List<Long>> beToTablets, long txnId)
            throws UserException {
        List<Future<CompactResponse>> futures = Lists.newArrayListWithCapacity(beToTablets.size());
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            Backend backend = systemInfoService.getBackend(entry.getKey());
            if (backend == null) {
                throw new UserException("Backend " + entry.getKey() + " has been dropped");
            }
            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = currentVersion;

            LakeService service = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
            futures.add(service.compact(request));
        }
        return futures;
    }

    @NotNull
    private Map<Long, List<Long>> collectPartitionTablets(Partition partition) {
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
    private long beginTransaction(PartitionIdentifier partition)
            throws BeginTransactionException, AnalysisException, LabelAlreadyUsedException, DuplicatedRequestException {
        long dbId = partition.getDbId();
        long tableId = partition.getTableId();
        long partitionId = partition.getPartitionId();
        long currentTs = System.currentTimeMillis();
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.LAKE_COMPACTION;
        TransactionState.TxnSourceType txnSourceType = TransactionState.TxnSourceType.FE;
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(txnSourceType, HOST_NAME);
        String label = String.format("COMPACTION_%d-%d-%d-%d", dbId, tableId, partitionId, currentTs);
        return transactionMgr.beginTransaction(dbId, Lists.newArrayList(tableId), label, coordinator,
                loadJobSourceType, TXN_TIMEOUT_SECOND);
    }

    private void commitCompaction(PartitionIdentifier partition, CompactionContext context)
            throws UserException, ExecutionException, InterruptedException {
        Preconditions.checkState(context.compactionFinishedOnBE());

        for (Future<CompactResponse> responseFuture : context.getResponseList()) {
            CompactResponse response = responseFuture.get();
            if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                throw new UserException("Fail to compact tablet " + response.failedTablets);
            }
        }

        List<TabletCommitInfo> commitInfoList = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : context.getBeToTablets().entrySet()) {
            for (Long tabletId : entry.getValue()) {
                commitInfoList.add(new TabletCommitInfo(tabletId, entry.getKey()));
            }
        }

        Database db = stateMgr.getDb(partition.getDbId());
        if (db == null) {
            throw new MetaNotFoundException("database not exist");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing compaction transaction. partition={} txnId={}", partition, context.getTxnId());
        }

        VisibleStateWaiter waiter;
        db.writeLock();
        try {
            waiter = transactionMgr.commitTransaction(db.getId(), context.getTxnId(), commitInfoList, Lists.newArrayList());
        } finally {
            db.writeUnlock();
        }
        context.setVisibleStateWaiter(waiter);
        context.setCommitTs(System.currentTimeMillis());
        if (LOG.isDebugEnabled()) {
            long numInputBytes = 0;
            long numInputRows = 0;
            long numOutputBytes = 0;
            long numOutputRows = 0;
            for (Future<CompactResponse> responseFuture : context.getResponseList()) {
                CompactResponse response = responseFuture.get();
                numInputBytes += response.numInputBytes;
                numInputRows += response.numInputRows;
                numOutputBytes += response.numOutputBytes;
                numOutputRows += response.numOutputRows;
            }
            LOG.debug("Committed compaction. {} inputBytes={} inputRows={} outputBytes={} outputRows={} time={}",
                    context.getDebugString(),
                    numInputBytes,
                    numInputRows,
                    numOutputBytes,
                    numOutputRows,
                    (context.getCommitTs() - context.getStartTs()));
        }
    }

    private void abortTransactionIgnoreError(long dbId, long txnId, String reason) {
        try {
            transactionMgr.abortTransaction(dbId, txnId, reason);
        } catch (UserException ex) {
            LOG.error(ex);
        }
    }

    private static class CompactionContext {
        private long txnId;
        private long startTs;
        private long commitTs;
        private String partitionName;
        private Map<Long, List<Long>> beToTablets;
        private List<Future<CompactResponse>> responseList;
        private VisibleStateWaiter visibleStateWaiter;

        CompactionContext() {
            responseList = Lists.newArrayList();
        }

        void setTxnId(long txnId) {
            this.txnId = txnId;
        }

        long getTxnId() {
            return txnId;
        }

        void setResponseList(@NotNull List<Future<CompactResponse>> futures) {
            responseList = futures;
        }

        List<Future<CompactResponse>> getResponseList() {
            return responseList;
        }

        void setVisibleStateWaiter(VisibleStateWaiter visibleStateWaiter) {
            this.visibleStateWaiter = visibleStateWaiter;
        }

        boolean waitTransactionVisible(long timeout, TimeUnit unit) {
            return visibleStateWaiter.await(timeout, unit);
        }

        boolean transactionHasCommitted() {
            return visibleStateWaiter != null;
        }

        void setBeToTablets(@NotNull Map<Long, List<Long>> beToTablets) {
            this.beToTablets = beToTablets;
        }

        Map<Long, List<Long>> getBeToTablets() {
            return beToTablets;
        }

        boolean compactionFinishedOnBE() {
            return responseList.stream().allMatch(Future::isDone);
        }

        int getNumCompactionTasks() {
            return beToTablets.values().stream().mapToInt(List::size).sum();
        }

        void setStartTs(long startTs) {
            this.startTs = startTs;
        }

        long getStartTs() {
            return startTs;
        }

        void setCommitTs(long commitTs) {
            this.commitTs = commitTs;
        }

        long getCommitTs() {
            return commitTs;
        }

        void setPartitionName(String partitionName) {
            this.partitionName = partitionName;
        }

        String getPartitionName() {
            return partitionName;
        }

        String getDebugString() {
            return String.format("TxnId=%d partition=%s", txnId, partitionName);
        }
    }
}
