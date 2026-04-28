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
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.proto.AggregateCompactRequest;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.ComputeNodePB;
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
import com.starrocks.warehouse.cngroup.ComputeResource;
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
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class CompactionScheduler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(CompactionScheduler.class);
    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();
    private static final long LOOP_INTERVAL_MS = 1000L;
    public static long PARTITION_CLEAN_INTERVAL_SECOND = 30;
    private final CompactionMgr compactionManager;
    private final SystemInfoService systemInfoService;
    private final GlobalTransactionMgr transactionMgr;
    private final GlobalStateMgr stateMgr;
    private final ConcurrentHashMap<PartitionIdentifier, CompactionJob> runningCompactions;
    private final SynchronizedCircularQueue<CompactionRecord> history;
    private long lastPartitionCleanTime;
    private Set<Long> disabledIds; // copy-on-write, table id or partition id

    CompactionScheduler(@NotNull CompactionMgr compactionManager, @NotNull SystemInfoService systemInfoService,
                        @NotNull GlobalTransactionMgr transactionMgr, @NotNull GlobalStateMgr stateMgr,
                        @NotNull String disableIdsStr) {
        super("compaction-dispatch", LOOP_INTERVAL_MS);
        this.compactionManager = compactionManager;
        this.systemInfoService = systemInfoService;
        this.transactionMgr = transactionMgr;
        this.stateMgr = stateMgr;
        this.runningCompactions = new ConcurrentHashMap<>();
        this.lastPartitionCleanTime = System.currentTimeMillis();
        this.history = new SynchronizedCircularQueue<>(Config.lake_compaction_history_size);
        this.disabledIds = Collections.unmodifiableSet(new HashSet<>());

        disableTableOrPartitionId(disableIdsStr);
    }

    @Override
    protected void runOneCycle() {
        if (FeConstants.runningUnitTest)  {
            return;
        }

        List<PartitionIdentifier> deletedPartitionIdentifiers = cleanPhysicalPartition();

        // Schedule compaction tasks only when this is a leader FE and all edit logs have finished replay.
        if (stateMgr.isLeader() && stateMgr.isReady()) {
            abortStaleCompaction(deletedPartitionIdentifiers);
            scheduleNewCompaction();
            if (Config.enable_lake_autonomous_compaction) {
                if (Config.enable_file_bundling) {
                    // Aggregate publish path uses a different RPC (aggregate_compact /
                    // aggregate_publish_version) that the autonomous COLLECT_AND_PUBLISH
                    // flow does not understand. Mixing both would cause two publish
                    // sources to race for the same partition. Log and skip.
                    LOG.warn("enable_lake_autonomous_compaction is ON but enable_file_bundling is also ON; "
                            + "skipping autonomous publish to avoid conflicts. "
                            + "Disable file bundling to use autonomous compaction.");
                } else {
                    schedulePartitionPublish();
                }
            }
            history.changeMaxSize(Config.lake_compaction_history_size);
        }
    }

    /**
     * For each partition not currently busy with a COMPACT_AND_PUBLISH job, evaluate the
     * three autonomous-compaction trigger strategies. If any matches, build and dispatch
     * a PUBLISH_ONLY job that asks every owning BE to drain its local CompactionResultPB
     * cache for this partition and publish_version with force_publish=true so all tablets
     * reach the same new_version even when some have no local results.
     */
    private void schedulePartitionPublish() {
        for (PartitionIdentifier partition : compactionManager.getAllPartitions()) {
            if (runningCompactions.containsKey(partition)) {
                continue; // partition is busy with COMPACT_AND_PUBLISH or an earlier PUBLISH_ONLY
            }
            if (disabledIds.contains(partition.getTableId())) {
                continue;
            }
            PartitionStatistics stats = compactionManager.getStatistics(partition);
            if (stats == null) {
                continue;
            }
            PartitionStatisticsSnapshot snapshot = stats.getSnapshot();
            long currentVersion = stats.getCurrentVersion() == null ? 0L : stats.getCurrentVersion().getVersion();
            long lastPublishVersion = stats.getLastPublishVisibleVersion();
            long versionDelta = currentVersion - lastPublishVersion;
            if (versionDelta <= 0) {
                continue;
            }
            double lastScore = stats.getLastPublishScore();
            long now = System.currentTimeMillis();
            long timeSinceLastPublish = now - stats.getLastPublishTimeMs();

            boolean trigger = false;
            String reason = null;
            if (versionDelta >= Config.lake_compaction_version_delta_threshold) {
                trigger = true;
                reason = "version_delta>=" + Config.lake_compaction_version_delta_threshold;
            } else if (lastScore > Config.lake_compaction_high_score_threshold &&
                    versionDelta >= Config.lake_compaction_min_version_delta_for_high_score) {
                trigger = true;
                reason = "high_score(" + lastScore + ")&&version_delta>="
                        + Config.lake_compaction_min_version_delta_for_high_score;
            } else if (stats.getLastPublishTimeMs() > 0 &&
                    timeSinceLastPublish > Config.lake_compaction_max_interval_ms) {
                // Skip max-interval check until the first successful publish records a
                // timestamp; otherwise lastPublishTimeMs == 0 makes "now - 0" always
                // exceed the interval and bursts trigger on every partition the
                // moment the feature is enabled.
                trigger = true;
                reason = "max_interval_ms exceeded";
            }
            if (!trigger) {
                continue;
            }

            CompactionJob job = startPublishOnly(snapshot, currentVersion, reason);
            if (job != null) {
                // NOTE: lastPublish* markers are intentionally NOT updated here. Updating
                // before the txn becomes VISIBLE leaves stale state if commit/publish
                // fails: version_delta would compute as 0 and the partition would never
                // be retried until a new visible version appears. The completion handler
                // in scheduleNewCompaction updates these once the txn reaches VISIBLE.
                runningCompactions.put(partition, job);
            }
        }
    }

    /**
     * Bring up a PUBLISH_ONLY {@link CompactionJob}: open a transaction, build per-BE
     * CompactRequest in COLLECT_AND_PUBLISH mode, dispatch them. Returns null on failure;
     * caller must NOT register a null result.
     */
    private CompactionJob startPublishOnly(PartitionStatisticsSnapshot snapshot, long visibleVersion, String reason) {
        PartitionIdentifier partitionIdentifier = snapshot.getPartition();
        Database db = stateMgr.getLocalMetastore().getDb(partitionIdentifier.getDbId());
        if (db == null) {
            compactionManager.removePartition(partitionIdentifier);
            return null;
        }
        long txnId;
        long newVersion;
        OlapTable table;
        PhysicalPartition partition;
        Map<Long, List<Long>> beToTablets;
        ComputeResource computeResource;
        Warehouse warehouse;
        try {
            computeResource = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getCompactionComputeResource(partitionIdentifier.getTableId());
            warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(computeResource.getWarehouseId());
        } catch (ErrorReportException e) {
            LOG.debug("Resolve warehouse for autonomous publish failed partition={} err={}",
                    partitionIdentifier, e.getMessage());
            return null;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            table = (OlapTable) stateMgr.getLocalMetastore()
                    .getTable(db.getId(), partitionIdentifier.getTableId());
            if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                // Don't autonomous-publish during schema change; cross-version apply with
                // alter_metadata is unsupported and FE-side schema change uses its own version sequence.
                return null;
            }
            partition = (table != null) ? table.getPhysicalPartition(partitionIdentifier.getPartitionId()) : null;
            if (partition == null) {
                compactionManager.removePartition(partitionIdentifier);
                return null;
            }
            beToTablets = collectPartitionTablets(partition, computeResource);
            if (beToTablets.isEmpty()) {
                return null;
            }
            txnId = beginTransaction(partitionIdentifier, partition, computeResource);
            // Existing flow: visible_version moves forward by exactly one per transaction.
            // The FE transaction manager assigns the actual commit version internally;
            // BE only needs to know base_version (visible) and new_version for publish_version.
            newVersion = visibleVersion + 1;
            partition.setMinRetainVersion(visibleVersion);
        } catch (RunningTxnExceedException | AnalysisException | LabelAlreadyUsedException
                | DuplicatedRequestException e) {
            LOG.warn("Fail to begin txn for autonomous publish partition={} reason={} err={}",
                    partitionIdentifier, reason, e.getMessage());
            return null;
        } catch (Throwable e) {
            LOG.warn("Unexpected error in autonomous publish setup partition={} err={}", partitionIdentifier,
                    e.getMessage());
            return null;
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        CompactionJob job = new CompactionJob(db, table, partition, txnId, /*allowPartialSuccess=*/true,
                computeResource, warehouse.getName());
        job.setJobType(CompactionJob.JobType.PUBLISH_ONLY);
        try {
            List<CompactionTask> tasks = createPublishOnlyTasks(visibleVersion, newVersion, beToTablets, txnId);
            for (CompactionTask t : tasks) {
                t.sendRequest();
            }
            job.setTasks(tasks);
            LOG.debug("Started PUBLISH_ONLY job partition={} reason={} visible={} new={} txn={}",
                    partitionIdentifier, reason, visibleVersion, newVersion, txnId);
            return job;
        } catch (Exception e) {
            LOG.warn("Fail to dispatch PUBLISH_ONLY tasks partition={} err={}", partitionIdentifier, e.getMessage());
            abortTransactionIgnoreError(job, e.getMessage());
            return null;
        }
    }

    private void scheduleNewCompaction() {
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
                        // PUBLISH_ONLY jobs always need force_publish=true:
                        // tablets without local results have no TxnLog and rely on the
                        // ignore_txn_log + observe_empty_compaction path on the BE to
                        // bump version uniformly with the rest of the partition.
                        boolean forceCommit = (taskResult == CompactionTask.TaskResult.PARTIAL_SUCCESS) ||
                                (job.getJobType() == CompactionJob.JobType.PUBLISH_ONLY);
                        commitCompaction(partition, job, forceCommit);
                        if (!job.transactionHasCommitted()) { // should not happen
                            errorMsg = String.format("Fail to commit transaction %s", job.getDebugString());
                            LOG.error(errorMsg);
                        }
                    } catch (Exception e) {
                        LOG.error("Fail to commit compaction, {} error={}", job.getDebugString(), e.getMessage());
                        errorMsg = "Fail to commit transaction: " + e.getMessage();
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
                    compactionManager.enableCompactionAfter(partition, Config.lake_compaction_interval_ms_on_failure);
                    abortTransactionIgnoreException(job, errorMsg);
                    continue;
                }
            }
            if (job.transactionHasCommitted() && job.waitTransactionVisible(50, TimeUnit.MILLISECONDS)) {
                // For PUBLISH_ONLY jobs, record the lastPublish markers ONLY now that
                // the txn has reached VISIBLE. If we updated them at dispatch time and
                // the publish later failed, version_delta would compute as 0 and the
                // partition would never be retried until a new version appeared.
                if (job.getJobType() == CompactionJob.JobType.PUBLISH_ONLY && statistics != null) {
                    long publishedVersion = job.getPartition().getVisibleVersion();
                    statistics.setLastPublishVisibleVersion(publishedVersion);
                    statistics.setLastPublishTimeMs(System.currentTimeMillis());
                    Quantiles score = statistics.getCompactionScore();
                    if (score != null) {
                        statistics.setLastPublishScore(score.getMax());
                    }
                }
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
                compactionManager.enableCompactionAfter(partition, Config.lake_compaction_interval_ms_on_success * factor);
            }
        }

        List<PartitionStatisticsSnapshot> partitions = compactionManager.choosePartitionsToCompact(
                runningCompactions.keySet(), disabledIds);

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        int limitReachCnt = 0;
        int workerGroupCnt = warehouseManager.getAllWorkerGroupCount();
        Map<ComputeResource, Integer /* Running */> runningTaskInfo = getRunningTaskInfo();
        Map<ComputeResource, CompactionWarehouseInfo> warehouseTaskInfo = new HashMap<>();
        int index = 0;
        while (limitReachCnt < workerGroupCnt && index < partitions.size()) {
            PartitionStatisticsSnapshot partitionStatisticsSnapshot = partitions.get(index++);
            CompactionWarehouseInfo info = null;
            try {
                ComputeResource computeResource =
                        warehouseManager.getCompactionComputeResource(partitionStatisticsSnapshot.getPartition().getTableId());
                Warehouse warehouse = warehouseManager.getWarehouse(computeResource.getWarehouseId());
                info = warehouseTaskInfo.get(computeResource);
                if (info == null) {
                    int running = runningTaskInfo.getOrDefault(computeResource, 0);
                    int limit = compactionTaskLimit(computeResource);
                    info = new CompactionWarehouseInfo(warehouse.getName(), computeResource, limit, running);
                    warehouseTaskInfo.put(computeResource, info);
                }
            } catch (ErrorReportException e) { // warehouse not exist or no alive nodes
                // TODO: if warehouse not exist, we will use `lake_compaction_warehouse` next round
                //       if no alive nodes, it might be this warehouse is undergoing a reboot,
                //       do not fall back to `lake_compaction_warehouse` for now
                LOG.debug("get compaction warehouse info for partition {} error, {}",
                        partitionStatisticsSnapshot.getPartition(), e);
                continue;
            }
            if (info.taskRunning >= info.taskLimit) {
                if (!info.limitReached) {
                    limitReachCnt++;
                }
                info.limitReached = true;
                continue;
            }
            CompactionJob job = startCompaction(partitionStatisticsSnapshot, info);
            if (job == null) {
                continue;
            }
            info.taskRunning += job.getNumTabletCompactionTasks();
            runningCompactions.put(partitionStatisticsSnapshot.getPartition(), job);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created new compaction job, {}", job.getDebugString());
            }
        }
    }

    private void abortStaleCompaction(List<PartitionIdentifier> deletedCompactionIdentifiers) {
        if (deletedCompactionIdentifiers == null || deletedCompactionIdentifiers.isEmpty()) {
            return;
        }

        for (PartitionIdentifier partitionIdentifier : deletedCompactionIdentifiers) {
            CompactionJob job = runningCompactions.get(partitionIdentifier);
            if (job != null) {
                job.abort();
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
    protected int compactionTaskLimit(ComputeResource computeResource) {
        if (Config.lake_compaction_max_tasks >= 0) {
            return Config.lake_compaction_max_tasks;
        }
        List<ComputeNode> aliveComputeNodes =
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getAliveComputeNodes(computeResource);
        return aliveComputeNodes.size() * 16;
    }

    // return deleted partition
    private List<PartitionIdentifier> cleanPhysicalPartition() {
        long now = System.currentTimeMillis();
        if (now - lastPartitionCleanTime >= PARTITION_CLEAN_INTERVAL_SECOND * 1000L) {
            List<PartitionIdentifier> deletedPartitionIdentifiers = compactionManager.getAllPartitions()
                    .stream()
                    .filter(p -> !MetaUtils.isPhysicalPartitionExist(stateMgr, p.getDbId(), p.getTableId(), p.getPartitionId()))
                    .collect(Collectors.toList());

            // ignore those partitions in runningCompactions
            deletedPartitionIdentifiers
                    .stream()
                    .filter(p -> !runningCompactions.containsKey(p)).forEach(compactionManager::removePartition);
            lastPartitionCleanTime = now;
            return deletedPartitionIdentifiers;
        }
        return null;
    }

    protected CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot,
            CompactionWarehouseInfo info) {
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

        // Intensive path: IS on DB + READ on this one table. The critical section
        // only reads / mutates state of one known table (partition lookup,
        // collectPartitionTablets, beginTransaction, setMinRetainVersion). Concurrent
        // schema change on this table takes IX + table-WRITE which still conflicts
        // with our table-READ, preserving the "no shadow index appears before
        // beginTransaction" invariant the original comment below relies on.
        Locker locker = new Locker();
        long tableId = partitionIdentifier.getTableId();
        locker.lockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);

        try {
            // lake table or lake materialized view
            table = (OlapTable) stateMgr.getLocalMetastore()
                        .getTable(db.getId(), tableId);

            // Compact a table of SCHEMA_CHANGE state does not make much sense, because the compacted data
            // will not be used after the schema change job finished.
            if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                compactionManager.enableCompactionAfter(partitionIdentifier, Config.lake_compaction_interval_ms_on_failure);
                return null;
            }
            partition = (table != null) ? table.getPhysicalPartition(partitionIdentifier.getPartitionId()) : null;
            if (partition == null) {
                compactionManager.removePartition(partitionIdentifier);
                return null;
            }

            currentVersion = partition.getVisibleVersion();

            beToTablets = collectPartitionTablets(partition, info.computeResource);
            if (beToTablets.isEmpty()) {
                compactionManager.enableCompactionAfter(partitionIdentifier, Config.lake_compaction_interval_ms_on_failure);
                return null;
            }

            // Note: call `beginTransaction()` while holding the per-table READ lock (intensive path) to make sure
            // no shadow index will be added to this table (i.e., no schema change) before calling `beginTransaction()`.
            txnId = beginTransaction(partitionIdentifier, partition, info.computeResource);

            partition.setMinRetainVersion(currentVersion);

        } catch (RunningTxnExceedException | AnalysisException | LabelAlreadyUsedException | DuplicatedRequestException e) {
            LOG.error("Fail to create transaction for compaction job. {}", e.getMessage());
            return null;
        } catch (Throwable e) {
            LOG.error("Unknown error: {}", e.getMessage());
            return null;
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }

        long nextCompactionInterval = Config.lake_compaction_interval_ms_on_success;
        CompactionJob job = new CompactionJob(db, table, partition, txnId, Config.lake_compaction_allow_partial_success,
                                              info.computeResource, info.warehouseName);
        try {
            if (table.isFileBundling()) {
                CompactionTask task = createAggregateCompactionTask(currentVersion, beToTablets, txnId,
                        partitionStatisticsSnapshot.getPriority(), info.computeResource, partition.getId(), table);
                task.sendRequest();
                job.setAggregateTask(task);
                LOG.debug("Create aggregate compaction task. {}", job.getDebugString());
            } else {
                List<CompactionTask> tasks = createCompactionTasks(currentVersion, beToTablets, txnId,
                        job.getAllowPartialSuccess(), partitionStatisticsSnapshot.getPriority(), table);
                for (CompactionTask task : tasks) {
                    task.sendRequest();
                }
                job.setTasks(tasks);
            }
            return job;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            partition.setMinRetainVersion(0);
            nextCompactionInterval = Config.lake_compaction_interval_ms_on_failure;
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
            boolean allowPartialSuccess, PartitionStatistics.CompactionPriority priority, OlapTable table)
            throws StarRocksException, RpcException {
        List<CompactionTask> tasks = new ArrayList<>();

        // Get parallel compaction configuration from table property
        // maxParallel > 0 means parallel compaction is enabled
        int maxParallel = table.getTableProperty().getLakeCompactionMaxParallel();

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

            // Set parallel compaction configuration if enabled via table property
            // maxParallel > 0 means parallel compaction is enabled
            if (maxParallel > 0) {
                com.starrocks.proto.TabletParallelConfig parallelConfig = new com.starrocks.proto.TabletParallelConfig();
                parallelConfig.enableParallel = true;
                parallelConfig.maxParallelPerTablet = maxParallel;
                // maxBytesPerSubtask is controlled by BE config lake_compaction_max_bytes_per_subtask
                // Set to 0 to let BE use its own config
                parallelConfig.maxBytesPerSubtask = 0L;
                request.parallelConfig = parallelConfig;
            }

            // NOTE: do NOT set writeToLocalResult here. Legacy COMPACT_AND_PUBLISH jobs
            // must keep writing TxnLog to remote storage so the existing publish flow
            // can pick them up. Autonomous EXECUTE-side compactions that should write
            // local results are dispatched by Phase 4's BE-side LakeCompactionManager,
            // not by this FE-driven path.
            CompactionTask task = new CompactionTask(node.getId(), service, request);
            tasks.add(task);
        }
        return tasks;
    }

    /**
     * Build PUBLISH_ONLY tasks: one CompactRequest per BE that owns tablets in this partition,
     * with mode=COLLECT_AND_PUBLISH. BE will gather locally-cached CompactionResultPB files
     * (filtered by visible_version) into OpParallelCompaction and publish_version with
     * force_publish=true so all tablets reach new_version uniformly.
     */
    @NotNull
    private List<CompactionTask> createPublishOnlyTasks(long visibleVersion, long newVersion,
            Map<Long, List<Long>> beToTablets, long txnId) throws StarRocksException, RpcException {
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
            // mode requires both visible_version and new_version
            request.mode = com.starrocks.proto.CompactionMode.COLLECT_AND_PUBLISH;
            request.visibleVersion = visibleVersion;
            request.newVersion = newVersion;
            // version field is unused by BE in COLLECT_AND_PUBLISH path but the proto needs it set
            // to keep older validation happy; we use visibleVersion.
            request.version = visibleVersion;
            request.timeoutMs = Config.lake_compaction_publish_timeout_seconds * 1000L;
            request.allowPartialSuccess = true;
            request.encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();

            CompactionTask task = new CompactionTask(node.getId(), service, request);
            tasks.add(task);
        }
        return tasks;
    }

    @NotNull
    private CompactionTask createAggregateCompactionTask(long currentVersion, Map<Long, List<Long>> beToTablets, long txnId,
            PartitionStatistics.CompactionPriority priority, ComputeResource computeResource, long partitionId,
            OlapTable table) throws StarRocksException, RpcException {
        // 1. build AggregateCompactRequest
        AggregateCompactRequest aggRequest = new AggregateCompactRequest();
        aggRequest.requests = Lists.newArrayList();
        aggRequest.computeNodes = Lists.newArrayList();
        aggRequest.partitionId = partitionId;

        // Get parallel compaction configuration from table property
        // maxParallel > 0 means parallel compaction is enabled
        int maxParallel = table.getTableProperty().getLakeCompactionMaxParallel();

        // Track the candidate aggregator nodes (those that actually own tablets in the
        // batch). We prefer one of them as aggregator so the BE side can resolve the
        // first tablet id locally through the staros worker cache when deriving the
        // combined_txn_log / bundle_tablet_metadata file path.
        List<ComputeNode> candidateAggregatorNodes = Lists.newArrayList();

        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            ComputeNode node = systemInfoService.getBackendOrComputeNode(entry.getKey());
            if (node == null) {
                throw new StarRocksException("Node " + entry.getKey() + " has been dropped");
            }
            candidateAggregatorNodes.add(node);
            ComputeNodePB nodePB = new ComputeNodePB();
            nodePB.setHost(node.getHost());
            nodePB.setBrpcPort(node.getBrpcPort());
            nodePB.setId(entry.getKey());

            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = currentVersion;
            request.timeoutMs = LakeService.TIMEOUT_COMPACT;
            request.allowPartialSuccess = false;
            request.encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();
            request.forceBaseCompaction = (priority == PartitionStatistics.CompactionPriority.MANUAL_COMPACT);
            request.skipWriteTxnlog = true;

            // Set parallel compaction configuration if enabled via table property
            // maxParallel > 0 means parallel compaction is enabled
            if (maxParallel > 0) {
                com.starrocks.proto.TabletParallelConfig parallelConfig = new com.starrocks.proto.TabletParallelConfig();
                parallelConfig.enableParallel = true;
                parallelConfig.maxParallelPerTablet = maxParallel;
                // maxBytesPerSubtask is controlled by BE config lake_compaction_max_bytes_per_subtask
                // Set to 0 to let BE use its own config
                parallelConfig.maxBytesPerSubtask = 0L;
                request.parallelConfig = parallelConfig;
            }

            aggRequest.requests.add(request);
            aggRequest.computeNodes.add(nodePB);
        }

        // 2. pick aggregator node and build lake service
        ComputeNode aggregatorNode = LakeAggregator.chooseAggregatorNode(computeResource, candidateAggregatorNodes);
        if (aggregatorNode == null) {
            throw new NoAliveBackendException("No alive compute node available for aggregate compaction");
        }
        LakeService service = BrpcProxy.getLakeService(aggregatorNode.getHost(), aggregatorNode.getBrpcPort());

        // 3. build AggregateCompactionTask
        return new AggregateCompactionTask(aggregatorNode.getId(), service, aggRequest);
    }

    @NotNull
    protected Map<Long, List<Long>> collectPartitionTablets(PhysicalPartition partition, ComputeResource computeResource) {
        List<MaterializedIndex> visibleIndexes = partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Map<Long, List<Long>> beToTablets = new HashMap<>();

        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        for (MaterializedIndex index : visibleIndexes) {
            for (Tablet tablet : index.getTablets()) {
                ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, tablet.getId());
                if (computeNode == null) {
                    beToTablets.clear();
                    return beToTablets;
                }

                beToTablets.computeIfAbsent(computeNode.getId(), k -> Lists.newArrayList()).add(tablet.getId());
            }
        }
        return beToTablets;
    }

    // REQUIRE: has acquired the read lock of Database.
    protected long beginTransaction(PartitionIdentifier partition, PhysicalPartition physicalPartition,
            ComputeResource computeResource)
            throws RunningTxnExceedException, AnalysisException, LabelAlreadyUsedException, DuplicatedRequestException {
        long dbId = partition.getDbId();
        long tableId = partition.getTableId();
        long partitionId = partition.getPartitionId();
        long currentTs = System.currentTimeMillis();
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.LAKE_COMPACTION;
        TransactionState.TxnSourceType txnSourceType = TransactionState.TxnSourceType.FE;
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(txnSourceType, HOST_NAME);
        String label = String.format("COMPACTION_%d-%d-%d-%d", dbId, tableId, partitionId, currentTs);

        long txnId = transactionMgr.beginTransaction(dbId, Lists.newArrayList(tableId), label, coordinator,
                loadJobSourceType, Config.lake_compaction_default_timeout_second, computeResource);

        // Register loaded indexes so preCommit() validates the same indexes that were collected,
        // not the latest (which may change due to tablet split).
        TransactionState txnState = transactionMgr.getTransactionState(dbId, txnId);
        if (txnState != null) {
            List<Long> indexIds = physicalPartition.getLatestMaterializedIndices(
                    MaterializedIndex.IndexExtState.VISIBLE)
                    .stream().map(MaterializedIndex::getId)
                    .collect(Collectors.toList());
            txnState.addPartitionLoadedIndexes(tableId, physicalPartition.getId(), indexIds);
        }

        return txnId;
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
            job.getPartition().incExtraFileSize(job.getSuccessCompactInputFileSize());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), tableIdList, LockType.WRITE);
        }
        if (waiter != null) {
            job.setVisibleStateWaiter(waiter);
            job.setCommitTs(System.currentTimeMillis());
        }
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

    // get running compaction and history compaction, sorted by descending start time
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
                return l.getStartTs() > r.getStartTs() ? 1 : (l.getStartTs() < r.getStartTs()) ? -1 : 0;
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
        return disabledIds.contains(tableId);
    }

    public boolean isPartitionDisabled(Long partitionId) {
        return disabledIds.contains(partitionId);
    }

    public void disableTableOrPartitionId(String disableIdsStr) {
        Set<Long> newDisabledIds = new HashSet<>();
        if (!disableIdsStr.isEmpty()) {
            String[] arr = disableIdsStr.split(";");
            for (String a : arr) {
                try {
                    long l = Long.parseLong(a);
                    newDisabledIds.add(l);
                } catch (NumberFormatException e) {
                    LOG.warn("Bad format of disable string: {}, now is {}, should be like \"Id1;Id2\"",
                            e, disableIdsStr);
                    return;
                }
            }
        }
        disabledIds = Collections.unmodifiableSet(newDisabledIds);
    }

    private Map<ComputeResource, Integer> getRunningTaskInfo() {
        Map<ComputeResource, Integer> runningTaskInfo = new HashMap<>();
        runningCompactions.values().stream().forEach((job) -> {
            ComputeResource computeResource = job.getComputeResource();
            int running = runningTaskInfo.getOrDefault(computeResource, 0);
            runningTaskInfo.put(computeResource, running + job.getNumTabletCompactionTasks());
        });
        return runningTaskInfo;
    }
}
