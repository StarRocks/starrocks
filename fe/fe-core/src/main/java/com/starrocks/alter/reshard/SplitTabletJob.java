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

package com.starrocks.alter.reshard;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.PlacementPolicy;
import com.starrocks.alter.reshard.ReshardingPhysicalPartition.PublishResult;
import com.starrocks.alter.reshard.ReshardingPhysicalPartition.PublishState;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.metric.MetricRepo;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletReshardJobsItem;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/*
 * SplitTabletJob is for tablet splitting.
 */
public class SplitTabletJob extends TabletReshardJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    @SerializedName(value = "dbId")
    protected final long dbId;

    @SerializedName(value = "tableId")
    protected final long tableId;

    @SerializedName(value = "reshardingPhysicalPartitions")
    protected final Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions;

    @SerializedName(value = "transactionId")
    protected long transactionId;

    @SerializedName(value = "gtid")
    protected long gtid;

    @SerializedName(value = "endTransactionId")
    protected long endTransactionId;

    public SplitTabletJob(long jobId, long dbId, long tableId,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) {
        super(jobId, JobType.SPLIT_TABLET);
        this.dbId = dbId;
        this.tableId = tableId;
        this.reshardingPhysicalPartitions = reshardingPhysicalPartitions;
    }

    public long getDbId() {
        return dbId;
    }

    @Override
    public long getTableId() {
        return tableId;
    }

    public Map<Long, ReshardingPhysicalPartition> getReshardingPhysicalPartitions() {
        return reshardingPhysicalPartitions;
    }

    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            parallelTablets += reshardingPhysicalPartition.getParallelTablets();
        }
        return parallelTablets;
    }

    @Override
    public void init() throws StarRocksException {
        try {
            setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);
        } catch (TabletReshardException e) {
            // Surface admission rejection (table not NORMAL / dropped) as a checked exception so
            // callers' StarRocksException handling (e.g. TabletPreSplitCoordinator) takes effect.
            throw new StarRocksException(e.getMessage(), e);
        }
    }

    /*
     * The table was already moved to TABLET_RESHARD by init() at admission time.
     * 1. Begin transaction (allocate transaction id)
     * 2. Create new shards on StarOS
     * 3. Commit transaction (update next version)
     * 4. Add new tablets to inverted index
     * 5. Register resharding tablets
     * 6. Set job state to PREPARING
     * Job cannot be cancelled after this step
     */
    @Override
    protected void runPendingJob() {
        // 1. Begin transaction (allocate transaction id)
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        transactionId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        gtid = globalStateMgr.getGtidGenerator().nextGtid();

        // 2. Create new shards on StarOS — the last "abortable" step before the no-abort
        //    boundary below. No table lock needed: init() already moved the
        //    table to TABLET_RESHARD, which blocks concurrent DDL. If this throws, run()
        //    catches, abort() fires (state still PENDING), runAbortingJob unwinds the
        //    FE-side mutations, and any orphan staros shards from a partial RPC are reaped
        //    by StarMgrMetaSyncer's diff-and-purge cycle.
        createShardsOnStarOS();

        // 3. Commit transaction (update next version)
        // NOTE: After updateNextVersions(), the table's next version is advanced.
        // From this point the job must not abort or throw, because the run() wrapper would attempt to abort.
        updateNextVersions();

        // 4. Add new tablets to inverted index
        addTabletsToInvertedIndex();

        // 5. Register resharding tablets
        registerReshardingTablets();

        // 6. Set job state to PREPARING
        setJobState(JobState.PREPARING);
    }

    /*
     * 1. Wait for previous versions published
     * 2. Set job state to RUNNING
     */
    @Override
    protected void runPreparingJob() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }
                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                long visibleVersion = physicalPartition.getVisibleVersion();
                if (commitVersion != visibleVersion + 1) {
                    Preconditions.checkState(visibleVersion < commitVersion,
                            "partition=" + physicalPartition.getId() + " visibleVersion="
                                    + visibleVersion + " commitVersion=" + commitVersion);
                    return;
                }
            }
        }

        setJobState(JobState.RUNNING);
    }

    /*
     * 1. Publish the split transaction, update new tablet ranges
     * 2. Add the new versions of materialized index to catalog
     * 3. Get end transaction id
     * 4. Set job state to CLEANING
     */
    @Override
    protected void runRunningJob() {
        // 1. Publish the split transaction, update new tablet ranges
        boolean allPartitionFinished = true;
        ThreadPoolExecutor publishThreadPool = GlobalStateMgr.getCurrentState().getPublishVersionDaemon()
                .getTaskExecutor();

        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            boolean useAggregatePublish = olapTable.isFileBundling();
            ComputeResource computeResource = resolveComputeResource(tableId);
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                long visibleVersion = physicalPartition.getVisibleVersion();
                Preconditions.checkState(commitVersion == visibleVersion + 1,
                        "partition=" + physicalPartition.getId() + " visibleVersion="
                                + visibleVersion + " commitVersion=" + commitVersion);

                PublishResult publishResult = reshardingPhysicalPartition.getPublishResult();
                if (publishResult.publishState() == PublishState.NOT_STARTED
                        || publishResult.publishState() == PublishState.FAILED) {
                    // Publish not started or publish failed
                    allPartitionFinished = false;
                    // Start publish asynchronously
                    List<Tablet> tablets = new ArrayList<>();
                    for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(IndexExtState.ALL)) {
                        tablets.addAll(index.getTablets());
                    }
                    Future<Map<Long, TabletRange>> future = publishThreadPool.submit(() -> publishVersion(
                            tablets, commitVersion, useAggregatePublish, computeResource));
                    reshardingPhysicalPartition.setPublishFuture(future);
                } else if (publishResult.publishState() == PublishState.IN_PROGRESS) {
                    // Publish is in progress
                    allPartitionFinished = false;
                } else if (publishResult.publishState() == PublishState.SUCCESS) {
                    // Publish success, update new tablet ranges
                    // Note this will be executed repeatedly when retry job, it should be idempotent
                    Map<Long, TabletRange> tabletRanges = publishResult.tabletRanges();
                    for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                            .getReshardingIndexes().values()) {
                        MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                        for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                            SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                            if (splittingTablet != null) {
                                List<Long> newTabletIds = splittingTablet.getNewTabletIds();
                                for (Long tabletId : newTabletIds) {
                                    TabletRange tabletRange = tabletRanges.get(tabletId);
                                    if (tabletRange != null) {
                                        Tablet newTablet = newIndex.getTablet(tabletId);
                                        Preconditions.checkNotNull(newTablet, "Not found tablet " + tabletId);
                                        newTablet.setRange(tabletRange);
                                    } else {
                                        // If splitting tablet failed, will fallback to identical tablet,
                                        // in this case, BE will only return the range of the first tablet
                                        List<Long> toRemoveTabletIds = newTabletIds.subList(1, newTabletIds.size());
                                        Preconditions.checkState(tabletId == toRemoveTabletIds.get(0),
                                                "Range of tablet " + tabletId + " not found");
                                        for (long toRemoveTabletId : toRemoveTabletIds) {
                                            newIndex.removeTablet(toRemoveTabletId);
                                        }
                                        splittingTablet.fallbackToIdenticalTablet();
                                        break;
                                    }
                                }
                            }
                        }
                        // Share adjacent tablet range bounds to reduce memory usage
                        newIndex.shareAdjacentTabletRangeBounds();
                    }
                } else {
                    LOG.error("Unknown publish state {} in {}", publishResult.publishState(), this);
                    throw new TabletReshardException("Unknown publish state " + publishResult.publishState());
                }
            }
        }

        if (!allPartitionFinished) {
            return;
        }

        // Splice any canonical colocate-boundary new tablets into ColocateRangeMgr and mark
        // the group unstable. Legacy / buggy BE outputs that straddle an existing boundary
        // log a warning and still mark unstable so the scan-time alignment guard catches
        // transient mis-alignment.
        applyColocateRangeSplitResult();

        // 3. Add the new versions of materialized index to catalog
        addNewMaterializedIndexes();

        // 3. Get end transaction id
        endTransactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                .peekNextTransactionId();

        // 4. Set job state to CLEANING
        setJobState(JobState.CLEANING);
    }

    /*
     * 1. Abort in-flight lake compactions, then wait for the remaining previous transactions finished
     * 2. Remove old versions of materialized index
     * 3. Unregister resharding tablets
     * 4. Set tablet state to NORMAL
     * 5. Set job state to FINISHED
     */
    @Override
    protected void runCleaningJob() {
        // 1. Cancel previous in-flight compactions on the resharded partitions so cleaning does not wait
        //    for slow compaction, then wait for the rest. Compactions on partitions this job does not
        //    reshard are neither cancelled nor waited on (see CompactionScheduler#cancelPreviousCompactions).
        Set<Long> ignoredCompactionTxnIds = GlobalStateMgr.getCurrentState().getCompactionMgr()
                .cancelPreviousCompactions(endTransactionId, dbId, tableId, reshardingPhysicalPartitions.keySet());
        try {
            if (!GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().isPreviousTransactionsFinished(
                    endTransactionId, dbId, List.of(tableId), ignoredCompactionTxnIds)) {
                return;
            }
        } catch (AnalysisException e) { // Db is dropped, ignore exception
            LOG.warn("Ignore exception when waiting previous transactions finished. " + this, e);
        }

        // 2. Remove old versions of materialized index
        removeOldMaterializedIndexes();

        // 3. Unregister resharding tablets
        unregisterReshardingTablets();

        // 4. Set tablet state to NORMAL
        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);

        // 5. Update metrics
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_RESHARD_SPLIT_JOB_FINISHED.increase(1L);
            MetricRepo.HISTO_TABLET_RESHARD_JOB_DURATION.update(System.currentTimeMillis() - createdTimeMs);
        }

        // 6. Set job state to FINISHED
        setJobState(JobState.FINISHED);
    }

    @Override
    protected void runFinishedJob() {
        LOG.info("Split tablet job is finished. {}", this);
    }

    /*
     * 1. Unregister resharding tablets
     * 2. Remove new tablets from inverted index
     * 3. Set table state to NORMAL
     * 4. Update metrics
     * 5. Set job state to ABORTED
     *
     * Any orphan staros shards from a partial createShardsOnStarOS in runPendingJob are
     * reaped by StarMgrMetaSyncer's diff-and-purge cycle; no explicit deleteShards here.
     */
    @Override
    protected void runAbortingJob() {
        try {
            // 1. Unregister resharding tablets
            unregisterReshardingTablets();

            // 2. Remove new tablets from inverted index
            removeTabletsFromInvertedIndex();

            // 3. Set table state to NORMAL
            setTableState(null, OlapTable.OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("Ignore exception when aborting tablet reshard job. {}. ", this, e);
        }

        // 4. Update metrics
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_RESHARD_SPLIT_JOB_ABORTED.increase(1L);
        }

        // 5. Set job state to ABORTED
        setJobState(JobState.ABORTED);
    }

    @Override
    protected void runAbortedJob() {
        LOG.info("Split tablet job is aborted. {}", this);
    }

    // Can abort only when job state is PENDING
    @Override
    protected boolean canAbort() {
        return jobState == JobState.PENDING;
    }

    // Correspond to init() at admission time
    @Override
    protected void replayPendingJob() {
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);
        LOG.info("Split tablet job replayed pending job. {}", this);
    }

    // Correspond to runPendingJob()
    @Override
    protected void replayPreparingJob() {
        updateNextVersions();
        addTabletsToInvertedIndex();
        registerReshardingTablets();
        LOG.info("Split tablet job replayed preparing job. {}", this);
    }

    // Correspond to runPreparingJob()
    @Override
    protected void replayRunningJob() {
        LOG.info("Split tablet job replayed running job. {}", this);
    }

    // Correspond to runRunningJob()
    @Override
    protected void replayCleaningJob() {
        addNewMaterializedIndexes();
        LOG.info("Split tablet job replayed cleaning job. {}", this);
    }

    // Correspond to runCleaningJob()
    @Override
    protected void replayFinishedJob() {
        removeOldMaterializedIndexes();
        unregisterReshardingTablets();
        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);
        LOG.info("Split tablet job replayed finished job. {}", this);
    }

    // Correspond to job abort
    @Override
    protected void replayAbortingJob() {
        LOG.info("Split tablet job replayed aborting job. {}", this);
    }

    // Correspond to runAbortingJob()
    @Override
    protected void replayAbortedJob() {
        unregisterReshardingTablets();
        removeTabletsFromInvertedIndex();
        setTableState(null, OlapTable.OlapTableState.NORMAL);
        LOG.info("Split tablet job replayed aborted job. {}", this);
    }

    @Override
    protected void registerReshardingTabletsOnRestart() {
        if (jobState == JobState.PENDING || jobState.isFinalState()) {
            return;
        }

        registerReshardingTablets();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SplitTabletJob: {");
        sb.append("job_id: ").append(jobId);
        sb.append(", job_type: ").append(jobType);
        sb.append(", job_state: ").append(jobState);
        sb.append(", db_id: ").append(dbId);
        sb.append(", table_id: ").append(tableId);
        sb.append(", created_time: ").append(TimeUtils.longToTimeString(createdTimeMs));
        if (finishedTimeMs > 0) {
            sb.append(", finished_time:").append(TimeUtils.longToTimeString(finishedTimeMs));
        } else {
            sb.append(", state_started_time: ").append(TimeUtils.longToTimeString(stateStartedTimeMs));
        }
        if (errorMessage != null) {
            sb.append(", error_message: ").append(errorMessage);
        }
        if (transactionId > 0) {
            sb.append(", txn_id: ").append(transactionId);
        }
        if (endTransactionId > 0) {
            sb.append(", end_txn_id: ").append(endTransactionId);
        }
        sb.append(", parallel_partitions: ").append(reshardingPhysicalPartitions.size());
        sb.append("}");
        return sb.toString();
    }

    @Override
    public TTabletReshardJobsItem getInfo() {
        TTabletReshardJobsItem item = new TTabletReshardJobsItem();
        item.setJob_id(jobId);
        item.setDb_id(dbId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            item.setDb_name("");
            LOG.warn("Failed to get database name for tablet reshard job. {}", this);
        } else {
            item.setDb_name(db.getFullName());
        }

        item.setTable_id(tableId);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            item.setTable_name("");
            LOG.warn("Failed to get table name for tablet reshard job. {}", this);
        } else {
            item.setTable_name(table.getName());
        }
        item.setJob_type(jobType.name());
        item.setJob_state(jobState.name());
        item.setTransaction_id(transactionId);
        item.setParallel_partitions(reshardingPhysicalPartitions.size());
        item.setParallel_tablets(getParallelTablets());
        item.setCreated_time(createdTimeMs / 1000);
        item.setFinished_time(finishedTimeMs / 1000);
        if (errorMessage != null) {
            item.setError_message(errorMessage);
        } else {
            item.setError_message("");
        }
        return item;
    }

    private void setTableState(OlapTable.OlapTableState expectedState, OlapTable.OlapTableState newState) {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            if (expectedState != null && olapTable.getState() != expectedState) {
                throw new TabletReshardException(
                        "Unexpected table state " + olapTable.getState() + " in table " + olapTable.getName());
            }
            olapTable.setState(newState);
        }
    }

    private void updateNextVersions() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                if (commitVersion <= 0) {
                    commitVersion = physicalPartition.getNextVersion();
                    reshardingPhysicalPartition.setCommitVersion(commitVersion);
                }

                physicalPartition.setNextVersion(commitVersion + 1);
            }
        }
    }

    private Map<Long, TabletRange> publishVersion(List<Tablet> tablets, long commitVersion,
            boolean useAggregatePublish, ComputeResource computeResource) {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = transactionId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = stateStartedTimeMs / 1000;
            txnInfo.txnType = TxnTypePB.TXN_TABLET_RESHARD;
            txnInfo.gtid = gtid;
            // Carry the table's colocate column count once at the txn level (single split job =
            // single txn). 0 = non-colocate range table, BE keeps pre-P3 splitter behavior.
            ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
            ColocateTableIndex.GroupId rangeGroupId = idx.getRangeColocateGroupId(tableId);
            txnInfo.colocateColumnCount = rangeGroupId == null
                    ? 0 : idx.getGroupSchema(rangeGroupId).getColocateColumnCount();

            Map<Long, TabletRange> tabletRange = new HashMap<>();
            Utils.publishVersion(tablets, txnInfo, commitVersion - 1, commitVersion, null, tabletRange,
                    computeResource, null, useAggregatePublish);

            return tabletRange;
        } catch (Exception e) {
            LOG.warn("Failed to publish version for tablet reshard job {}. ", this, e);
            throw new TabletReshardException("Failed to publish version: " + e.getMessage(), e);
        }
    }

    /**
     * Splice any canonical colocate-boundary new tablets produced by this split into
     * {@link ColocateRangeMgr} and mark the group unstable. Skipped for non-range-colocate
     * tables.
     *
     * <p>Three distinct triggers fire applyRangeSplitResult:
     * <ul>
     * <li><b>New canonical boundary</b>: a new tablet's lower bound has the canonical
     *     {@code (k, NULL...)} shape AND that prefix is not yet a boundary in
     *     {@link ColocateRangeMgr}. Splice + mark unstable.</li>
     * <li><b>Old tablet straddles an existing boundary</b>: indicates retry-after-partial-commit
     *     (boundary was already journaled but mark-unstable was lost). The splice is idempotent;
     *     the mark-unstable record is re-emitted.</li>
     * <li><b>Non-canonical crossing</b>: a buggy / colocate-unaware BE produced a child whose
     *     range straddles an existing boundary without canonicalizing. Log a warning, mark
     *     unstable so the scan-time alignment guard fails closed.</li>
     * </ul>
     */
    private void applyColocateRangeSplitResult() {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(tableId);
        if (groupId == null) {
            return;
        }
        long grpId = groupId.grpId;
        int colocateColumnCount = colocateTableIndex.getGroupSchema(groupId).getColocateColumnCount();
        List<ColocateRange> currentRanges = colocateTableIndex.getColocateRanges(grpId);

        Set<Tuple> canonicalLowers = new LinkedHashSet<>();
        boolean oldStradlesBoundary = false;
        boolean nonCanonicalCrossing = false;
        // True if any source index of this split was empty (rowCount == 0), i.e. a pre-split — whose new
        // shards were created in the SPREAD group only. Recomputed here from the catalog (not threaded
        // from the PENDING phase) so it is replay-safe; used to arm the backstop for a boundary-less
        // pre-split below.
        boolean anySpreadPreSplit = false;
        // New child ranges captured during the walk below, reused after the splice for the best-effort
        // immediate PACK reassignment. sortKeyColumns is hoisted out of the lock so that same reassign
        // can reuse it (it is immutable schema, always assigned inside the lock before the walk).
        Map<Long, Range<Tuple>> newChildRanges = new HashMap<>();
        List<Column> sortKeyColumns = null;

        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            sortKeyColumns = MetaUtils.getRangeDistributionColumns(olapTable);

            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }
                for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                        .getReshardingIndexes().values()) {
                    MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                    MaterializedIndex oldIndex = physicalPartition.getIndex(reshardingIndex.getMaterializedIndexId());
                    anySpreadPreSplit |= oldIndex != null && oldIndex.getRowCount() == 0;
                    for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                        SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                        // getSplittingTablet() returns null for a fallback-to-identical replacement (BE
                        // returned only the first child range -> fallbackToIdenticalTablet) and for merges,
                        // so use the ReshardingTablet interface accessors below to still process an identical
                        // replacement: a pre-split creates even that replacement in the SPREAD group only, so
                        // its range must be collected for the reconcile or it is stranded SPREAD-only and
                        // loses its colocate placement. An identical replacement introduces no boundary, so
                        // skip the boundary/straddle logic for it.
                        boolean identical = reshardingTablet.getIdenticalTablet() != null;
                        if (splittingTablet == null && !identical) {
                            continue;
                        }
                        Tablet oldTablet = oldIndex == null ? null
                                : oldIndex.getTablet(reshardingTablet.getFirstOldTabletId());
                        if (!identical && oldTablet != null && oldTablet.getRange() != null
                                && !ColocateRangeUtils.isContainedInOwningColocateRange(
                                        oldTablet.getRange().getRange(),
                                        currentRanges, sortKeyColumns, colocateColumnCount)) {
                            oldStradlesBoundary = true;
                        }
                        for (Long newTabletId : reshardingTablet.getNewTabletIds()) {
                            Tablet newTablet = newIndex.getTablet(newTabletId);
                            if (newTablet == null || newTablet.getRange() == null) {
                                continue;
                            }
                            Range<Tuple> newRange = newTablet.getRange().getRange();
                            newChildRanges.put(newTabletId, newRange);
                            if (identical) {
                                continue;
                            }
                            boolean canonicalLow = ColocateRangeUtils.hasCanonicalLowerBound(
                                    newRange, sortKeyColumns, colocateColumnCount);
                            if (canonicalLow) {
                                canonicalLowers.add(newRange.getLowerBound());
                            } else if (!ColocateRangeUtils.isContainedInOwningColocateRange(
                                    newRange, currentRanges, sortKeyColumns, colocateColumnCount)) {
                                LOG.warn("New tablet {} range {} crosses an existing ColocateRange boundary "
                                        + "in colocate group {}; marking group unstable. Verify FE↔BE versions.",
                                        newTabletId, newRange, grpId);
                                nonCanonicalCrossing = true;
                            }
                        }
                    }
                }
            }
        }

        boolean hasNewCanonical = canonicalLowers.stream().anyMatch(lower ->
                !ColocateRangeMgr.hasBoundaryAt(currentRanges,
                        new Tuple(lower.getValues().subList(0, colocateColumnCount))));
        // Splice a new colocate boundary + mark the group unstable ONLY when this split actually
        // introduced or crossed one. A within-prefix (Level-2) split introduces no boundary, so this
        // block is skipped and the group stays stable — but the PACK reconcile below still runs.
        if (hasNewCanonical || oldStradlesBoundary || nonCanonicalCrossing) {
            try {
                colocateTableIndex.applyRangeSplitResult(grpId, canonicalLowers, colocateColumnCount,
                        () -> GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                                dbId, tableId, 0L, 0L, PlacementPolicy.PACK));
            } catch (DdlException e) {
                throw new TabletReshardException(
                        "Failed to apply range split result for grpId " + grpId + ": " + e.getMessage(), e);
            }
        } else if (anySpreadPreSplit && !newChildRanges.isEmpty()) {
            // Boundary-less (Level-2) pre-split: nothing was spliced above, so the group would otherwise
            // stay stable and the periodic ColocateChecker backstop would never revisit it. But the new
            // shards were created SPREAD-only, so mark the group unstable to arm that backstop as a safety
            // net should the immediate best-effort reconcile below fail — otherwise their PACK placement
            // could be lost. The backstop re-stabilizes the group once the children are placed (ranges are
            // unchanged, so its alignment step is a no-op).
            colocateTableIndex.markAllGroupsWithSameColocateGroupIdUnstable(grpId, true);
        }

        // Place each new child into its owning ColocateRange's PACK shard group. This MUST run for every
        // range-colocate split, not only when a boundary was spliced above: a pre-split creates its new
        // shards in the SPREAD group ONLY (so StarOS spreads them across CNs at creation), so even a
        // within-prefix (Level-2) split — whose boundary is not spliced, leaving the group stable so the
        // periodic ColocateChecker backstop never revisits it — still needs its children reconciled into
        // the existing owning PACK group, or their colocate placement is lost permanently.
        // reconcileTabletPackPlacement is idempotent: for an online split (children already carry the
        // right PACK group) it is a no-op; for a SPREAD-only pre-split child it adds the owning PACK group
        // (removing none). Best-effort: a failure here must never abort the already-published split — for
        // a boundary-splicing split the group is left unstable so the backstop still converges it; a
        // within-prefix split relies on this immediate pass, so the shared helper logs and retries on the
        // expected StarOS checked failures but is intentionally not blanket-wrapped.
        try {
            List<ColocateRange> updatedRanges = colocateTableIndex.getColocateRanges(grpId);
            // Only reconcile children that map cleanly into exactly one post-split ColocateRange. A child
            // whose range straddles a boundary (legacy/mismatched BE, or a multi-way split with a
            // non-canonical crossing) has no single well-defined PACK group; leave it to the backstop.
            Map<Long, Range<Tuple>> containedChildRanges = new HashMap<>();
            for (Map.Entry<Long, Range<Tuple>> entry : newChildRanges.entrySet()) {
                if (ColocateRangeUtils.isContainedInOwningColocateRange(
                        entry.getValue(), updatedRanges, sortKeyColumns, colocateColumnCount)) {
                    containedChildRanges.put(entry.getKey(), entry.getValue());
                }
            }
            ColocateChecker.reconcileTabletPackPlacement(containedChildRanges, updatedRanges,
                    colocateColumnCount, "split of table " + tableId);
        } catch (Exception e) {
            LOG.warn("best-effort immediate PACK reassignment after split of table {} failed; "
                    + "leaving convergence to the colocate checker: {}", tableId, e.getMessage());
        }
    }

    private void addNewMaterializedIndexes() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                Preconditions.checkState(commitVersion == physicalPartition.getVisibleVersion() + 1,
                        "commit version: " + commitVersion + ", visible version: "
                                + physicalPartition.getVisibleVersion());

                physicalPartition.setVisibleVersion(commitVersion, stateStartedTimeMs);

                for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                        .getReshardingIndexes().values()) {
                    MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                    physicalPartition.addMaterializedIndex(newIndex,
                            newIndex.getMetaId() == olapTable.getBaseIndexMetaId());
                }
            }

            // Installing the new materialized indexes changes the partition's tablet layout:
            // PhysicalPartition.getLatestIndex() now returns a different tablet set. A query that
            // captured the old layout during planning (OlapScanNode fills scanTabletIds from
            // getLatestIndex(), then mapTabletsToPartitions() re-reads it) would otherwise hard-fail
            // at plan build ("Invalid tablet id ... may have been dropped") or hand a CN a stale
            // tablet/version whose metadata object no longer exists. Bump the table's optimistic
            // version so StatementPlanner's retry loop (OptimisticVersion.validateTableUpdate) detects
            // the change and re-plans against the new layout. Mirrors MergePartitionJob's bump at its
            // partition-replace commit point. This runs on both the leader (runRunningJob) and the
            // replay path (replayCleaningJob), so followers re-plan too.
            olapTable.lastSchemaUpdateTime.set(System.nanoTime());
        }
    }

    private void removeOldMaterializedIndexes() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                        .getReshardingIndexes().values()) {
                    MaterializedIndex oldIndex = physicalPartition
                            .deleteMaterializedIndexByIndexId(reshardingIndex.getMaterializedIndexId());
                    if (oldIndex == null) {
                        continue;
                    }
                    // Remove old tablets from inverted index
                    for (Tablet tablet : oldIndex.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    private void addTabletsToInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                MaterializedIndex index = reshardingIndex.getMaterializedIndex();
                // Use HDD in shared-data mode
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId,
                        reshardingPhysicalPartition.getPhysicalPartitionId(),
                        index.getId(), TStorageMedium.HDD, true);
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.addTablet(tablet.getId(), tabletMeta);
                }
            }
        }
    }

    private void removeTabletsFromInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                MaterializedIndex index = reshardingIndex.getMaterializedIndex();
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }
    }

    private LockedObject<OlapTable> getLockedTable(LockType lockType) {
        return new LockedObject<>(dbId, List.of(tableId), lockType, getOlapTable());
    }

    private OlapTable getOlapTable() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) { // Table is dropped
            // Only force ABORTING when the job is past the abortable PENDING window. At admission
            // (PENDING, not yet queued) and during runPendingJob (still PENDING), the run()
            // wrapper's abort() can handle the transition cleanly — avoiding a journal entry for
            // a job that may never be queued (admission-time table-dropped race). The errorMessage
            // assignment is paired with setJobState here so it only fires when it is actually
            // preserved in the journaled ABORTING state; in the PENDING path abort() overwrites it.
            if (!canAbort()) {
                errorMessage = "Table not found";
                setJobState(JobState.ABORTING);
            }
            throw new TabletReshardException("Table not found. " + this);
        }
        return (OlapTable) table;
    }

    private void registerReshardingTablets() {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            long visibleVersion = reshardingPhysicalPartition.getCommitVersion();
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    for (long tabletId : reshardingTablet.getOldTabletIds()) {
                        tabletReshardJobMgr.registerReshardingTablet(tabletId, reshardingTablet,
                                visibleVersion);
                    }
                }
            }
        }
    }

    protected void unregisterReshardingTablets() {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    for (long tabletId : reshardingTablet.getOldTabletIds()) {
                        tabletReshardJobMgr.unregisterReshardingTablet(tabletId);
                    }
                }
            }
        }
    }

    /**
     * Create StarOS shards for this split job.
     *
     * <p>Called from {@link #runPendingJob} as the last "abortable" step, immediately
     * before {@code updateNextVersions} (the no-abort boundary). A failure surfaces as
     * a {@link TabletReshardException} that {@code run()} catches and abort()s on;
     * {@link #runAbortingJob} unwinds the FE-side mutations and any orphan staros
     * shards from a partial RPC are reaped by {@code StarMgrMetaSyncer}'s
     * diff-and-purge cycle.
     */
    void createShardsOnStarOS() {
        OlapTable table = getOlapTable();
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId rangeColocateGroupId =
                colocateTableIndex.getRangeColocateGroupId(table.getId());
        // Snapshot colocate ranges once: the createShard RPCs in the inner loop only ever
        // read the same grpId, and the ranges list is stable for the duration of this DDL.
        List<ColocateRange> colocateRanges = rangeColocateGroupId == null ? null
                : colocateTableIndex.getColocateRanges(rangeColocateGroupId.grpId);
        int colocateColumnCount = rangeColocateGroupId == null ? 0
                : colocateTableIndex.getGroupSchema(rangeColocateGroupId).getColocateColumnCount();

        try {
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition =
                        table.getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }
                for (ReshardingMaterializedIndex reshardingIndex :
                        reshardingPhysicalPartition.getReshardingIndexes().values()) {
                    createShardsForOneIndex(table, physicalPartition, reshardingIndex,
                            colocateRanges, colocateColumnCount);
                }
            }
        } catch (StarRocksException e) {
            throw new TabletReshardException(
                    "Failed to create new shards on StarOS: " + e.getMessage(), e);
        }
    }

    private void createShardsForOneIndex(OlapTable table,
                                         PhysicalPartition physicalPartition,
                                         ReshardingMaterializedIndex reshardingIndex,
                                         List<ColocateRange> colocateRanges,
                                         int colocateColumnCount) throws StarRocksException {
        MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
        MaterializedIndex oldIndex = physicalPartition.getIndex(reshardingIndex.getMaterializedIndexId());
        if (colocateRanges != null) {
            // Range-colocate invariant (P1): the old MaterializedIndex must exist so we can
            // derive each tablet's TabletRange for the PACK lookup.
            Preconditions.checkState(oldIndex != null,
                    "Missing old MaterializedIndex for range-colocate split");
        }

        // Pre-split (empty source, rowCount == 0) is the case whose following load wants to fan out.
        // Its new shards must NOT be created into a PACK colocate group here: at PENDING the group's
        // ColocateRanges are still the OLD/parent ones (the per-bucket ranges are only spliced in
        // post-publish), so every new shard would resolve to the single parent PACK group. PACK
        // affinity (+10000) dwarfs the SPREAD penalty (-100), so a batch sharing one PACK group herds
        // onto a single CN (StarOS #1275) and the following load opens ChannelNum=1. Hoisted above the
        // loop so addShardPlacementsForTablet can drop the PACK group for pre-split; the correct
        // per-bucket PACK groups + alignment are still established by the post-publish
        // applyColocateRangeSplitResult()/reconcile backstop (unchanged). Online split (non-empty)
        // keeps its PACK grouping (and the WITH_SHARD pin below).
        boolean spreadNewShards = oldIndex != null && oldIndex.getRowCount() == 0;

        // LinkedHashMap so the CreateShardInfo list within each (partition, index) RPC
        // payload follows the ReshardingTablet iteration order; the same batch produced
        // by a leader-switch re-run emits a byte-equivalent payload.
        Map<Long, Long> newToOldShardId = new LinkedHashMap<>();
        Map<Long, List<Long>> newShardIdToGroupIds = new LinkedHashMap<>();
        for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
            addShardPlacementsForTablet(reshardingTablet, oldIndex, newIndex,
                    colocateRanges, colocateColumnCount, spreadNewShards,
                    newToOldShardId, newShardIdToGroupIds);
        }
        if (newToOldShardId.isEmpty()) {
            return;
        }

        long physicalPartitionId = physicalPartition.getId();
        Map<String, String> shardProperties = new HashMap<>();
        shardProperties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        shardProperties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
        shardProperties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(newIndex.getId()));

        // spreadNewShards (computed above) also drops the WITH_SHARD pin here: pre-split splits a
        // freshly created EMPTY tablet, so there is no warm cache to preserve and the new shards
        // should spread rather than pin to the source worker. Online split (non-empty source) keeps
        // the WITH_SHARD pin to reuse the source worker's warm cache.
        // Schedule the new shards to the triggering load's warehouse when one was set (pre-split),
        // otherwise the background warehouse (online split) — unified with the publish path.
        ComputeResource computeResource = resolveComputeResource(table.getId());
        GlobalStateMgr.getCurrentState().getStarOSAgent().createShardsForSplit(
                newToOldShardId,
                newShardIdToGroupIds,
                table.getPartitionFilePathInfo(physicalPartitionId),
                table.getPartitionFileCacheInfo(physicalPartitionId),
                shardProperties, computeResource,
                spreadNewShards);
    }

    @VisibleForTesting
    static void addShardPlacementsForTablet(ReshardingTablet reshardingTablet,
                                                    MaterializedIndex oldIndex,
                                                    MaterializedIndex newIndex,
                                                    List<ColocateRange> colocateRanges,
                                                    int colocateColumnCount,
                                                    boolean spreadNewShards,
                                                    Map<Long, Long> newToOldShardId,
                                                    Map<Long, List<Long>> newShardIdToGroupIds) {
        long oldTabletId = reshardingTablet.getFirstOldTabletId();
        List<Long> newTabletIds = reshardingTablet.getNewTabletIds();
        SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
        List<TabletRange> perNewTabletRanges = splittingTablet != null
                ? splittingTablet.getNewTabletRanges()
                : List.of();

        Tablet oldTablet = null;
        if (colocateRanges != null) {
            oldTablet = oldIndex.getTablet(oldTabletId);
            // Range-colocate invariant (P1): every tablet of a range-colocate table has a
            // TabletRange. Fail fast rather than fall back to prefix=null, which would
            // silently route new shards into the first ColocateRange's PACK group — wrong
            // once the group has multiple ranges.
            Preconditions.checkState(oldTablet != null && oldTablet.getRange() != null,
                    "Old tablet %s in range-colocate group has no TabletRange", oldTabletId);
        }

        for (int i = 0; i < newTabletIds.size(); i++) {
            long newTabletId = newTabletIds.get(i);
            List<Long> groupIds = new ArrayList<>(2);
            groupIds.add(newIndex.getShardGroupId()); // SPREAD group is unchanged
            // Pre-split (spreadNewShards): deliberately omit the PACK colocate group. At this point the
            // group's registered ColocateRanges are still the OLD/parent ones (the per-bucket ranges are
            // spliced only post-publish), so every new shard would resolve to the single parent PACK
            // group and StarOS would herd the whole batch onto one CN (PACK +10000 ≫ SPREAD -100). With
            // only the SPREAD group the fresh shards spread at creation; the post-publish
            // applyColocateRangeSplitResult()/reconcile backstop then assigns each shard its correct
            // per-bucket PACK group. Online split keeps its PACK grouping (its ranges are already final).
            if (colocateRanges != null && !spreadNewShards) {
                Range<Tuple> rangeForPackLookup = i < perNewTabletRanges.size()
                        ? perNewTabletRanges.get(i).getRange()
                        : oldTablet.getRange().getRange();
                groupIds.add(lookupPackGroupId(colocateRanges, colocateColumnCount,
                        rangeForPackLookup, newTabletId));
            }
            newToOldShardId.put(newTabletId, oldTabletId);
            newShardIdToGroupIds.put(newTabletId, groupIds);
        }
    }

    private static long lookupPackGroupId(List<ColocateRange> colocateRanges, int colocateColumnCount,
                                          Range<Tuple> range, long newTabletId) {
        long packShardGroupId = ColocateRangeUtils.lookupPackShardGroupId(
                range, colocateRanges, colocateColumnCount);
        Preconditions.checkState(packShardGroupId != PhysicalPartition.INVALID_SHARD_GROUP_ID,
                "Tablet %s has no covering ColocateRange", newTabletId);
        return packShardGroupId;
    }
}
