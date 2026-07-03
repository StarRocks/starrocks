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

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.Utils;
import com.starrocks.lake.vector.VectorIndexBuildScheduler;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.Warehouse;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;

/**
 * Abstract parent for shared-data (lake) online rewrite schema-change jobs that re-route every
 * physical partition's data into a fresh K-tablet shadow index, double-write incoming loads into
 * the shadow once a watershed is allocated, rewrite the historical snapshot into the shadow via an
 * internal INSERT, and finish with a single atomic catalog flip.
 *
 * <p>This base holds the generic state machine shared by such jobs: the per-partition rewrite state
 * ({@link PartitionRewriteState} in {@link #partitionStates}), the shadow index identity, the
 * watershed allocation / shadow exposure, the resumable per-partition rewrite, the reserve / publish
 * / flip chain, force-cancel handling, and journal replay. The rewrite-flavor specifics (how a
 * partition's shadow layout is planned, how the shadow index meta is registered, what the atomic
 * flip mutates, which columns inactivate dependent MVs, and what the rewrite INSERT selects) are
 * left to the subclass through the abstract hooks declared below.
 *
 * <p>The job drives the full {@code PENDING -> WAITING_TXN -> RUNNING -> FINISHED_REWRITING ->
 * FINISHED} lifecycle (with {@code CANCELLED} as the failure/force terminus).
 */
public abstract class LakeOnlineRewriteJobBase
        extends LakeTableSchemaChangeJobBase {
    private static final Logger LOG = LogManager.getLogger(LakeOnlineRewriteJobBase.class);

    // The shadow index meta id shared across all physical partitions (one new index per table).
    @SerializedName(value = "shadowIndexMetaId")
    protected long shadowIndexMetaId = -1;
    // The origin (base) index meta id this shadow replaces at flip.
    @SerializedName(value = "originIndexMetaId")
    protected long originIndexMetaId = -1;
    // The shadow index name (__starrocks_shadow_xxx).
    @SerializedName(value = "shadowIndexName")
    protected String shadowIndexName;

    // Transient: cached full DB name (e.g. "catalog.db") resolved once in runPendingJob and
    // reused by runRunningJob. DB rename is not supported during an ALTER, so the name is
    // stable for the job's lifetime. Not serialized; re-resolved after replay on first tick.
    private transient String cachedDbName;

    /**
     * Durable per-partition state of the rewrite, journaled inside {@link #partitionStates}. Built
     * incrementally: shadowIndex/tabletCount/boundaries in PENDING, watershedVersion in WAITING_TXN,
     * rewriteTxnId in RUNNING, commitVersion at the flip. The version/txn fields are boxed (null = not
     * yet set) to preserve the absent-key semantics the six maps had.
     */
    static final class PartitionRewriteState {
        @SerializedName(value = "shadowIndex")
        MaterializedIndex shadowIndex;
        @SerializedName(value = "tabletCount")
        int tabletCount;
        @SerializedName(value = "boundaries")
        List<Tuple> boundaries;
        @SerializedName(value = "watershedVersion")
        Long watershedVersion;
        @SerializedName(value = "rewriteTxnId")
        Long rewriteTxnId;
        @SerializedName(value = "commitVersion")
        Long commitVersion;

        PartitionRewriteState() {}

        PartitionRewriteState(PartitionRewriteState other) {
            this.shadowIndex = other.shadowIndex;
            this.tabletCount = other.tabletCount;
            this.boundaries = other.boundaries == null ? null : new ArrayList<>(other.boundaries);
            this.watershedVersion = other.watershedVersion;
            this.rewriteTxnId = other.rewriteTxnId;
            this.commitVersion = other.commitVersion;
        }
    }

    // physical partition id -> durable per-partition rewrite state.
    @SerializedName(value = "partitionStates")
    protected Map<Long, PartitionRewriteState> partitionStates = Maps.newHashMap();

    protected PartitionRewriteState stateOf(long physicalPartitionId) {
        return partitionStates.computeIfAbsent(physicalPartitionId, k -> new PartitionRewriteState());
    }

    // for deserialization
    public LakeOnlineRewriteJobBase(JobType jobType) {
        super(jobType);
    }

    public LakeOnlineRewriteJobBase(long jobId, JobType jobType, long dbId, long tableId, String tableName,
                                    long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    // Deep-copy constructor used by copyForPersist(): the WAL must record a snapshot, not a live
    // reference, so journaling never mutates the running job's state. Chains through
    // AlterJobV2(AlterJobV2)/LakeTableSchemaChangeJobBase(...) for the base + watershed fields and
    // deep-copies the generic rewrite state. Transient executors/seams are intentionally NOT copied.
    protected LakeOnlineRewriteJobBase(LakeOnlineRewriteJobBase other) {
        super(other);
        this.shadowIndexMetaId = other.shadowIndexMetaId;
        this.originIndexMetaId = other.originIndexMetaId;
        this.shadowIndexName = other.shadowIndexName;
        this.partitionStates = Maps.newHashMap();
        for (Map.Entry<Long, PartitionRewriteState> entry : other.partitionStates.entrySet()) {
            this.partitionStates.put(entry.getKey(), new PartitionRewriteState(entry.getValue()));
        }
    }

    public long getShadowIndexMetaId() {
        return shadowIndexMetaId;
    }

    @VisibleForTesting
    public MaterializedIndex getShadowIndex(long physicalPartitionId) {
        PartitionRewriteState partitionState = partitionStates.get(physicalPartitionId);
        return partitionState == null ? null : partitionState.shadowIndex;
    }

    @VisibleForTesting
    public int getTabletCount(long physicalPartitionId) {
        PartitionRewriteState partitionState = partitionStates.get(physicalPartitionId);
        return partitionState == null ? 0 : partitionState.tabletCount;
    }

    @VisibleForTesting
    public List<Tuple> getBoundaries(long physicalPartitionId) {
        PartitionRewriteState partitionState = partitionStates.get(physicalPartitionId);
        return partitionState == null ? null : partitionState.boundaries;
    }

    @VisibleForTesting
    public Long getWatershedVersion(long physicalPartitionId) {
        PartitionRewriteState partitionState = partitionStates.get(physicalPartitionId);
        return partitionState == null ? null : partitionState.watershedVersion;
    }

    @VisibleForTesting
    public Long getRewriteTxnId(long physicalPartitionId) {
        PartitionRewriteState partitionState = partitionStates.get(physicalPartitionId);
        return partitionState == null ? null : partitionState.rewriteTxnId;
    }

    @VisibleForTesting
    public void setRewriteTxnIdForTest(long physicalPartitionId, long txnId) {
        stateOf(physicalPartitionId).rewriteTxnId = txnId;
    }

    // ---- Abstract hooks: the rewrite-flavor specifics ---------------------------------------------

    /** Plan one partition's shadow layout and build its shadow MaterializedIndex (runPendingJob stage 2,
     *  lock-free). Sets plan.boundaries/shadowTabletCount/shadowIndex. Range rewrite samples by the new sort key;
     *  a future optimize derives layout from the new distribution. */
    protected abstract void planPartitionShadow(PendingPartitionPlan plan, OlapTable table, String dbName)
            throws AlterCancelException;

    /** Register the shadow index's MaterializedIndexMeta (PENDING stage 3, under the write lock).
     *  Idempotent (re-runnable on replay). */
    protected abstract void registerShadowIndexMeta(OlapTable table);

    /** The atomic catalog flip (under the edit-log applier's write lock). MUST iterate all physical
     *  partitions, assert each has a reserved commitVersion, and be idempotent (guard a second replay). */
    protected abstract void visualiseShadowIndex(OlapTable table);

    /** Columns whose dependent async MVs are inactivated at the flip (empty if none). */
    protected abstract Set<String> affectedColumnsForMvInactivation(OlapTable table);

    /** Non-generated column names the rewrite INSERT selects (the rewrite is INSERT INTO ... SELECT these
     *  FROM the same partition). Range rewrite returns the new-schema column names (== base column names,
     *  column set unchanged); kept as a hook so a future flow can differ. */
    protected abstract List<String> rewriteSelectColumnNames(OlapTable table);

    /** Validate subclass-specific job configuration before the PENDING stages run. Throws
     *  AlterCancelException if the rewrite config is incomplete. Called once at the top of runPendingJob,
     *  before stage 1 — the base cannot see subclass config fields. */
    protected abstract void validateRewriteConfig() throws AlterCancelException;

    // ---- Overridable defaults: the range rewrite keeps these; a sibling job may override -----------

    /**
     * The {@link OlapTable.OlapTableState} this job holds the table in while it runs (asserted and set
     * across the PENDING -&gt; RUNNING phases and re-applied on replay). Each subclass declares its own
     * working state ({@code LakeRangeRewriteSchemaChangeJob} uses {@code SCHEMA_CHANGE}; a range OPTIMIZE
     * would use {@code OPTIMIZE}); it should agree with the {@code JobType} the subclass passes to the
     * constructor. Must be a stable, non-{@code NORMAL} state: {@code NORMAL} is the idle terminus this job
     * resets to at flip/cancel, so returning it would collapse the idle and in-progress states and defeat
     * the entry/replay guards.
     */
    protected abstract OlapTable.OlapTableState jobTableState();

    /**
     * Validate the logical-to-physical partition shape before a partition's rewrite INSERT in
     * {@link #runRunningJob}. The INSERT scans the whole logical parent but the read-version override pins
     * only one physical partition, so the default requires a 1:1 logical:physical mapping — a &gt;1:1
     * mapping would leave the other physical partitions reading their live (post-watershed) version and
     * double-count. A sibling job whose rewrite does not rely on that invariant can relax this.
     */
    protected void validateRewritePartitionShape(Partition logicalParent) throws AlterCancelException {
        if (logicalParent.getSubPartitions().size() != 1) {
            throw new AlterCancelException("range-rewrite requires a 1:1 logical-to-physical partition "
                    + "mapping, but logical partition " + logicalParent.getId() + " has "
                    + logicalParent.getSubPartitions().size() + " physical partitions");
        }
    }

    // ---- PENDING ---------------------------------------------------------------------------------

    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        Preconditions.checkState(shadowIndexMetaId != -1, "shadow index meta id is not set");
        validateRewriteConfig();

        // Stage 1 (READ-lock snapshot): validate table state and capture the immutable per-partition
        // inputs needed for sampling and shard building. A DB WRITE lock must NOT be held across the
        // sampling SQL round-trip or the StarOS createShards RPC (fe/CLAUDE.md "Database and Table
        // Locks"), so the heavy work in stage 2 runs lock-free. Mirrors LakeTableSchemaChangeJob's
        // READ-snapshot -> network-outside-lock -> WRITE-commit shape.
        OlapTable table;
        List<PendingPartitionPlan> pendingPlans = new ArrayList<>();
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.READ)) {
            table = getTableOrThrow();
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.NORMAL
                            || table.getState() == jobTableState(),
                    table.getState());
            cachedDbName = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId).getFullName();
            String tableName = table.getName();

            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                long physicalPartitionId = physicalPartition.getId();
                // Resolve by META id, not physical index id: a range-distribution table that has gone
                // through tablet split/merge resharding keeps the same index meta id but the latest
                // MaterializedIndex carries a new physical index id, so getIndex(metaId) (a physical-id
                // lookup) would miss it. getLatestIndex(metaId) is the meta-id lookup the rest of the
                // alter/reshard code uses (e.g. LakeTableAlterJobV2Builder).
                MaterializedIndex baseIndex = physicalPartition.getLatestIndex(originIndexMetaId);
                if (baseIndex == null) {
                    throw new AlterCancelException("base index missing in partition " + physicalPartitionId);
                }
                // The sampler addresses the logical parent by name; capture it now so stage 2 needs no lock.
                String partitionName = table.getPartition(physicalPartition.getParentId()).getName();
                pendingPlans.add(new PendingPartitionPlan(physicalPartitionId, baseIndex,
                        baseIndex.getShardGroupId(), partitionName, baseIndex.getDataSize(),
                        tableName, physicalPartition));
            }
        }

        // Stages 2-3 run after the read lock is released and create StarOS shards before recording them
        // in partitionStates (stage 3). cancelImpl cleans only recorded shadow indexes, so on any
        // failure here drop the shards built in stage 2 that were not yet recorded, otherwise a transient
        // create failure or a concurrent partition change would orphan them until lake GC.
        // The applier below advances this.jobState to WAITING_TXN as its first action once the journal is
        // durable (the edit log runs the applier only after writing the entry). The finally uses that same
        // jobState as the single commit marker: PENDING means no durable WAITING_TXN entry, so drop the
        // shards this run built; WAITING_TXN means the journal already references them, so keep them.
        try {
            // Stage 2 (lock-free): per partition, plan the shadow layout and build the shadow
            // MaterializedIndex via the StarOS createShards RPC. NO db lock is held here. The work reads
            // stable path/medium/id metadata off the live table/partition references captured above,
            // exactly as LakeTableSchemaChangeJob.runPendingJob reads its snapshotted index objects when
            // sending create-replica tasks outside the lock.
            for (PendingPartitionPlan plan : pendingPlans) {
                planPartitionShadow(plan, table, cachedDbName);
            }

            // Stage 3 (WRITE-lock commit): re-fetch and re-validate the table (TOCTOU: it may have been
            // dropped/altered while the lock was released), then install the built shadow indexes, journal
            // the durable state, and transition to WAITING_TXN.
            try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
                OlapTable committedTable = getTableOrThrow();
                Preconditions.checkState(committedTable.getState() == OlapTable.OlapTableState.NORMAL
                                || committedTable.getState() == jobTableState(),
                        committedTable.getState());
                // The partition set must be unchanged between snapshot and commit; a partition that was
                // dropped/added in the window means the sampled boundaries no longer describe the data, so
                // fail the alter cleanly rather than silently skipping.
                for (PendingPartitionPlan plan : pendingPlans) {
                    if (committedTable.getPhysicalPartition(plan.physicalPartitionId) == null) {
                        throw new AlterCancelException("partition " + plan.physicalPartitionId
                                + " was dropped while building the range-rewrite shadow index");
                    }
                }
                if (committedTable.getPhysicalPartitions().size() != pendingPlans.size()) {
                    throw new AlterCancelException(
                            "partition set changed while building the range-rewrite shadow index");
                }

                for (PendingPartitionPlan plan : pendingPlans) {
                    PartitionRewriteState partitionState = stateOf(plan.physicalPartitionId);
                    partitionState.shadowIndex = plan.shadowIndex;
                    partitionState.tabletCount = plan.shadowTabletCount;
                    partitionState.boundaries = plan.boundaries;
                }

                // Journal the durable job state (the shadow index id, K, boundaries, and shadow tablet ids
                // recorded in partitionStates above) FIRST, then apply the catalog mutations in the
                // applier. The edit log runs the applier only after the WAITING_TXN entry is durably
                // written, so the catalog install is atomic with the journal: a journal/serialization
                // failure leaves no shadow-index meta or inverted-index entry to orphan, and replay
                // re-applies the install from the journaled partitionStates (reconstructShadowCatalogState).
                // The shadow index meta/tablets are registered, but the per-partition shadow
                // MaterializedIndex is intentionally NOT exposed to the catalog index map yet: incoming
                // loads must not double-write the shadow until the watershed is allocated (WAITING_TXN).
                persistStateChange(this, JobState.WAITING_TXN, () -> {
                    // The journal is durable by the time this applier runs. Advance the live jobState to
                    // WAITING_TXN as the FIRST action -- before the catalog install -- so the live job can
                    // never sit at PENDING against a durable WAITING_TXN entry: even an (effectively
                    // impossible, write-lock + TOCTOU-guarded) throw from the install below cannot then make
                    // the scheduler re-run runPendingJob and build a second shadow set, nor make the finally
                    // delete shards the durable entry already references. persistStateChange assigns jobState
                    // again after the applier returns (a harmless no-op). If the install does throw, restart
                    // replays the entry and reconstructShadowCatalogState re-applies it idempotently.
                    this.jobState = JobState.WAITING_TXN;
                    registerShadowIndexMeta(committedTable);
                    addShadowTabletsToInvertedIndex(committedTable);
                    committedTable.setState(jobTableState());
                });
            }
        } finally {
            if (jobState != JobState.WAITING_TXN) {
                // jobState is still PENDING: the applier advances it to WAITING_TXN as its first action once
                // the journal is durable, so PENDING here means no WAITING_TXN entry was journaled and
                // everything this run built is an orphan. Drop ALL stage-2 shards (including any already
                // copied into partitionStates) on ANY failure -- AlterCancelException or an unchecked one (a
                // Preconditions check, a createShards RPC failure, or a journal/serialization failure) -- and
                // clear the partitionStates entries populated this run so a retry starts clean and cannot
                // lose the cleanup handle. The catalog install (shadow meta + inverted-index entries) runs
                // only in the post-journal applier above, so a failure before the journal commits leaves no
                // catalog entry to undo.
                dropOrphanedShadowShards(pendingPlans);
                for (PendingPartitionPlan plan : pendingPlans) {
                    partitionStates.remove(plan.physicalPartitionId);
                }
            }
        }

        if (span != null) {
            span.addEvent("setWaitingTxn");
        }
        LOG.info("transfer online rewrite job {} state to {}", jobId, this.jobState);
    }

    /**
     * Drop the StarOS shards built in {@link #runPendingJob} stage 2 on a pending-phase failure.
     * Called from the {@code runPendingJob} cleanup when the WAITING_TXN journal did not commit, so
     * every built shadow shard is unreferenced -- the catalog install (shadow index meta + inverted-index
     * entries) runs only in the post-journal applier, so there is no catalog or inverted-index entry to
     * undo. Best-effort: lake GC reclaims the shards even if this delete fails.
     */
    private void dropOrphanedShadowShards(@NotNull List<PendingPartitionPlan> plans) {
        Set<Long> orphanShardIds = Sets.newHashSet();
        for (PendingPartitionPlan plan : plans) {
            if (plan.shadowIndex == null) {
                continue;
            }
            for (Tablet tablet : plan.shadowIndex.getTablets()) {
                orphanShardIds.add(tablet.getId());
            }
        }
        if (orphanShardIds.isEmpty()) {
            return;
        }
        try {
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(orphanShardIds);
        } catch (Exception e) {
            LOG.warn("online rewrite job {}: failed to drop {} orphaned shadow shards; "
                    + "lake GC will reclaim them: {}", jobId, orphanShardIds.size(), e.getMessage());
        }
    }

    /**
     * Mutable per-partition working state for {@link #runPendingJob}: the immutable inputs captured
     * under the read lock (ids + base index + shard group), and the boundaries / shadow tablet count / built
     * shadow index produced lock-free in stage 2 by {@link #planPartitionShadow} and consumed under the
     * write lock in stage 3.
     */
    protected static final class PendingPartitionPlan {
        final long physicalPartitionId;
        final MaterializedIndex baseIndex;
        final long shardGroupId;
        final String partitionName;
        final long partitionDataSize;
        final String tableName;
        // Live references captured under the read lock; read lock-free in stage 2 for stable metadata.
        final PhysicalPartition physicalPartition;
        List<Tuple> boundaries;
        int shadowTabletCount;
        MaterializedIndex shadowIndex;

        PendingPartitionPlan(long physicalPartitionId, MaterializedIndex baseIndex, long shardGroupId,
                             String partitionName, long partitionDataSize, String tableName,
                             PhysicalPartition physicalPartition) {
            this.physicalPartitionId = physicalPartitionId;
            this.baseIndex = baseIndex;
            this.shardGroupId = shardGroupId;
            this.partitionName = partitionName;
            this.partitionDataSize = partitionDataSize;
            this.tableName = tableName;
            this.physicalPartition = physicalPartition;
        }
    }

    /**
     * Idempotently add the per-partition shadow tablets to the tablet inverted index. Guarded so a
     * second call (e.g. a replay re-run) does not double-add: a tablet already present is skipped.
     * The caller holds the database write lock.
     */
    protected void addShadowTabletsToInvertedIndex(@NotNull OlapTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Map.Entry<Long, PartitionRewriteState> entry : partitionStates.entrySet()) {
            long physicalPartitionId = entry.getKey();
            MaterializedIndex shadowIndex = entry.getValue().shadowIndex;
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                continue;
            }
            TStorageMedium medium = table.getPartitionInfo()
                    .getDataProperty(physicalPartition.getParentId()).getStorageMedium();
            TabletMeta shadowTabletMeta =
                    new TabletMeta(dbId, tableId, physicalPartitionId, shadowIndexMetaId, medium, true);
            for (Tablet shadowTablet : shadowIndex.getTablets()) {
                if (invertedIndex.getTabletMeta(shadowTablet.getId()) == null) {
                    invertedIndex.addTablet(shadowTablet.getId(), shadowTabletMeta);
                }
            }
        }
    }

    // ---- WAITING_TXN -----------------------------------------------------------------------------

    /**
     * Drives the {@code WAITING_TXN} phase, mirroring the standard lake schema-change watershed.
     *
     * <p>On the first run ({@code watershedTxnId == -1}) it allocates the watershed transaction id,
     * exposes each partition's shadow {@link MaterializedIndex} to the catalog with
     * {@code visibleTxnId = watershedTxnId} (so loads after the watershed double-write base+shadow
     * while loads before it ignore the shadow), runs the next-txn guard, and re-journals
     * {@code WAITING_TXN}.
     *
     * <p>On each subsequent run it waits until every transaction before the watershed has finished;
     * only then does it capture each physical partition's visible version as the per-partition
     * watershed version {@code W} (the rewrite must re-sort everything visible at the watershed) and
     * transition to {@code RUNNING}.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        if (watershedTxnId == -1) {
            allocateWatershedAndExposeShadow();
            return;
        }

        try {
            if (!isPreviousLoadFinished(dbId, tableId, watershedTxnId)) {
                LOG.info("wait transactions before {} to be finished, online rewrite job: {}",
                        watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, capturing watershed versions. job: {}", jobId);

        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTableOrThrow();
            Preconditions.checkState(table.getState() == jobTableState(), table.getState());
            // Capture W = visible version only after the drain: anything visible at the watershed must
            // be re-sorted by the new key in RUNNING.
            for (long physicalPartitionId : partitionStates.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);
                long watershedVersion = physicalPartition.getVisibleVersion();
                stateOf(physicalPartitionId).watershedVersion = watershedVersion;
                // Block vacuum of the watershed snapshot W the RUNNING rewrite SELECT will read. Pin it
                // here, at the durable WAITING_TXN -> RUNNING transition, so it is in place before any
                // leader crash; minRetainVersion is not persisted, so replay re-applies it (see
                // reconstructShadowCatalogState).
                physicalPartition.setMinRetainVersion(watershedVersion);
            }
            // Journal the captured watershed versions before transitioning to RUNNING.
            persistStateChange(this, JobState.RUNNING);
        }

        if (span != null) {
            span.addEvent("setRunning");
        }
        LOG.info("transfer online rewrite job {} state to {}", jobId, this.jobState);
    }

    /**
     * First {@code WAITING_TXN} run: allocate the watershed transaction id and expose every
     * partition's shadow index to the catalog so post-watershed loads double-write it. Mirrors the
     * standard lake schema-change watershed, including the next-txn guard that detects a transaction
     * sneaking in between allocating the watershed and exposing the shadow.
     */
    private void allocateWatershedAndExposeShadow() throws AlterCancelException {
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTableOrThrow();
            Preconditions.checkState(table.getState() == jobTableState(), table.getState());
            watershedTxnId = getNextTransactionId();
            watershedGtid = getNextGtid();
            exposeShadowIndexToCatalog(table);

            // Getting the watershed and exposing the shadow are not atomic. A transaction beginning in
            // between is safe as long as it gets the tablet list (under the database lock) after
            // beginTransaction(), so it sees the shadow and writes to it; all import transactions do
            // this. beginTransaction()-first is a convention not a requirement, so guard against a new
            // beginTransaction() succeeding in the window, which would let a txn > watershedTxnId miss
            // the shadow.
            long nextTxnId = peekNextTransactionId();
            if (nextTxnId != watershedTxnId + 1) {
                throw new AlterCancelException(
                        "concurrent transaction detected while adding shadow index, please re-run the alter table command");
            }

            // Re-journal WAITING_TXN so the allocated watershed and the exposure are durable; the
            // exposure is idempotent, so a leader transfer re-runs it harmlessly.
            persistStateChange(this, JobState.WAITING_TXN);
        }

        if (span != null) {
            span.setAttribute("watershedTxnId", this.watershedTxnId);
            span.addEvent("exposeShadow");
        }
        LOG.info("online rewrite job {} allocated watershed txn id {} and exposed shadow index",
                jobId, watershedTxnId);
    }

    /**
     * Idempotently expose every partition's shadow {@link MaterializedIndex} to the catalog index map
     * with {@code visibleTxnId = watershedTxnId}, so loads after the watershed double-write the shadow
     * while loads before it ignore it. A partition already carrying the shadow (e.g. a replay re-run)
     * is skipped. The caller holds the database write lock.
     */
    private void exposeShadowIndexToCatalog(@NotNull OlapTable table) {
        Preconditions.checkState(watershedTxnId != -1, "watershed txn id is not set");
        for (Map.Entry<Long, PartitionRewriteState> entry : partitionStates.entrySet()) {
            long physicalPartitionId = entry.getKey();
            MaterializedIndex shadowIndex = entry.getValue().shadowIndex;
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                continue;
            }
            if (physicalPartition.getLatestIndex(shadowIndexMetaId) != null) {
                // Already exposed (replay re-run): do not double-add.
                continue;
            }
            Preconditions.checkState(shadowIndex.getState() == MaterializedIndex.IndexState.SHADOW,
                    shadowIndex.getState());
            shadowIndex.setVisibleTxnId(watershedTxnId);
            physicalPartition.createRollupIndex(shadowIndex);
        }
    }

    // ---- RUNNING ---------------------------------------------------------------------------------

    /**
     * Drives the {@code RUNNING} phase: for each physical partition not yet rewritten, run a
     * version-pinned internal {@code INSERT...SELECT} that re-sorts/dedups the watershed snapshot into
     * the shadow index only, and resume safely across FE leader failover.
     *
     * <p>The SELECT is pinned to the per-partition watershed version {@code W} via
     * {@code ConnectContext.setScanVersionOverride}; the write targets only the shadow index
     * ({@code targetWriteIndexId}); and the publish converts the committed {@code op_write} into
     * {@code op_schema_change@W} keyed by {@code watershedTxnId} (the carrier is wired into the
     * {@link com.starrocks.transaction.InsertTxnCommitAttachment} via {@code InsertStmt}). The commit
     * version is <em>not</em> pinned ({@code isVersionOverwrite} is deliberately not used).
     *
     * <p>Resume is three-state per partition's journaled rewrite txn id:
     * <ul>
     *   <li>published/converted (VISIBLE) &rarr; skip;</li>
     *   <li>committed-not-yet-visible (COMMITTED/PREPARE/PREPARED) &rarr; stay in {@code RUNNING} and
     *       wait for the next scheduler tick;</li>
     *   <li>not-started (no journaled id / txn dropped) or aborted &rarr; re-run the same
     *       watershed-pinned INSERT under a fresh txn id.</li>
     * </ul>
     * The shadow tablets are never dropped/rebuilt on resume: post-watershed double-writes have
     * already published into them, so dropping would lose data.
     *
     * <p>When every partition's rewrite has published, the job transitions to
     * {@code FINISHED_REWRITING}; the atomic flip is performed there (a later phase).
     */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        // Snapshot the per-partition INSERT plans (logical partition name + base column projection)
        // under the database lock and pin the watershed snapshot against vacuum; the executions
        // themselves run without holding the lock.
        List<RewritePlan> plans = new ArrayList<>();
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTableOrThrow();
            Preconditions.checkState(table.getState() == jobTableState(), table.getState());
            if (cachedDbName == null) {
                // Fallback for replayed jobs where runPendingJob was not re-run in this JVM lifetime.
                cachedDbName = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId).getFullName();
            }

            List<String> selectColumnNames = rewriteSelectColumnNames(table);

            for (long physicalPartitionId : partitionStates.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    throw new AlterCancelException("partition " + physicalPartitionId + " missing during rewrite");
                }
                Long watershedVersion = partitionStates.get(physicalPartitionId).watershedVersion;
                Preconditions.checkState(watershedVersion != null,
                        "watershed version not captured for partition " + physicalPartitionId);
                // Re-assert the vacuum pin on the watershed snapshot the SELECT reads (first set at the
                // WAITING_TXN capture / re-applied by replay; re-asserting here is idempotent).
                physicalPartition.setMinRetainVersion(watershedVersion);
                // A physical partition is not directly addressable in SQL; address its parent logical
                // partition by name, exactly as the PENDING-phase sampler does. The read-version
                // override (keyed by physical partition id) pins the SELECT to the watershed snapshot.
                Partition logicalParent = table.getPartition(physicalPartition.getParentId());
                // Guard the logical-to-physical partition shape the watershed-pinned rewrite relies on
                // (default: a 1:1 mapping). Extracted to an overridable hook so a sibling job can relax it.
                validateRewritePartitionShape(logicalParent);
                String partitionName = logicalParent.getName();
                plans.add(new RewritePlan(physicalPartitionId, partitionName, watershedVersion, selectColumnNames));
            }
        }

        for (RewritePlan plan : plans) {
            switch (classifyRewrite(plan.physicalPartitionId)) {
                case DONE:
                    continue;
                case IN_FLIGHT:
                    // Committed-not-yet-visible: stay in RUNNING; the scheduler re-invokes this method.
                    LOG.info("online rewrite job {}: rewrite txn {} for partition {} in flight, waiting",
                            jobId, partitionStates.get(plan.physicalPartitionId).rewriteTxnId,
                            plan.physicalPartitionId);
                    return;
                case NEEDS_RUN:
                default:
                    runPartitionRewrite(cachedDbName, plan);
                    // After kicking off (or completing) one partition's rewrite, yield: re-check status on
                    // the next scheduler tick rather than blocking the scheduler thread on every partition.
                    return;
            }
        }

        // All partitions' rewrites have published: hand off to the flip phase.
        this.finishedTimeMs = System.currentTimeMillis();
        persistStateChange(this, JobState.FINISHED_REWRITING);

        if (span != null) {
            span.addEvent("finishedRewriting");
        }
        LOG.info("online rewrite job {} finished rewriting historical data, state to {}",
                jobId, this.jobState);
    }

    protected enum RewriteStatus {
        DONE,        // the journaled rewrite txn published (op_schema_change@W converted) -> skip
        IN_FLIGHT,   // committed but not yet visible -> wait for the next tick
        NEEDS_RUN    // never started / dropped / aborted -> (re-)run the watershed-pinned INSERT
    }

    /**
     * Classify a partition's rewrite by the status of its journaled rewrite txn id. A missing journaled
     * id, a txn the manager no longer knows about, or an ABORTED txn all mean "(re-)run". Never reuse an
     * aborted txn id (BE forbids it), so the next attempt journals a fresh id.
     *
     * <p>The journaled id is the next-txn-id <em>peeked</em> before the INSERT began, then reconciled to
     * the actual id only after {@code execute()} returns. A concurrent txn slipping into the peek-&gt;begin
     * window can make the journaled id belong to a foreign load txn; a leader crash between the peek-journal
     * and the reconcile-journal leaves that foreign id persisted. So a VISIBLE txn is treated as DONE only
     * when it is genuinely this job's shadow-rewrite carrier: {@code isShadowRewrite()} with a watershed txn
     * id matching this job and an alter version matching this partition's captured watershed {@code W}.
     * Otherwise (a foreign or mismatched VISIBLE txn) the journaled id does not count as DONE and the
     * partition is re-run; re-running is harmless because foreign/aborted txns published nothing into the
     * shadow and the real rewrite is re-issued under a fresh id.
     */
    protected RewriteStatus classifyRewrite(long physicalPartitionId) {
        PartitionRewriteState partitionState = partitionStates.get(physicalPartitionId);
        Long txnId = (partitionState == null) ? null : partitionState.rewriteTxnId;
        if (txnId == null) {
            return RewriteStatus.NEEDS_RUN;
        }
        TransactionState txnState =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionState(dbId, txnId);
        if (txnState == null) {
            // The manager no longer has this txn (never committed, or expired before publish): re-run.
            return RewriteStatus.NEEDS_RUN;
        }
        TransactionStatus status = txnState.getTransactionStatus();
        if (status == TransactionStatus.VISIBLE) {
            // Count VISIBLE as DONE only if this is genuinely THIS job's shadow-rewrite txn: a foreign
            // load txn that slipped into the peek->begin window and was journaled by mistake must NOT
            // skip the partition (that would flip a shadow missing op_schema_change@W -> data loss).
            Long watershedVersion = partitionState.watershedVersion;
            if (txnState.isShadowRewrite()
                    && txnState.getShadowRewriteWatershedTxnId() == watershedTxnId
                    && watershedVersion != null
                    && txnState.getShadowRewriteAlterVersion() == watershedVersion) {
                return RewriteStatus.DONE;
            }
            return RewriteStatus.NEEDS_RUN;
        }
        if (status == TransactionStatus.ABORTED || status == TransactionStatus.UNKNOWN) {
            return RewriteStatus.NEEDS_RUN;
        }
        // PREPARE / PREPARED / COMMITTED: in flight, wait.
        return RewriteStatus.IN_FLIGHT;
    }

    /**
     * Build, journal, and execute one partition's watershed-pinned shadow-rewrite INSERT.
     *
     * <p>The rewrite txn id is journaled <em>before</em> the INSERT executes (an aborted txn id cannot
     * be reused, so the next attempt must record a fresh id). The scan-version override is cleared in a
     * {@code finally}.
     */
    private void runPartitionRewrite(String dbName, RewritePlan plan) throws AlterCancelException {
        String sql = "insert into " + ParseUtil.backquote(dbName) + "." + ParseUtil.backquote(tableName)
                + " partition (" + ParseUtil.backquote(plan.partitionName) + ") select "
                + String.join(", ", plan.selectColumnNames)
                + " from " + ParseUtil.backquote(dbName) + "." + ParseUtil.backquote(tableName)
                + " partition (" + ParseUtil.backquote(plan.partitionName) + ")";

        ConnectContext context = buildConnectContext();
        context.setScanVersionOverride(Map.of(plan.physicalPartitionId, plan.watershedVersion));
        try (var scope = context.bindScope()) {
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            Preconditions.checkState(parsedStmt instanceof InsertStmt, "rewrite SQL did not parse to an INSERT");
            InsertStmt insertStmt = (InsertStmt) parsedStmt;
            // Write only the shadow index; convert its op_write to op_schema_change@W under
            // watershedTxnId at publish. Do NOT pin the commit version (no setIsVersionOverwrite).
            insertStmt.setShadowRewrite(true);
            insertStmt.setTargetWriteIndexId(shadowIndexMetaId);
            insertStmt.setShadowRewriteWatershedTxnId(watershedTxnId);
            insertStmt.setShadowRewriteAlterVersion(plan.watershedVersion);

            SessionVariable sessionVariable = context.getSessionVariable();
            sessionVariable.setUsePageCache(false);
            sessionVariable.setEnableMaterializedViewRewrite(false);
            sessionVariable.setInsertTimeoutS((int) (timeoutMs / 2000));

            // Journal the rewrite txn id BEFORE executing: peekNextTransactionId() is the id the
            // INSERT's beginTransaction will allocate, so record it as durable evidence of which txn
            // this attempt commits/converts. The actual id is re-confirmed from the parsed stmt below.
            long rewriteTxnId = peekNextTransactionId();
            stateOf(plan.physicalPartitionId).rewriteTxnId = rewriteTxnId;
            persistStateChange(this, JobState.RUNNING);

            getRewriteExecutor().execute(context, insertStmt);

            // Re-confirm the txn id the INSERT actually used (defends against a concurrent txn slipping
            // in between the peek and beginTransaction) and re-journal it for the resume classifier.
            long actualTxnId = insertStmt.getTxnId();
            if (actualTxnId != DmlStmt.INVALID_TXN_ID && actualTxnId != rewriteTxnId) {
                stateOf(plan.physicalPartitionId).rewriteTxnId = actualTxnId;
                persistStateChange(this, JobState.RUNNING);
            }

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("online rewrite job {}: rewrite INSERT failed for partition {}: {}",
                        jobId, plan.physicalPartitionId, context.getState().getErrorMessage());
                throw new AlterCancelException(context.getState().getErrorMessage());
            }
        } catch (AlterCancelException e) {
            throw e;
        } catch (Exception e) {
            throw new AlterCancelException("rewrite INSERT failed for partition " + plan.physicalPartitionId
                    + ": " + e.getMessage());
        } finally {
            context.setScanVersionOverride(null);
        }
    }

    /** Mirrors {@code OnlineOptimizeJobV2.buildConnectContext}: an inner ROOT context for the INSERT. */
    private ConnectContext buildConnectContext() {
        ConnectContext context = ConnectContext.buildInner();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setCurrentComputeResource(computeResource);
        return context;
    }

    /**
     * Seam over the internal-INSERT execution (which needs a backend). Production runs the statement via
     * {@link StmtExecutor}; tests inject a stub to assert the built {@link InsertStmt} and the
     * scan-version override without executing.
     */
    @FunctionalInterface
    interface RewriteExecutor {
        void execute(ConnectContext context, InsertStmt insertStmt) throws Exception;
    }

    // Execution seam. Production runs the real INSERT; tests inject a stub. Not serialized.
    private transient RewriteExecutor rewriteExecutor;

    @VisibleForTesting
    public void setRewriteExecutor(RewriteExecutor rewriteExecutor) {
        this.rewriteExecutor = rewriteExecutor;
    }

    private RewriteExecutor getRewriteExecutor() {
        if (rewriteExecutor == null) {
            rewriteExecutor = (context, insertStmt) -> {
                StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
                context.setExecutor(executor);
                context.setQueryId(UUIDUtil.genUUID());
                context.setStartTime();
                executor.execute();
            };
        }
        return rewriteExecutor;
    }

    /** Immutable per-partition rewrite plan snapshotted under the database lock. */
    private static final class RewritePlan {
        final long physicalPartitionId;
        final String partitionName;
        final long watershedVersion;
        final List<String> selectColumnNames;

        RewritePlan(long physicalPartitionId, String partitionName, long watershedVersion,
                    List<String> selectColumnNames) {
            this.physicalPartitionId = physicalPartitionId;
            this.partitionName = partitionName;
            this.watershedVersion = watershedVersion;
            this.selectColumnNames = selectColumnNames;
        }
    }

    // ---- FINISHED_REWRITING (reserve / publish / flip) -------------------------------------------

    /**
     * Drives the {@code FINISHED_REWRITING} phase: the single, idempotent atomic flip. It reuses the
     * proven lake schema-change flip chain (reserve commit version &rarr; {@code lakePublishVersion}
     * &rarr; {@code visualiseShadowIndex}), adapted to the K-tablet shadow index this job builds.
     *
     * <p>Step by step (mirrors {@link LakeTableSchemaChangeJob#runFinishedRewritingJob}):
     * <ol>
     *   <li><b>Reserve</b> a per-partition {@code commitVersion = physicalPartition.getNextVersion()}
     *       and bump {@code nextVersion} so no new load reuses it. Done once and journaled with the
     *       {@code FINISHED_REWRITING} re-entry record (idempotent: a re-run reuses the reserved map).</li>
     *   <li><b>Ready check</b>: every partition's {@code commitVersion == visibleVersion + 1}
     *       (all prior loads published), else return and re-check next tick.</li>
     *   <li><b>Publish</b>: {@code lakePublishVersion} publishes the K shadow tablets
     *       {@code baseVersion=1 -> commitVersion} (BE replays the {@code op_schema_change@W} base plus
     *       the post-watershed double-write vlogs) and the origin/base index tablets as a {@code TXN_EMPTY}
     *       no-op {@code commitVersion-1 -> commitVersion}.</li>
     *   <li><b>Flip</b>: {@code visualiseShadowIndex} atomically swaps each partition's base index for
     *       the shadow (and so the table-level schema / keysType / sortKeyIdxes / range distribution,
     *       all derived from the base index meta, follow), under the edit-log lock and the table write
     *       lock; then inactivates dependent async MVs, releases the GC pin, and resets the table state
     *       to {@code NORMAL}.</li>
     * </ol>
     */
    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.FINISHED_REWRITING, jobState);

        // Reserve the per-partition commit version once (idempotent across re-entry/replay): only the
        // first run with no reserved commit versions reserves + bumps nextVersion.
        reserveCommitVersionIfNeeded();

        // If the table or database has been dropped, readyToPublishVersion() throws AlterCancelException
        // and the job is cancelled.
        if (!readyToPublishVersion()) {
            return;
        }

        if (!getPublishExecutor().publish(this)) {
            return;
        }

        // Atomic catalog flip: replace each partition's base index with the shadow, flip the table-level
        // schema/keysType/sortKeyIdxes/distribution, and inactivate dependent async MVs.
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTable();
            if (table == null) {
                LOG.info("database or table dropped while flipping online rewrite job {}", jobId);
                return;
            }
            // Collect the columns whose meaning changed (keyness flip / sort-key reorder) BEFORE the flip,
            // so dependent async MVs hanging on the old schema can be inactivated.
            Set<String> modifiedColumns = affectedColumnsForMvInactivation(table);
            this.finishedTimeMs = System.currentTimeMillis();

            persistStateChange(this, JobState.FINISHED, () -> {
                // Below this point all queries and loads use the new schema/sort key.
                visualiseShadowIndex(table);
                AlterMVJobExecutor.inactiveRelatedMaterializedViewsRecursive(table, modifiedColumns);
                table.onReload();
            });
        }

        if (span != null) {
            span.end();
        }
        LOG.info("online rewrite job finished: {}", jobId);
    }

    /**
     * Reserve a per-partition commit version for the flip and bump each partition's {@code nextVersion}
     * past it, exactly as {@link LakeTableSchemaChangeJob#runRunningJob} does at its
     * {@code FINISHED_REWRITING} transition. Idempotent: once the commit versions are populated (a
     * re-entry after a non-ready tick, or a replayed job), the already-reserved versions are reused. The
     * reservation is journaled with the {@code FINISHED_REWRITING} re-entry record so a leader transfer
     * recovers the same versions.
     */
    private void reserveCommitVersionIfNeeded() throws AlterCancelException {
        if (partitionStates.values().stream().anyMatch(state -> state.commitVersion != null)) {
            return;
        }
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTableOrThrow();
            for (long physicalPartitionId : partitionStates.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);
                // Release the watershed-snapshot GC pin set in RUNNING: the historical snapshot has been
                // rewritten into the shadow, so it no longer needs protecting from vacuum.
                physicalPartition.setMinRetainVersion(0);
                long commitVersion = physicalPartition.getNextVersion();
                stateOf(physicalPartitionId).commitVersion = commitVersion;
            }

            persistStateChange(this, JobState.FINISHED_REWRITING, () -> {
                // NOTE: below this point the flip must succeed unless the database/table is dropped.
                updateNextVersion(table);
            });
        }
    }

    /**
     * Ready when every partition's reserved {@code commitVersion == visibleVersion + 1}: all loads before
     * the flip have published, so publishing the shadow at {@code commitVersion} is the next dense
     * version. Throws {@link AlterCancelException} iff the database/table has been dropped. Mirrors
     * {@link LakeTableSchemaChangeJob#readyToPublishVersion}.
     */
    private boolean readyToPublishVersion() throws AlterCancelException {
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.READ)) {
            OlapTable table = getTableOrThrow();
            for (long physicalPartitionId : partitionStates.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkState(physicalPartition != null, physicalPartitionId);
                long commitVersion = partitionStates.get(physicalPartitionId).commitVersion;
                if (commitVersion != physicalPartition.getVisibleVersion() + 1) {
                    Preconditions.checkState(physicalPartition.getVisibleVersion() < commitVersion,
                            "partition=" + physicalPartitionId + " visibleVersion="
                                    + physicalPartition.getVisibleVersion() + " commitVersion=" + commitVersion);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Publish the flip on BE, adapted from {@link LakeTableSchemaChangeJob#lakePublishVersion} for the
     * K-tablet shadow. For each partition: publish the shadow tablets {@code 1 -> commitVersion} (BE
     * applies the {@code op_schema_change@W} base and replays the post-watershed double-write vlogs to
     * materialize the shadow at {@code commitVersion}); publish the origin/base index tablets as a
     * {@code TXN_EMPTY} / txnId=-1 no-op {@code commitVersion-1 -> commitVersion} so their version chain
     * advances without applying any change.
     */
    protected boolean lakePublishVersion() {
        // Collect all per-partition tablet lists under a single lock, then publish outside the lock.
        boolean isFileBundling;
        List<PartitionPublishInfo> publishInfos = new ArrayList<>();
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.READ)) {
            OlapTable table = getTableOrThrow();
            isFileBundling = table.isFileBundling();
            for (long physicalPartitionId : partitionStates.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);
                MaterializedIndex shadowIndex = physicalPartition.getLatestIndex(shadowIndexMetaId);
                Preconditions.checkNotNull(shadowIndex, shadowIndexMetaId);
                List<Tablet> shadowTablets = new ArrayList<>(shadowIndex.getTablets());
                // The base index (and any other unchanged visible index) only needs its version
                // upgraded via the no-op publish.
                List<Tablet> originTablets = new ArrayList<>();
                for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    originTablets.addAll(index.getTablets());
                }
                PartitionRewriteState state = partitionStates.get(physicalPartitionId);
                long commitVersion = state.commitVersion;
                Long rewriteTxnId = state.rewriteTxnId;
                Preconditions.checkState(rewriteTxnId != null,
                        "rewrite txn id not journaled for partition " + physicalPartitionId);
                Long watershedVersion = state.watershedVersion;
                Preconditions.checkState(watershedVersion != null,
                        "watershed version not captured for partition " + physicalPartitionId);
                publishInfos.add(new PartitionPublishInfo(shadowTablets, originTablets,
                        commitVersion, rewriteTxnId, watershedVersion));
            }
        } catch (AlterCancelException e) {
            LOG.warn("online rewrite job {}: table dropped before publish", jobId);
            return false;
        }

        try {
            // txnId == -1 means BE does nothing, just upgrades the tablet_meta version (no-op publish).
            TxnInfoPB originTxnInfo = new TxnInfoPB();
            originTxnInfo.txnId = -1L;
            originTxnInfo.combinedTxnLog = false;
            originTxnInfo.commitTime = finishedTimeMs / 1000;
            originTxnInfo.txnType = TxnTypePB.TXN_EMPTY;
            originTxnInfo.gtid = watershedGtid;

            for (PartitionPublishInfo info : publishInfos) {
                // The shadow tablets publish through the ordinary tablet_ids path (base_version 1); the
                // TXN_SHADOW_REWRITE txn type is what tells BE to anchor their op_write as
                // op_schema_change@watershedVersion and replay the post-watershed double-writes. So this
                // reuses the same Utils.publishVersion / createSubRequestForAggregatePublish as any publish,
                // just with a shadow-rewrite TxnInfoPB.
                TxnInfoPB shadowTxnInfo = new TxnInfoPB();
                shadowTxnInfo.txnId = info.rewriteTxnId;
                shadowTxnInfo.txnType = TxnTypePB.TXN_SHADOW_REWRITE;
                shadowTxnInfo.combinedTxnLog = false;
                shadowTxnInfo.commitTime = finishedTimeMs / 1000;
                shadowTxnInfo.gtid = watershedGtid;
                shadowTxnInfo.shadowRewriteAlterVersion = info.watershedVersion;
                if (!isFileBundling) {
                    Utils.publishVersion(info.shadowTablets, shadowTxnInfo, 1, info.commitVersion,
                            computeResource, false);
                    Utils.publishVersion(info.originTablets, originTxnInfo, info.commitVersion - 1,
                            info.commitVersion, computeResource, false);
                } else {
                    AggregatePublishVersionRequest request = new AggregatePublishVersionRequest();
                    Utils.createSubRequestForAggregatePublish(info.shadowTablets, Lists.newArrayList(shadowTxnInfo),
                            1, info.commitVersion, null, computeResource, request);
                    Utils.createSubRequestForAggregatePublish(info.originTablets, Lists.newArrayList(originTxnInfo),
                            info.commitVersion - 1, info.commitVersion, null, computeResource, request);
                    List<VectorIndexBuildInfoPB> vectorIndexBuildInfos = new ArrayList<>();
                    Utils.sendAggregatePublishVersionRequest(request, 1, computeResource, null, null,
                            vectorIndexBuildInfos);
                    VectorIndexBuildScheduler.onPublishComplete(vectorIndexBuildInfos, /* fromCompaction= */ false);
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to publish version for online rewrite job {}", jobId, e);
            return false;
        }
    }

    private static final class PartitionPublishInfo {
        final List<Tablet> shadowTablets;
        final List<Tablet> originTablets;
        final long commitVersion;
        final long rewriteTxnId;
        final long watershedVersion;

        PartitionPublishInfo(List<Tablet> shadowTablets, List<Tablet> originTablets,
                             long commitVersion, long rewriteTxnId, long watershedVersion) {
            this.shadowTablets = shadowTablets;
            this.originTablets = originTablets;
            this.commitVersion = commitVersion;
            this.rewriteTxnId = rewriteTxnId;
            this.watershedVersion = watershedVersion;
        }
    }

    /**
     * No-op publish for the FORCE-cancel escape hatch, adapted from
     * {@link LakeTableSchemaChangeJob#lakePublishVersionWithSkip} for the K-tablet shadow. Sends a
     * publish_version RPC with {@code TxnInfoPB.noOpPublish=true} for each partition's visible (non-shadow)
     * indices at the alter's reserved {@code commitVersion}, so the partition version chain advances past
     * the cancelled alter without applying any of its changes. The shadow indices are deliberately skipped:
     * {@link #removeShadowIndexOnCancel} is about to drop them, so publishing them would only produce orphan
     * metadata. Returns {@code false} if any RPC fails or throws; the caller then leaves the job at
     * {@code FINISHED_REWRITING} so the operator can retry.
     */
    protected boolean lakePublishVersionWithSkip(String reason) {
        // Dispatch keys off the table's CURRENT file_bundling format read fresh here (not a cached field
        // that is not serialized): a replayed job force-cancelled before the publish daemon runs must
        // still route correctly. Mirrors LakeTableSchemaChangeJob.lakePublishVersionWithSkip.
        // Collect all per-partition tablet lists under a single lock, then publish outside the lock.
        boolean useAggregatePublish = false;
        Map<Long, List<Tablet>> tabletsByPartition = new HashMap<>();
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.READ)) {
            // Use the null-returning getTable so a concurrent db/table drop is a benign skip.
            OlapTable table = getTable();
            if (table != null) {
                useAggregatePublish = table.isFileBundling();
                for (long physicalPartitionId : partitionStates.keySet()) {
                    PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                    if (physicalPartition == null) {
                        continue;
                    }
                    List<Tablet> regularTablets = new ArrayList<>();
                    for (MaterializedIndex index :
                            physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                        regularTablets.addAll(index.getTablets());
                    }
                    tabletsByPartition.put(physicalPartitionId, regularTablets);
                }
            }
        }
        return Utils.noOpPublishForForceSkip(jobId, reason, watershedTxnId, watershedGtid,
                commitVersionMapView(), tabletsByPartition, computeResource, useAggregatePublish);
    }

    /**
     * Update each partition's {@code nextVersion} past the reserved commit version, mirroring
     * {@link LakeTableSchemaChangeJob#updateNextVersion}. The caller holds the database write lock.
     */
    protected void updateNextVersion(@NotNull OlapTable table) {
        for (long physicalPartitionId : partitionStates.keySet()) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            Long commitVersion = partitionStates.get(physicalPartitionId).commitVersion;
            if (commitVersion == null) {
                // The first FINISHED_REWRITING journal entry (the RUNNING -> FINISHED_REWRITING transition
                // at runRunningJob) is written before reserveCommitVersionIfNeeded reserves a commit
                // version, so its replayed image carries no commit version yet. Nothing is reserved, so
                // there is nothing to bump; the reservation is applied by the second FINISHED_REWRITING
                // entry. Skip rather than unbox a null (replaying this entry must not NPE leader recovery).
                continue;
            }
            if (physicalPartition.getNextVersion() == commitVersion + 1) {
                // Already bumped (a second replay of the same FINISHED_REWRITING log entry): no-op.
                continue;
            }
            Preconditions.checkState(physicalPartition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + physicalPartition.getNextVersion() + " commitVersion=" + commitVersion);
            physicalPartition.setNextVersion(commitVersion + 1);
        }
    }

    /**
     * Seam over the flip publish (which sends RPCs to BE). Production runs the inherited async
     * {@link #publishVersion()} wrapper (submits {@link #lakePublishVersion()} and polls the future);
     * tests inject a stub returning {@code true} so the catalog flip can be asserted without a backend.
     * The argument binds to the concrete job type so a test can pass {@code Subclass::lakePublishVersion}.
     * Not serialized.
     */
    @FunctionalInterface
    public interface PublishExecutor {
        boolean publish(LakeOnlineRewriteJobBase job);
    }

    private transient PublishExecutor publishExecutor;

    @VisibleForTesting
    public void setPublishExecutor(PublishExecutor publishExecutor) {
        this.publishExecutor = publishExecutor;
    }

    private PublishExecutor getPublishExecutor() {
        if (publishExecutor == null) {
            publishExecutor = LakeOnlineRewriteJobBase::publishVersion;
        }
        return publishExecutor;
    }

    protected Map<Long, Long> commitVersionMapView() {
        Map<Long, Long> view = Maps.newHashMap();
        for (Map.Entry<Long, PartitionRewriteState> entry : partitionStates.entrySet()) {
            if (entry.getValue().commitVersion != null) {
                view.put(entry.getKey(), entry.getValue().commitVersion);
            }
        }
        return view;
    }

    // ---- CANCEL ----------------------------------------------------------------------------------

    /**
     * All-or-nothing cancel: abort any in-flight per-partition rewrite load txn, drop all shadow
     * indexes and their tablets from the catalog and the inverted index, remove the shadow
     * {@code MaterializedIndexMeta} from the table, release the GC pin, reset the table state
     * to {@link OlapTable.OlapTableState#NORMAL}, and journal CANCELLED. No-op when the job is
     * already FINISHED or CANCELLED. Safe if cancel runs before the shadow was exposed (PENDING):
     * only drops what exists, guarded by null/contains checks.
     */
    @Override
    protected boolean cancelImpl(String errMsg) {
        return cancelImpl(errMsg, false);
    }

    /**
     * Force-aware cancel, mirroring {@link LakeTableSchemaChangeJob#cancelImpl(String, boolean)}.
     *
     * <p>Once the job reaches {@code FINISHED_REWRITING}, {@link #reserveCommitVersionIfNeeded()} has
     * bumped each partition's {@code nextVersion} to {@code commitVersion + 1} while its
     * {@code visibleVersion} is still {@code commitVersion - 1}. Dropping the shadow then leaves a
     * permanent version hole at {@code commitVersion} that blocks all future publishes on those
     * partitions. So:
     * <ul>
     *   <li>a normal cancel ({@code force == false}) from {@code FINISHED_REWRITING} while the table
     *       still exists is refused (return false) — the flip should complete/retry;</li>
     *   <li>a force cancel ({@code force == true}) from {@code FINISHED_REWRITING} first no-op publishes
     *       the partitions' visible/origin indices to {@code commitVersion} (so the BE version chain
     *       advances past the cancelled alter) and advances FE {@code visibleVersion} to match, then
     *       drops the shadow — no hole;</li>
     *   <li>other states (PENDING/WAITING_TXN/RUNNING) keep the existing cleanup unchanged.</li>
     * </ul>
     */
    @Override
    protected boolean cancelImpl(String errMsg, boolean force) {
        if (jobState == JobState.CANCELLED || jobState == JobState.FINISHED) {
            return false;
        }

        // Refuse a normal cancel from FINISHED_REWRITING while the table still exists: the reserved
        // commit version has already bumped nextVersion, so dropping the shadow now would leave a
        // permanent version hole. Only an operator-driven force cancel (or a dropped table/db) may
        // proceed past this point. Mirrors LakeTableSchemaChangeJob.cancelImpl.
        if (jobState == JobState.FINISHED_REWRITING && tableExists() && !force) {
            return false;
        }

        // Force-cancel from FINISHED_REWRITING: advance the partition version chain past the reserved
        // commit version BEFORE the cancel cleanup resets the table state, so no version hole remains
        // and new loads do not queue forever waiting for the cancelled alter's missing version.
        boolean advanceVersionForForce = force && jobState == JobState.FINISHED_REWRITING && tableExists();
        if (advanceVersionForForce) {
            if (!lakePublishVersionWithSkip(errMsg)) {
                return false;
            }
            // Mark the job force-skipped only after the no-op publish actually advanced the partition
            // version on BE, so copyForPersist snapshots it into the edit log and replay re-applies the
            // VisibleVersion bump identically.
            forceSkippedAtCommitted = true;
        }

        // Abort any in-flight per-partition rewrite load txns (PREPARE/PREPARED/COMMITTED state).
        // Guard against txns that are unknown (never started, already expired), already terminal
        // (VISIBLE/ABORTED), or that throw (e.g. txn manager says the db does not exist).
        for (PartitionRewriteState partitionState : partitionStates.values()) {
            Long txnId = partitionState.rewriteTxnId;
            if (txnId == null) {
                continue;
            }
            TransactionState txnState =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionState(dbId, txnId);
            if (txnState == null) {
                continue;
            }
            TransactionStatus txnStatus = txnState.getTransactionStatus();
            if (txnStatus.isFinalStatus()) {
                continue;
            }
            try {
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .abortTransaction(dbId, txnId, errMsg);
            } catch (Exception e) {
                LOG.warn("online rewrite job {}: failed to abort rewrite txn {}: {}",
                        jobId, txnId, e.getMessage());
            }
        }

        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();

        persistStateChange(this, JobState.CANCELLED, () -> {
            try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
                OlapTable table = getTable();
                if (table != null) {
                    if (advanceVersionForForce) {
                        // The no-op publish wrote tablet_metadata at commitVersion on BE; advance the
                        // FE-side partition visible version to match so subsequent loads compute their
                        // publish base correctly. Shared with the replay CANCELLED branch so the live and
                        // replay paths bump identically.
                        advanceVisibleVersionForForceSkip(table, commitVersionMapView());
                    }
                    removeShadowIndexOnCancel(table);
                }
            }
        });

        if (span != null) {
            span.setStatus(StatusCode.ERROR, errMsg);
            span.end();
        }

        LOG.info("online rewrite job cancelled, jobId: {}, error: {}", jobId, errMsg);
        return true;
    }

    /**
     * Drop all shadow artifacts built by this job from the catalog in-memory state. Mirrors
     * {@link LakeTableSchemaChangeJob#removeShadowIndex} adapted to this job's per-partition
     * structure. Each step is guarded so a partial-exposure cancel (e.g. PENDING before the
     * shadow was added to the partition catalog map) does not throw. The caller holds the
     * database write lock.
     */
    protected void removeShadowIndexOnCancel(@NotNull OlapTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        for (Map.Entry<Long, PartitionRewriteState> entry : partitionStates.entrySet()) {
            long physicalPartitionId = entry.getKey();
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                continue;
            }
            // Release the GC pin (may have been set in RUNNING or not at all).
            physicalPartition.setMinRetainVersion(0);
            // Drop the shadow index from the partition's catalog index map (if it was exposed).
            physicalPartition.deleteMaterializedIndexByMetaId(shadowIndexMetaId);
        }

        // Remove the shadow index meta registered on the table (if it was registered).
        if (shadowIndexName != null) {
            table.deleteIndexInfo(shadowIndexName);
        }

        // Remove all shadow tablets from the tablet inverted index.
        for (PartitionRewriteState partitionState : partitionStates.values()) {
            if (partitionState.shadowIndex == null) {
                continue;
            }
            for (Tablet tablet : partitionState.shadowIndex.getTablets()) {
                invertedIndex.deleteTablet(tablet.getId());
            }
        }

        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    // ---- SHOW / replay ---------------------------------------------------------------------------

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // Progress: partitions whose rewrite has published / total. Only meaningful while RUNNING.
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && !partitionStates.isEmpty()) {
            int published = 0;
            for (long physicalPartitionId : partitionStates.keySet()) {
                if (classifyRewrite(physicalPartitionId) == RewriteStatus.DONE) {
                    published++;
                }
            }
            progress = published + "/" + partitionStates.size();
        }

        // One display row for the single shadow index this job builds.
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        // Only show the origin index name (strip the shadow column-name prefix).
        info.add(Column.removeNamePrefix(shadowIndexName));
        info.add(shadowIndexMetaId);
        info.add(originIndexMetaId);
        info.add("0:0"); // schema version and schema hash
        info.add(watershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        info.add(progress);
        info.add(timeoutMs / 1000);

        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
        if (warehouse == null) {
            info.add("null");
        } else {
            info.add(warehouse.getName());
        }

        infos.add(info);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        LakeOnlineRewriteJobBase other = (LakeOnlineRewriteJobBase) replayedJob;

        LOG.info("Replaying online rewrite job. state={} jobId={}",
                replayedJob.jobState, replayedJob.jobId);

        if (this != other) {
            Preconditions.checkState(this.type.equals(other.type));
            Preconditions.checkState(this.jobId == other.jobId);
            Preconditions.checkState(this.dbId == other.dbId);
            Preconditions.checkState(this.tableId == other.tableId);

            this.jobState = other.jobState;
            this.createTimeMs = other.createTimeMs;
            this.finishedTimeMs = other.finishedTimeMs;
            this.errMsg = other.errMsg;
            this.timeoutMs = other.timeoutMs;

            this.shadowIndexMetaId = other.shadowIndexMetaId;
            this.originIndexMetaId = other.originIndexMetaId;
            this.shadowIndexName = other.shadowIndexName;
            this.partitionStates = other.partitionStates;
            this.watershedTxnId = other.watershedTxnId;
            this.watershedGtid = other.watershedGtid;
            this.forceSkippedAtCommitted = other.forceSkippedAtCommitted;
        }

        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTable();
            if (table == null) {
                return; // do nothing if the database or table has been dropped.
            }

            if (jobState == JobState.PENDING) {
                // A PENDING job has installed NO durable catalog state yet. On the leader the shadow index
                // meta + tablets are registered only inside the WAITING_TXN applier (runPendingJob),
                // atomically with the WAITING_TXN journal entry, so the only PENDING journal entry (the
                // initial ALTER log) carries empty partitionStates and nothing installed. Reconstructing
                // here would call registerShadowIndexMeta with no per-partition shadow index, leaving the
                // table with a shadow schema that OlapTableSink.createSchema() sees while createPartition()
                // has no matching partition index -- loads to the table then fail until the job advances.
                // So replay installs nothing for PENDING; the resuming leader's runPendingJob builds the
                // shadow fresh.
                LOG.info("Replaying PENDING online rewrite job {}; no durable shadow state to reconstruct.", jobId);
            } else if (jobState == JobState.WAITING_TXN || jobState == JobState.RUNNING) {
                // WAITING_TXN and RUNNING share the same durable catalog state: the shadow index meta is
                // registered, the shadow tablets are in the inverted index, the table is in the job's
                // working state (jobTableState()), and (watershed allocated from WAITING_TXN onwards) the
                // shadow index is exposed to the catalog with visibleTxnId = watershedTxnId so
                // post-watershed loads double-write it.
                // RUNNING only adds in-flight rewrite txns on top of the WAITING_TXN catalog state; those
                // are resumed by the live scheduler's three-state resume (rewriteTxnId is journaled) after
                // the leader takes over, never by replay. So replay just reconstructs the shared shadow
                // catalog state idempotently for both states.
                reconstructShadowCatalogState(table);
            } else if (jobState == JobState.FINISHED_REWRITING) {
                // The reserved commit versions are journaled in partitionStates (GSON); re-apply the
                // nextVersion bump that the live reserve performed so the follower's partition version
                // chain matches the leader's before the flip.
                updateNextVersion(table);
                // Mirror the live reserve's GC-pin release. reserveCommitVersionIfNeeded releases the
                // watershed snapshot pin (setMinRetainVersion(0)) when it reserves the commit version, but
                // on a journal-replay failover that live call short-circuits (a commit version is already
                // reserved) while RUNNING replay re-pinned W via reapplyWatershedGcPin -- so replay of the
                // post-reserve FINISHED_REWRITING entry must release the pin itself, or vacuum can never
                // reclaim versions below W on the recovered leader.
                releaseWatershedGcPinForReservedPartitions(table);
            } else if (jobState == JobState.FINISHED) {
                // Re-apply the atomic flip from the journaled image: the catalog state (commit versions,
                // the exposed shadow index, baseIndexMetaId) is what visualiseShadowIndex consumes, all
                // journaled. onReload first, exactly as the standard lake schema change does. The flip
                // drops the origin index meta and repoints the base meta at the shadow, so if it has
                // already been applied (a second replay of the same FINISHED log entry) the origin index
                // meta is gone; guard on that so the flip runs at most once (no double-flip).
                if (table.getIndexMetaByMetaId(originIndexMetaId) != null) {
                    table.onReload();
                    visualiseShadowIndex(table);
                }
            } else if (jobState == JobState.CANCELLED) {
                // A FORCE-cancel out of FINISHED_REWRITING left BE with a no-op tablet_metadata at
                // commitVersion and the live path bumped partition.VisibleVersion to match. Replay must
                // do the same, otherwise an FE recovering from a pre-cancel image keeps
                // visibleVersion=commitVersion-1 and subsequent load publishes compute the wrong base.
                if (forceSkippedAtCommitted) {
                    advanceVisibleVersionForForceSkip(table, commitVersionMapView());
                }
                // Remove all shadow artifacts so the catalog is clean after cancel replay.
                removeShadowIndexOnCancel(table);
            } else {
                throw new RuntimeException("unknown job state '" + jobState.name() + "'");
            }
        }
    }

    /**
     * Idempotently reconstruct the durable shadow catalog state shared by WAITING_TXN and
     * RUNNING on a follower replaying the journal. (PENDING installs nothing — see replay().)
     * The shadow {@link MaterializedIndex}(es) are
     * themselves serialized in {@link #partitionStates}, so GSON has already rebuilt them (with their
     * tablet ids and ranges) on deserialization; the StarOS shards they reference were created in the
     * live run and are not re-created here. We only re-attach the durable catalog state: the shadow
     * index meta, the inverted-index entries, the table's working state ({@code jobTableState()}) and,
     * once the watershed has been allocated, the catalog exposure of the shadow index. Every step is
     * guarded so a second replay is a no-op (no double-add of meta, tablets or the exposed index).
     *
     * <p>The catalog state in RUNNING is identical to WAITING_TXN; the only RUNNING-specific state is
     * the in-flight rewrite txns, which are resumed by the live scheduler's three-state resume
     * ({@code rewriteTxnId} is journaled) after the leader takes over — never by replay, which must not
     * re-run the rewrite INSERT. The caller holds the database write lock.
     */
    private void reconstructShadowCatalogState(@NotNull OlapTable table) {
        registerShadowIndexMeta(table);
        // Make the shadow tablets discoverable to the tablet inverted index (skips tablets already
        // present, so a second replay does not double-add).
        addShadowTabletsToInvertedIndex(table);
        table.setState(jobTableState());
        // Once the watershed has been allocated (WAITING_TXN onwards) the live path also exposed the
        // shadow index to the catalog so post-watershed loads double-write it. Re-apply that exposure
        // idempotently (a partition already carrying the shadow is skipped). When watershedTxnId is
        // still -1 (journaled at PENDING -> WAITING_TXN but the first WAITING_TXN run has not happened
        // yet) there is nothing to expose; the next live WAITING_TXN run allocates the watershed and
        // exposes.
        if (watershedTxnId != -1) {
            exposeShadowIndexToCatalog(table);
        }
        // Re-apply the watershed GC pin: minRetainVersion is not persisted, so a follower that took
        // over after RUNNING was journaled must re-pin W before its first rewrite SELECT, or vacuum
        // could GC the exact snapshot the rewrite reads. Gated per partition on a captured W, so this
        // is a no-op for PENDING/WAITING_TXN (no W yet) and only pins for RUNNING.
        reapplyWatershedGcPin(table);
    }

    /**
     * Re-pin each partition's captured watershed version against vacuum on a follower replaying a
     * RUNNING image. {@code minRetainVersion} is not serialized, so it resets to 0 on failover; the
     * rewrite SELECT in RUNNING still reads W, so the pin must be restored. Gated on a captured W
     * (no captured watershed version before RUNNING means nothing to pin). The pin is released at
     * FINISHED_REWRITING by the live {@code reserveCommitVersionIfNeeded} and, on a journal-replay
     * failover, by {@link #releaseWatershedGcPinForReservedPartitions}; and by cancel. The caller holds
     * the database write lock.
     */
    private void reapplyWatershedGcPin(@NotNull OlapTable table) {
        for (Map.Entry<Long, PartitionRewriteState> entry : partitionStates.entrySet()) {
            Long watershedVersion = entry.getValue().watershedVersion;
            if (watershedVersion == null) {
                continue;
            }
            PhysicalPartition physicalPartition = table.getPhysicalPartition(entry.getKey());
            if (physicalPartition != null) {
                physicalPartition.setMinRetainVersion(watershedVersion);
            }
        }
    }

    /**
     * Release the watershed-snapshot GC pin on a follower replaying a post-reserve FINISHED_REWRITING
     * image. The live {@link #reserveCommitVersionIfNeeded} releases the pin ({@code setMinRetainVersion(0)})
     * when it reserves the commit version, but on a journal-replay failover that live call short-circuits
     * (a commit version is already reserved) and the RUNNING replay re-pinned W via
     * {@link #reapplyWatershedGcPin}; so replay of the FINISHED_REWRITING entry must release the pin, or
     * vacuum can never reclaim versions below W on the recovered leader. Gated per partition on a reserved
     * commit version, so it is a no-op for the first (pre-reserve, null-commit) FINISHED_REWRITING entry --
     * matching the leader, which had not released the pin yet at that point. The caller holds the database
     * write lock.
     */
    private void releaseWatershedGcPinForReservedPartitions(@NotNull OlapTable table) {
        for (Map.Entry<Long, PartitionRewriteState> entry : partitionStates.entrySet()) {
            if (entry.getValue().commitVersion == null) {
                continue;
            }
            PhysicalPartition physicalPartition = table.getPhysicalPartition(entry.getKey());
            if (physicalPartition != null) {
                physicalPartition.setMinRetainVersion(0);
            }
        }
    }
}
