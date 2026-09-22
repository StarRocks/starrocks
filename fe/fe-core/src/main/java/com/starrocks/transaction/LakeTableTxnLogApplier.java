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

package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class LakeTableTxnLogApplier implements TransactionLogApplier {
    private static final Logger LOG = LogManager.getLogger(LakeTableTxnLogApplier.class);
    // lake table or lake materialized view
    private final OlapTable table;

    LakeTableTxnLogApplier(OlapTable table) {
        this.table = table;
    }

    @Override
    public void applyCommitLog(TransactionState txnState, TableCommitInfo commitInfo) {
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPhysicalPartitionId();
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            if (partition == null) {
                LOG.warn("ignored dropped partition {} when applying commit log", partitionId);
                continue;
            }

            // A shadow-rewrite txn does not allocate or advance any partition version.
            if (txnState.isShadowRewrite()) {
                continue;
            }

            // Derive the new nextVersion from the version this transaction was allocated, which
            // unprotectedCommitPreparedTransaction took from partition.getNextVersion() under the
            // database lock and journaled with this entry. Deriving it is equivalent to the running
            // increment this used to do -- but idempotent, which the increment was not.
            //
            // A relative increment makes nextVersion a counter that the leader and every replaying
            // FE maintain independently, and nothing ever reconciles it against the journal. So a
            // single increment applied twice, or not at all, drifts them apart by one -- permanently
            // and silently, because no other operation reads nextVersion for correctness. The drift
            // surfaces only when a lake alter job reserves commitVersion = nextVersion on the leader
            // and a replaying FE then asserts nextVersion == commitVersion: that assert fails, journal
            // replay aborts, and every FE exits on that record on every restart (StarRocksTest#12225,
            // where two followers independently derived 3828 from the journal while the leader held
            // 3829). Deriving from the journaled version instead makes replay reproduce the leader
            // exactly, re-applying an entry a no-op, and any pre-existing drift self-heal on the next
            // transaction.
            //
            // The replication branch already worked this way; it is called out separately only
            // because a replication transaction's versions are not contiguous.
            //
            // nextDataVersion is derived the same way and for the same reason: leaving it on the
            // increment while nextVersion is derived would make a re-applied entry advance one
            // counter and not the other, which is worse than the drift this fixes -- it breaks the
            // committed-vs-visible data version equality that ReplicationJob.commitTransaction()
            // preconditions on. A compaction is the exception: it allocates no data version and
            // must not advance one.
            long commitVersion = partitionCommitInfo.getVersion();
            long commitDataVersion = partitionCommitInfo.getDataVersion();

            // Report a counter that has fallen BEHIND the journal. Such a drift is otherwise
            // undetectable: nothing else reads nextVersion for correctness, so it stays silent until a
            // lake alter job's reserved commitVersion disagrees with it and aborts journal replay on
            // every FE. Reporting it at the first transaction that observes it names that transaction,
            // so the operation that introduced the drift can be found in the window before it. This
            // cannot fire on the leader: unprotectedCommitPreparedTransaction allocated commitVersion
            // from this very field under the same database lock.
            //
            // Report only a counter that is behind on a transaction whose version is supposed to be
            // contiguous with it. Everything else is normal and must stay silent, or the signal is
            // useless:
            //   - being at or past commitVersion: applying an entry a second time legitimately leaves
            //     nextVersion at commitVersion + 1, which is the idempotence this change provides;
            //   - replication: versions are intentionally noncontiguous;
            //   - version overwrite: overwriting an EMPTY partition deliberately names a version above
            //     the counter ("it's next version will less than overwrite version", per
            //     OlapTableTxnLogApplier);
            //   - double write: the commit info carries the ORIGINAL partition's version, which can sit
            //     above this partition's own counter.
            // These are the same three exemptions applyVisibleLog makes from its continuity
            // precondition, and for the same reason.
            boolean versionIsContiguousWithCounter =
                    txnState.getSourceType() != TransactionState.LoadJobSourceType.REPLICATION
                            && !txnState.isVersionOverwrite()
                            && !partitionCommitInfo.isDoubleWrite();
            if (versionIsContiguousWithCounter && commitVersion > 0
                    && partition.getNextVersion() < commitVersion) {
                // Wording kept stable on purpose: this string is what operators grep for, and a
                // cluster already running the previous build reports it the same way.
                LOG.warn("partition {} nextVersion {} disagrees with the version transaction {} was " +
                                "allocated ({}), so an earlier operation advanced this FE's counter out " +
                                "of step with the journal; converging on the journaled version. source={}",
                        partitionId, partition.getNextVersion(), txnState.getTransactionId(),
                        commitVersion, txnState.getSourceType());
            }

            if (txnState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION) {
                // A replication transaction's versions are not contiguous, so they are taken as given
                // rather than derived from the counter. Unchanged.
                partition.setNextVersion(commitVersion + 1);
                partition.setNextDataVersion(commitDataVersion + 1);
            } else if (commitVersion > 0) {
                // Advance to the version this entry carries, but never below where the counter already
                // is. Overshooting only skips versions; undershooting hands the same version out twice,
                // so on a mismatch the higher value is always the safe one. This also keeps
                // INSERT OVERWRITE's documented behaviour -- overwriting a non-empty partition names a
                // version below the counter and must not move it -- which the shared-nothing applier
                // spells out in OlapTableTxnLogApplier#applyCommitLog.
                advanceTo(partition::getNextVersion, partition::setNextVersion, commitVersion + 1);
                if (txnState.getSourceType() != TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
                    // A compaction allocates no data version and must not advance one.
                    if (commitDataVersion > 0) {
                        advanceTo(partition::getNextDataVersion, partition::setNextDataVersion,
                                commitDataVersion + 1);
                    } else {
                        partition.setNextDataVersion(partition.getNextDataVersion() + 1);
                    }
                }
            } else {
                // No version was allocated for this partition (the sentinel survived commit), or the
                // record predates the field. Deriving from the sentinel would corrupt the version
                // chain, so keep the historical increment.
                LOG.warn("partition {} has no committed version in transaction {}; falling back " +
                        "to incrementing nextVersion", partitionId, txnState.getTransactionId());
                partition.setNextVersion(partition.getNextVersion() + 1);
                if (txnState.getSourceType() != TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
                    partition.setNextDataVersion(partition.getNextDataVersion() + 1);
                }
            }
        }
    }

    /** Move a version counter forward to {@code target}, never backwards. */
    private static void advanceTo(java.util.function.LongSupplier get, java.util.function.LongConsumer set,
                                  long target) {
        if (get.getAsLong() < target) {
            set.accept(target);
        }
    }

    public void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo, Database db) {
        applyVisibleLog(txnState, commitInfo, db, null);
    }

    /**
     * @param deferredPublishes when non-null, the mutations a reader could pair inconsistently - the visible
     *                          version and the UNSHARE query-layout cutover - are not applied here but recorded
     *                          per physical partition id, for {@link #applyVisibleLogBatch} to apply, in that
     *                          order, once the whole batch has been applied.
     */
    private void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo, Database db,
                                 @Nullable Map<Long, DeferredPublish> deferredPublishes) {
        List<ColumnId> validDictCacheColumns = Lists.newArrayList();
        List<Long> dictCollectedVersions = Lists.newArrayList();

        long maxPartitionVersionTime = -1;
        long tableId = table.getId();
        CompactionMgr compactionManager = GlobalStateMgr.getCurrentState().getCompactionMgr();
        for (PartitionCommitInfo partitionCommitInfo : commitInfo.getIdToPartitionCommitInfo().values()) {
            long partitionId = partitionCommitInfo.getPhysicalPartitionId();
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            if (partition == null) {
                LOG.warn("ignored dropped partition {} when applying visible log", partitionId);
                continue;
            }
            // A shadow-rewrite txn does not advance the partition's visible version; its rowsets
            // are anchored later when the schema-change flip publishes the converted op_schema_change log.
            if (txnState.isShadowRewrite()) {
                continue;
            }
            long version = partitionCommitInfo.getVersion();
            long versionTime = partitionCommitInfo.getVersionTime();
            Quantiles compactionScore = partitionCommitInfo.getCompactionScore();

            DeferredPublish pending = deferredPublishes == null ? null
                    : deferredPublishes.computeIfAbsent(partitionId, k -> new DeferredPublish());

            // Within a batch the earlier transactions have not published their version yet, so the
            // continuity check must compare against the version this partition is going to end up on.
            long currentVisibleVersion = pending != null && pending.finalCommitInfo != null
                    ? pending.finalCommitInfo.getVersion() : partition.getVisibleVersion();

            // The version of a replication transaction may not continuously
            Preconditions.checkState(txnState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION
                    || txnState.isVersionOverwrite()
                    || partitionCommitInfo.isDoubleWrite()
                    || version == currentVisibleVersion + 1);

            if (pending != null) {
                pending.finalCommitInfo = partitionCommitInfo;
            } else {
                partition.updateVisibleVersion(version, versionTime);
            }
            if (txnState.isUserWriteSource()) {
                partition.updateLastUpdateTime(versionTime);
            }
            if (txnState.getSourceType() != TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
                partition.setDataVersion(partitionCommitInfo.getDataVersion());
                if (partitionCommitInfo.getVersionEpoch() > 0) {
                    partition.setVersionEpoch(partitionCommitInfo.getVersionEpoch());
                }
                partition.setVersionTxnType(txnState.getTransactionType());
            }

            PartitionIdentifier partitionIdentifier =
                    new PartitionIdentifier(txnState.getDbId(), table.getId(), partition.getId());
            if (txnState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
                boolean isPartialSuccess = false;
                boolean isUnshare = false;
                if (txnState.getTxnCommitAttachment() instanceof CompactionTxnCommitAttachment attachment) {
                    isPartialSuccess = attachment.getForceCommit();
                    isUnshare = attachment.isUnshare();
                }
                compactionManager.handleCompactionFinished(partitionIdentifier, version, versionTime, compactionScore,
                        txnState.getTransactionId(), isPartialSuccess);
                if (isUnshare) {
                    if (pending != null) {
                        // In a batch the version this cutover belongs to is not published yet; cutting the
                        // layout over now would let a lock-free planner pair the child layout with a version
                        // older than the UNSHARE. Hand it to applyVisibleLogBatch, which runs it after the
                        // version publication, preserving the single-transaction order below.
                        pending.unshareCutoverPending = true;
                    } else if (partition.finishUnshare()) {
                        // This method runs under the transaction-visible table write lock. Make the query-layout
                        // cutover part of the same catalog mutation as the UNSHARE version, then invalidate any
                        // optimistic plan that captured the parent layout before this point.
                        table.lastSchemaUpdateTime.set(System.nanoTime());
                    }
                }
            } else {
                compactionManager.handleLoadingFinished(partitionIdentifier, version, versionTime, compactionScore);
            }
            if (!partitionCommitInfo.getInvalidDictCacheColumns().isEmpty()) {
                for (ColumnId column : partitionCommitInfo.getInvalidDictCacheColumns()) {
                    IDictManager.getInstance().removeGlobalDict(table, column);
                }
            }
            if (!partitionCommitInfo.getValidDictCacheColumns().isEmpty()) {
                validDictCacheColumns = partitionCommitInfo.getValidDictCacheColumns();
            }
            if (!partitionCommitInfo.getDictCollectedVersions().isEmpty()) {
                dictCollectedVersions = partitionCommitInfo.getDictCollectedVersions();
            }
            // Publish-driven real-time reshard triggering + transient stat refresh. Leader-only; on
            // followers / replay / checkpoint the transient tabletStats map is empty, so skip there.
            if (GlobalStateMgr.getCurrentState().isLeader() && !GlobalStateMgr.isCheckpointThread()) {
                Map<Long, TabletStatPB> tabletStats = partitionCommitInfo.getTabletStats();
                if (tabletStats != null && !tabletStats.isEmpty()) {
                    refreshTabletStatsAndMarkReshardCandidate(partition, tabletStats, db, version, versionTime);
                }
            }
            maxPartitionVersionTime = Math.max(maxPartitionVersionTime, versionTime);
        }

        if (txnState.getSourceType() != TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            warehouseManager.recordWarehouseInfoForTable(tableId, txnState.getComputeResource());
        }

        if (!GlobalStateMgr.isCheckpointThread() && dictCollectedVersions.size() == validDictCacheColumns.size()) {
            for (int i = 0; i < validDictCacheColumns.size(); i++) {
                ColumnId columnName = validDictCacheColumns.get(i);
                long collectedVersion = dictCollectedVersions.get(i);
                IDictManager.getInstance()
                        .updateGlobalDict(table, columnName, collectedVersion, maxPartitionVersionTime);
            }
        }
    }

    /**
     * Leader-only post-publish bookkeeping for one lake partition: refresh each LakeTablet's data size
     * and row count from the BE-reported {@code tabletStats}, then mark the table a reshard candidate
     * with the largest just-published tablet size ({@code addReshardCandidate} applies the split threshold).
     *
     * <p>The candidate carries only the split signal (merge = Long.MAX_VALUE): split is the publish
     * path's real-time benefit and is never gated by the merge parallelism floor, so this stays a pure
     * in-memory max with no StarMgr RPC on the write-locked publish path. maxTabletSize is taken over
     * only the just-published tablets (those in {@code tabletStats}); a tablet only grows when written,
     * so this is the real-time signal, and the periodic TabletStatMgr scan is the backstop for any
     * already-oversized tablet this publish did not touch. It is also monotone (table-wide &gt;= this
     * partition's), so a per-partition crossing is decision-safe. Merge is left to the periodic scan,
     * whose adjacency signal requires every neighbor to be fresh — a single publish rarely satisfies that.
     *
     * <p>{@code tabletStats} is transient transport: it is consumed here and cleared to bound FE heap,
     * since the LakeTablet row counts set above persist independently and the post-visible first-load
     * statistics collector samples from LakeTablet.getFuzzyRowCount(), not from this map.
     */
    private void refreshTabletStatsAndMarkReshardCandidate(PhysicalPartition partition,
            Map<Long, TabletStatPB> tabletStats, Database db, long version, long versionTime) {
        List<MaterializedIndex> indexes = partition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
        long maxTabletSize = 0L;
        // Walk only the tablets this publish actually reported, not every tablet in the partition: this
        // runs under the table write lock, so resolve each reported id directly (O(1) per index).
        for (Map.Entry<Long, TabletStatPB> entry : tabletStats.entrySet()) {
            Tablet tablet = null;
            for (MaterializedIndex index : indexes) {
                tablet = index.getTablet(entry.getKey());
                if (tablet != null) {
                    break;
                }
            }
            if (!(tablet instanceof LakeTablet)) {
                continue;
            }
            LakeTablet lakeTablet = (LakeTablet) tablet;
            TabletStatPB tabletStat = entry.getValue();
            long dataSize = tabletStat.dataSize != null ? tabletStat.dataSize : 0L;
            lakeTablet.setDataSize(dataSize);
            // These stats came back with the publish of exactly this version.
            lakeTablet.setRowCount(tabletStat.numRows != null ? tabletStat.numRows : 0L, version);
            lakeTablet.setDataSizeUpdateTime(versionTime);
            // Only apply the observation while the table is NORMAL. A split's children become
            // catalog-visible while the table is still TABLET_RESHARD, and cross-published
            // transactions flow through this method and update those same child LakeTablet objects. A
            // cross-publish that happens to contribute no shared file would mark the tablet clean, and
            // a merge could be planned in that window before the next cross-publish corrects it -- and
            // a planned merge's transaction is already committed by publish time, so it cannot be
            // abandoned. This method already runs under the table write lock that makes the
            // transaction visible, so the state read here is not a TOCTOU.
            if (table.getState() == OlapTable.OlapTableState.NORMAL) {
                // See LakeTablet#observeSharedFiles(Boolean): an absent field fails closed too.
                lakeTablet.observeSharedFiles(tabletStat.hasSharedFiles);
            }
            maxTabletSize = Math.max(maxTabletSize, dataSize);
        }
        if (maxTabletSize > 0 && table.isRangeDistribution()) {
            GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                    .addReshardCandidate(db.getId(), table.getId(), maxTabletSize, Long.MAX_VALUE);
        }
        tabletStats.clear();
    }

    /** Partition state a batch publish holds back until the whole batch has been applied. */
    private static class DeferredPublish {
        // Commit info of the last transaction in the batch that touched this partition.
        private PartitionCommitInfo finalCommitInfo;
        // An UNSHARE compaction in the batch asked for the query-layout cutover.
        private boolean unshareCutoverPending;
    }

    /**
     * A batch publish materializes a tablet metadata object for the batch's FINAL version only; the
     * versions in between never get one. Advancing the partition's visible version transaction by
     * transaction would briefly expose such an intermediate version, and a query that captured it -
     * planning reads the shared mutable PhysicalPartition after releasing the db lock when
     * {@code cbo_use_lock_db} is off - would then ask the BE for an object that will never exist and
     * fail the query. So collect each partition's target version while applying the batch and advance
     * the partition straight from its pre-batch version to the batch's final version.
     */
    public void applyVisibleLogBatch(TransactionStateBatch txnStateBatch, Database db) {
        Map<Long, DeferredPublish> deferredPublishes = new LinkedHashMap<>();
        for (TransactionState txnState : txnStateBatch.getTransactionStates()) {
            TableCommitInfo tableCommitInfo = txnState.getTableCommitInfo(table.getId());
            if (tableCommitInfo == null) {
                // in a multi-table batch this txn does not write this applier's table
                continue;
            }
            applyVisibleLog(txnState, tableCommitInfo, db, deferredPublishes);
        }
        for (Map.Entry<Long, DeferredPublish> entry : deferredPublishes.entrySet()) {
            // Resolved under the same table write lock that resolved it above, so it is still present.
            PhysicalPartition partition = table.getPhysicalPartition(entry.getKey());
            DeferredPublish pending = entry.getValue();
            PartitionCommitInfo partitionCommitInfo = pending.finalCommitInfo;
            partition.updateVisibleVersion(partitionCommitInfo.getVersion(), partitionCommitInfo.getVersionTime());
            // Strictly after the version publication above, matching the single-transaction order: the
            // planner resolves the queryable layout before it reads the visible version, so a layout
            // cutover that landed first could be paired with a version older than the UNSHARE, whose
            // child tablets have no metadata object at that version.
            if (pending.unshareCutoverPending && partition.finishUnshare()) {
                table.lastSchemaUpdateTime.set(System.nanoTime());
            }
        }
    }
}
