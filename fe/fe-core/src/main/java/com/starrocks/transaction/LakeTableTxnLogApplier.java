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

            // The version of a replication transaction may not continuously
            if (txnState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION) {
                partition.setNextVersion(partitionCommitInfo.getVersion() + 1);
                partition.setNextDataVersion(partitionCommitInfo.getDataVersion() + 1);
            } else {
                partition.setNextVersion(partition.getNextVersion() + 1);
                if (txnState.getSourceType() != TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
                    partition.setNextDataVersion(partition.getNextDataVersion() + 1);
                }
            }
        }
    }

    public void applyVisibleLog(TransactionState txnState, TableCommitInfo commitInfo, Database db) {
        applyVisibleLog(txnState, commitInfo, db, null);
    }

    /**
     * @param deferredPublishes when non-null, the mutation a reader could pair inconsistently - the
     *                          visible version - is not applied here but recorded per physical partition
     *                          id, for {@link #applyVisibleLogBatch} to apply once the whole batch has
     *                          been applied.
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
                if (txnState.getTxnCommitAttachment() != null) {
                    isPartialSuccess = ((CompactionTxnCommitAttachment) txnState.getTxnCommitAttachment()).getForceCommit();
                }
                compactionManager.handleCompactionFinished(partitionIdentifier, version, versionTime, compactionScore,
                        txnState.getTransactionId(), isPartialSuccess);
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
            PartitionCommitInfo partitionCommitInfo = entry.getValue().finalCommitInfo;
            partition.updateVisibleVersion(partitionCommitInfo.getVersion(), partitionCommitInfo.getVersionTime());
        }
    }
}
