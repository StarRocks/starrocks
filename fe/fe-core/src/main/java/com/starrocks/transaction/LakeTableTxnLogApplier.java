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
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

            // The version of a replication transaction may not continuously
            Preconditions.checkState(txnState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION
                    || txnState.isVersionOverwrite()
                    || partitionCommitInfo.isDoubleWrite()
                    || version == partition.getVisibleVersion() + 1);

            partition.updateVisibleVersion(version, versionTime);
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
     * <p>The candidate also carries the largest tablet living in an index whose tablet count is still
     * below the early-split ceiling, so a partition created under continuous load reaches useful write
     * parallelism without waiting for the next periodic scan. That signal costs nothing until some
     * just-published tablet is large enough for an early split, and resolving it is probe-free and
     * best-effort: a failure drops the hint and leaves the publish itself untouched.
     *
     * <p>{@code tabletStats} is transient transport: it is consumed here and cleared to bound FE heap,
     * since the LakeTablet row counts set above persist independently and the post-visible first-load
     * statistics collector samples from LakeTablet.getFuzzyRowCount(), not from this map.
     */
    private void refreshTabletStatsAndMarkReshardCandidate(PhysicalPartition partition,
            Map<Long, TabletStatPB> tabletStats, Database db, long version, long versionTime) {
        List<MaterializedIndex> indexes = partition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
        long maxTabletSize = 0L;
        // Per-index maxima, keyed by INDEX ID. Never key by the MaterializedIndex object: it overrides
        // equals/hashCode over mutable content, so it is both an unstable and an O(tablets) hash key.
        Map<Long, Long> maxSizeByIndexId = new HashMap<>();
        Map<Long, MaterializedIndex> indexById = new HashMap<>();
        // Walk only the tablets this publish actually reported, not every tablet in the partition: this
        // runs under the table write lock, so resolve each reported id directly (O(1) per index).
        for (Map.Entry<Long, TabletStatPB> entry : tabletStats.entrySet()) {
            Tablet tablet = null;
            MaterializedIndex owningIndex = null;
            for (MaterializedIndex index : indexes) {
                tablet = index.getTablet(entry.getKey());
                if (tablet != null) {
                    owningIndex = index;
                    break;
                }
            }
            if (!(tablet instanceof LakeTablet) || owningIndex == null) {
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
            maxSizeByIndexId.merge(owningIndex.getId(), dataSize, Math::max);
            indexById.putIfAbsent(owningIndex.getId(), owningIndex);
        }
        long maxUnderProvisionedTabletSize = 0L;
        // Pure arithmetic gate: a publish with no early-split-sized tablet pays nothing below.
        if (TabletReshardUtils.needEarlySplit(maxTabletSize)) {
            // Best-effort O(1) fast path: the cluster-wide node total is an upper bound on any single
            // worker group's count, hence on the ceiling. It only helps when the table's group is most
            // of the cluster, and a concurrent node addition can make it briefly stale — the worst
            // outcome is one skipped early signal, never a wrong split.
            SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            int ceilingUpperBound = clusterInfo.getTotalBackendNumber() + clusterInfo.getTotalComputeNodeNumber();
            boolean anyPossiblyUnderProvisioned = indexById.values().stream()
                    .anyMatch(idx -> idx.getTablets().size() < ceilingUpperBound);
            if (anyPossiblyUnderProvisioned) {
                try {
                    // NOT txnState.getComputeResource(): publishVersionBatch substitutes the background
                    // resource when the transaction's is unavailable without writing it back, so it is
                    // not reliably the resource publish ran on. Probe-free — this runs under the table
                    // write lock and must not reach StarMgr.
                    int cn = TabletReshardUtils.computeNodeCount(
                            GlobalStateMgr.getCurrentState().getWarehouseMgr()
                                    .getBackgroundComputeResourceWithoutProbe(table.getId()));
                    int ceiling = TabletReshardUtils.earlySplitCeiling(cn,
                            Config.tablet_reshard_max_split_count);
                    for (Map.Entry<Long, Long> e : maxSizeByIndexId.entrySet()) {
                        MaterializedIndex idx = indexById.get(e.getKey());
                        if (idx != null && idx.getTablets().size() < ceiling) {
                            maxUnderProvisionedTabletSize = Math.max(maxUnderProvisionedTabletSize, e.getValue());
                        }
                    }
                } catch (RuntimeException e) {
                    // Optional scheduling must never fail visible-log application.
                    LOG.warn("Skipping the early-split signal for table {}: {}", table.getId(), e.getMessage());
                }
            }
        }
        if (maxTabletSize > 0 && table.isRangeDistribution()) {
            GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                    .addReshardCandidate(db.getId(), table.getId(), maxTabletSize, Long.MAX_VALUE,
                            maxUnderProvisionedTabletSize);
        }
        tabletStats.clear();
    }

    public void applyVisibleLogBatch(TransactionStateBatch txnStateBatch, Database db) {
        for (TransactionState txnState : txnStateBatch.getTransactionStates()) {
            TableCommitInfo tableCommitInfo = txnState.getTableCommitInfo(table.getId());
            if (tableCommitInfo == null) {
                // in a multi-table batch this txn does not write this applier's table
                continue;
            }
            applyVisibleLog(txnState, tableCommitInfo, db);
        }
    }
}
