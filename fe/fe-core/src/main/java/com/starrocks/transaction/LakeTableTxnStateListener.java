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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.lake.CommitRateLimiter;
import com.starrocks.lake.TxnInfoHelper;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class LakeTableTxnStateListener implements TransactionStateListener {
    private static final Logger LOG = LogManager.getLogger(LakeTableTxnStateListener.class);
    private final DatabaseTransactionMgr dbTxnMgr;
    // lake table or lake materialized view
    private final OlapTable table;

    private Set<Long> dirtyPartitionSet;
    private Set<ColumnId> invalidDictCacheColumns;
    private Map<ColumnId, Long> validDictCacheColumns;
    private final CompactionMgr compactionMgr;

    public LakeTableTxnStateListener(@NotNull DatabaseTransactionMgr dbTxnMgr, @NotNull OlapTable table) {
        this.dbTxnMgr = Objects.requireNonNull(dbTxnMgr, "dbTxnMgr is null");
        this.table = Objects.requireNonNull(table, "table is null");
        this.compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
        Preconditions.checkState(this.table.isCloudNativeTableOrMaterializedView(),
                "expect LakeTable or LakeMaterializedView but real type is " + this.table.getClass().getName());
    }

    @Override
    public String getTableName() {
        return table.getName();
    }

    @Override
    public void prePrepared(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
                            List<TabletFailInfo> failedTablets) throws TransactionException {
        Preconditions.checkState(txnState.getTransactionStatus() != TransactionStatus.COMMITTED);
        txnState.clearAutomaticPartitionSnapshot();
        if (!finishedTablets.isEmpty()) {
            txnState.setTabletCommitInfos(finishedTablets);
        }
        if (table.getState() == OlapTable.OlapTableState.RESTORE) {
            throw new TransactionCommitFailedException("Cannot write RESTORE state table \"" + table.getName() + "\"");
        }
        dirtyPartitionSet = Sets.newHashSet();
        invalidDictCacheColumns = Sets.newHashSet();
        validDictCacheColumns = Maps.newHashMap();

        Set<Long> finishedTabletsOfThisTable = Sets.newHashSet();

        TabletInvertedIndex tabletInvertedIndex = dbTxnMgr.getGlobalStateMgr().getTabletInvertedIndex();

        List<Long> tabletIds = finishedTablets.stream().map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            TabletMeta tabletMeta = tabletMetaList.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                continue;
            }
            if (tabletMeta.getTableId() != table.getId()) {
                continue;
            }
            if (table.getPhysicalPartition(tabletMeta.getPhysicalPartitionId()) == null) {
                // this can happen when partitionId == -1 (tablet being dropping) or partition really not exist.
                continue;
            }
            dirtyPartitionSet.add(tabletMeta.getPhysicalPartitionId());

            // Invalid column set should union
            invalidDictCacheColumns.addAll(finishedTablets.get(i).getInvalidDictCacheColumns());

            // Valid column set should intersect and remove all invalid columns
            // Only need to add valid column set once
            if (validDictCacheColumns.isEmpty() &&
                    !finishedTablets.get(i).getValidDictCacheColumns().isEmpty()) {
                TabletCommitInfo tabletCommitInfo = finishedTablets.get(i);
                List<Long> validDictCollectedVersions = tabletCommitInfo.getValidDictCollectedVersions();
                List<ColumnId> validDictCacheColumns = tabletCommitInfo.getValidDictCacheColumns();
                for (int j = 0; j < validDictCacheColumns.size(); j++) {
                    long version = 0;
                    // validDictCollectedVersions != validDictCacheColumns means be has not upgrade
                    if (validDictCollectedVersions.size() == validDictCacheColumns.size()) {
                        version = validDictCollectedVersions.get(j);
                    }
                    this.validDictCacheColumns.put(validDictCacheColumns.get(j), version);
                }
            }
            if (i == tabletMetaList.size() - 1) {
                validDictCacheColumns.entrySet().removeIf(entry -> invalidDictCacheColumns.contains(entry.getKey()));
            }

            finishedTabletsOfThisTable.add(finishedTablets.get(i).getTabletId());
        }

        List<Long> unfinishedTablets = null;
        for (Long partitionId : dirtyPartitionSet) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            List<MaterializedIndex> allIndices = txnState.getPartitionLoadedIndexesWithoutLock(table.getId(), partition);
            for (MaterializedIndex index : allIndices) {
                Optional<Tablet> unfinishedTablet =
                        index.getTablets().stream().filter(t -> !finishedTabletsOfThisTable.contains(t.getId()))
                                .findAny();
                if (!unfinishedTablet.isPresent()) {
                    continue;
                }
                if (unfinishedTablets == null) {
                    unfinishedTablets = Lists.newArrayList();
                }
                unfinishedTablets.add(unfinishedTablet.get().getId());
            }
        }

        if (unfinishedTablets != null && !unfinishedTablets.isEmpty()) {
            throw new TransactionCommitFailedException(
                    "table '" + table.getName() + "\" has unfinished tablets: " + unfinishedTablets);
        }

        checkShadowIndexesLoaded(txnState);
        checkSynchronousMaterializedViewIndexesLoaded(txnState);
    }

    /**
     * Reject a transaction that resolved its index set before a shadow index it was required to write
     * existed. A lake alter job exposes each shadow index under the table WRITE lock and stamps it with
     * its watershed txn id W (MaterializedIndex#getShadowWatershedTxnId). Every transaction with id >= W
     * must carry that shadow index: the shadow tablets replay a log version for each partition version
     * published after the job samples its conversion version, and a version without one wedges the job's
     * final publish.
     *
     * <p>A writer that begins its transaction before resolving tablets always passes: its id was drawn
     * after W and it resolves under the table READ lock, so it sees the shadow index. Only a writer that
     * resolves first can fail -- ReplicationJob, which snapshots tablets before beginning, or a bypass-write
     * client reusing cached metadata.
     *
     * <p>This is deliberately conservative: such a writer that publishes before the job samples its
     * conversion version would be harmless (its rows reach the shadow through the conversion), but the
     * commit path cannot see that point, so it is rejected as well. Do not narrow this to "published after
     * the sample" without a reliable marker for it.
     *
     * <p>Not checked: lake compaction, which compacts only visible tablets by design; a shadow index with no
     * stamp (loaded from an image written by an FE without the field); and a transaction with no recorded
     * index set, which falls back to the partition's latest indexes and is already enforced by the
     * unfinished-tablet check above.
     *
     * <p>The compaction exemption is safe only while CompactionScheduler keeps compaction off the table for
     * the whole alter. Its table-state gate skips every table in SCHEMA_CHANGE or ROLLUP state, which covers
     * schema changes and both rollup paths (LakeRollupJob for hash-distributed tables and sync MVs,
     * LakeRangeRollupJob for range-distribution tables). A compaction committed after W would carry no shadow
     * vlog and wedge the job's final publish just like a writer this method rejects.
     *
     * <p>The check runs only while the index is still SHADOW. The alter jobs wait only for transactions below W,
     * so a writer that resolved its tablets early and is still open when the alter makes the index NORMAL is not
     * caught here. After a schema change its tablets no longer resolve and it commits nothing; after a rollup it
     * publishes the base index only. NORMAL range sync MVs are checked separately below.
     */
    private void checkShadowIndexesLoaded(TransactionState txnState) throws TransactionCommitFailedException {
        if (txnState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION) {
            return;
        }
        long txnId = txnState.getTransactionId();
        for (Long partitionId : dirtyPartitionSet) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            List<Long> loadedIndexIds = txnState.getPartitionLoadedIndexIdsWithoutLock(table.getId(), partitionId);
            if (partition == null || loadedIndexIds == null) {
                continue;
            }
            for (MaterializedIndex shadowIndex : partition.getLatestMaterializedIndices(
                    MaterializedIndex.IndexExtState.SHADOW)) {
                long requiredSinceTxnId = shadowIndex.getShadowWatershedTxnId();
                if (requiredSinceTxnId <= 0 || requiredSinceTxnId > txnId
                        || loadedIndexIds.contains(shadowIndex.getId())) {
                    continue;
                }
                String msg = "transaction " + txnId + " (" + txnState.getSourceType() + ") wrote partition "
                        + partitionId + " of table '" + table.getName() + "' without shadow index "
                        + shadowIndex.getId() + ", which every transaction since " + requiredSinceTxnId
                        + " must write while the table is being altered. The writer resolved the table's"
                        + " tablets before the alter added the index; refresh the table metadata and retry"
                        + " the load";
                LOG.warn(msg);
                throw new TransactionCommitFailedException(msg);
            }
        }
    }

    /**
     * A range sync MV keeps its required-since watermark in logical metadata after activation. Writers
     * may carry retained predecessor layouts during split/merge, so coverage is checked by logical meta
     * id, after the tablet completeness check has validated the recorded physical layouts.
     */
    private void checkSynchronousMaterializedViewIndexesLoaded(TransactionState txnState)
            throws TransactionCommitFailedException {
        // Compaction is index-local. A later additive job's internal rewrite writes only its own SHADOW.
        // Exemptions follow the existing persisted transaction-source classification contract, not the index list.
        // Persistence does not authenticate the source's origin; transaction ingress owns source validation.
        if (!table.isRangeDistribution()
                || txnState.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION
                || txnState.isShadowRewrite()) {
            return;
        }
        long txnId = txnState.getTransactionId();
        for (Long partitionId : dirtyPartitionSet) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            List<Long> loadedIndexIds = txnState.getPartitionLoadedIndexIdsWithoutLock(table.getId(), partitionId);
            if (partition == null || loadedIndexIds == null) {
                // No snapshot: the existing latest-index tablet completeness check already enforces coverage.
                continue;
            }
            Set<Long> loadedMetaIds = Sets.newHashSet();
            for (long physicalIndexId : loadedIndexIds) {
                MaterializedIndex loadedIndex = partition.getIndex(physicalIndexId);
                if (loadedIndex != null) {
                    loadedMetaIds.add(loadedIndex.getMetaId());
                }
            }
            for (MaterializedIndex index : partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                MaterializedIndexMeta meta = table.getIndexMetaByMetaId(index.getMetaId());
                if (index.getState() != MaterializedIndex.IndexState.NORMAL || meta == null
                        || meta.getSyncMvDefinition() == null) {
                    continue;
                }
                long requiredSinceTxnId = meta.getSyncMvRequiredTxnId();
                if (requiredSinceTxnId <= 0 || txnId < requiredSinceTxnId || loadedMetaIds.contains(index.getMetaId())) {
                    continue;
                }
                String msg = "transaction " + txnId + " (" + txnState.getSourceType() + ") wrote partition "
                        + partitionId + " of table '" + table.getName() + "' (id " + table.getId()
                        + ") without synchronous materialized view '" + table.getIndexNameByMetaId(index.getMetaId())
                        + "' (logical index " + index.getMetaId() + "), which every transaction since "
                        + requiredSinceTxnId + " must write after activation. The writer resolved the table's"
                        + " tablets before the materialized view was added; refresh the table metadata and retry the load";
                LOG.warn(msg);
                throw new TransactionCommitFailedException(msg);
            }
        }
    }

    @Override
    public void preCommit(TransactionState txnState) throws TransactionException {
        if (!enableIngestSlowdown()) {
            return;
        }

        TableCommitInfo tableCommitInfo = txnState.getTableCommitInfo(table.getId());
        if (tableCommitInfo == null || tableCommitInfo.getIdToPartitionCommitInfo() == null) {
            return;
        }

        Set<Long> partitionIds = Sets.newHashSet();
        for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionCommitInfo.getPhysicalPartitionId());
            if (partition != null) {
                partitionIds.add(partition.getId());
            }
        }
        new CommitRateLimiter(compactionMgr, txnState, table.getId())
                .check(partitionIds, System.currentTimeMillis());
    }

    @Override
    public void preWriteCommitLog(TransactionState txnState) {
        Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.COMMITTED
                || txnState.getTransactionStatus() == TransactionStatus.PREPARED);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        boolean isFirstPartition = true;
        for (long partitionId : dirtyPartitionSet) {
            PartitionCommitInfo partitionCommitInfo;
            if (isFirstPartition) {
                List<ColumnId> validDictCacheColumnNames = Lists.newArrayList();
                List<Long> validDictCacheColumnVersions = Lists.newArrayList();

                validDictCacheColumns.forEach((name, dictVersion) -> {
                    validDictCacheColumnNames.add(name);
                    validDictCacheColumnVersions.add(dictVersion);
                });

                partitionCommitInfo = new PartitionCommitInfo(partitionId, -1, 0,
                        Lists.newArrayList(invalidDictCacheColumns),
                        validDictCacheColumnNames,
                        validDictCacheColumnVersions);
            } else {
                partitionCommitInfo = new PartitionCommitInfo(partitionId, -1, 0);
            }
            // A shadow-rewrite txn writes a real, non-version-advancing PartitionCommitInfo so the
            // publish daemon has a work item. The sentinel version (-1) and the txn sourceType
            // (txnState.isShadowRewrite()) keep it out of all version allocation, adjacency checks,
            // and visible-version advances.
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            isFirstPartition = false;
        }

        txnState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);
    }

    @Override
    public void postAbort(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
            List<TabletFailInfo> failedTablets) {
        // If a transaction is prepared then aborted, the commit infos in txn state may be already assigned
        if (!finishedTablets.isEmpty()) {
            txnState.setTabletCommitInfos(finishedTablets);
        }
        if (CollectionUtils.isEmpty(txnState.getTabletCommitInfos())) {
            abortTxnSkipCleanup(txnState);
        } else {
            abortTxnWithCleanup(txnState);
        }
        txnState.clearAutomaticPartitionSnapshot();
    }

    private void abortTxnSkipCleanup(TransactionState txnState) {
        List<TxnInfoPB> txnInfos = Collections.singletonList(TxnInfoHelper.fromTransactionState(txnState));
        List<ComputeNode> nodes = getAllAliveNodes();
        for (ComputeNode node : nodes) { // Send abortTxn() request to all nodes
            AbortTxnRequest request = new AbortTxnRequest();
            request.skipCleanup = true;
            request.tabletIds = null; // unused when skipCleanup is true
            request.txnInfos = txnInfos;

            sendAbortTxnRequestIgnoreResponse(request, node);
        }
    }

    private void abortTxnWithCleanup(TransactionState txnState) {
        List<TxnInfoPB> txnInfos = Collections.singletonList(TxnInfoHelper.fromTransactionState(txnState));
        Map<Long, List<Long>> tabletGroup = new HashMap<>();
        for (TabletCommitInfo info : txnState.getTabletCommitInfos()) {
            tabletGroup.computeIfAbsent(info.getBackendId(), k -> Lists.newArrayList()).add(info.getTabletId());
        }
        Map<Long, ComputeNode> allNodes = new HashMap<>();
        for (ComputeNode node : getAllAliveNodes()) {
            allNodes.put(node.getId(), node);
        }
        for (Map.Entry<Long, List<Long>> entry : tabletGroup.entrySet()) {
            ComputeNode node = getAliveNode(entry.getKey());
            if (node == null) {
                continue;
            }
            AbortTxnRequest request = new AbortTxnRequest();
            request.txnInfos = txnInfos;
            request.tabletIds = entry.getValue();
            request.skipCleanup = false;

            sendAbortTxnRequestIgnoreResponse(request, node);
            allNodes.remove(node.getId());
        }
        // Send abortTxn() request to rest nodes
        for (ComputeNode node : allNodes.values()) {
            AbortTxnRequest request = new AbortTxnRequest();
            request.txnInfos = txnInfos;
            request.skipCleanup = true;
            request.tabletIds = null; // unused when skipCleanup is true

            sendAbortTxnRequestIgnoreResponse(request, node);
        }
    }

    static void sendAbortTxnRequestIgnoreResponse(AbortTxnRequest request, ComputeNode node) {
        try {
            BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort()).abortTxn(request);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    static List<ComputeNode> getAllAliveNodes() {
        List<ComputeNode> nodes = new ArrayList<>();
        nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableComputeNodes());
        nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableBackends());
        return nodes;
    }

    @Nullable
    static ComputeNode getAliveNode(Long nodeId) {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
    }

    static boolean enableIngestSlowdown() {
        return Config.lake_enable_ingest_slowdown;
    }
}
