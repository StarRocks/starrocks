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
import com.starrocks.proto.AbortTxnResponse;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.RpcException;
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
import java.util.concurrent.Future;
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
    public void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
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

        if (enableIngestSlowdown()) {
            long currentTimeMs = System.currentTimeMillis();
            Set<Long> partitionIds = Sets.newHashSet();
            for (Long partitionId : dirtyPartitionSet) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                partitionIds.add(partition.getParentId());
            }
            new CommitRateLimiter(compactionMgr, txnState, table.getId()).check(partitionIds, currentTimeMs);
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
        if (txnState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION) {
            abortReplicationTxnWithCleanup(txnState);
        } else if (CollectionUtils.isEmpty(txnState.getTabletCommitInfos())) {
            abortTxnSkipCleanup(txnState);
        } else {
            abortTxnWithCleanup(txnState);
        }
        txnState.clearAutomaticPartitionSnapshot();
    }

    private void abortReplicationTxnWithCleanup(TransactionState txnState) {
        List<TxnInfoPB> txnInfos = Collections.singletonList(TxnInfoHelper.fromTransactionState(txnState));
        Set<Long> tabletIds = Sets.newHashSet();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            for (MaterializedIndex index :
                    txnState.getPartitionLoadedIndexesWithoutLock(table.getId(), partition)) {
                for (Tablet tablet : index.getTablets()) {
                    tabletIds.add(tablet.getId());
                }
            }
        }
        if (tabletIds.isEmpty()) {
            return;
        }

        List<ComputeNode> fenceNodes = getAllNodes();
        if (fenceNodes.isEmpty()) {
            LOG.error("Cannot clean aborted replication transaction {}, no compute node", txnState.getTransactionId());
            return;
        }

        // Phase 1 fences every CN before any object is removed. Each RPC rejects new replication
        // tasks and returns only after active tasks have exited and cleaned their own late writes.
        boolean allNodesFenced = true;
        for (ComputeNode node : fenceNodes) {
            AbortTxnRequest request = new AbortTxnRequest();
            request.skipCleanup = true;
            request.tabletIds = new ArrayList<>(tabletIds);
            request.txnInfos = txnInfos;
            try {
                AbortTxnResponse response = sendAbortTxnRequest(request, node).get();
                if (response == null) {
                    allNodesFenced = false;
                    LOG.error("Empty fence response for aborted replication transaction {} on node {}",
                            txnState.getTransactionId(), node.getId());
                } else if (response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    allNodesFenced = false;
                    LOG.error("Failed to persist abort markers for replication transaction {} tablets {} on node {}",
                            txnState.getTransactionId(), response.failedTablets, node.getId());
                }
            } catch (Throwable e) {
                allNodesFenced = false;
                LOG.error("Failed to fence aborted replication transaction {} on node {}",
                        txnState.getTransactionId(), node.getId(), e);
            }
        }
        if (!allNodesFenced) {
            // A node that became unavailable after accepting a task can still be writing. Deleting
            // without its acknowledgement is unsafe, so keep the immutable PREPARED manifests;
            // txn-log vacuum consumes their data-file cleanup lists after the watermark advances.
            return;
        }

        // All lake data is in shared storage, so any live CN can clean every target tablet. Retry
        // another CN if the selected worker fails during the cleanup phase.
        List<ComputeNode> nodes = getAllAliveNodes();
        for (ComputeNode node : nodes) {
            AbortTxnRequest request = new AbortTxnRequest();
            request.skipCleanup = false;
            request.tabletIds = new ArrayList<>(tabletIds);
            request.txnInfos = txnInfos;
            try {
                AbortTxnResponse response = sendAbortTxnRequest(request, node).get();
                if (response == null) {
                    LOG.warn("Empty cleanup response for aborted replication transaction {} on node {}, "
                                    + "retry another node", txnState.getTransactionId(), node.getId());
                } else if (response.failedTablets == null || response.failedTablets.isEmpty()) {
                    return;
                } else {
                    LOG.warn("Cleanup of aborted replication transaction {} failed for tablets {} on node {}",
                            txnState.getTransactionId(), response.failedTablets, node.getId());
                }
            } catch (Throwable e) {
                LOG.warn("Failed to clean aborted replication transaction {} on node {}, retry another node",
                        txnState.getTransactionId(), node.getId(), e);
            }
        }
        LOG.error("Failed to clean aborted replication transaction {} on every alive node",
                txnState.getTransactionId());
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

    static Future<AbortTxnResponse> sendAbortTxnRequest(AbortTxnRequest request, ComputeNode node) throws RpcException {
        return BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort()).abortTxn(request);
    }

    static List<ComputeNode> getAllAliveNodes() {
        List<ComputeNode> nodes = new ArrayList<>();
        nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableComputeNodes());
        nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableBackends());
        return nodes;
    }

    static List<ComputeNode> getAllNodes() {
        List<ComputeNode> nodes = new ArrayList<>();
        nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodes());
        nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends());
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
