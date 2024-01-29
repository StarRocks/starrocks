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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class LakeTableTxnStateListener implements TransactionStateListener {
    private static final Logger LOG = LogManager.getLogger(LakeTableTxnStateListener.class);
    private final DatabaseTransactionMgr dbTxnMgr;
    // lake table or lake materialized view
    private final OlapTable table;

    private Set<Long> dirtyPartitionSet;
    private Set<String> invalidDictCacheColumns;
    private Map<String, Long> validDictCacheColumns;

    public LakeTableTxnStateListener(DatabaseTransactionMgr dbTxnMgr, OlapTable table) {
        this.dbTxnMgr = dbTxnMgr;
        this.table = table;
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
            if (table.getPartition(tabletMeta.getPartitionId()) == null) {
                // this can happen when partitionId == -1 (tablet being dropping) or partition really not exist.
                continue;
            }
            dirtyPartitionSet.add(tabletMeta.getPartitionId());

            // Invalid column set should union
            invalidDictCacheColumns.addAll(finishedTablets.get(i).getInvalidDictCacheColumns());

            // Valid column set should intersect and remove all invalid columns
            // Only need to add valid column set once
            if (validDictCacheColumns.isEmpty() &&
                    !finishedTablets.get(i).getValidDictCacheColumns().isEmpty()) {
                TabletCommitInfo tabletCommitInfo = finishedTablets.get(i);
                List<Long> validDictCollectedVersions = tabletCommitInfo.getValidDictCollectedVersions();
                List<String> validDictCacheColumns = tabletCommitInfo.getValidDictCacheColumns();
                for (int j = 0; j < validDictCacheColumns.size(); j++) {
                    long version = 0;
                    // validDictCollectedVersions != validDictCacheColumns means be has not upgrade
                    if (validDictCollectedVersions.size() == validDictCacheColumns.size()) {
                        version = validDictCollectedVersions.get(j);
                    }
                    this.validDictCacheColumns.put(validDictCacheColumns.get(i), version);
                }
            }
            if (i == tabletMetaList.size() - 1) {
                validDictCacheColumns.entrySet().removeIf(entry -> invalidDictCacheColumns.contains(entry.getKey()));
            }

            finishedTabletsOfThisTable.add(finishedTablets.get(i).getTabletId());
        }

        List<Long> unfinishedTablets = null;
        for (Long partitionId : dirtyPartitionSet) {
            Partition partition = table.getPartition(partitionId);
            List<MaterializedIndex> allIndices = txnState.getPartitionLoadedTblIndexes(table.getId(), partition);
            for (MaterializedIndex index : allIndices) {
                Optional<Tablet> unfinishedTablet =
                        index.getTablets().stream().filter(t -> !finishedTabletsOfThisTable.contains(t.getId())).findAny();
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
            Partition partition = table.getPartition(partitionId);
            PartitionCommitInfo partitionCommitInfo;
            long version = -1;
            if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                version = partition.getNextVersion();
            }
            if (isFirstPartition) {
                List<String> validDictCacheColumnNames = Lists.newArrayList();
                List<Long> validDictCacheColumnVersions = Lists.newArrayList();

                validDictCacheColumns.forEach((name, dictVersion) -> {
                    validDictCacheColumnNames.add(name);
                    validDictCacheColumnVersions.add(dictVersion);
                });

                partitionCommitInfo = new PartitionCommitInfo(partitionId, version, 0,
                        Lists.newArrayList(invalidDictCacheColumns),
                        validDictCacheColumnNames,
                        validDictCacheColumnVersions);
            } else {
                partitionCommitInfo = new PartitionCommitInfo(partitionId, version, 0);
            }
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            isFirstPartition = false;
        }

        // The new versions in a replication transaction depend on the versions in ReplicationTxnCommitAttachment
        if (txnState.getSourceType() == TransactionState.LoadJobSourceType.REPLICATION) {
            ReplicationTxnCommitAttachment attachment = (ReplicationTxnCommitAttachment) txnState
                    .getTxnCommitAttachment();
            Map<Long, Long> partitionVersions = attachment.getPartitionVersions();
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                partitionCommitInfo.setVersion(partitionVersions.get(partitionCommitInfo.getPartitionId()));
            }
        }

        txnState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);
    }

    @Override
<<<<<<< HEAD
    public void postWriteCommitLog(TransactionState txnState) {
        // nothing to do
    }

    @Override
    public void postAbort(TransactionState txnState, List<TabletFailInfo> failedTablets) {
=======
    public void postAbort(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
            List<TabletFailInfo> failedTablets) {
        // If a transaction is prepared then aborted, the commit infos in txn state may be already assigned
        if (!finishedTablets.isEmpty()) {
            txnState.setTabletCommitInfos(finishedTablets);
        }
>>>>>>> 203e9d07d6 ([Enhancement] Aborting transaction supports carrying finished tablets info to help clean dirty data for shared-data mode (#39834))
        if (CollectionUtils.isEmpty(txnState.getTabletCommitInfos())) {
            abortTxnSkipCleanup(txnState);
        } else {
            abortTxnWithCleanup(txnState);
        }
        txnState.clearAutomaticPartitionSnapshot();
    }

    private void abortTxnSkipCleanup(TransactionState txnState) {
        List<Long> txnIds = Collections.singletonList(txnState.getTransactionId());
        List<TxnTypePB> txnTypes = Collections.singletonList(txnState.getTxnTypePB());
        List<ComputeNode> nodes = getAllAliveNodes();
        for (ComputeNode node : nodes) { // Send abortTxn() request to all nodes
            AbortTxnRequest request = new AbortTxnRequest();
            request.txnIds = txnIds;
            request.txnTypes = txnTypes;
            request.skipCleanup = true;
            request.tabletIds = null; // unused when skipCleanup is true

            sendAbortTxnRequestIgnoreResponse(request, node);
        }
    }

    private void abortTxnWithCleanup(TransactionState txnState) {
        List<Long> txnIds = Collections.singletonList(txnState.getTransactionId());
        List<TxnTypePB> txnTypes = Collections.singletonList(txnState.getTxnTypePB());
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
            request.txnIds = txnIds;
            request.txnTypes = txnTypes;
            request.tabletIds = entry.getValue();
            request.skipCleanup = false;

            sendAbortTxnRequestIgnoreResponse(request, node);
            allNodes.remove(node.getId());
        }
        // Send abortTxn() request to rest nodes
        for (ComputeNode node : allNodes.values()) {
            AbortTxnRequest request = new AbortTxnRequest();
            request.txnIds = txnIds;
            request.txnTypes = txnTypes;
            request.skipCleanup = true;
            request.tabletIds = null; // unused when skipCleanup is true

            sendAbortTxnRequestIgnoreResponse(request, node);
        }
    }

    static void sendAbortTxnRequestIgnoreResponse(AbortTxnRequest request, ComputeNode node) {
        try {
            BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort()).abortTxn(request);
        } catch (Throwable e) {
            LOG.error(e);
        }
    }

    static List<ComputeNode> getAllAliveNodes() {
        List<ComputeNode> nodes = new ArrayList<>();
        nodes.addAll(GlobalStateMgr.getCurrentSystemInfo().getAvailableComputeNodes());
        nodes.addAll(GlobalStateMgr.getCurrentSystemInfo().getAvailableBackends());
        return nodes;
    }

    @Nullable
    static ComputeNode getAliveNode(Long nodeId) {
        return GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId);
    }
}
