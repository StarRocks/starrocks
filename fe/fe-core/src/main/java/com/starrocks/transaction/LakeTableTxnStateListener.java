// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.Utils;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LakeTableTxnStateListener implements TransactionStateListener {
    private static final Logger LOG = LogManager.getLogger(LakeTableTxnStateListener.class);
    private final DatabaseTransactionMgr dbTxnMgr;
    private final LakeTable table;

    private Set<Long> dirtyPartitionSet;

    public LakeTableTxnStateListener(DatabaseTransactionMgr dbTxnMgr, LakeTable table) {
        this.dbTxnMgr = dbTxnMgr;
        this.table = table;
    }

    @Override
    public void preCommit(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
            List<TabletFailInfo> failedTablets) throws TransactionException {
        Preconditions.checkState(txnState.getTransactionStatus() != TransactionStatus.COMMITTED);
        if (table.getState() == OlapTable.OlapTableState.RESTORE) {
            throw new TransactionCommitFailedException("Cannot write RESTORE state table \"" + table.getName() + "\"");
        }
        dirtyPartitionSet = Sets.newHashSet();
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
        Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.COMMITTED);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        for (long partitionId : dirtyPartitionSet) {
            Partition partition = table.getPartition(partitionId);
            long version = partition.getNextVersion();
            PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId, version, 0);
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        }
        txnState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);
    }

    @Override
    public void postWriteCommitLog(TransactionState txnState) {
        // nothing to do
    }

    @Override
    public void postAbort(TransactionState txnState, List<TabletFailInfo> failedTablets) {
        Map<Long, List<Long>> tabletGroup = null;
        Database db = GlobalStateMgr.getCurrentState().getDb(txnState.getDbId());
        if (db == null) {
            return;
        }

        db.readLock();
        try {
            // Preconditions: has acquired the database's reader or writer lock.
            tabletGroup = Utils.groupTabletID(table);
        } catch (NoAliveBackendException e) {
            LOG.warn(e);
        } finally {
            db.readUnlock();
        }

        if (tabletGroup == null) {
            return;
        }

        for (Map.Entry<Long, List<Long>> entry : tabletGroup.entrySet()) {
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(entry.getKey());
            if (backend == null) {
                // It's ok to skip sending abort transaction request.
                continue;
            }
            AbortTxnRequest request = new AbortTxnRequest();
            request.txnIds = Lists.newArrayList();
            request.txnIds.add(txnState.getTransactionId());
            request.tabletIds = entry.getValue();

            try {
                LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
                lakeService.abortTxn(request);
            } catch (Throwable e) {
                LOG.error(e);
            }
        }
    }
}
