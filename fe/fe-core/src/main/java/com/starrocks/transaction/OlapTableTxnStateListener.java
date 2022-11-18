// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.ClearTransactionTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OlapTableTxnStateListener implements TransactionStateListener {
    private static final Logger LOG = LogManager.getLogger(OlapTableTxnStateListener.class);
    private static final List<ClearTransactionTask> CLEAR_TRANSACTION_TASKS = Lists.newArrayList();

    private final DatabaseTransactionMgr dbTxnMgr;
    private final OlapTable table;

    private Set<Long> totalInvolvedBackends;
    private Set<Long> errorReplicaIds;
    private Set<Long> dirtyPartitionSet;
    private Set<String> invalidDictCacheColumns;
    private Set<String> validDictCacheColumns;

    public OlapTableTxnStateListener(DatabaseTransactionMgr dbTxnMgr, OlapTable table) {
        this.dbTxnMgr = dbTxnMgr;
        this.table = table;
    }

    @Override
    public void preCommit(TransactionState txnState, List<TabletCommitInfo> tabletCommitInfos,
            List<TabletFailInfo> failedTablets) throws TransactionException {
        Preconditions.checkState(txnState.getTransactionStatus() != TransactionStatus.COMMITTED);
        if (table.getState() == OlapTable.OlapTableState.RESTORE) {
            throw new TransactionCommitFailedException("Cannot write RESTORE state table \"" + table.getName() + "\"");
        }
        totalInvolvedBackends = Sets.newHashSet();
        errorReplicaIds = Sets.newHashSet();
        dirtyPartitionSet = Sets.newHashSet();
        invalidDictCacheColumns = Sets.newHashSet();
        validDictCacheColumns = Sets.newHashSet();

        TabletInvertedIndex tabletInvertedIndex = dbTxnMgr.getGlobalStateMgr().getTabletInvertedIndex();
        Map<Long, Set<Long>> tabletToBackends = new HashMap<>();
        Set<Long> allCommittedBackends = new HashSet<>();

        // 2. validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if index is dropped, it does not matter.
        // if table or partition is dropped during load, just ignore that tablet,
        // because we should allow dropping rollup or partition during load
        List<Long> tabletIds = tabletCommitInfos.stream().map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            TabletMeta tabletMeta = tabletMetaList.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                continue;
            }
            long tabletId = tabletIds.get(i);
            long tableId = tabletMeta.getTableId();
            if (tableId != table.getId()) {
                continue;
            }
            long partitionId = tabletMeta.getPartitionId();
            if (table.getPartition(partitionId) == null) {
                // this can happen when partitionId == -1 (tablet being dropping)
                // or partition really not exist.
                continue;
            }
            dirtyPartitionSet.add(partitionId);
            tabletToBackends.computeIfAbsent(tabletId, id -> new HashSet<>())
                    .add(tabletCommitInfos.get(i).getBackendId());
            allCommittedBackends.add(tabletCommitInfos.get(i).getBackendId());

            // Invalid column set should union
            invalidDictCacheColumns.addAll(tabletCommitInfos.get(i).getInvalidDictCacheColumns());

            // Valid column set should intersect and remove all invalid columns
            // Only need to add valid column set once
            if (validDictCacheColumns.isEmpty() &&
                    !tabletCommitInfos.get(i).getValidDictCacheColumns().isEmpty()) {
                validDictCacheColumns.addAll(tabletCommitInfos.get(i).getValidDictCacheColumns());
            }

            if (i == tabletMetaList.size() - 1) {
                validDictCacheColumns.removeAll(invalidDictCacheColumns);
            }
        }

        // update write failed backend/replica
        // use for selection of primary replica for replicated storage
        for (TabletFailInfo failedTablet : failedTablets) {
            if (failedTablet.getTabletId() == -1) {
                Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(failedTablet.getBackendId());
                if (backend != null) {
                    backend.setLastWriteFail(true);
                }
            } else {
                Replica replica = tabletInvertedIndex.getReplica(failedTablet.getTabletId(), failedTablet.getBackendId());
                if (replica != null) {
                    replica.setLastWriteFail(true);
                }
            }
        }

        for (Long committedBackend : allCommittedBackends) {
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(committedBackend);
            if (backend != null) {
                backend.setLastWriteFail(false);
            }
        }

        for (Partition partition : table.getAllPartitions()) {
            if (!dirtyPartitionSet.contains(partition.getId())) {
                continue;
            }

            List<MaterializedIndex> allIndices = txnState.getPartitionLoadedTblIndexes(table.getId(), partition);
            int quorumReplicaNum = table.getPartitionInfo().getQuorumNum(partition.getId(), table.writeQuorum());
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();
                    Set<Long> commitBackends = tabletToBackends.get(tabletId);

                    Set<Long> tabletBackends = tablet.getBackendIds();
                    totalInvolvedBackends.addAll(tabletBackends);

                    // save the error replica ids for current tablet
                    // this param is used for log
                    StringBuilder failedReplicaInfoSB = new StringBuilder();
                    int successReplicaNum = 0;
                    for (long tabletBackend : tabletBackends) {
                        Replica replica = tabletInvertedIndex.getReplica(tabletId, tabletBackend);
                        if (replica == null) {
                            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(tabletBackend);
                            throw new TransactionCommitFailedException("Not found replicas of tablet. "
                                    + "tablet_id: " + tabletId + ", backend_id: " + backend.getHost());
                        }
                        // if the tablet have no replica's to commit or the tablet is a rolling up tablet, the commit backends maybe null
                        // if the commit backends is null, set all replicas as error replicas
                        if (commitBackends != null && commitBackends.contains(tabletBackend)) {
                            // if the backend load success but the backend has some errors previously, then it is not a normal replica
                            // ignore it but not log it
                            // for example, a replica is in clone state
                            long lfv = replica.getLastFailedVersion();
                            if (lfv < 0) {
                                ++successReplicaNum;
                            } else {
                                Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(tabletBackend);
                                failedReplicaInfoSB.append(
                                        String.format("%d:{be:%d %s V:%d LFV:%d},", replica.getId(), tabletBackend,
                                                backend == null ? "" : backend.getHost(), replica.getVersion(), lfv));
                            }
                            replica.setLastWriteFail(false);
                        } else {
                            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(tabletBackend);
                            failedReplicaInfoSB.append(
                                    String.format("%d:{be:%d %s V:%d LFV:%d},", replica.getId(), tabletBackend,
                                            backend == null ? "" : backend.getHost(), replica.getVersion(),
                                            replica.getLastSuccessVersion()));
                            // not remove rollup task here, because the commit maybe failed
                            // remove rollup task when commit successfully
                            errorReplicaIds.add(replica.getId());

                            replica.setLastWriteFail(true);
                        }
                    }

                    if (successReplicaNum < quorumReplicaNum) {
                        String msg = String.format("Commit failed. txn: %d table: %s tablet: %d quorum: %d<%d errorReplicas: %s",
                                txnState.getTransactionId(), table.getName(), tablet.getId(), successReplicaNum, quorumReplicaNum,
                                failedReplicaInfoSB.toString());
                        LOG.warn(msg);
                        throw new TabletQuorumFailedException(msg);
                    }
                }
            }
        }
    }

    @Override
    public void preWriteCommitLog(TransactionState txnState) {
        Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.COMMITTED
                || txnState.getTransactionStatus() == TransactionStatus.PREPARED);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        boolean isFirstPartition = true;
        txnState.getErrorReplicas().addAll(errorReplicaIds);
        for (long partitionId : dirtyPartitionSet) {
            Partition partition = table.getPartition(partitionId);
            PartitionCommitInfo partitionCommitInfo;
            long version = -1;
            if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                version = partition.getNextVersion();
            }
            if (isFirstPartition) {
                partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        version,
                        System.currentTimeMillis(),
                        Lists.newArrayList(invalidDictCacheColumns),
                        Lists.newArrayList(validDictCacheColumns));
            } else {
                partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        version,
                        System.currentTimeMillis() /* use as partition visible time */);
            }
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            isFirstPartition = false;
        }
        txnState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);
    }

    @Override
    public void postWriteCommitLog(TransactionState txnState) {
        // add publish version tasks. set task to null as a placeholder.
        // tasks will be created when publishing version.
        for (long backendId : totalInvolvedBackends) {
            txnState.addPublishVersionTask(backendId, null);
        }
    }

    @Override
    public void postAbort(TransactionState txnState, List<TabletFailInfo> failedTablets) {
        Database db = GlobalStateMgr.getCurrentState().getDb(txnState.getDbId());
        if (db != null) {
            db.readLock();
            try {
                TabletInvertedIndex tabletInvertedIndex = dbTxnMgr.getGlobalStateMgr().getTabletInvertedIndex();
                // update write failed backend/replica
                // use for selection of primary replica for replicated storage
                for (TabletFailInfo failedTablet : failedTablets) {
                    LOG.info(failedTablet);
                    if (failedTablet.getTabletId() == -1) {
                        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(failedTablet.getBackendId());
                        if (backend != null) {
                            backend.setLastWriteFail(true);
                        }
                    } else {
                        Replica replica = tabletInvertedIndex.getReplica(failedTablet.getTabletId(),
                                failedTablet.getBackendId());
                        if (replica != null) {
                            replica.setLastWriteFail(true);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn(e);
            } finally {
                db.readUnlock();
            }
        }

        // Optimization for multi-table transaction: avoid sending duplicated requests to BE nodes.
        if (table.getId() != txnState.getTableIdList().get(0)) {
            return;
        }
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task to all backends.
        List<Long> allBeIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (CLEAR_TRANSACTION_TASKS) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(beId, txnState.getTransactionId(), Lists.newArrayList());
                CLEAR_TRANSACTION_TASKS.add(task);
            }
            // try to group send tasks, not sending every time a txn is aborted. to avoid too many task rpc.
            if (CLEAR_TRANSACTION_TASKS.size() > allBeIds.size() * 2) {
                batchTask = new AgentBatchTask();
                for (ClearTransactionTask clearTransactionTask : CLEAR_TRANSACTION_TASKS) {
                    batchTask.addTask(clearTransactionTask);
                }
                CLEAR_TRANSACTION_TASKS.clear();
            }
        }
        if (batchTask != null) {
            AgentTaskExecutor.submit(batchTask);
        }
    }
}
