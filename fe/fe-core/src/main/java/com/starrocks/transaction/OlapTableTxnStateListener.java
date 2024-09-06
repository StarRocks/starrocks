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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
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
    // olap table or olap materialized view
    private final OlapTable table;

    private Set<Long> totalInvolvedBackends;
    private Set<Long> errorReplicaIds;
    private Set<Long> dirtyPartitionSet;
    private Set<ColumnId> invalidDictCacheColumns;
    private Map<ColumnId, Long> validDictCacheColumns;

    public OlapTableTxnStateListener(DatabaseTransactionMgr dbTxnMgr, OlapTable table) {
        this.dbTxnMgr = dbTxnMgr;
        this.table = table;
    }

    @Override
    public String getTableName() {
        return table.getName();
    }

    @Override
    public void preCommit(TransactionState txnState, List<TabletCommitInfo> tabletCommitInfos,
                          List<TabletFailInfo> failedTablets) throws TransactionException {
        Preconditions.checkState(txnState.getTransactionStatus() != TransactionStatus.COMMITTED);
        txnState.clearAutomaticPartitionSnapshot();
        if (table.getState() == OlapTable.OlapTableState.RESTORE) {
            throw new TransactionCommitFailedException("Cannot write RESTORE state table \"" + table.getName() + "\"");
        }
        totalInvolvedBackends = Sets.newHashSet();
        errorReplicaIds = Sets.newHashSet();
        dirtyPartitionSet = Sets.newHashSet();
        invalidDictCacheColumns = Sets.newHashSet();
        validDictCacheColumns = Maps.newHashMap();

        TabletInvertedIndex tabletInvertedIndex = dbTxnMgr.getGlobalStateMgr().getTabletInvertedIndex();
        Map<Long, Set<Long>> tabletToBackends = new HashMap<>();
        Set<Long> allCommittedBackends = new HashSet<>();

        // 1. record tablet commit infos in TransactionState,
        // so we can decide to update version in replica when finish transaction
        if (!tabletCommitInfos.isEmpty()) {
            txnState.setTabletCommitInfos(tabletCommitInfos);
        }

        // 2. validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if index is dropped, it does not matter.
        // if table or partition is dropped during load, just ignore that tablet,
        // because we should allow dropping rollup or partition during load
        List<Long> tabletIds =
                tabletCommitInfos.stream().map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
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
            if (table.getPhysicalPartition(partitionId) == null) {
                // this can happen when partitionId == -1 (tablet being dropping)
                // or partition really not exist.
                LOG.warn("partition {} not exist, ignore tablet {}", partitionId, tabletId);
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
                TabletCommitInfo tabletCommitInfo = tabletCommitInfos.get(i);
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
        }

        // update write failed backend/replica
        // use for selection of primary replica for replicated storage
        for (TabletFailInfo failedTablet : failedTablets) {
            if (failedTablet.getTabletId() == -1) {
                Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .getBackend(failedTablet.getBackendId());
                if (backend != null) {
                    backend.setLastWriteFail(true);
                }
            } else {
                Replica replica =
                        tabletInvertedIndex.getReplica(failedTablet.getTabletId(), failedTablet.getBackendId());
                if (replica != null) {
                    replica.setLastWriteFail(true);
                }
            }
        }

        for (Long committedBackend : allCommittedBackends) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(committedBackend);
            if (backend != null) {
                backend.setLastWriteFail(false);
            }
        }

        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            if (!dirtyPartitionSet.contains(partition.getId())) {
                continue;
            }

            List<MaterializedIndex> allIndices = txnState.getPartitionLoadedTblIndexes(table.getId(), partition);
            int quorumReplicaNum = table.getPartitionInfo().getQuorumNum(partition.getParentId(), table.writeQuorum());
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
                            Backend backend =
                                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(tabletBackend);
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
                                Backend backend =
                                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(tabletBackend);
                                failedReplicaInfoSB.append(
                                        String.format("%d:{be:%d %s st:%s V:%d LFV:%d},", replica.getId(), tabletBackend,
                                                backend == null ? "" : backend.getHost(),
                                                replica.getState(), replica.getVersion(), lfv));
                            }
                            replica.setLastWriteFail(false);
                        } else {
                            Backend backend =
                                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(tabletBackend);
                            failedReplicaInfoSB.append(
                                    String.format("%d:{be:%d %s st:%s V:%d LFV:%d},", replica.getId(), tabletBackend,
                                            backend == null ? "" : backend.getHost(),
                                            replica.getState(), replica.getVersion(), replica.getLastFailedVersion()));
                            // not remove rollup task here, because the commit maybe failed
                            // remove rollup task when commit successfully
                            errorReplicaIds.add(replica.getId());

                            replica.setLastWriteFail(true);
                        }
                    }

                    if (successReplicaNum < quorumReplicaNum) {
                        String msg = String.format(
                                "Commit failed. txn: %d table: %s tablet: %d quorum: %d<%d errorReplicas: %s commitBackends: %s",
                                txnState.getTransactionId(), table.getName(), tablet.getId(), successReplicaNum,
                                quorumReplicaNum,
                                failedReplicaInfoSB,
                                commitBackends == null ? "" : commitBackends.toString());
                        LOG.warn(msg);
                        throw new TabletQuorumFailedException(msg);
                    }
                }
            }
        }
    }

    @Override
    public void preWriteCommitLog(TransactionState txnState) {
        Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.PREPARED);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        boolean isFirstPartition = true;
        txnState.getErrorReplicas().addAll(errorReplicaIds);
        for (long partitionId : dirtyPartitionSet) {
            PartitionCommitInfo partitionCommitInfo;
            if (isFirstPartition) {

                List<ColumnId> validDictCacheColumnNames = Lists.newArrayList();
                List<Long> validDictCacheColumnVersions = Lists.newArrayList();

                validDictCacheColumns.forEach((name, dictVersion) -> {
                    validDictCacheColumnNames.add(name);
                    validDictCacheColumnVersions.add(dictVersion);
                });
                partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        -1,
                        System.currentTimeMillis(),
                        Lists.newArrayList(invalidDictCacheColumns),
                        validDictCacheColumnNames,
                        validDictCacheColumnVersions);
            } else {
                partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        -1,
                        System.currentTimeMillis() /* use as partition visible time */);
            }
            tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            isFirstPartition = false;
        }

        txnState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);

        // add publish version tasks. set task to null as a placeholder.
        // tasks will be created when publishing version.
        for (long backendId : totalInvolvedBackends) {
            txnState.addPublishVersionTask(backendId, null);
        }
    }

    @Override
    public void postAbort(TransactionState txnState, List<TabletCommitInfo> finishedTablets,
                          List<TabletFailInfo> failedTablets) {
        txnState.clearAutomaticPartitionSnapshot();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(txnState.getDbId());
        if (db != null) {
            Locker locker = new Locker();
            locker.lockTablesWithIntensiveDbLock(db.getId(), txnState.getTableIdList(), LockType.READ);
            try {
                TabletInvertedIndex tabletInvertedIndex = dbTxnMgr.getGlobalStateMgr().getTabletInvertedIndex();
                // update write failed backend/replica
                // use for selection of primary replica for replicated storage
                for (TabletFailInfo failedTablet : failedTablets) {
                    LOG.debug("failed tablet ", failedTablet);
                    if (failedTablet.getTabletId() == -1) {
                        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                                .getBackend(failedTablet.getBackendId());
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
                LOG.warn("Fail to execute postAbort", e);
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), txnState.getTableIdList(), LockType.READ);
            }
        }

        // Optimization for multi-table transaction: avoid sending duplicated requests to BE nodes.
        if (table.getId() != txnState.getTableIdList().get(0)) {
            return;
        }
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task to all backends.
        List<Long> allBeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (CLEAR_TRANSACTION_TASKS) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(beId, txnState.getTransactionId(),
                        Lists.newArrayList(), txnState.getTransactionType());
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
