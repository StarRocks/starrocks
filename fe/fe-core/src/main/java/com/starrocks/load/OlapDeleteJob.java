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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/DeleteJob.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.qe.QueryStateException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.PushTask;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TPriority;
import com.starrocks.thrift.TPushType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TabletCommitInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class OlapDeleteJob extends DeleteJob {
    private static final Logger LOG = LogManager.getLogger(OlapDeleteJob.class);

    private static final int CHECK_INTERVAL = 1000;

    private Set<Long> totalTablets;
    private Set<Long> quorumTablets;
    private Set<Long> finishedTablets;
    Map<Long, TabletDeleteInfo> tabletDeleteInfoMap;
    private Set<PushTask> pushTasks;
    private Map<Long, Short> partitionToReplicateNum;

    public OlapDeleteJob(long id, long transactionId, String label, Map<Long, Short> partitionToReplicateNum,
                         MultiDeleteInfo deleteInfo) {
        super(id, transactionId, label, deleteInfo);
        totalTablets = Sets.newHashSet();
        finishedTablets = Sets.newHashSet();
        quorumTablets = Sets.newHashSet();
        tabletDeleteInfoMap = Maps.newConcurrentMap();
        pushTasks = Sets.newHashSet();
        this.partitionToReplicateNum = partitionToReplicateNum;
    }

    @Override
    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    public void run(DeleteStmt stmt, Database db, Table table, List<Partition> partitions)
            throws DdlException, QueryStateException {
        Preconditions.checkState(table.isOlapTable());
        OlapTable olapTable = (OlapTable) table;
        MarkedCountDownLatch<Long, Long> countDownLatch;
        List<Predicate> conditions = getDeleteConditions();

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            // task sent to be
            AgentBatchTask batchTask = new AgentBatchTask();
            // count total replica num
            int totalReplicaNum = 0;
            for (Partition partition : partitions) {
                for (MaterializedIndex index : partition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        totalReplicaNum += ((LocalTablet) tablet).getImmutableReplicas().size();
                    }
                }
            }

            countDownLatch = new MarkedCountDownLatch<>(totalReplicaNum);

            for (Partition partition : partitions) {
                for (MaterializedIndex index : partition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                    List<TColumn> columnsDesc = new ArrayList<>();
                    for (Column column : olapTable.getSchemaByIndexId(indexId)) {
                        columnsDesc.add(column.toThrift());
                    }

                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();

                        // set push type
                        TPushType type = TPushType.DELETE;

                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            long replicaId = replica.getId();
                            long backendId = replica.getBackendId();
                            countDownLatch.addMark(backendId, tabletId);

                            // create push task for each replica
                            PushTask pushTask = new PushTask(null,
                                    replica.getBackendId(), db.getId(), olapTable.getId(),
                                    partition.getId(), indexId,
                                    tabletId, replicaId, schemaHash,
                                    -1, 0,
                                    -1, type, conditions,
                                    TPriority.NORMAL,
                                    TTaskType.REALTIME_PUSH,
                                    getTransactionId(),
                                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                                            .getNextTransactionId(), columnsDesc);
                            pushTask.setIsSchemaChanging(false);
                            pushTask.setCountDownLatch(countDownLatch);

                            if (AgentTaskQueue.addTask(pushTask)) {
                                batchTask.addTask(pushTask);
                                addPushTask(pushTask);
                                addTablet(tabletId);
                            }
                        }
                    }
                }
            }

            // submit push tasks
            if (batchTask.getTaskNum() > 0) {
                AgentTaskExecutor.submit(batchTask);
            }
        } catch (Throwable t) {
            LOG.warn("error occurred during delete process", t);
            // if transaction has been begun, need to abort it
            if (GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTransactionState(db.getId(), getTransactionId()) != null) {
                cancel(DeleteMgr.CancelType.UNKNOWN, t.getMessage());
            }
            throw new DdlException(t.getMessage(), t);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        LOG.info("countDownLatch count: {}", countDownLatch.getCount());

        long timeoutMs = getTimeoutMs();
        LOG.info("waiting delete Job finish, signature: {}, timeout: {}", getTransactionId(), timeoutMs);
        boolean ok = false;
        try {
            long countDownTime = timeoutMs;
            while (countDownTime > 0) {
                if (countDownTime > CHECK_INTERVAL) {
                    countDownTime -= CHECK_INTERVAL;
                    if (GlobalStateMgr.getCurrentState().getDeleteMgr().removeKillJob(getId())) {
                        cancel(DeleteMgr.CancelType.USER, "user cancelled");
                        throw new DdlException("Cancelled");
                    }
                    ok = countDownLatch.await(CHECK_INTERVAL, TimeUnit.MILLISECONDS);
                    if (ok) {
                        break;
                    }
                } else {
                    ok = countDownLatch.await(countDownTime, TimeUnit.MILLISECONDS);
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
        }
        LOG.info("delete job finish, countDownLatch count: {}", countDownLatch.getCount());

        String errMsg = "";
        List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
        Status st = countDownLatch.getStatus();
        // only show at most 5 results
        List<Map.Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 5));
        if (!subList.isEmpty()) {
            errMsg = "unfinished replicas: " + Joiner.on(", ").join(subList);
        } else if (!st.ok()) {
            errMsg = st.toString();
        }
        LOG.warn(errMsg);

        try {
            checkAndUpdateQuorum();
        } catch (MetaNotFoundException e) {
            cancel(DeleteMgr.CancelType.METADATA_MISSING, e.getMessage());
            throw new DdlException(e.getMessage(), e);
        }
        DeleteState state = getState();
        switch (state) {
            case DELETING:
                LOG.warn("delete job failed: transactionId {}, timeout {}, {}", getTransactionId(), timeoutMs,
                        errMsg);
                if (countDownLatch.getCount() > 0) {
                    cancel(DeleteMgr.CancelType.TIMEOUT, "delete job timeout");
                } else {
                    cancel(DeleteMgr.CancelType.UNKNOWN, "delete job failed");
                }
                throw new DdlException("failed to execute delete. transaction id " + getTransactionId() +
                        ", timeout(ms) " + timeoutMs + ", " + errMsg);
            case QUORUM_FINISHED:
            case FINISHED:
                try {
                    long nowQuorumTimeMs = System.currentTimeMillis();
                    long endQuorumTimeoutMs = nowQuorumTimeMs + timeoutMs / 2;
                    // if job's state is quorum_finished then wait for a period of time and commit it.
                    while (getState() == DeleteState.QUORUM_FINISHED && endQuorumTimeoutMs > nowQuorumTimeMs
                            && countDownLatch.getCount() > 0) {
                        checkAndUpdateQuorum();
                        Thread.sleep(1000);
                        nowQuorumTimeMs = System.currentTimeMillis();
                        LOG.debug("wait for quorum finished delete job: {}, txn_id: {}", getId(),
                                getTransactionId());
                    }
                } catch (MetaNotFoundException e) {
                    cancel(DeleteMgr.CancelType.METADATA_MISSING, e.getMessage());
                    throw new DdlException(e.getMessage(), e);
                } catch (InterruptedException e) {
                    cancel(DeleteMgr.CancelType.UNKNOWN, e.getMessage());
                    throw new DdlException(e.getMessage(), e);
                }
                commit(db, timeoutMs);
                break;
            default:
                throw new IllegalStateException("wrong delete job state: " + state.name());
        }

    }

    /**
     * check and update if this job's state is QUORUM_FINISHED or FINISHED
     * The meaning of state:
     * QUORUM_FINISHED: For each tablet there are more than half of its replicas have been finished
     * FINISHED: All replicas of this jobs have finished
     */
    public void checkAndUpdateQuorum() throws MetaNotFoundException {
        long dbId = deleteInfo.getDbId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("can not find database " + dbId + " when commit delete");
        }

        for (TabletDeleteInfo tDeleteInfo : getTabletDeleteInfo()) {
            Short replicaNum = partitionToReplicateNum.get(tDeleteInfo.getPartitionId());
            if (replicaNum == null) {
                // should not happen
                throw new MetaNotFoundException("Unknown partition " + tDeleteInfo.getPartitionId() +
                        " when commit delete job");
            }
            if (tDeleteInfo.getFinishedReplicas().size() == replicaNum) {
                finishedTablets.add(tDeleteInfo.getTabletId());
            }
            if (tDeleteInfo.getFinishedReplicas().size() >= replicaNum / 2 + 1) {
                quorumTablets.add(tDeleteInfo.getTabletId());
            }
        }
        LOG.info("check delete job quorum, txn_id: {}, total tablets: {}, quorum tablets: {},",
                signature, totalTablets.size(), quorumTablets.size());

        if (finishedTablets.containsAll(totalTablets)) {
            setState(DeleteState.FINISHED);
        } else if (quorumTablets.containsAll(totalTablets)) {
            setState(DeleteState.QUORUM_FINISHED);
        }
    }

    public boolean addTablet(long tabletId) {
        return totalTablets.add(tabletId);
    }

    public boolean addPushTask(PushTask pushTask) {
        return pushTasks.add(pushTask);
    }

    public Set<PushTask> getPushTasks() {
        return pushTasks;
    }

    public boolean addFinishedReplica(long partitionId, long tabletId, Replica replica) {
        tabletDeleteInfoMap.putIfAbsent(tabletId, new TabletDeleteInfo(partitionId, tabletId));
        TabletDeleteInfo tDeleteInfo = tabletDeleteInfoMap.get(tabletId);
        return tDeleteInfo.addFinishedReplica(replica);
    }

    public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
        return tabletDeleteInfoMap.values();
    }

    @Override
    public long getTimeoutMs() {
        if (FeConstants.runningUnitTest) {
            // for making unit test run fast
            return 1000;
        }
        // timeout is between 30 seconds to 5 min
        long timeout = Math.max(totalTablets.size() * Config.tablet_delete_timeout_second * 1000L, 30000L);
        return Math.min(timeout, Config.load_straggler_wait_second * 1000L);
    }

    @Override
    public void clear() {
        for (PushTask pushTask : getPushTasks()) {
            AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                    pushTask.getVersion(),
                    pushTask.getPushType(), pushTask.getTaskType());
        }
    }

    @Override
    public boolean cancel(DeleteMgr.CancelType cancelType, String reason) {
        if (!super.cancel(cancelType, reason)) {
            return false;
        }

        // create cancel delete push task for each backends
        List<Long> backendIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
        for (Long backendId : backendIds) {
            PushTask cancelDeleteTask = new PushTask(backendId, TPushType.CANCEL_DELETE, TPriority.HIGH,
                    TTaskType.REALTIME_PUSH, getTransactionId(),
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                            .getNextTransactionId());
            AgentTaskQueue.removePushTaskByTransactionId(backendId, getTransactionId(),
                    TPushType.DELETE, TTaskType.REALTIME_PUSH);
            AgentTaskExecutor.submit(new AgentBatchTask(cancelDeleteTask));
        }
        return true;
    }

    @Override
    public boolean commitImpl(Database db, long timeoutMs) throws UserException {
        long transactionId = getTransactionId();
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (TabletDeleteInfo tDeleteInfo : getTabletDeleteInfo()) {
            for (Replica replica : tDeleteInfo.getFinishedReplicas()) {
                // the inverted index contains rolling up replica
                Long tabletId = invertedIndex.getTabletIdByReplica(replica.getId());
                if (tabletId == null) {
                    LOG.warn("could not find tablet id for replica {}, the tablet maybe dropped", replica);
                    continue;
                }
                tabletCommitInfos.add(new TabletCommitInfo(tabletId, replica.getBackendId()));
            }
        }
        return globalTransactionMgr.commitAndPublishTransaction(db, transactionId, tabletCommitInfos,
                Lists.newArrayList(), timeoutMs,
                new InsertTxnCommitAttachment());
    }
}