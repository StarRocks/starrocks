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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/BackendLoadStatistic.java

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

package com.starrocks.replication;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.ReplicationTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TIndexReplicationInfo;
import com.starrocks.thrift.TPartitionReplicationInfo;
import com.starrocks.thrift.TReplicaReplicationInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableReplicationRequest;
import com.starrocks.thrift.TTabletReplicationInfo;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicationJob implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ReplicationJob.class);

    private static class PartitionInfo {
        @SerializedName(value = "partitionId")
        private final long partitionId;

        @SerializedName(value = "srcVersion")
        private final long srcVersion;

        @SerializedName(value = "indexInfos")
        private final Map<Long, IndexInfo> indexInfos;

        public PartitionInfo(long partitionId, long srcVersion, Map<Long, IndexInfo> indexInfos) {
            this.partitionId = partitionId;
            this.srcVersion = srcVersion;
            this.indexInfos = indexInfos;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public long getSrcVersion() {
            return srcVersion;
        }

        public Map<Long, IndexInfo> getIndexInfos() {
            return indexInfos;
        }
    }

    private static class IndexInfo {
        @SerializedName(value = "indexId")
        private final long indexId;

        @SerializedName(value = "schemaHash")
        private final int schemaHash;

        @SerializedName(value = "tabletInfos")
        private final Map<Long, TabletInfo> tabletInfos;

        public IndexInfo(long indexId, int schemaHash, Map<Long, TabletInfo> tabletInfos) {
            this.indexId = indexId;
            this.schemaHash = schemaHash;
            this.tabletInfos = tabletInfos;
        }

        public long getIndexId() {
            return indexId;
        }

        public int getSchemaHash() {
            return schemaHash;
        }

        public Map<Long, TabletInfo> getTabletInfos() {
            return tabletInfos;
        }
    }

    private static class TabletInfo {
        @SerializedName(value = "tabletId")
        private final long tabletId;

        @SerializedName(value = "srcTabletId")
        private final long srcTabletId;

        @SerializedName(value = "replicaInfos")
        private final Map<Long, ReplicaInfo> replicaInfos;

        public TabletInfo(long tabletId, long srcTabletId, Map<Long, ReplicaInfo> replicaInfos) {
            this.tabletId = tabletId;
            this.srcTabletId = srcTabletId;
            this.replicaInfos = replicaInfos;
        }

        public long getTabletId() {
            return tabletId;
        }

        public long getSrcTabletId() {
            return srcTabletId;
        }

        public Map<Long, ReplicaInfo> getReplicaInfos() {
            return replicaInfos;
        }
    }

    private static class ReplicaInfo {
        @SerializedName(value = "replicaId")
        private final long replicaId;

        @SerializedName(value = "backendId")
        private final long backendId;

        @SerializedName(value = "backendHost")
        private final String backendHost;
        @SerializedName(value = "backendBePort")
        private final int backendBePort;
        @SerializedName(value = "backendHttpPort")
        private final int backendHttpPort;

        public ReplicaInfo(long replicaId, long backendId, TBackend srcBackend) {
            this.replicaId = replicaId;
            this.backendId = backendId;
            this.backendHost = srcBackend.host;
            this.backendBePort = srcBackend.be_port;
            this.backendHttpPort = srcBackend.http_port;
        }

        public long getReplicaId() {
            return replicaId;
        }

        public long getBackendId() {
            return backendId;
        }

        public TBackend getSrcBackend() {
            return new TBackend(backendHost, backendBePort, backendHttpPort);
        }
    }

    @SerializedName(value = "srcToken")
    private final String srcToken;

    @SerializedName(value = "databaseId")
    private final long databaseId;

    @SerializedName(value = "tableId")
    private final long tableId;

    @SerializedName(value = "partitionInfos")
    private final Map<Long, PartitionInfo> partitionInfos;

    @SerializedName(value = "transactionId")
    private volatile long transactionId;

    @SerializedName(value = "state")
    private volatile ReplicationJobState state;

    private Map<Long, AgentTask> runningTasks = Maps.newConcurrentMap();
    private volatile int taskNum = 0;
    private Map<Long, AgentTask> finishedTasks = Maps.newConcurrentMap();

    public long getDatabaseId() {
        return databaseId;
    }

    public long getTableId() {
        return tableId;
    }

    public ReplicationJobState getState() {
        return state;
    }

    public ReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        this.srcToken = request.src_token;
        this.databaseId = request.database_id;
        this.tableId = request.table_id;
        this.partitionInfos = initPartitionInfos(request);
        this.transactionId = 0;
        this.state = ReplicationJobState.INITIALIZING;
    }

    public void run() {
        try {
            if (state.equals(ReplicationJobState.INITIALIZING)) {
                beginTransaction();
                sendReplicationTasks();
                state = ReplicationJobState.REPLICATING;
                GlobalStateMgr.getServingState().getEditLog().logReplicationJob(this);
                LOG.info("Replication job state: {}, database id: {}, table id: {}, transaction id: {}", state,
                        databaseId, tableId, transactionId);
            } else if (state.equals(ReplicationJobState.REPLICATING)) {
                if (isTransactionFailed()) {
                    state = ReplicationJobState.ABORTED;
                    GlobalStateMgr.getServingState().getEditLog().logReplicationJob(this);
                    LOG.warn("Replication job replicate timeout, database id: {}, table id: {}, transaction id: {}, ",
                            databaseId, tableId, transactionId);
                } else if (isAllTaskFinished()) {
                    commitTransaction();
                    finishTransaction();
                    state = ReplicationJobState.FINISHED;
                    GlobalStateMgr.getServingState().getEditLog().logReplicationJob(this);
                    LOG.info("Replication job state: {}, database id: {}, table id: {}, transaction id: {}", state,
                            databaseId, tableId, transactionId);
                }
            }
        } catch (Exception e) {
            state = ReplicationJobState.ABORTED;
            abortTransaction(e.getMessage());
            GlobalStateMgr.getServingState().getEditLog().logReplicationJob(this);
            LOG.warn("Replication job run failed, abort, database id: {}, table id: {}, transaction id: {}, ",
                    databaseId, tableId, transactionId, e);
        }
    }

    public void finishReplicationTask(ReplicationTask task, TFinishTaskRequest request) {
        if (!runningTasks.remove(task.getSignature(), task)) {
            LOG.warn("Replicate snapshot task {} is finished, but cannot find it in running tasks", task);
            return;
        }

        if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
            task.setFailed(true);
            task.setErrorMsg(request.getTask_status().getError_msgs().get(0));
            LOG.warn("Replication task failed: {}", task.getErrorMsg());
        }

        finishedTasks.put(task.getSignature(), task);
    }

    private Map<Long, PartitionInfo> initPartitionInfos(TTableReplicationRequest request) throws MetaNotFoundException {
        if (request.partition_replication_infos == null || request.partition_replication_infos.isEmpty()) {
            throw new RuntimeException("Partition replication infos is null or empty");
        }

        Map<Long, PartitionInfo> partitionInfos = Maps.newHashMap();

        Database db = GlobalStateMgr.getCurrentState().getDb(request.database_id);
        if (db == null) {
            throw new MetaNotFoundException("Database " + request.database_id + " not found");
        }

        db.readLock();
        try {
            Table table = db.getTable(request.table_id);
            if (table == null) {
                throw new MetaNotFoundException(
                        "Table " + request.table_id + " in database " + db.getFullName() + " not found");
            }
            if (!(table instanceof OlapTable)) {
                throw new MetaNotFoundException(
                        "Table " + request.table_id + " in database " + db.getFullName() + " is not olap table");
            }
            OlapTable olapTable = (OlapTable) table;
            for (TPartitionReplicationInfo tPartitionInfo : request.partition_replication_infos.values()) {
                Partition partition = olapTable.getPartition(tPartitionInfo.partition_id);
                if (partition == null) {
                    throw new MetaNotFoundException("Partition " + tPartitionInfo.partition_id + " in table "
                            + table.getName() + " in database " + db.getFullName() + " not found");
                }
                PartitionInfo partitionInfo = initPartitionInfo(olapTable, tPartitionInfo, partition);
                partitionInfos.put(partitionInfo.getPartitionId(), partitionInfo);
            }
        } finally {
            db.readUnlock();
        }
        return partitionInfos;
    }

    private PartitionInfo initPartitionInfo(OlapTable olapTable, TPartitionReplicationInfo tPartitionInfo,
            Partition partition) throws MetaNotFoundException {
        Map<Long, IndexInfo> indexInfos = Maps.newHashMap();
        for (TIndexReplicationInfo tIndexInfo : tPartitionInfo.index_replication_infos.values()) {
            MaterializedIndex index = partition.getIndex(tIndexInfo.index_id);
            if (index == null) {
                throw new MetaNotFoundException("Index " + tIndexInfo.index_id + " in partition " + partition.getName()
                        + " in table " + olapTable.getName() + " not found");
            }
            IndexInfo indexInfo = initIndexInfo(olapTable, tIndexInfo, index);
            indexInfos.put(indexInfo.getIndexId(), indexInfo);
        }
        return new PartitionInfo(tPartitionInfo.partition_id, tPartitionInfo.src_version, indexInfos);
    }

    private IndexInfo initIndexInfo(OlapTable olapTable, TIndexReplicationInfo tIndexInfo, MaterializedIndex index)
            throws MetaNotFoundException {
        Map<Long, TabletInfo> tabletInfos = Maps.newHashMap();
        for (TTabletReplicationInfo tTabletInfo : tIndexInfo.tablet_replication_infos.values()) {
            Tablet tablet = index.getTablet(tTabletInfo.tablet_id);
            if (tablet == null) {
                throw new MetaNotFoundException("Tablet " + tTabletInfo.tablet_id + " in index " + tIndexInfo.index_id
                        + " in table " + olapTable.getName() + " not found");
            }
            if (!(tablet instanceof LocalTablet)) {
                throw new MetaNotFoundException("Tablet " + tTabletInfo.tablet_id + " in index " + tIndexInfo.index_id
                        + " in table " + olapTable.getName() + " is not local tablet");
            }
            TabletInfo tabletInfo = initTabletInfo(tTabletInfo, (LocalTablet) tablet);
            tabletInfos.put(tabletInfo.getTabletId(), tabletInfo);
        }
        int schemaHash = olapTable.getSchemaHashByIndexId(tIndexInfo.index_id);
        return new IndexInfo(tIndexInfo.index_id, schemaHash, tabletInfos);
    }

    private TabletInfo initTabletInfo(TTabletReplicationInfo tTabletInfo, LocalTablet localTablet)
            throws MetaNotFoundException {
        Map<Long, ReplicaInfo> replicaInfos = Maps.newHashMap();
        for (TReplicaReplicationInfo tReplicaInfo : tTabletInfo.replica_replication_infos.values()) {
            Replica replica = localTablet.getReplicaById(tReplicaInfo.replica_id);
            if (replica == null) {
                throw new MetaNotFoundException(
                        "Replica " + tReplicaInfo.replica_id + " in tablet " + tTabletInfo.tablet_id + " not found");
            }
            ReplicaInfo replicaInfo = new ReplicaInfo(tReplicaInfo.replica_id, replica.getBackendId(),
                    tReplicaInfo.src_backend);
            replicaInfos.put(replicaInfo.getReplicaId(), replicaInfo);
        }
        return new TabletInfo(tTabletInfo.tablet_id, tTabletInfo.src_tablet_id, replicaInfos);
    }

    private void beginTransaction()
            throws LabelAlreadyUsedException, DuplicatedRequestException, AnalysisException, BeginTransactionException {
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.REPLICATION;
        TransactionState.TxnCoordinator coordinator = TransactionState.TxnCoordinator.fromThisFE();
        String label = String.format("REPLICATION_%d-%d-%d", databaseId, tableId, System.currentTimeMillis());
        transactionId = GlobalStateMgr.getServingState().getGlobalTransactionMgr().beginTransaction(databaseId,
                Lists.newArrayList(tableId), label, coordinator, loadJobSourceType,
                Config.replication_transaction_timeout_sec);
    }

    private void commitTransaction() throws UserException {
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        List<TabletFailInfo> tabletFailInfos = Lists.newArrayList();
        for (AgentTask task : finishedTasks.values()) {
            if (task.isFailed()) {
                tabletFailInfos.add(new TabletFailInfo(task.getTabletId(), task.getBackendId()));
            } else {
                tabletCommitInfos.add(new TabletCommitInfo(task.getTabletId(), task.getBackendId()));
            }
        }

        Map<Long, Long> partitionVersions = Maps.newHashMap();
        for (PartitionInfo partitionInfo : partitionInfos.values()) {
            partitionVersions.put(partitionInfo.getPartitionId(), partitionInfo.getSrcVersion());
        }
        ReplicationTxnCommitAttachment attachment = new ReplicationTxnCommitAttachment(partitionVersions);

        GlobalStateMgr.getServingState().getGlobalTransactionMgr().commitTransaction(databaseId,
                transactionId, tabletCommitInfos, tabletFailInfos, attachment);
    }

    private void finishTransaction() throws UserException {
        Set<Long> errorReplicas = Sets.newHashSet();
        for (AgentTask task : finishedTasks.values()) {
            ReplicationTask replicationTask = (ReplicationTask) task;
            if (task.isFailed()) {
                errorReplicas.add(replicationTask.getReplicaId());
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getServingState().getGlobalTransactionMgr();
        globalTransactionMgr.finishTransaction(databaseId, transactionId, errorReplicas);
        TransactionStatus status = globalTransactionMgr.getTransactionState(databaseId, transactionId)
                .getTransactionStatus();
        if (status != TransactionStatus.VISIBLE) {
            throw new RuntimeException("Invalid transaction status: " + status);
        }
    }

    private void abortTransaction(String reason) {
        try {
            GlobalStateMgr.getServingState().getGlobalTransactionMgr().abortTransaction(databaseId, transactionId,
                    reason);
        } catch (Exception e) {
            LOG.warn("Replication job abort transaction failed, ignore, database id: {}, table id: {}, ", databaseId,
                    tableId, e);
        }
    }

    private boolean isTransactionFailed() {
        return GlobalStateMgr.getServingState().getGlobalTransactionMgr().getTransactionState(databaseId, transactionId)
                .getTransactionStatus() == TransactionStatus.ABORTED;
    }

    private void sendReplicationTasks() {
        runningTasks.clear();
        for (PartitionInfo partitionInfo : partitionInfos.values()) {
            for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                for (TabletInfo tabletInfo : indexInfo.getTabletInfos().values()) {
                    for (ReplicaInfo replicaInfo : tabletInfo.getReplicaInfos().values()) {
                        ReplicationTask task = new ReplicationTask(replicaInfo.getBackendId(), databaseId,
                                tableId, partitionInfo.getPartitionId(), indexInfo.getIndexId(),
                                tabletInfo.getTabletId(), replicaInfo.getReplicaId(),
                                indexInfo.getSchemaHash(), partitionInfo.getSrcVersion(), srcToken,
                                tabletInfo.getSrcTabletId(), Lists.newArrayList(replicaInfo.getSrcBackend()),
                                Config.replication_transaction_timeout_sec);
                        runningTasks.put(task.getSignature(), task);
                    }
                }
            }
        }

        taskNum = runningTasks.size();

        sendRunningTasks();
    }

    private void sendRunningTasks() {
        if (runningTasks.isEmpty()) {
            throw new RuntimeException("Running tasks is empty");
        }

        finishedTasks.clear();

        AgentBatchTask batchTask = new AgentBatchTask();
        for (AgentTask task : runningTasks.values()) {
            batchTask.addTask(task);
            AgentTaskQueue.addTask(task);
        }

        AgentTaskExecutor.submit(batchTask);
    }

    private boolean isAllTaskFinished() {
        return runningTasks.isEmpty() && finishedTasks.size() == taskNum;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        runningTasks = Maps.newConcurrentMap();
        finishedTasks = Maps.newConcurrentMap();
    }
}
