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

package com.starrocks.replication;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.RemoteSnapshotTask;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TIndexReplicationInfo;
import com.starrocks.thrift.TPartitionReplicationInfo;
import com.starrocks.thrift.TReplicaReplicationInfo;
import com.starrocks.thrift.TSnapshotInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableReplicationRequest;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTabletReplicationInfo;
import com.starrocks.thrift.TTabletType;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReplicationJob implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ReplicationJob.class);

    private static class TableInfo {
        private final long tableId;
        private final Table.TableType tableType;
        private final Table.TableType srcTableType;
        private final long tableDataSize;
        private final long srcTableDataSize;
        private final long replicationReplicaCount;
        private final Map<Long, PartitionInfo> partitionInfos;

        public TableInfo(long tableId, TableType tableType, TableType srcTableType, long tableDataSize,
                long srcTableDataSize, Map<Long, PartitionInfo> partitionInfos) {
            this.tableId = tableId;
            this.tableType = tableType;
            this.srcTableType = srcTableType;
            this.tableDataSize = tableDataSize;
            this.srcTableDataSize = srcTableDataSize;
            this.replicationReplicaCount = calcReplicationReplicaCount(partitionInfos);
            this.partitionInfos = partitionInfos;
        }

        public long getTableId() {
            return tableId;
        }

        public Table.TableType getTableType() {
            return tableType;
        }

        public Table.TableType getSrcTableType() {
            return srcTableType;
        }

        public long getTableDataSize() {
            return tableDataSize;
        }

        public long getSrcTableDataSize() {
            return srcTableDataSize;
        }

        public long getReplicationDataSize() {
            return srcTableDataSize - tableDataSize;
        }

        public long getReplicationReplicaCount() {
            return replicationReplicaCount;
        }

        public Map<Long, PartitionInfo> getPartitionInfos() {
            return partitionInfos;
        }

        private static long calcReplicationReplicaCount(Map<Long, PartitionInfo> partitionInfos) {
            long replicationReplicaSize = 0;
            for (PartitionInfo partitionInfo : partitionInfos.values()) {
                for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                    for (TabletInfo tabletInfo : indexInfo.getTabletInfos().values()) {
                        replicationReplicaSize += tabletInfo.getReplicaInfos().size();
                    }
                }
            }
            return replicationReplicaSize;
        }
    }

    private static class PartitionInfo {
        @SerializedName(value = "partitionId")
        private final long partitionId;

        @SerializedName(value = "version")
        private final long version;

        @SerializedName(value = "srcVersion")
        private final long srcVersion;

        @SerializedName(value = "srcVersionEpoch")
        private final long srcVersionEpoch;

        @SerializedName(value = "indexInfos")
        private final Map<Long, IndexInfo> indexInfos;

        public PartitionInfo(long partitionId, long version, long srcVersion, long srcVersionEpoch,
                Map<Long, IndexInfo> indexInfos) {
            this.partitionId = partitionId;
            this.version = version;
            this.srcVersion = srcVersion;
            this.srcVersionEpoch = srcVersionEpoch;
            this.indexInfos = indexInfos;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public long getVersion() {
            return version;
        }

        public long getSrcVersion() {
            return srcVersion;
        }

        public long getSrcVersionEpoch() {
            return srcVersionEpoch;
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

        @SerializedName(value = "srcSchemaHash")
        private final int srcSchemaHash;

        @SerializedName(value = "tabletInfos")
        private final Map<Long, TabletInfo> tabletInfos;

        public IndexInfo(long indexId, int schemaHash, int srcSchemaHash, Map<Long, TabletInfo> tabletInfos) {
            this.indexId = indexId;
            this.schemaHash = schemaHash;
            this.srcSchemaHash = srcSchemaHash;
            this.tabletInfos = tabletInfos;
        }

        public long getIndexId() {
            return indexId;
        }

        public int getSchemaHash() {
            return schemaHash;
        }

        public int getSrcSchemaHash() {
            return srcSchemaHash;
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
        @SerializedName(value = "backendId")
        private final long backendId;

        @SerializedName(value = "srcBackendInfos")
        private final List<BackendInfo> srcBackendInfos;

        @SerializedName(value = "srcSnapshotInfo")
        private volatile SnapshotInfo srcSnapshotInfo;

        public ReplicaInfo(long backendId, List<BackendInfo> srcBackendInfos) {
            this.backendId = backendId;
            this.srcBackendInfos = srcBackendInfos;
        }

        public long getBackendId() {
            return backendId;
        }

        public List<BackendInfo> getSrcBackendInfos() {
            return srcBackendInfos;
        }

        public List<TBackend> getSrcBackends() {
            return srcBackendInfos.stream().map(BackendInfo::getBackend).collect(Collectors.toList());
        }

        public SnapshotInfo getSrcSnapshotInfo() {
            return srcSnapshotInfo;
        }

        public void setSrcSnapshotInfo(SnapshotInfo srcSnapshotInfo) {
            this.srcSnapshotInfo = srcSnapshotInfo;
        }
    }

    private static class BackendInfo {
        @SerializedName(value = "host")
        private final String host;

        @SerializedName(value = "bePort")
        private final int bePort;

        @SerializedName(value = "httpPort")
        private final int httpPort;

        public BackendInfo(String host, int bePort, int httpPort) {
            this.host = host;
            this.bePort = bePort;
            this.httpPort = httpPort;
        }

        public BackendInfo(Backend backend) {
            this.host = backend.getHost();
            this.bePort = backend.getBePort();
            this.httpPort = backend.getHttpPort();
        }

        public BackendInfo(TBackend backend) {
            this.host = backend.host;
            this.bePort = backend.be_port;
            this.httpPort = backend.http_port;
        }

        public TBackend getBackend() {
            return new TBackend(host, bePort, httpPort);
        }
    }

    private static class SnapshotInfo {
        @SerializedName(value = "backendInfo")
        private final BackendInfo backendInfo;

        @SerializedName(value = "snapshotPath")
        private final String snapshotPath;

        @SerializedName(value = "incrementalSnapshot")
        private final boolean incrementalSnapshot;

        public SnapshotInfo(BackendInfo backendInfo, String snapshotPath, boolean incrementalSnapshot) {
            this.backendInfo = backendInfo;
            this.snapshotPath = snapshotPath;
            this.incrementalSnapshot = incrementalSnapshot;
        }

        public SnapshotInfo(TSnapshotInfo snapshotInfo) {
            this.backendInfo = new BackendInfo(snapshotInfo.backend);
            this.snapshotPath = snapshotInfo.snapshot_path;
            this.incrementalSnapshot = snapshotInfo.incremental_snapshot;
        }

        public TSnapshotInfo getSnapshotInfo() {
            TSnapshotInfo tSnapshotInfo = new TSnapshotInfo();
            tSnapshotInfo.setBackend(backendInfo.getBackend());
            tSnapshotInfo.setSnapshot_path(snapshotPath);
            tSnapshotInfo.setIncremental_snapshot(incrementalSnapshot);
            return tSnapshotInfo;
        }
    }

    @SerializedName(value = "jobId")
    private final String jobId;

    @SerializedName(value = "srcToken")
    private final String srcToken;

    @SerializedName(value = "databaseId")
    private final long databaseId;

    @SerializedName(value = "tableId")
    private final long tableId;

    @SerializedName(value = "tableType")
    private final Table.TableType tableType;

    @SerializedName(value = "srcTableType")
    private final Table.TableType srcTableType;

    @SerializedName(value = "replicationDataSize")
    private final long replicationDataSize;

    @SerializedName(value = "replicationReplicaCount")
    private final long replicationReplicaCount;

    @SerializedName(value = "partitionInfos")
    private final Map<Long, PartitionInfo> partitionInfos;

    @SerializedName(value = "transactionId")
    private volatile long transactionId;

    @SerializedName(value = "state")
    private volatile ReplicationJobState state;

    private Map<AgentTask, AgentTask> runningTasks = Maps.newConcurrentMap();
    private volatile int taskNum = 0;
    private Map<AgentTask, AgentTask> finishedTasks = Maps.newConcurrentMap();

    public String getJobId() {
        return jobId;
    }

    public long getDatabaseId() {
        return databaseId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getReplicationDataSize() {
        return replicationDataSize;
    }

    public long getReplicationReplicaCount() {
        return replicationReplicaCount;
    }

    public ReplicationJobState getState() {
        return state;
    }

    private void setState(ReplicationJobState state) {
        this.state = state;
        GlobalStateMgr.getServingState().getEditLog().logReplicationJob(this);
        LOG.info("Replication job state: {}, database id: {}, table id: {}, transaction id: {}",
                state, databaseId, tableId, transactionId);
    }

    public ReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        Preconditions.checkState(request.src_table_type == TTableType.OLAP_TABLE);

        if (Strings.isNullOrEmpty(request.job_id)) {
            this.jobId = UUIDUtil.genUUID().toString();
        } else {
            this.jobId = request.job_id;
        }
        this.srcToken = request.src_token;
        this.databaseId = request.database_id;
        TableInfo tableInfo = initTableInfo(request);
        this.tableId = tableInfo.getTableId();
        this.tableType = tableInfo.getTableType();
        this.srcTableType = tableInfo.getSrcTableType();
        this.replicationDataSize = tableInfo.getReplicationDataSize();
        this.replicationReplicaCount = tableInfo.getReplicationReplicaCount();
        this.partitionInfos = tableInfo.getPartitionInfos();
        this.transactionId = 0;
        this.state = ReplicationJobState.INITIALIZING;

        if (partitionInfos.isEmpty()) {
            throw new RuntimeException("No data need to replicate");
        }
    }

    public ReplicationJob(String jobId, String srcToken, long databaseId, OlapTable table, OlapTable srcTable,
            SystemInfoService srcSystemInfoService) {
        if (Strings.isNullOrEmpty(jobId)) {
            this.jobId = UUIDUtil.genUUID().toString();
        } else {
            this.jobId = jobId;
        }
        this.srcToken = srcToken;
        this.databaseId = databaseId;
        TableInfo tableInfo = initTableInfo(table, srcTable, srcSystemInfoService);
        this.tableId = tableInfo.getTableId();
        this.tableType = tableInfo.getTableType();
        this.srcTableType = tableInfo.getSrcTableType();
        this.replicationDataSize = tableInfo.getReplicationDataSize();
        this.replicationReplicaCount = tableInfo.getReplicationReplicaCount();
        this.partitionInfos = tableInfo.getPartitionInfos();
        this.transactionId = 0;
        this.state = ReplicationJobState.INITIALIZING;

        if (partitionInfos.isEmpty()) {
            throw new RuntimeException("No data need to replicate");
        }
    }

    public void cancel() {
        if (state.equals(ReplicationJobState.COMMITTED) || state.equals(ReplicationJobState.ABORTED)) {
            return;
        }

        if (transactionId != 0) {
            abortTransaction("Replication job cancelled");
        }

        setState(ReplicationJobState.ABORTED);
    }

    public void run() {
        try {
            if (state.equals(ReplicationJobState.INITIALIZING)) {
                beginTransaction();
                sendRemoteSnapshotTasks();
                setState(ReplicationJobState.SNAPSHOTING);
            } else if (state.equals(ReplicationJobState.SNAPSHOTING)) {
                if (isTransactionAborted()) {
                    setState(ReplicationJobState.ABORTED);
                } else if (isCrashRecovery()) {
                    sendRemoteSnapshotTasks();
                    LOG.info("Replication job recovered, state: {}, database id: {}, table id: {}, transaction id: {}",
                            state, databaseId, tableId, transactionId);
                } else if (isAllTaskFinished()) {
                    sendReplicateSnapshotTasks();
                    setState(ReplicationJobState.REPLICATING);
                }
            } else if (state.equals(ReplicationJobState.REPLICATING)) {
                if (isTransactionAborted()) {
                    setState(ReplicationJobState.ABORTED);
                } else if (isCrashRecovery()) {
                    sendReplicateSnapshotTasks();
                    LOG.info("Replication job recovered, state: {}, database id: {}, table id: {}, transaction id: {}",
                            state, databaseId, tableId, transactionId);
                } else if (isAllTaskFinished()) {
                    commitTransaction();
                    setState(ReplicationJobState.COMMITTED);
                }
            }
        } catch (Exception e) {
            LOG.warn("Replication job exception, state: {}, database id: {}, table id: {}, transaction id: {}, ",
                    state, databaseId, tableId, transactionId, e);
            abortTransaction(e.getMessage());
            setState(ReplicationJobState.ABORTED);
        }
    }

    public void finishRemoteSnapshotTask(RemoteSnapshotTask task, TFinishTaskRequest request) {
        if (!runningTasks.remove(task, task)) {
            LOG.warn("Remote snapshot task {} is finished, but cannot find it in running tasks", task);
            return;
        }

        if (request.getTask_status().getStatus_code() == TStatusCode.OK) {
            if (request.isSetSnapshot_info()) {
                PartitionInfo partitionInfo = partitionInfos.get(task.getPartitionId());
                Preconditions.checkNotNull(partitionInfo);
                IndexInfo indexInfo = partitionInfo.getIndexInfos().get(task.getIndexId());
                Preconditions.checkNotNull(indexInfo);
                TabletInfo tabletInfo = indexInfo.getTabletInfos().get(task.getTabletId());
                Preconditions.checkNotNull(tabletInfo);
                ReplicaInfo replicaInfo = tabletInfo.getReplicaInfos().get(task.getBackendId());
                Preconditions.checkNotNull(replicaInfo);

                replicaInfo.setSrcSnapshotInfo(new SnapshotInfo(request.snapshot_info));
                task.setFinished(true);
            } else {
                task.setFailed(true);
                task.setErrorMsg("No snapshot info");
                LOG.warn("Remote snapshot task failed, task: {}, error: {}", task, task.getErrorMsg());
            }
        } else {
            task.setFailed(true);
            task.setErrorMsg(request.getTask_status().getError_msgs().get(0));
            LOG.warn("Remote snapshot task failed, task: {}, error: {}", task, task.getErrorMsg());
        }

        finishedTasks.put(task, task);
    }

    public void finishReplicateSnapshotTask(ReplicateSnapshotTask task, TFinishTaskRequest request) {
        if (!runningTasks.remove(task, task)) {
            LOG.warn("Replicate snapshot task {} is finished, but cannot find it in running tasks", task);
            return;
        }

        if (request.getTask_status().getStatus_code() == TStatusCode.OK) {
            task.setFinished(true);
        } else {
            task.setFailed(true);
            task.setErrorMsg(request.getTask_status().getError_msgs().get(0));
            LOG.warn("Replicate snapshot task failed, task: {}, error: {}", task, task.getErrorMsg());
        }

        finishedTasks.put(task, task);
    }

    private static TableInfo initTableInfo(TTableReplicationRequest request) throws MetaNotFoundException {
        Table.TableType tableType;
        long tableDataSize;
        Map<Long, PartitionInfo> partitionInfos = Maps.newHashMap();

        Database db = GlobalStateMgr.getCurrentState().getDb(request.database_id);
        if (db == null) {
            throw new MetaNotFoundException("Database " + request.database_id + " not found");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
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
            tableType = olapTable.getType();
            tableDataSize = olapTable.getDataSize();

            for (TPartitionReplicationInfo tPartitionInfo : request.partition_replication_infos.values()) {
                PhysicalPartition partition = olapTable.getPhysicalPartition(tPartitionInfo.partition_id);
                if (partition == null) {
                    throw new MetaNotFoundException("Partition " + tPartitionInfo.partition_id + " in table "
                            + table.getName() + " in database " + db.getFullName() + " not found");
                }
                Preconditions.checkState(partition.getCommittedVersion() == partition.getVisibleVersion(),
                        "Partition " + tPartitionInfo.partition_id + " in table " + table.getName()
                                + " in database " + db.getFullName() + " publish version not finished");
                Preconditions.checkState(partition.getVisibleVersion() <= tPartitionInfo.src_version,
                        "Target visible version: " + partition.getVisibleVersion()
                                + " is larger than source visible version: " + tPartitionInfo.src_version);
                if (partition.getVisibleVersion() == tPartitionInfo.src_version) {
                    continue;
                }
                PartitionInfo partitionInfo = initPartitionInfo(olapTable, tPartitionInfo, partition);
                partitionInfos.put(partitionInfo.getPartitionId(), partitionInfo);
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        return new TableInfo(request.table_id, tableType, Table.TableType.OLAP, tableDataSize,
                request.src_table_data_size, partitionInfos);
    }

    private static PartitionInfo initPartitionInfo(OlapTable olapTable, TPartitionReplicationInfo tPartitionInfo,
            PhysicalPartition partition) throws MetaNotFoundException {
        Map<Long, IndexInfo> indexInfos = Maps.newHashMap();
        for (TIndexReplicationInfo tIndexInfo : tPartitionInfo.index_replication_infos.values()) {
            MaterializedIndex index = partition.getIndex(tIndexInfo.index_id);
            if (index == null) {
                throw new MetaNotFoundException("Index " + tIndexInfo.index_id + " in partition " + partition.getId()
                        + " in table " + olapTable.getName() + " not found");
            }
            IndexInfo indexInfo = initIndexInfo(olapTable, tIndexInfo, index);
            indexInfos.put(indexInfo.getIndexId(), indexInfo);
        }
        return new PartitionInfo(tPartitionInfo.partition_id, partition.getVisibleVersion(),
                tPartitionInfo.src_version, tPartitionInfo.src_version_epoch, indexInfos);
    }

    private static IndexInfo initIndexInfo(OlapTable olapTable, TIndexReplicationInfo tIndexInfo,
            MaterializedIndex index)
            throws MetaNotFoundException {
        Map<Long, TabletInfo> tabletInfos = Maps.newHashMap();
        for (TTabletReplicationInfo tTabletInfo : tIndexInfo.tablet_replication_infos.values()) {
            Tablet tablet = index.getTablet(tTabletInfo.tablet_id);
            if (tablet == null) {
                throw new MetaNotFoundException("Tablet " + tTabletInfo.tablet_id + " in index " + tIndexInfo.index_id
                        + " in table " + olapTable.getName() + " not found");
            }
            TabletInfo tabletInfo = initTabletInfo(tTabletInfo, tablet);
            tabletInfos.put(tabletInfo.getTabletId(), tabletInfo);
        }
        int schemaHash = olapTable.getSchemaHashByIndexId(tIndexInfo.index_id);
        return new IndexInfo(tIndexInfo.index_id, schemaHash, tIndexInfo.src_schema_hash, tabletInfos);
    }

    private static TabletInfo initTabletInfo(TTabletReplicationInfo tTabletInfo, Tablet tablet)
            throws MetaNotFoundException {
        Map<Long, ReplicaInfo> replicaInfos = Maps.newHashMap();
        List<Replica> replicas = tablet.getAllReplicas();
        List<TReplicaReplicationInfo> tReplicaInfos = tTabletInfo.replica_replication_infos;

        final int splitSize = tReplicaInfos.size() / replicas.size();
        final int remainSize = tReplicaInfos.size() % replicas.size();
        int offset = 0;
        for (int i = 0; i < replicas.size(); ++i) {
            Replica replica = replicas.get(i);
            int size = i < remainSize ? splitSize + 1 : splitSize;
            List<BackendInfo> backendInfos = Lists.newArrayList();
            for (int j = 0; j < size; ++j) {
                TReplicaReplicationInfo tReplicaInfo = tReplicaInfos.get(offset);
                backendInfos.add(new BackendInfo(tReplicaInfo.src_backend));
                ++offset;
            }
            ReplicaInfo replicaInfo = new ReplicaInfo(replica.getBackendId(), backendInfos);
            replicaInfos.put(replicaInfo.getBackendId(), replicaInfo);
        }
        return new TabletInfo(tTabletInfo.tablet_id, tTabletInfo.src_tablet_id, replicaInfos);
    }

    private static TableInfo initTableInfo(OlapTable table, OlapTable srcTable,
            SystemInfoService srcSystemInfoService) {
        Map<Long, PartitionInfo> partitionInfos = initPartitionInfos(table, srcTable, srcSystemInfoService);
        return new TableInfo(table.getId(), table.getType(), srcTable.getType(), table.getDataSize(),
                srcTable.getDataSize(), partitionInfos);
    }

    private static Map<Long, PartitionInfo> initPartitionInfos(OlapTable table, OlapTable srcTable,
            SystemInfoService srcSystemInfoService) {
        Map<Long, PartitionInfo> partitionInfos = Maps.newHashMap();
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            PhysicalPartition srcPartition = srcTable.getPhysicalPartition(partition.getName());
            Preconditions.checkState(partition.getCommittedVersion() == partition.getVisibleVersion(),
                    "Partition " + partition.getName() + " in table " + table.getName()
                            + " publish version not finished");
            Preconditions.checkState(partition.getVisibleVersion() <= srcPartition.getVisibleVersion(),
                    "Target visible version: " + partition.getVisibleVersion()
                            + " is larger than source visible version: " + srcPartition.getVisibleVersion());
            if (partition.getVisibleVersion() == srcPartition.getVisibleVersion()) {
                continue;
            }
            PartitionInfo partitionInfo = initPartitionInfo(table, srcTable, partition, srcPartition,
                    srcSystemInfoService);
            partitionInfos.put(partitionInfo.getPartitionId(), partitionInfo);
        }
        return partitionInfos;
    }

    private static PartitionInfo initPartitionInfo(OlapTable table, OlapTable srcTable, PhysicalPartition partition,
            PhysicalPartition srcPartition, SystemInfoService srcSystemInfoService) {
        Map<Long, IndexInfo> indexInfos = Maps.newHashMap();
        for (Map.Entry<String, Long> indexNameToId : table.getIndexNameToId().entrySet()) {
            long indexId = indexNameToId.getValue();
            long srcIndexId = srcTable.getIndexIdByName(indexNameToId.getKey());
            MaterializedIndex index = partition.getIndex(indexId);
            MaterializedIndex srcIndex = srcPartition.getIndex(srcIndexId);
            IndexInfo indexInfo = initIndexInfo(table, srcTable, index, srcIndex, srcSystemInfoService);
            indexInfos.put(indexInfo.getIndexId(), indexInfo);
        }
        return new PartitionInfo(partition.getId(), partition.getVisibleVersion(), srcPartition.getVisibleVersion(),
                srcPartition.getVersionEpoch(), indexInfos);
    }

    private static IndexInfo initIndexInfo(OlapTable table, OlapTable srcTable, MaterializedIndex index,
            MaterializedIndex srcIndex,
            SystemInfoService srcSystemInfoService) {
        int schemaHash = table.getSchemaHashByIndexId(index.getId());
        int srcSchemaHash = srcTable.getSchemaHashByIndexId(srcIndex.getId());

        Map<Long, TabletInfo> tabletInfos = Maps.newHashMap();
        List<Tablet> tablets = index.getTablets();
        List<Tablet> srcTablets = srcIndex.getTablets();
        Preconditions.checkState(tablets.size() == srcTablets.size());
        for (int i = 0; i < tablets.size(); ++i) {
            Tablet tablet = tablets.get(i);
            Tablet srcTablet = srcTablets.get(i);
            TabletInfo tabletInfo = initTabletInfo(tablet, srcTablet, srcSystemInfoService);
            tabletInfos.put(tabletInfo.getTabletId(), tabletInfo);
        }
        return new IndexInfo(index.getId(), schemaHash, srcSchemaHash, tabletInfos);
    }

    private static TabletInfo initTabletInfo(Tablet tablet, Tablet srcTablet,
            SystemInfoService srcSystemInfoService) {
        Map<Long, ReplicaInfo> replicaInfos = Maps.newHashMap();
        List<Replica> replicas = tablet.getAllReplicas();
        List<Replica> srcReplicas = srcTablet.getAllReplicas();

        final int splitSize = srcReplicas.size() / replicas.size();
        final int remainSize = srcReplicas.size() % replicas.size();
        int offset = 0;
        for (int i = 0; i < replicas.size(); ++i) {
            Replica replica = replicas.get(i);
            int size = i < remainSize ? splitSize + 1 : splitSize;
            List<BackendInfo> backendInfos = Lists.newArrayList();
            for (int j = 0; j < size; ++j) {
                Replica srcReplica = srcReplicas.get(offset);
                Backend srcBackend = srcSystemInfoService.getBackend(srcReplica.getBackendId());
                backendInfos.add(new BackendInfo(srcBackend));
                ++offset;
            }
            ReplicaInfo replicaInfo = new ReplicaInfo(replica.getBackendId(), backendInfos);
            replicaInfos.put(replicaInfo.getBackendId(), replicaInfo);
        }
        return new TabletInfo(tablet.getId(), srcTablet.getId(), replicaInfos);
    }

    private static TTabletType getTabletType(Table.TableType tableType) {
        return tableType == Table.TableType.CLOUD_NATIVE ? TTabletType.TABLET_TYPE_LAKE
                : TTabletType.TABLET_TYPE_DISK;
    }

    private void beginTransaction()
            throws LabelAlreadyUsedException, DuplicatedRequestException, AnalysisException, RunningTxnExceedException {
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.REPLICATION;
        TransactionState.TxnCoordinator coordinator = TransactionState.TxnCoordinator.fromThisFE();
        String label = String.format("REPLICATION_%d_%d_%s", databaseId, tableId, jobId);
        transactionId = GlobalStateMgr.getServingState().getGlobalTransactionMgr().beginTransaction(databaseId,
                Lists.newArrayList(tableId), label, coordinator, loadJobSourceType,
                Config.replication_transaction_timeout_sec);
    }

    private void commitTransaction() throws UserException {
        Pair<List<TabletCommitInfo>, List<TabletFailInfo>> tabletsCommitInfo = getTabletsCommitInfo();

        Map<Long, Long> partitionVersions = Maps.newHashMap();
        Map<Long, Long> partitionVersionEpochs = Maps.newHashMap();
        for (PartitionInfo partitionInfo : partitionInfos.values()) {
            partitionVersions.put(partitionInfo.getPartitionId(), partitionInfo.getSrcVersion());
            partitionVersionEpochs.put(partitionInfo.getPartitionId(), partitionInfo.getSrcVersionEpoch());
        }
        ReplicationTxnCommitAttachment attachment = new ReplicationTxnCommitAttachment(
                partitionVersions, partitionVersionEpochs);

        GlobalStateMgr.getServingState().getGlobalTransactionMgr().commitTransaction(databaseId,
                transactionId, tabletsCommitInfo.first, tabletsCommitInfo.second, attachment);
    }

    private void abortTransaction(String reason) {
        Pair<List<TabletCommitInfo>, List<TabletFailInfo>> tabletsCommitInfo = getTabletsCommitInfo();

        try {
            GlobalStateMgr.getServingState().getGlobalTransactionMgr().abortTransaction(databaseId, transactionId,
                    reason, tabletsCommitInfo.first, tabletsCommitInfo.second, null);
        } catch (Exception e) {
            LOG.warn("Abort transaction failed, ignore, database id: {}, table id: {}, transaction id: {}, ",
                    databaseId, tableId, transactionId, e);
        }

        removeRunningTasks();
    }

    private Pair<List<TabletCommitInfo>, List<TabletFailInfo>> getTabletsCommitInfo() {
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        List<TabletFailInfo> tabletFailInfos = Lists.newArrayList();
        for (AgentTask task : finishedTasks.values()) {
            if (task.isFailed()) {
                tabletFailInfos.add(new TabletFailInfo(task.getTabletId(), task.getBackendId()));
            } else {
                tabletCommitInfos.add(new TabletCommitInfo(task.getTabletId(), task.getBackendId()));
            }
        }
        return Pair.create(tabletCommitInfos, tabletFailInfos);
    }

    private boolean isTransactionAborted() {
        TransactionState txnState = GlobalStateMgr.getServingState().getGlobalTransactionMgr()
                .getTransactionState(databaseId, transactionId);
        if (txnState == null || txnState.getTransactionStatus() == TransactionStatus.ABORTED) {
            removeRunningTasks();
            return true;
        }

        if (txnState.getTransactionStatus() == TransactionStatus.PREPARE) {
            Database db = GlobalStateMgr.getServingState().getDb(databaseId);
            if (db == null || db.getTable(tableId) == null) {
                abortTransaction("Table is deleted");
                return true;
            }
        }
        return false;
    }

    private void sendRemoteSnapshotTasks() {
        runningTasks.clear();
        for (PartitionInfo partitionInfo : partitionInfos.values()) {
            for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                for (TabletInfo tabletInfo : indexInfo.getTabletInfos().values()) {
                    for (ReplicaInfo replicaInfo : tabletInfo.getReplicaInfos().values()) {
                        if (!replicaInfo.getSrcBackendInfos().isEmpty()) {
                            RemoteSnapshotTask task = new RemoteSnapshotTask(replicaInfo.getBackendId(), databaseId,
                                    tableId, partitionInfo.getPartitionId(), indexInfo.getIndexId(),
                                    tabletInfo.getTabletId(), getTabletType(tableType), transactionId,
                                    indexInfo.getSchemaHash(), partitionInfo.getVersion(),
                                    srcToken, tabletInfo.getSrcTabletId(), getTabletType(srcTableType),
                                    indexInfo.getSrcSchemaHash(), partitionInfo.getSrcVersion(),
                                    replicaInfo.getSrcBackends(),
                                    Config.replication_transaction_timeout_sec);
                            runningTasks.put(task, task);
                        }
                    }
                }
            }
        }

        taskNum = runningTasks.size();

        sendRunningTasks();
    }

    private void sendReplicateSnapshotTasks() throws Exception {
        runningTasks.clear();
        byte[] encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();
        for (PartitionInfo partitionInfo : partitionInfos.values()) {
            for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                for (TabletInfo tabletInfo : indexInfo.getTabletInfos().values()) {
                    List<ReplicaInfo> replicaInfos = Lists.newArrayList(tabletInfo.getReplicaInfos().values());
                    List<TSnapshotInfo> srcSnapshotInfos = Lists.newArrayList();
                    for (ReplicaInfo replicaInfo : replicaInfos) {
                        if (replicaInfo.getSrcSnapshotInfo() != null) {
                            srcSnapshotInfos.add(replicaInfo.getSrcSnapshotInfo().getSnapshotInfo());
                        }
                    }
                    if (srcSnapshotInfos.isEmpty()) {
                        throw new RuntimeException("Source snapshots is empty");
                    }
                    for (int i = 0; i < replicaInfos.size(); ++i) {
                        ReplicaInfo replicaInfo = replicaInfos.get(i);
                        List<TSnapshotInfo> flippedSrcSnapshotInfos = Lists.newArrayList();
                        int offset = i % srcSnapshotInfos.size();
                        flippedSrcSnapshotInfos.addAll(srcSnapshotInfos.subList(offset, srcSnapshotInfos.size()));
                        flippedSrcSnapshotInfos.addAll(srcSnapshotInfos.subList(0, offset));
                        ReplicateSnapshotTask task = new ReplicateSnapshotTask(replicaInfo.getBackendId(), databaseId,
                                tableId, partitionInfo.getPartitionId(), indexInfo.getIndexId(),
                                tabletInfo.getTabletId(), getTabletType(tableType), transactionId,
                                indexInfo.getSchemaHash(), partitionInfo.getVersion(),
                                srcToken, tabletInfo.getSrcTabletId(), getTabletType(srcTableType),
                                indexInfo.getSrcSchemaHash(), partitionInfo.getSrcVersion(),
                                flippedSrcSnapshotInfos, encryptionMeta);
                        runningTasks.put(task, task);
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

    private void removeRunningTasks() {
        for (AgentTask task : runningTasks.values()) {
            AgentTaskQueue.removeTask(task.getBackendId(), task.getTaskType(), task.getSignature());
        }
        runningTasks.clear();
    }

    private boolean isAllTaskFinished() {
        if (runningTasks.isEmpty() && finishedTasks.size() == taskNum) {
            return true;
        }
        LOG.info("Replication job state: {}, finished tasks: {}/{}, database id: {}, table id: {}, transaction id: {}",
                state, finishedTasks.size(), taskNum, databaseId, tableId, transactionId);
        return false;
    }

    private boolean isCrashRecovery() {
        return runningTasks.isEmpty() && finishedTasks.isEmpty();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        runningTasks = Maps.newConcurrentMap();
        finishedTasks = Maps.newConcurrentMap();
    }
}