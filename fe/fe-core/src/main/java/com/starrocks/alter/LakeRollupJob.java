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

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class LakeRollupJob extends LakeTableSchemaChangeJobBase {
    private static final Logger LOG = LogManager.getLogger(LakeRollupJob.class);

    @SerializedName(value = "commitVersionMap")
    // Mapping from partition id to commit version
    private Map<Long, Long> commitVersionMap;

    //------------------
    @SerializedName(value = "partitionIdToBaseRollupTabletIdMap")
    protected Map<Long, Map<Long, Long>> physicalPartitionIdToBaseRollupTabletIdMap = Maps.newHashMap();
    @SerializedName(value = "partitionIdToRollupIndex")
    protected Map<Long, MaterializedIndex> physicalPartitionIdToRollupIndex = Maps.newHashMap();

    // rollup and base schema info
    @SerializedName(value = "baseIndexId")
    protected long baseIndexId;
    @SerializedName(value = "rollupIndexId")
    protected long rollupIndexId;
    @SerializedName(value = "baseIndexName")
    protected String baseIndexName;
    @SerializedName(value = "rollupIndexName")
    protected String rollupIndexName;

    @SerializedName(value = "rollupSchema")
    protected List<Column> rollupSchema = Lists.newArrayList();
    @SerializedName(value = "rollupSchemaVersion")
    protected int rollupSchemaVersion;
    @SerializedName(value = "baseSchemaHash")
    protected int baseSchemaHash;
    @SerializedName(value = "rollupSchemaHash")
    protected int rollupSchemaHash;

    @SerializedName(value = "rollupKeysType")
    protected KeysType rollupKeysType;
    @SerializedName(value = "rollupShortKeyColumnCount")
    protected short rollupShortKeyColumnCount;
    @SerializedName(value = "origStmt")
    protected OriginStatement origStmt;

    @SerializedName(value = "viewDefineSql")
    protected String viewDefineSql;
    @SerializedName(value = "isColocateMVIndex")
    protected boolean isColocateMVIndex = false;

    protected Expr whereClause;

    // save all create rollup tasks
    protected AgentBatchTask rollupBatchTask = new AgentBatchTask();

    public LakeRollupJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                         long baseIndexId, long rollupIndexId, String baseIndexName, String rollupIndexName,
                         int rollupSchemaVersion, List<Column> rollupSchema, Expr whereClause, int baseSchemaHash,
                         int rollupSchemaHash, KeysType rollupKeysType, short rollupShortKeyColumnCount,
                         OriginStatement origStmt, String viewDefineSql, boolean isColocateMVIndex) {
        super(jobId, JobType.ROLLUP, dbId, tableId, tableName, timeoutMs);
        this.baseIndexId = baseIndexId;
        this.rollupIndexId = rollupIndexId;
        this.baseIndexName = baseIndexName;
        this.rollupIndexName = rollupIndexName;
        this.rollupSchemaVersion = rollupSchemaVersion;
        this.rollupSchema = rollupSchema;
        this.whereClause = whereClause;
        this.baseSchemaHash = baseSchemaHash;
        this.rollupSchemaHash = rollupSchemaHash;
        this.rollupKeysType = rollupKeysType;
        this.rollupShortKeyColumnCount = rollupShortKeyColumnCount;
        this.origStmt = origStmt;
        this.viewDefineSql = viewDefineSql;
        this.isColocateMVIndex = isColocateMVIndex;
    }

    // for deserialization
    public LakeRollupJob() {
        super(JobType.ROLLUP);
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        boolean enableTabletCreationOptimization = Config.lake_enable_tablet_creation_optimization;
        long numTablets = 0;
        AgentBatchTask batchTask = new AgentBatchTask();
        MarkedCountDownLatch<Long, Long> countDownLatch;
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.ROLLUP);

            if (enableTabletCreationOptimization) {
                numTablets = physicalPartitionIdToRollupIndex.size();
            } else {
                numTablets = physicalPartitionIdToRollupIndex.values().stream().map(MaterializedIndex::getTablets)
                        .mapToLong(List::size).sum();
            }
            countDownLatch = new MarkedCountDownLatch<>((int) numTablets);

            long gtid = getNextGtid();
            for (Map.Entry<Long, MaterializedIndex> entry : this.physicalPartitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = table.getPartitionInfo()
                        .getDataProperty(partition.getParentId()).getStorageMedium();
                MaterializedIndex rollupIndex = entry.getValue();

                TTabletSchema tabletSchema = SchemaInfo.newBuilder()
                        .setId(rollupIndexId) // For newly created materialized, schema id equals to index id
                        .setVersion(rollupSchemaVersion)
                        .setKeysType(rollupKeysType)
                        .setShortKeyColumnCount(rollupShortKeyColumnCount)
                        .setSchemaHash(rollupSchemaHash)
                        .setStorageType(TStorageType.COLUMN)
                        .setBloomFilterColumnNames(table.getBfColumnIds())
                        .setBloomFilterFpp(table.getBfFpp())
                        .setIndexes(table.getCopiedIndexes())
                        .setSortKeyIndexes(null) // Rollup tablets does not have sort key
                        .setSortKeyUniqueIds(null)
                        .addColumns(rollupSchema)
                        .build().toTabletSchema();

                boolean createSchemaFile = true;
                Map<Long, Long> tabletIdMap = this.physicalPartitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                            .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) rollupTablet);
                    if (computeNode == null) {
                        //todo: fix the error message.
                        throw new AlterCancelException("No alive backend");
                    }
                    countDownLatch.addMark(computeNode.getId(), rollupTabletId);

                    CreateReplicaTask task = CreateReplicaTask.newBuilder()
                            .setNodeId(computeNode.getId())
                            .setDbId(dbId)
                            .setTableId(tableId)
                            .setPartitionId(partitionId)
                            .setIndexId(rollupIndexId)
                            .setTabletId(rollupTabletId)
                            .setVersion(Partition.PARTITION_INIT_VERSION)
                            .setStorageMedium(storageMedium)
                            .setLatch(countDownLatch)
                            .setEnablePersistentIndex(table.enablePersistentIndex())
                            .setPrimaryIndexCacheExpireSec(table.primaryIndexCacheExpireSec())
                            .setTabletType(TTabletType.TABLET_TYPE_LAKE)
                            .setCompressionType(table.getCompressionType())
                            .setCreateSchemaFile(createSchemaFile)
                            .setTabletSchema(tabletSchema)
                            .setEnableTabletCreationOptimization(enableTabletCreationOptimization)
                            .setGtid(gtid)
                            .build();

                    // For each partition, the schema file is created only when the first Tablet is created
                    createSchemaFile = false;
                    batchTask.addTask(task);

                    if (enableTabletCreationOptimization) {
                        break;
                    }
                } // end for rollupTablets
            }
        }

        sendAgentTaskAndWait(batchTask, countDownLatch, Config.tablet_create_timeout_second * numTablets);

        // Add shadow indexes to table.
        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            if (table.getState() != OlapTable.OlapTableState.ROLLUP) {
                throw new IllegalStateException("Table State doesn't equal to ROLLUP, it is " + table.getState() + ".");
            }
            watershedTxnId = getNextTransactionId();
            watershedGtid = getNextGtid();
            addRollIndexToCatalog(table);
        }

        // Getting the `watershedTxnId` and adding the shadow index are not atomic. It's possible a
        // transaction A begins between these operations. This is safe as long as A gets the tablet
        // list(with database lock) after beginTransaction(), so that it sees the shadow index and
        // writes to it. All current import transactions do this (beginTransaction first), so even
        // without checking the `nextTxnId` here it should be safe. However, beginTransaction() first
        // is just a convention not a requirement. If violated, transactions with IDs greater than
        // the `watershedTxnId` may ignore the shadow index. To avoid this, we ensure no new
        // beginTransaction() succeeds between getting the `watershedTxnId` and adding the shadow index.
        long nextTxnId = peekNextTransactionId();
        if (nextTxnId != watershedTxnId + 1) {
            throw new AlterCancelException(
                    "concurrent transaction detected while adding shadow index, please re-run the alter table command");
        }

        jobState = JobState.WAITING_TXN;
        if (span != null) {
            span.setAttribute("watershedTxnId", this.watershedTxnId);
            span.addEvent("setWaitingTxn");
        }

        writeEditLog(this);

        LOG.info("transfer roll up job {} state to {}, watershed txn_id: {}", jobId, this.jobState,
                watershedTxnId);
    }

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished(dbId, tableId, watershedTxnId)) {
                LOG.info("wait transactions before {} to be finished, rollup job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send rollup tasks. job: {}", jobId);

        Map<Long, List<TColumn>> indexToThriftColumns = new HashMap<>();
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable tbl = getTableOrThrow(db, tableId);
            Preconditions.checkState(tbl.getState() == OlapTable.OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.physicalPartitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the rollup task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();

                MaterializedIndex rollupIndex = entry.getValue();
                Map<Long, Long> tabletIdMap = this.physicalPartitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    long baseTabletId = tabletIdMap.get(rollupTabletId);
                    AlterReplicaTask.RollupJobV2Params rollupJobV2Params =
                            RollupJobV2.analyzeAndCreateRollupJobV2Params(tbl, rollupSchema, whereClause, db.getFullName());

                    ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                            .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) rollupTablet);
                    if (computeNode == null) {
                        //todo: fix the error message.
                        throw new AlterCancelException("No alive compute node");
                    }

                    TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                    long baseIndexId = invertedIndex.getTabletMeta(baseTabletId).getIndexId();
                    List<TColumn> baseTColumn = indexToThriftColumns.get(baseIndexId);
                    if (baseTColumn == null) {
                        baseTColumn = tbl.getIndexMetaByIndexId(baseIndexId).getSchema()
                                .stream()
                                .map(Column::toThrift)
                                .collect(Collectors.toList());
                        indexToThriftColumns.put(baseIndexId, baseTColumn);
                    }
                    AlterReplicaTask rollupTask = AlterReplicaTask.rollupLakeTablet(
                            computeNode.getId(), dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                            baseTabletId, visibleVersion, jobId,
                            rollupJobV2Params, baseTColumn, watershedTxnId);
                    rollupBatchTask.addTask(rollupTask);
                }
                partition.setMinRetainVersion(visibleVersion);
            }
        }

        sendAgentTask(rollupBatchTask);
        this.jobState = JobState.RUNNING;
        span.addEvent("setRunning");

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer rollup job {} state to {}", jobId, this.jobState);
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        if (tableHasBeenDropped()) {
            throw new AlterCancelException("Table or database does not exist");
        }

        if (!rollupBatchTask.isFinished()) {
            LOG.info("rollup tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = rollupBatchTask.getUnfinishedTasks(2000);
            AgentTask task = tasks.stream().filter(t -> (t.isFailed() || t.getFailedTimes() >= 3)).findAny().orElse(null);
            if (task != null) {
                throw new AlterCancelException(
                        "rollup task failed after try three times: " + task.getErrorMsg());
            } else {
                return;
            }
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            commitVersionMap = new HashMap<>();
            for (long physicalPartitionId : physicalPartitionIdToRollupIndex.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);
                physicalPartition.setMinRetainVersion(0);
                long commitVersion = physicalPartition.getNextVersion();
                commitVersionMap.put(physicalPartitionId, commitVersion);
                LOG.debug("commit version of partition {} is {}. jobId={}", physicalPartitionId, commitVersion, jobId);
            }
            this.jobState = JobState.FINISHED_REWRITING;
            this.finishedTimeMs = System.currentTimeMillis();

            writeEditLog(this);

            // NOTE: !!! below this point, this roll up job must success unless the database or table been dropped. !!!
            updateNextVersion(table);
        }

        if (span != null) {
            span.addEvent("finishedRewriting");
        }
        LOG.info("lake rollup job finished rewriting historical data: {}", jobId);
    }

    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.FINISHED_REWRITING);
        // If the table or database has been dropped, `readyToPublishVersion()` will throw AlterCancelException and
        // this schema change job will be cancelled.
        if (!readyToPublishVersion()) {
            return;
        }

        if (!publishVersion()) {
            LOG.info("publish version failed, will retry later. jobId={}", jobId);
            return;
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                LOG.info("database or table been dropped while doing schema change job {}", jobId);
                return;
            }

            visualiseRollupIndex(table);

            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            // There is no need to set the table state to normal,
            // because it will be set in MaterializedViewHandler `onJobDone`
        }

        writeEditLog(this);
        if (span != null) {
            span.end();
        }
        LOG.info("roll up job finished: {}", jobId);
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState == JobState.CANCELLED || jobState == JobState.FINISHED) {
            return false;
        }

        // Cancel a job of state `FINISHED_REWRITING` only when the database or table has been dropped.
        if (jobState == JobState.FINISHED_REWRITING && tableExists()) {
            return false;
        }

        if (rollupBatchTask != null) {
            AgentTaskQueue.removeBatchTask(rollupBatchTask, TTaskType.ALTER);
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = (db != null) ? db.getTable(tableId) : null;
            if (table != null) {
                removeRollupIndex(table);
            }
        }

        this.jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        if (span != null) {
            span.setStatus(StatusCode.ERROR, errMsg);
            span.end();
        }

        writeEditLog(this);
        LOG.info("Lake Rollup job canceled, jobId: {}, error: {}", jobId, errMsg);

        return true;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        info.add(baseIndexName);
        info.add(rollupIndexName);
        info.add(rollupIndexId);
        info.add(watershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        // progress
        if (jobState == JobState.RUNNING && rollupBatchTask.getTaskNum() > 0) {
            info.add(rollupBatchTask.getFinishedTaskNum() + "/" + rollupBatchTask.getTaskNum());
        } else {
            info.add(FeConstants.NULL_STRING);
        }
        info.add(timeoutMs / 1000);
        infos.add(info);
    }

    @Override
    public void replay(AlterJobV2 lakeRollupJob) {
        LakeRollupJob other = (LakeRollupJob) lakeRollupJob;

        LOG.info("Replaying lake table rollup job. state={} jobId={}", lakeRollupJob.jobState, lakeRollupJob.jobId);

        if (this != other) {
            Preconditions.checkState(this.type.equals(other.type));
            Preconditions.checkState(this.jobId == other.jobId);
            Preconditions.checkState(this.dbId == other.dbId);
            Preconditions.checkState(this.tableId == other.tableId);

            this.jobState = other.jobState;
            this.createTimeMs = other.createTimeMs;
            this.finishedTimeMs = other.finishedTimeMs;
            this.errMsg = other.errMsg;
            this.timeoutMs = other.timeoutMs;

            this.watershedTxnId = other.watershedTxnId;
            this.watershedGtid = other.watershedGtid;
            this.commitVersionMap = other.commitVersionMap;

            this.physicalPartitionIdToBaseRollupTabletIdMap = other.physicalPartitionIdToBaseRollupTabletIdMap;
            this.physicalPartitionIdToRollupIndex = other.physicalPartitionIdToRollupIndex;
            this.baseIndexId = other.baseIndexId;
            this.rollupIndexId = other.rollupIndexId;
            this.baseIndexName = other.baseIndexName;
            this.rollupIndexName = other.rollupIndexName;
            this.rollupSchema = other.rollupSchema;
            this.rollupSchemaVersion = other.rollupSchemaVersion;
            this.baseSchemaHash = other.baseSchemaHash;
            this.rollupSchemaHash = other.rollupSchemaHash;
            this.rollupKeysType = other.rollupKeysType;
            this.rollupShortKeyColumnCount = other.rollupShortKeyColumnCount;
            this.viewDefineSql = other.viewDefineSql;
            this.isColocateMVIndex = other.isColocateMVIndex;
            this.rollupBatchTask = new AgentBatchTask();
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                return; // do nothing if the table has been dropped.
            }

            if (jobState == JobState.PENDING) {
                addTabletToInvertedIndex(table);
                table.setState(OlapTable.OlapTableState.ROLLUP);
            } else if (jobState == JobState.WAITING_TXN) {
                addRollIndexToCatalog(table);
            } else if (jobState == JobState.RUNNING) {
                    // do nothing
            } else if (jobState == JobState.FINISHED_REWRITING) {
                updateNextVersion(table);
            } else if (jobState == JobState.FINISHED) {
                visualiseRollupIndex(table);
            } else if (jobState == JobState.CANCELLED) {
                removeRollupIndex(table);
            } else {
                throw new RuntimeException("unknown job state '{}'" + jobState.name());
            }
        }
    }

    @VisibleForTesting
    public static void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                            long timeoutSeconds) throws AlterCancelException {
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
        long timeout = 1000L * Math.min(timeoutSeconds, Config.max_create_table_timeout_second);
        boolean ok = false;
        try {
            ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS) && countDownLatch.getStatus().ok();
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
        }

        if (!ok) {
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
            String errMsg;
            if (!countDownLatch.getStatus().ok()) {
                errMsg = countDownLatch.getStatus().getErrorMsg();
            } else {
                // only show at most 3 results
                List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Map.Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                errMsg = "Error tablets:" + Joiner.on(", ").join(subList);
            }
            throw new AlterCancelException("Create tablet failed. Error: " + errMsg);
        }
    }

    @VisibleForTesting
    public static void writeEditLog(LakeRollupJob job) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(job);
    }

    @VisibleForTesting
    public static long getNextTransactionId() {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
    }

    @VisibleForTesting
    public static long peekNextTransactionId() {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().peekNextTransactionId();
    }

    public static long getNextGtid() {
        return GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid();
    }

    void addRollIndexToCatalog(@NotNull LakeTable tbl) {
        for (Partition partition : tbl.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long partitionId = physicalPartition.getId();
                MaterializedIndex rollupIndex = this.physicalPartitionIdToRollupIndex.get(partitionId);
                Preconditions.checkNotNull(rollupIndex);
                Preconditions.checkState(rollupIndex.getState() == MaterializedIndex.IndexState.SHADOW, rollupIndex.getState());
                physicalPartition.createRollupIndex(rollupIndex);
            }
        }

        tbl.setIndexMeta(rollupIndexId, rollupIndexName, rollupSchema, rollupSchemaVersion /* initial schema version */,
                rollupSchemaHash, rollupShortKeyColumnCount, TStorageType.COLUMN, rollupKeysType, origStmt);
        MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(rollupIndexId);
        Preconditions.checkNotNull(indexMeta);
        indexMeta.setDbId(dbId);
        indexMeta.setViewDefineSql(viewDefineSql);
        indexMeta.setColocateMVIndex(isColocateMVIndex);
        indexMeta.setWhereClause(whereClause);
        tbl.rebuildFullSchema();
    }

    boolean tableHasBeenDropped() {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            return db == null || db.getTable(tableId) == null;
        }
    }

    void updateNextVersion(@NotNull LakeTable table) {
        for (long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Preconditions.checkState(partition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + partition.getNextVersion() + " commitVersion=" + commitVersion);
            partition.setNextVersion(commitVersion + 1);
        }
    }

    boolean readyToPublishVersion() throws AlterCancelException {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            for (long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                Preconditions.checkState(partition != null, partitionId);
                long commitVersion = commitVersionMap.get(partitionId);
                if (commitVersion != partition.getVisibleVersion() + 1) {
                    Preconditions.checkState(partition.getVisibleVersion() < commitVersion,
                            "partition=" + partitionId + " visibleVersion=" + partition.getVisibleVersion() +
                                    " commitVersion=" + commitVersion);
                    return false;
                }
            }
        }
        return true;
    }

    private boolean publishVersion() {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            for (long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
                Preconditions.checkState(physicalPartition != null, partitionId);
                List<MaterializedIndex> allMaterializedIndex = physicalPartition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
                List<Tablet> allOtherPartitionTablets = new ArrayList<>();
                for (MaterializedIndex index : allMaterializedIndex) {
                    allOtherPartitionTablets.addAll(index.getTablets());
                }
                long commitVersion = commitVersionMap.get(partitionId);

                TxnInfoPB rollUpTxnInfo = new TxnInfoPB();
                rollUpTxnInfo.txnId = watershedTxnId;
                rollUpTxnInfo.combinedTxnLog = false;
                rollUpTxnInfo.commitTime = finishedTimeMs / 1000;
                rollUpTxnInfo.txnType = TxnTypePB.TXN_NORMAL;
                rollUpTxnInfo.gtid = watershedGtid;
                // publish rollup tablets
                Utils.publishVersion(physicalPartitionIdToRollupIndex.get(partitionId).getTablets(), rollUpTxnInfo,
                        1, commitVersion, warehouseId);

                TxnInfoPB originTxnInfo = new TxnInfoPB();
                originTxnInfo.txnId = -1L;
                originTxnInfo.combinedTxnLog = false;
                originTxnInfo.commitTime = finishedTimeMs / 1000;
                originTxnInfo.txnType = TxnTypePB.TXN_EMPTY;
                originTxnInfo.gtid = watershedGtid;
                // publish origin tablets
                Utils.publishVersion(allOtherPartitionTablets, originTxnInfo, commitVersion - 1,
                        commitVersion, warehouseId);

            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to publish version for schema change job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

    void removeRollupIndex(@NotNull LakeTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
            MaterializedIndex rollupIndex = physicalPartitionIdToRollupIndex.get(partitionId);
            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                invertedIndex.deleteTablet(rollupTablet.getId());
            }
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            partition.deleteRollupIndex(rollupIndexId);
            partition.setMinRetainVersion(0);
        }
        table.deleteIndexInfo(rollupIndexName);
    }

    public void visualiseRollupIndex(OlapTable table) {
        for (Partition partition : table.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                Preconditions.checkState(commitVersionMap.containsKey(physicalPartition.getId()));
                long commitVersion = commitVersionMap.get(physicalPartition.getId());
                LOG.debug("update partition visible version. partition=" + physicalPartition.getId() + " commitVersion=" +
                        commitVersion);
                // Update Partition's visible version
                Preconditions.checkState(commitVersion == physicalPartition.getVisibleVersion() + 1,
                        commitVersion + " vs " + physicalPartition.getVisibleVersion());
                physicalPartition.setVisibleVersion(commitVersion, finishedTimeMs);
                LOG.debug("update visible version of partition {} to {}. jobId={}", physicalPartition.getId(),
                        commitVersion, jobId);
                MaterializedIndex rollupIndex = physicalPartition.getIndex(rollupIndexId);
                Preconditions.checkNotNull(rollupIndex, rollupIndexId);
                physicalPartition.visualiseShadowIndex(rollupIndexId, false);
            }
        }
        table.rebuildFullSchema();
        table.lastSchemaUpdateTime.set(System.nanoTime());
    }

    private void addTabletToInvertedIndex(OlapTable tbl) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        // add all rollup replicas to tablet inverted index
        for (Long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
            MaterializedIndex rollupIndex = physicalPartitionIdToRollupIndex.get(partitionId);
            PhysicalPartition physicalPartition = tbl.getPhysicalPartition(partitionId);
            TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(physicalPartition.getParentId()).getStorageMedium();
            TabletMeta rollupTabletMeta = new TabletMeta(dbId, tableId, partitionId, rollupIndexId,
                    rollupSchemaHash, medium);

            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                invertedIndex.addTablet(rollupTablet.getId(), rollupTabletMeta);
            }
        }
    }

    @Override
    public void addTabletIdMap(long partitionId, long rollupTabletId, long baseTabletId) {
        Map<Long, Long> tabletIdMap =
                physicalPartitionIdToBaseRollupTabletIdMap.computeIfAbsent(partitionId, k -> Maps.newHashMap());
        tabletIdMap.put(rollupTabletId, baseTabletId);
    }

    @Override
    public void addMVIndex(long partitionId, MaterializedIndex mvIndex) {
        this.physicalPartitionIdToRollupIndex.put(partitionId, mvIndex);
    }
}
