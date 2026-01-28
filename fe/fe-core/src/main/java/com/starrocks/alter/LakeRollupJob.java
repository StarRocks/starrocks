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
import com.starrocks.catalog.Column;
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
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.lake.Utils;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
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
    // base index meta id, not change the SerializedName for compatibility
    @SerializedName(value = "baseIndexId")
    protected long baseIndexMetaId;
    // initially, rollup index id and rollup index meta id are the same
    // rollup index meta id, not change the SerializedName for compatibility
    @SerializedName(value = "rollupIndexId")
    protected long rollupIndexMetaId;
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
    protected OriginStatementInfo origStmt;

    @SerializedName(value = "viewDefineSql")
    protected String viewDefineSql;
    @SerializedName(value = "isColocateMVIndex")
    protected boolean isColocateMVIndex = false;

    protected Expr whereClause;

    // save all create rollup tasks
    protected AgentBatchTask rollupBatchTask = new AgentBatchTask();

    public LakeRollupJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                         long baseIndexMetaId, long rollupIndexMetaId, String baseIndexName, String rollupIndexName,
                         int rollupSchemaVersion, List<Column> rollupSchema, Expr whereClause, int baseSchemaHash,
                         int rollupSchemaHash, KeysType rollupKeysType, short rollupShortKeyColumnCount,
                         OriginStatement origStmt, String viewDefineSql, boolean isColocateMVIndex) {
        super(jobId, JobType.ROLLUP, dbId, tableId, tableName, timeoutMs);
        this.baseIndexMetaId = baseIndexMetaId;
        this.rollupIndexMetaId = rollupIndexMetaId;
        this.baseIndexName = baseIndexName;
        this.rollupIndexName = rollupIndexName;
        this.rollupSchemaVersion = rollupSchemaVersion;
        this.rollupSchema = rollupSchema;
        this.whereClause = whereClause;
        this.baseSchemaHash = baseSchemaHash;
        this.rollupSchemaHash = rollupSchemaHash;
        this.rollupKeysType = rollupKeysType;
        this.rollupShortKeyColumnCount = rollupShortKeyColumnCount;
        if (origStmt != null) {
            this.origStmt = new OriginStatementInfo(origStmt.getOrigStmt(), origStmt.getIdx());
        }
        this.viewDefineSql = viewDefineSql;
        this.isColocateMVIndex = isColocateMVIndex;
    }

    protected LakeRollupJob(LakeRollupJob job) {
        super(job);
        if (job.commitVersionMap != null) {
            this.commitVersionMap = Maps.newHashMap();
            this.commitVersionMap.putAll(job.commitVersionMap);
        } else {
            this.commitVersionMap = null;
        }
        if (job.physicalPartitionIdToBaseRollupTabletIdMap != null) {
            this.physicalPartitionIdToBaseRollupTabletIdMap = Maps.newHashMap();
            for (Map.Entry<Long, Map<Long, Long>> entry : job.physicalPartitionIdToBaseRollupTabletIdMap.entrySet()) {
                Map<Long, Long> tabletIdMap = Maps.newHashMap();
                if (entry.getValue() != null) {
                    tabletIdMap.putAll(entry.getValue());
                }
                this.physicalPartitionIdToBaseRollupTabletIdMap.put(entry.getKey(), tabletIdMap);
            }
        } else {
            this.physicalPartitionIdToBaseRollupTabletIdMap = null;
        }
        if (job.physicalPartitionIdToRollupIndex != null) {
            this.physicalPartitionIdToRollupIndex = Maps.newHashMap();
            this.physicalPartitionIdToRollupIndex.putAll(job.physicalPartitionIdToRollupIndex);
        } else {
            this.physicalPartitionIdToRollupIndex = null;
        }
        this.baseIndexMetaId = job.baseIndexMetaId;
        this.rollupIndexMetaId = job.rollupIndexMetaId;
        this.baseIndexName = job.baseIndexName;
        this.rollupIndexName = job.rollupIndexName;
        this.rollupSchema = job.rollupSchema == null ? null : new ArrayList<>(job.rollupSchema);
        this.rollupSchemaVersion = job.rollupSchemaVersion;
        this.baseSchemaHash = job.baseSchemaHash;
        this.rollupSchemaHash = job.rollupSchemaHash;
        this.rollupKeysType = job.rollupKeysType;
        this.rollupShortKeyColumnCount = job.rollupShortKeyColumnCount;
        this.origStmt = job.origStmt;
        this.viewDefineSql = job.viewDefineSql;
        this.isColocateMVIndex = job.isColocateMVIndex;
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
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.ROLLUP);

            // disable tablet creation optimaization to avoid overwriting files with the same name.
            if (table.isFileBundling()) {
                enableTabletCreationOptimization = false;
            }
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
                        .setId(rollupIndexMetaId) // For newly created materialized, schema id equals to index meta id
                        .setVersion(rollupSchemaVersion)
                        .setKeysType(rollupKeysType)
                        .setShortKeyColumnCount(rollupShortKeyColumnCount)
                        .setSchemaHash(rollupSchemaHash)
                        .setStorageType(TStorageType.COLUMN)
                        .setBloomFilterColumnNames(table.getBfColumnIds())
                        .setBloomFilterFpp(table.getBfFpp())
                        .setIndexes(OlapTable.getIndexesBySchema(table.getCopiedIndexes(), rollupSchema))
                        .setSortKeyIndexes(null) // Rollup tablets does not have sort key
                        .setSortKeyUniqueIds(null)
                        .addColumns(rollupSchema)
                        .build().toTabletSchema();

                boolean createSchemaFile = true;

                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    ComputeNode computeNode = null;
                    try {
                        computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, rollupTabletId);
                    } catch (ErrorReportException e) {
                        // computeNode is null
                    }
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
                            .setIndexId(rollupIndexMetaId)
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
                            .setCompactionStrategy(table.getCompactionStrategy())
                            .setRange(table.isRangeDistribution() ? rollupTablet.getRange() : null)
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
            OlapTable table = getTableOrThrow(db, tableId);
            if (table.getState() != OlapTable.OlapTableState.ROLLUP) {
                throw new IllegalStateException("Table State doesn't equal to ROLLUP, it is " + table.getState() + ".");
            }
            watershedTxnId = getNextTransactionId();
            watershedGtid = getNextGtid();
            addRollupIndexToCatalog(table);
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

        if (span != null) {
            span.setAttribute("watershedTxnId", this.watershedTxnId);
            span.addEvent("setWaitingTxn");
        }

        // can't add addRollIndexToCatalog into the applier, because of the nextTxnId check.
        // But addRollIndexToCatalog is idempotent, so it's ok to re-add if Leader transferred.
        persistStateChange(this, JobState.WAITING_TXN);

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

        // initially, rollup index id and rollup index meta id are the same
        long rollupIndexId = rollupIndexMetaId;
        Map<Long, TTabletSchema> indexToBaseTabletReadSchema = new HashMap<>();
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable tbl = getTableOrThrow(db, tableId);
            Preconditions.checkState(tbl.getState() == OlapTable.OlapTableState.ROLLUP);
            // only needs to analyze once
            AlterReplicaTask.RollupJobV2Params rollupJobV2Params =
                    RollupJobV2.analyzeAndCreateRollupJobV2Params(tbl, rollupSchema, whereClause, db.getFullName());
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

                    ComputeNode computeNode = null;
                    try {
                        computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource, rollupTabletId);
                    } catch (ErrorReportException e) {
                        // computeNode is null
                    }
                    if (computeNode == null) {
                        //todo: fix the error message.
                        throw new AlterCancelException("No alive compute node");
                    }
                    TTabletSchema baseTabletReadSchema = indexToBaseTabletReadSchema.get(baseIndexMetaId);
                    if (baseTabletReadSchema == null) {
                        baseTabletReadSchema = SchemaInfo.fromMaterializedIndex(
                                tbl, baseIndexMetaId, tbl.getIndexMetaByMetaId(baseIndexMetaId)).toTabletSchema();
                        indexToBaseTabletReadSchema.put(baseIndexMetaId, baseTabletReadSchema);
                    }
                    AlterReplicaTask rollupTask = AlterReplicaTask.rollupLakeTablet(
                            computeNode.getId(), dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                            baseTabletId, visibleVersion, jobId, rollupJobV2Params, baseTabletReadSchema, watershedTxnId);
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
            OlapTable table = getTableOrThrow(db, tableId);
            commitVersionMap = new HashMap<>();
            for (long physicalPartitionId : physicalPartitionIdToRollupIndex.keySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);
                physicalPartition.setMinRetainVersion(0);
                long commitVersion = physicalPartition.getNextVersion();
                commitVersionMap.put(physicalPartitionId, commitVersion);
                LOG.debug("commit version of partition {} is {}. jobId={}", physicalPartitionId, commitVersion, jobId);
            }
            this.finishedTimeMs = System.currentTimeMillis();

            persistStateChange(this, JobState.FINISHED_REWRITING, () -> {
                // NOTE: !!! below this point, this roll up job must success unless the database or table been dropped. !!!
                updateNextVersion(table);
            });
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
            return;
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                LOG.info("database or table been dropped while doing schema change job {}", jobId);
                return;
            }

            this.finishedTimeMs = System.currentTimeMillis();
            // There is no need to set the table state to normal,
            // because it will be set in MaterializedViewHandler `onJobDone`

            persistStateChange(this, JobState.FINISHED, () -> visualiseRollupIndex(table));
        }

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

        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        persistStateChange(this, JobState.CANCELLED, () -> {
            try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
                OlapTable table = (db != null) ? db.getTable(tableId) : null;
                if (table != null) {
                    removeRollupIndex(table);
                }
            }
        });

        if (span != null) {
            span.setStatus(StatusCode.ERROR, errMsg);
            span.end();
        }
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
        info.add(rollupIndexMetaId);
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
            this.baseIndexMetaId = other.baseIndexMetaId;
            this.rollupIndexMetaId = other.rollupIndexMetaId;
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
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                return; // do nothing if the table has been dropped.
            }

            if (jobState == JobState.PENDING) {
                addTabletToInvertedIndex(table);
                table.setState(OlapTable.OlapTableState.ROLLUP);
            } else if (jobState == JobState.WAITING_TXN) {
                addRollupIndexToCatalog(table);
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

    void addRollupIndexToCatalog(@NotNull OlapTable tbl) {
        for (Partition partition : tbl.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long partitionId = physicalPartition.getId();
                MaterializedIndex rollupIndex = this.physicalPartitionIdToRollupIndex.get(partitionId);
                Preconditions.checkNotNull(rollupIndex);
                Preconditions.checkState(rollupIndex.getState() == MaterializedIndex.IndexState.SHADOW, rollupIndex.getState());
                physicalPartition.createRollupIndex(rollupIndex);
            }
        }

        // If upgraded from an old version and do roll up,
        // the schema saved in indexSchemaMap is the schema in the old version, whose uniqueId is -1,
        // so here we initialize column uniqueId here.
        boolean restored = LakeTableHelper.restoreColumnUniqueId(rollupSchema);
        if (restored) {
            LOG.info("Columns of rollup index {} in table {} has reset all unique ids, column size: {}", rollupIndexMetaId,
                    tableName, rollupSchema.size());
        }

        tbl.setIndexMeta(rollupIndexMetaId, rollupIndexName, rollupSchema, rollupSchemaVersion /* initial schema version */,
                rollupSchemaHash, rollupShortKeyColumnCount, TStorageType.COLUMN, rollupKeysType, origStmt);
        MaterializedIndexMeta indexMeta = tbl.getIndexMetaByMetaId(rollupIndexMetaId);
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

    void updateNextVersion(@NotNull OlapTable table) {
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
            OlapTable table = getTableOrThrow(db, tableId);
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

    protected boolean lakePublishVersion() {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            boolean useAggregatePublish = table.isFileBundling();
            for (long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
                AggregatePublishVersionRequest request = new AggregatePublishVersionRequest();
                PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
                Preconditions.checkState(physicalPartition != null, partitionId);
                List<MaterializedIndex> allMaterializedIndex = physicalPartition
                        .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
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
                if (!useAggregatePublish) {
                    // publish rollup tablets
                    Utils.publishVersion(physicalPartitionIdToRollupIndex.get(partitionId).getTablets(), rollUpTxnInfo,
                            1, commitVersion, computeResource, false);
                } else {
                    Utils.createSubRequestForAggregatePublish(physicalPartitionIdToRollupIndex.get(partitionId).getTablets(), 
                            Lists.newArrayList(rollUpTxnInfo), 1, commitVersion, null, computeResource, request);
                }

                TxnInfoPB originTxnInfo = new TxnInfoPB();
                originTxnInfo.txnId = -1L;
                originTxnInfo.combinedTxnLog = false;
                originTxnInfo.commitTime = finishedTimeMs / 1000;
                originTxnInfo.txnType = TxnTypePB.TXN_EMPTY;
                originTxnInfo.gtid = watershedGtid;
                if (!useAggregatePublish) {
                    // publish origin tablets
                    Utils.publishVersion(allOtherPartitionTablets, originTxnInfo, commitVersion - 1,
                            commitVersion, computeResource, false);
                } else {
                    Utils.createSubRequestForAggregatePublish(allOtherPartitionTablets, Lists.newArrayList(originTxnInfo), 
                            commitVersion - 1, commitVersion, null, computeResource, request);
                }

                if (useAggregatePublish) {
                    Utils.sendAggregatePublishVersionRequest(request, 1, computeResource, null, null);
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to publish version for schema change job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

    void removeRollupIndex(@NotNull OlapTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Long partitionId : physicalPartitionIdToRollupIndex.keySet()) {
            MaterializedIndex rollupIndex = physicalPartitionIdToRollupIndex.get(partitionId);
            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                invertedIndex.deleteTablet(rollupTablet.getId());
            }
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            partition.deleteMaterializedIndexByMetaId(rollupIndexMetaId);
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
                MaterializedIndex rollupIndex = physicalPartition.getLatestIndex(rollupIndexMetaId);
                Preconditions.checkNotNull(rollupIndex, rollupIndexMetaId);
                physicalPartition.visualiseShadowIndex(rollupIndex.getId(), false);
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
            // initially, rollup index id and rollup index meta id are the same
            TabletMeta rollupTabletMeta = new TabletMeta(dbId, tableId, partitionId, rollupIndexMetaId,
                    medium);

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
    public AlterJobV2 copyForPersist() {
        return new LakeRollupJob(this);
    }

    public String getRollupIndexName() {
        return rollupIndexName;
    }

    @Override
    public void addMVIndex(long partitionId, MaterializedIndex mvIndex) {
        this.physicalPartitionIdToRollupIndex.put(partitionId, mvIndex);
    }
}
