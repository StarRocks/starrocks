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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Status;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.journal.JournalTask;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarMgrMetaSyncer;
import com.starrocks.lake.Utils;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.warehouse.Warehouse;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class LakeTableSchemaChangeJob extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(LakeTableSchemaChangeJob.class);

    // physical partition id -> (shadow index id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    private Table<Long, Long, Map<Long, Long>> physicalPartitionIndexTabletMap = HashBasedTable.create();
    // physical partition id -> (shadow index id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap = HashBasedTable.create();
    // shadow index id -> origin index id
    @SerializedName(value = "indexIdMap")
    private Map<Long, Long> indexIdMap = Maps.newHashMap();
    // shadow index id -> shadow index name(__starrocks_shadow_xxx)
    @SerializedName(value = "indexIdToName")
    private Map<Long, String> indexIdToName = Maps.newHashMap();
    // shadow index id -> index schema
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, List<Column>> indexSchemaMap = Maps.newHashMap();
    // shadow index id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    private Map<Long, Short> indexShortKeyMap = Maps.newHashMap();

    // bloom filter info
    @SerializedName(value = "hasBfChange")
    private boolean hasBfChange;
    @SerializedName(value = "bfColumns")
    private Set<ColumnId> bfColumns = null;
    @SerializedName(value = "bfFpp")
    private double bfFpp = 0;

    // alter index info
    @SerializedName(value = "indexChange")
    private boolean indexChange = false;
    @SerializedName(value = "indexes")
    private List<Index> indexes = null;

    // The schema change job will wait all transactions before this txn id finished, then send the schema change tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "startTime")
    private long startTime;

    @SerializedName(value = "commitVersionMap")
    // Mapping from partition id to commit version
    private Map<Long, Long> commitVersionMap;

    @SerializedName(value = "sortKeyIdxes")
    private List<Integer> sortKeyIdxes;
    @SerializedName(value = "sortKeyUniqueIds")
    private List<Integer> sortKeyUniqueIds;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();

    // runtime variable for synchronization between cancel and runPendingJob
    private MarkedCountDownLatch<Long, Long> createReplicaLatch = null;
    private AtomicBoolean waitingCreatingReplica = new AtomicBoolean(false);
    private AtomicBoolean isCancelling = new AtomicBoolean(false);

    // for deserialization
    public LakeTableSchemaChangeJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeTableSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    void setBloomFilterInfo(boolean hasBfChange, Set<ColumnId> bfColumns, double bfFpp) {
        this.hasBfChange = hasBfChange;
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    void setAlterIndexInfo(boolean indexChange, List<Index> indexes) {
        this.indexChange = indexChange;
        this.indexes = indexes;
    }

    void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    void setSortKeyIdxes(List<Integer> sortKeyIdxes) {
        this.sortKeyIdxes = sortKeyIdxes;
    }

    public void setSortKeyUniqueIds(List<Integer> sortKeyUniqueIds) {
        this.sortKeyUniqueIds = sortKeyUniqueIds;
    }

    void addTabletIdMap(long partitionId, long shadowIdxId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = physicalPartitionIndexTabletMap.get(partitionId, shadowIdxId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            physicalPartitionIndexTabletMap.put(partitionId, shadowIdxId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    void addPartitionShadowIndex(long partitionId, long shadowIdxId, MaterializedIndex shadowIdx) {
        physicalPartitionIndexMap.put(partitionId, shadowIdxId, shadowIdx);
    }

    void addIndexSchema(long shadowIdxId, long originIdxId, @NotNull String shadowIndexName,
                        short shadowIdxShortKeyCount,
                        @NotNull List<Column> shadowIdxSchema) {
        indexIdMap.put(shadowIdxId, originIdxId);
        indexIdToName.put(shadowIdxId, shadowIndexName);
        indexShortKeyMap.put(shadowIdxId, shadowIdxShortKeyCount);
        indexSchemaMap.put(shadowIdxId, shadowIdxSchema);
    }

    // REQUIRE: has acquired the exclusive lock of database
    void addShadowIndexToCatalog(@NotNull OlapTable table, long visibleTxnId) {
        Preconditions.checkState(visibleTxnId != -1);
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            Preconditions.checkState(partition != null);
            Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                shadowIndex.setVisibleTxnId(visibleTxnId);
                Preconditions.checkState(shadowIndex.getState() == MaterializedIndex.IndexState.SHADOW,
                        shadowIndex.getState());
                partition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxId : indexIdMap.keySet()) {
            long orgIndexId = indexIdMap.get(shadowIdxId);
            table.setIndexMeta(shadowIdxId, indexIdToName.get(shadowIdxId), indexSchemaMap.get(shadowIdxId), 0, 0,
                    indexShortKeyMap.get(shadowIdxId), TStorageType.COLUMN,
                    table.getKeysTypeByIndexId(indexIdMap.get(shadowIdxId)), null, sortKeyIdxes, sortKeyUniqueIds);
            MaterializedIndexMeta orgIndexMeta = table.getIndexMetaByIndexId(orgIndexId);
            Preconditions.checkNotNull(orgIndexMeta);
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(shadowIdxId);
            Preconditions.checkNotNull(indexMeta);
            rebuildMaterializedIndexMeta(orgIndexMeta, indexMeta);
        }

        table.rebuildFullSchema();
    }

    @VisibleForTesting
    public long getWatershedTxnId() {
        return watershedTxnId;
    }

    @VisibleForTesting
    public static void sendAgentTask(AgentBatchTask batchTask) {
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
    }

    @VisibleForTesting
    public static void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                            long timeoutSeconds, AtomicBoolean waitingCreatingReplica,
                                            AtomicBoolean isCancelling) throws AlterCancelException {
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
        long timeout = 1000L * Math.min(timeoutSeconds, Config.max_create_table_timeout_second);
        boolean ok = false;
        try {
            waitingCreatingReplica.set(true);
            if (isCancelling.get()) {
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
                return;
            }
            ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS) && countDownLatch.getStatus().ok();
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
            ok = false;
        } finally {
            waitingCreatingReplica.set(false);
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
    public static void writeEditLog(LakeTableSchemaChangeJob job) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(job);
    }

    @VisibleForTesting
    public static JournalTask writeEditLogAsync(LakeTableSchemaChangeJob job) {
        return GlobalStateMgr.getCurrentState().getEditLog().logAlterJobNoWait(job);
    }

    @VisibleForTesting
    public static long getNextTransactionId() {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
    }

    @VisibleForTesting
    public static long peekNextTransactionId() {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().peekNextTransactionId();
    }

    @VisibleForTesting
    public void setIsCancelling(boolean isCancelling) {
        this.isCancelling.set(isCancelling);
    }

    @VisibleForTesting
    public boolean isCancelling() {
        return this.isCancelling.get();
    }

    @VisibleForTesting
    public void setWaitingCreatingReplica(boolean waitingCreatingReplica) {
        this.waitingCreatingReplica.set(waitingCreatingReplica);
    }

    @VisibleForTesting
    public boolean waitingCreatingReplica() {
        return this.waitingCreatingReplica.get();
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        boolean enableTabletCreationOptimization = Config.lake_enable_tablet_creation_optimization;
        long numTablets = 0;
        AgentBatchTask batchTask = new AgentBatchTask();
        MarkedCountDownLatch<Long, Long> countDownLatch;
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE);
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(table.getBaseIndexId());
            if (enableTabletCreationOptimization) {
                numTablets = physicalPartitionIndexMap.size();
            } else {
                numTablets = physicalPartitionIndexMap.values().stream()
                        .map(materializedIndex -> GlobalStateMgr.getCurrentState().getTabletMetastore()
                                .getAllTablets(materializedIndex))
                        .mapToLong(List::size).sum();
            }
            countDownLatch = new MarkedCountDownLatch<>((int) numTablets);
            createReplicaLatch = countDownLatch;
            long baseIndexId = table.getBaseIndexId();
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                Preconditions.checkState(partition != null);
                TStorageMedium storageMedium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                    List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                    long originIndexId = indexIdMap.get(shadowIdxId);
                    KeysType originKeysType = table.getKeysTypeByIndexId(originIndexId);
                    TTabletSchema tabletSchema = SchemaInfo.newBuilder()
                            .setId(shadowIdxId) // For newly create materialized index, schema id equals to index id
                            .setKeysType(originKeysType)
                            .setShortKeyColumnCount(shadowShortKeyColumnCount)
                            .setSortKeyIndexes(originIndexId == baseIndexId ? sortKeyIdxes : null)
                            .setSortKeyUniqueIds(originIndexId == baseIndexId ? sortKeyUniqueIds : null)
                            .setIndexes(indexes)
                            .setBloomFilterColumnNames(bfColumns)
                            .setBloomFilterFpp(bfFpp)
                            .setStorageType(TStorageType.COLUMN)
                            .addColumns(shadowSchema)
                            .setSchemaHash(0)
                            .build().toTabletSchema();

                    boolean createSchemaFile = true;
                    for (Tablet shadowTablet : GlobalStateMgr.getCurrentState().getTabletMetastore().getAllTablets(shadowIdx)) {
                        long shadowTabletId = shadowTablet.getId();
                        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                                .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) shadowTablet);
                        if (computeNode == null) {
                            //todo: fix the error message.
                            throw new AlterCancelException("No alive backend");
                        }
                        countDownLatch.addMark(computeNode.getId(), shadowTabletId);

                        CreateReplicaTask task = CreateReplicaTask.newBuilder()
                                .setNodeId(computeNode.getId())
                                .setDbId(dbId)
                                .setTableId(tableId)
                                .setPartitionId(partitionId)
                                .setIndexId(shadowIdxId)
                                .setTabletId(shadowTabletId)
                                .setVersion(Partition.PARTITION_INIT_VERSION)
                                .setStorageMedium(storageMedium)
                                .setLatch(countDownLatch)
                                .setEnablePersistentIndex(table.enablePersistentIndex())
                                .setPersistentIndexType(table.getPersistentIndexType())
                                .setPrimaryIndexCacheExpireSec(table.primaryIndexCacheExpireSec())
                                .setTabletType(TTabletType.TABLET_TYPE_LAKE)
                                .setCompressionType(table.getCompressionType())
                                .setCompressionLevel(table.getCompressionLevel())
                                .setCreateSchemaFile(createSchemaFile)
                                .setTabletSchema(tabletSchema)
                                .setEnableTabletCreationOptimization(enableTabletCreationOptimization)
                                .build();
                        // For each partition, the schema file is created only when the first Tablet is created
                        createSchemaFile = false;
                        batchTask.addTask(task);

                        if (enableTabletCreationOptimization) {
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new AlterCancelException(e.getMessage());
        }

        sendAgentTaskAndWait(batchTask, countDownLatch, Config.tablet_create_timeout_second * numTablets,
                waitingCreatingReplica, isCancelling);

        // Add shadow indexes to table.
        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE);
            watershedTxnId = getNextTransactionId();
            addShadowIndexToCatalog(table, watershedTxnId);
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

        LOG.info("transfer schema change job {} state to {}, watershed txn_id: {}", jobId, this.jobState,
                watershedTxnId);
    }

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished(dbId, tableId, watershedTxnId)) {
                LOG.info("wait transactions before {} to be finished, schema change job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send schema change tasks. job: {}", jobId);

        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE);
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();
                    for (Tablet shadowTablet : GlobalStateMgr.getCurrentState().getTabletMetastore().getAllTablets(shadowIdx)) {
                        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                                .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) shadowTablet);
                        if (computeNode == null) {
                            throw new AlterCancelException("No alive backend");
                        }

                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId =
                                physicalPartitionIndexTabletMap.row(partitionId).get(shadowIdxId).get(shadowTabletId);
                        AlterReplicaTask alterTask =
                                AlterReplicaTask.alterLakeTablet(computeNode.getId(), dbId, tableId, partitionId,
                                        shadowIdxId, shadowTabletId, originTabletId, visibleVersion, jobId,
                                        watershedTxnId);
                        getOrCreateSchemaChangeBatchTask().addTask(alterTask);
                    }
                }

                partition.setMinRetainVersion(visibleVersion);

            } // end for partitions
        }

        sendAgentTask(getOrCreateSchemaChangeBatchTask());

        this.jobState = JobState.RUNNING;
        if (span != null) {
            span.addEvent("setRunning");
        }

        // DO NOT write edit log here, will send AlterReplicaTask again if FE restarted or master changed.
        LOG.info("transfer schema change job {} state to {}", jobId, this.jobState);
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

        if (!getOrCreateSchemaChangeBatchTask().isFinished()) {
            LOG.info("schema change tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = getOrCreateSchemaChangeBatchTask().getUnfinishedTasks(2000);
            AgentTask task = tasks.stream().filter(t -> (t.isFailed() || t.getFailedTimes() >= 3)).findAny().orElse(null);
            if (task != null) {
                throw new AlterCancelException(
                        "schema change task failed after try three times: " + task.getErrorMsg());
            } else {
                return;
            }
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            commitVersionMap = new HashMap<>();
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);
                partition.setMinRetainVersion(0);
                long commitVersion = partition.getNextVersion();
                commitVersionMap.put(partitionId, commitVersion);
                LOG.debug("commit version of partition {} is {}. jobId={}", partitionId, commitVersion, jobId);
            }
            this.jobState = JobState.FINISHED_REWRITING;
            this.finishedTimeMs = System.currentTimeMillis();

            writeEditLog(this);

            // NOTE: !!! below this point, this schema change job must success unless the database or table been dropped. !!!
            updateNextVersion(table);
        }

        if (span != null) {
            span.addEvent("finishedRewriting");
        }
        LOG.info("schema change job finished rewriting historical data: {}", jobId);
    }

    // Note: The only allowed situation to cancel the schema change job is the table has been dropped.
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

        JournalTask editLogFuture;
        // Replace the current index with shadow index.
        Set<String> modifiedColumns;
        List<MaterializedIndex> droppedIndexes;
        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                LOG.info("database or table been dropped while doing schema change job {}", jobId);
                return;
            }
            // collect modified columns for inactivating mv
            // Note: should collect before visualiseShadowIndex
            modifiedColumns = collectModifiedColumnsForRelatedMVs(table);
            // Below this point, all query and load jobs will use the new schema.
            droppedIndexes = visualiseShadowIndex(table);

            // inactivate related mv
            inactiveRelatedMv(modifiedColumns, table);
            table.onReload();
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();

            editLogFuture = writeEditLogAsync(this);
        }

        EditLog.waitInfinity(editLogFuture);

        // Delete tablet and shards
        for (MaterializedIndex droppedIndex : droppedIndexes) {
            List<Long> shards = GlobalStateMgr.getCurrentState().getTabletMetastore().getAllTabletIDs(droppedIndex);
            // TODO: what if unusedShards deletion is partially successful?
            StarMgrMetaSyncer.dropTabletAndDeleteShard(shards, GlobalStateMgr.getCurrentState().getStarOSAgent());
        }

        // since we use same shard group to do schema change, must clear old shard before
        // updating colocation info. it's possible that after edit log above is written, fe crashes,
        // and colocation info will not be updated , but it should be a rare case
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                LOG.info("database or table been dropped before updating colocation info, schema change job {}", jobId);
            } else {
                try {
                    GlobalStateMgr.getCurrentState().getColocateTableIndex().updateLakeTableColocationInfo(table, true, null);
                } catch (DdlException e) {
                    // log an error if update colocation info failed, schema change already succeeded
                    LOG.error("table {} update colocation info failed after schema change, {}.", tableId, e.getMessage());
                }
            }
        }

        if (span != null) {
            span.end();
        }
        LOG.info("schema change job finished: {}", jobId);
    }

    // Note: throws AlterCancelException iff the database or table has been dropped.
    boolean readyToPublishVersion() throws AlterCancelException {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            OlapTable table = getTableOrThrow(db, tableId);
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
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

    boolean publishVersion() {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.txnType = TxnTypePB.TXN_NORMAL;
            txnInfo.commitTime = finishedTimeMs / 1000;
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                long commitVersion = commitVersionMap.get(partitionId);
                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                    Utils.publishVersion(
                            GlobalStateMgr.getCurrentState().getTabletMetastore().getAllTablets(shadowIndex),
                            txnInfo, 1, commitVersion, warehouseId);
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to publish version for schema change job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

    private Set<String> collectModifiedColumnsForRelatedMVs(@NotNull OlapTable tbl) {
        if (tbl.getRelatedMaterializedViews().isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<Long, List<Column>> entry : indexSchemaMap.entrySet()) {
            Long shadowIdxId = entry.getKey();
            long originIndexId = indexIdMap.get(shadowIdxId);
            List<Column> shadowSchema = entry.getValue();
            List<Column> originSchema = tbl.getSchemaByIndexId(originIndexId);
            modifiedColumns.addAll(AlterHelper.collectDroppedOrModifiedColumns(originSchema, shadowSchema));
        }
        return modifiedColumns;
    }

    private void inactiveRelatedMv(Set<String> modifiedColumns, @NotNull OlapTable tbl) {
        if (modifiedColumns.isEmpty()) {
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        for (MvId mvId : tbl.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), mvId.getId());
            if (mv == null) {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
                continue;
            }
            for (Column mvColumn : mv.getColumns()) {
                if (modifiedColumns.contains(mvColumn.getName())) {
                    LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                    "the column {} of the table {} was modified.", mv.getName(), mv.getId(),
                            mvColumn.getName(), tbl.getName());
                    mv.setInactiveAndReason(
                            MaterializedViewExceptions.inactiveReasonForColumnChanged(modifiedColumns));
                    return;
                }
            }
        }
    }

    boolean tableHasBeenDropped() {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            return db == null || db.getTable(tableId) == null;
        }
    }

    // Update each Partition's nextVersion.
    // The caller must have acquired the database's exclusive lock.
    void updateNextVersion(@NotNull OlapTable table) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Preconditions.checkState(partition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + partition.getNextVersion() + " commitVersion=" + commitVersion);
            partition.setNextVersion(commitVersion + 1);
        }
    }

    @NotNull
    OlapTable getTableOrThrow(@Nullable LockedDatabase db, long tableId) throws AlterCancelException {
        if (db == null) {
            throw new AlterCancelException("Database does not exist");
        }
        OlapTable table = db.getTable(tableId);
        if (table == null) {
            throw new AlterCancelException("Table does not exist. tableId=" + tableId);
        }
        return table;
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        LakeTableSchemaChangeJob other = (LakeTableSchemaChangeJob) replayedJob;

        LOG.info("Replaying lake table schema change job. state={} jobId={}", replayedJob.jobState, replayedJob.jobId);

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

            this.physicalPartitionIndexTabletMap = other.physicalPartitionIndexTabletMap;
            this.physicalPartitionIndexMap = other.physicalPartitionIndexMap;
            this.indexIdMap = other.indexIdMap;
            this.indexIdToName = other.indexIdToName;
            this.indexSchemaMap = other.indexSchemaMap;
            this.indexShortKeyMap = other.indexShortKeyMap;
            this.hasBfChange = other.hasBfChange;
            this.bfColumns = other.bfColumns;
            this.bfFpp = other.bfFpp;
            this.indexChange = other.indexChange;
            this.indexes = other.indexes;
            this.watershedTxnId = other.watershedTxnId;
            this.startTime = other.startTime;
            this.commitVersionMap = other.commitVersionMap;
            // this.schemaChangeBatchTask = other.schemaChangeBatchTask;
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                return; // do nothing if the table has been dropped.
            }

            if (jobState == JobState.PENDING) {
                addTabletToTabletInvertedIndex(table);
                table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
            } else if (jobState == JobState.WAITING_TXN) {
                addShadowIndexToCatalog(table, watershedTxnId);
            } else if (jobState == JobState.FINISHED_REWRITING) {
                updateNextVersion(table);
            } else if (jobState == JobState.FINISHED) {
                table.onReload();
                visualiseShadowIndex(table);
            } else if (jobState == JobState.CANCELLED) {
                removeShadowIndex(table);
            } else {
                throw new RuntimeException("unknown job state '{}'" + jobState.name());
            }
        }
    }

    void addTabletToTabletInvertedIndex(@NotNull OlapTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Table.Cell<Long, Long, MaterializedIndex> cell : physicalPartitionIndexMap.cellSet()) {
            Long partitionId = cell.getRowKey();
            PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
            Long shadowIndexId = cell.getColumnKey();
            MaterializedIndex shadowIndex = cell.getValue();
            assert partitionId != null;
            assert shadowIndexId != null;
            assert shadowIndex != null;
            TStorageMedium medium = table.getPartitionInfo().getDataProperty(physicalPartition.getParentId()).getStorageMedium();
            TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId, 0, medium, true);
            for (Tablet shadowTablet : shadowIndex.getTablets()) {
                invertedIndex.addTablet(shadowTablet.getId(), shadowTabletMeta);
            }
        }
    }

    void removeShadowIndex(@NotNull OlapTable table) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            Preconditions.checkNotNull(partition, partitionId);
            partition.setMinRetainVersion(0);
            for (MaterializedIndex shadowIdx : physicalPartitionIndexMap.row(partitionId).values()) {
                partition.deleteRollupIndex(shadowIdx.getId());
            }
        }
        for (String shadowIndexName : indexIdToName.values()) {
            table.deleteIndexInfo(shadowIndexName);
        }
        // Delete tablet from TabletInvertedIndex
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            for (MaterializedIndex shadowIdx : physicalPartitionIndexMap.row(partitionId).values()) {
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    @NotNull
    List<MaterializedIndex> visualiseShadowIndex(@NotNull OlapTable table) {
        List<MaterializedIndex> droppedIndexes = new ArrayList<>();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        for (Column column : table.getColumns()) {
            if (Type.VARCHAR.equals(column.getType())) {
                IDictManager.getInstance().removeGlobalDict(table.getId(), column.getColumnId());
            }
        }

        // replace the origin index with shadow index, set index state as NORMAL
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            Preconditions.checkState(commitVersionMap.containsKey(partition.getId()));
            long commitVersion = commitVersionMap.get(partition.getId());

            LOG.debug("update partition visible version. partition=" + partition.getId() + " commitVersion=" +
                    commitVersion);
            // Update Partition's visible version
            Preconditions.checkState(commitVersion == partition.getVisibleVersion() + 1,
                    commitVersion + " vs " + partition.getVisibleVersion());
            partition.setVisibleVersion(commitVersion, finishedTimeMs);
            LOG.debug("update visible version of partition {} to {}. jobId={}", partition.getId(),
                    commitVersion, jobId);
            TStorageMedium medium = table.getPartitionInfo().getDataProperty(partition.getParentId()).getStorageMedium();
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                long shadowIdxId = entry.getKey();
                long originIdxId = entry.getValue();
                // get index from globalStateMgr, not from 'partitionIdToRollupIndex'.
                // because if this alter job is recovered from edit log, index in 'physicalPartitionIndexMap'
                // is not the same object in globalStateMgr. So modification on that index can not reflect to the index
                // in globalStateMgr.
                MaterializedIndex shadowIdx = partition.getIndex(shadowIdxId);
                Preconditions.checkNotNull(shadowIdx, shadowIdxId);
                MaterializedIndex droppedIdx;
                if (originIdxId == partition.getBaseIndex().getId()) {
                    droppedIdx = partition.getBaseIndex();
                } else {
                    droppedIdx = partition.deleteRollupIndex(originIdxId);
                }
                Preconditions.checkNotNull(droppedIdx, originIdxId + " vs. " + shadowIdxId);

                // Add Tablet to TabletInvertedIndex.
                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, tableId, partition.getId(), shadowIdxId, 0, medium, true);
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
                }

                partition.visualiseShadowIndex(shadowIdxId, originIdxId == partition.getBaseIndex().getId());

                // the origin tablet created by old schema can be deleted from FE metadata
                for (Tablet originTablet : droppedIdx.getTablets()) {
                    invertedIndex.deleteTablet(originTablet.getId());
                }

                droppedIndexes.add(droppedIdx);
            }
        }

        // update index schema info of each index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIdxId = entry.getKey();
            long originIdxId = entry.getValue();
            String shadowIdxName = table.getIndexNameById(shadowIdxId);
            String originIdxName = table.getIndexNameById(originIdxId);
            table.deleteIndexInfo(originIdxName);
            // the shadow index name is '__starrocks_shadow_xxx', rename it to origin name 'xxx'
            // this will also remove the prefix of columns
            table.renameIndexForSchemaChange(shadowIdxName, originIdxName);
            table.renameColumnNamePrefix(shadowIdxId);

            if (originIdxId == table.getBaseIndexId()) {
                table.setBaseIndexId(shadowIdxId);
            }
        }
        // rebuild table's full schema
        table.rebuildFullSchema();

        // update bloom filter
        if (hasBfChange) {
            table.setBloomFilterInfo(bfColumns, bfFpp);
        }
        // update index
        if (indexChange) {
            table.setIndexes(indexes);
        }

        table.setState(OlapTable.OlapTableState.NORMAL);

        return droppedIndexes;
    }

    @Override
    public final boolean cancel(String errMsg) {
        isCancelling.set(true);
        try {
            // If waitingCreatingReplica == false, we will assume that
            // cancel thread will get the object lock very quickly.
            if (waitingCreatingReplica.get()) {
                Preconditions.checkState(createReplicaLatch != null);
                createReplicaLatch.countDownToZero(new Status(TStatusCode.OK, ""));
            }
            synchronized (this) {
                return cancelImpl(errMsg);
            }
        } finally {
            isCancelling.set(false);
        }
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

        if (schemaChangeBatchTask != null) {
            AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER);
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table != null) {
                removeShadowIndex(table);
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

        return true;
    }

    AgentBatchTask getOrCreateSchemaChangeBatchTask() {
        if (schemaChangeBatchTask ==
                null) { // This would happen after FE restarted and this object was deserialized from Json.
            schemaChangeBatchTask = new AgentBatchTask();
        }
        return schemaChangeBatchTask;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && schemaChangeBatchTask.getTaskNum() > 0) {
            progress = schemaChangeBatchTask.getFinishedTaskNum() + "/" + schemaChangeBatchTask.getTaskNum();
        }

        // one line for one shadow index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIndexId = entry.getKey();
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            // only show the origin index name
            info.add(Column.removeNamePrefix(indexIdToName.get(shadowIndexId)));
            info.add(shadowIndexId);
            info.add(entry.getValue());
            info.add("0:0"); // schema version and schema hash
            info.add(watershedTxnId);
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);

            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
            if (warehouse == null) {
                info.add("null");
            } else {
                info.add(warehouse.getName());
            }

            infos.add(info);
        }
    }

    private boolean tableExists() {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            return db != null && db.getTable(tableId) != null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    @Nullable
    ReadLockedDatabase getReadLockedDatabase(long dbId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        return db != null ? new ReadLockedDatabase(db) : null;
    }

    @Nullable
    WriteLockedDatabase getWriteLockedDatabase(long dbId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        return db != null ? new WriteLockedDatabase(db) : null;
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    @VisibleForTesting
    public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
        GlobalTransactionMgr globalTxnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        return globalTxnMgr.isPreviousTransactionsFinished(txnId, dbId, Lists.newArrayList(tableId));
    }

    private abstract static class LockedDatabase implements AutoCloseable {
        private final Database db;
        protected Locker locker;

        LockedDatabase(@NotNull Database db) {
            this.locker = new Locker();
            lock(db);
            this.db = db;
        }

        abstract void lock(Database db);

        abstract void unlock(Database db);

        @Nullable
        OlapTable getTable(long tableId) {
            return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        }

        @Override
        public void close() {
            unlock(db);
        }
    }

    private static class ReadLockedDatabase extends LockedDatabase {
        ReadLockedDatabase(@NotNull Database db) {
            super(db);
        }

        @Override
        void lock(Database db) {
            locker.lockDatabase(db.getId(), LockType.READ);
        }

        @Override
        void unlock(Database db) {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    private static class WriteLockedDatabase extends LockedDatabase {
        WriteLockedDatabase(@NotNull Database db) {
            super(db);
        }

        @Override
        void lock(Database db) {
            locker.lockDatabase(db.getId(), LockType.WRITE);
        }

        @Override
        void unlock(Database db) {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
