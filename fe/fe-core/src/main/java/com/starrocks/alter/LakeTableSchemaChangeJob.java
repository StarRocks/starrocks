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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Status;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.lake.Utils;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SelectAnalyzer.RewriteAliasVisitor;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TAlterTabletMaterializedColumnReq;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.Warehouse;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class LakeTableSchemaChangeJob extends LakeTableSchemaChangeJobBase {
    private static final Logger LOG = LogManager.getLogger(LakeTableSchemaChangeJob.class);

    // initially, shadow index id and shadow index meta id are the same.
    // not change the SerializedName for compatibility

    // physical partition id -> (shadow index meta id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    private Table<Long, Long, Map<Long, Long>> physicalPartitionIndexTabletMap = HashBasedTable.create();
    // physical partition id -> (shadow index meta id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap = HashBasedTable.create();
    // shadow index meta id -> origin index meta id
    @SerializedName(value = "indexIdMap")
    private Map<Long, Long> indexMetaIdMap = Maps.newHashMap();
    // shadow index meta id -> shadow index name(__starrocks_shadow_xxx)
    @SerializedName(value = "indexIdToName")
    private Map<Long, String> indexMetaIdToName = Maps.newHashMap();
    // shadow index meta id -> index schema
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, List<Column>> indexMetaIdToSchema = Maps.newHashMap();
    // shadow index meta id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    private Map<Long, Short> indexMetaIdToShortKey = Maps.newHashMap();

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
    private boolean isFileBundling = false;

    final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();

    // for deserialization
    public LakeTableSchemaChangeJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeTableSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    protected LakeTableSchemaChangeJob(LakeTableSchemaChangeJob job) {
        super(job);
        if (job.physicalPartitionIndexTabletMap != null) {
            this.physicalPartitionIndexTabletMap = HashBasedTable.create();
            for (Table.Cell<Long, Long, Map<Long, Long>> cell : job.physicalPartitionIndexTabletMap.cellSet()) {
                Map<Long, Long> tabletMap = Maps.newHashMap();
                if (cell.getValue() != null) {
                    tabletMap.putAll(cell.getValue());
                }
                this.physicalPartitionIndexTabletMap.put(cell.getRowKey(), cell.getColumnKey(), tabletMap);
            }
        } else {
            this.physicalPartitionIndexTabletMap = null;
        }
        if (job.physicalPartitionIndexMap != null) {
            this.physicalPartitionIndexMap = HashBasedTable.create();
            this.physicalPartitionIndexMap.putAll(job.physicalPartitionIndexMap);
        } else {
            this.physicalPartitionIndexMap = null;
        }
        this.indexMetaIdMap = job.indexMetaIdMap == null ? null : Maps.newHashMap(job.indexMetaIdMap);
        this.indexMetaIdToName = job.indexMetaIdToName == null ? null : Maps.newHashMap(job.indexMetaIdToName);
        if (job.indexMetaIdToSchema != null) {
            this.indexMetaIdToSchema = Maps.newHashMap();
            for (Map.Entry<Long, List<Column>> entry : job.indexMetaIdToSchema.entrySet()) {
                List<Column> columns = entry.getValue() == null ? null : new ArrayList<>(entry.getValue());
                this.indexMetaIdToSchema.put(entry.getKey(), columns);
            }
        } else {
            this.indexMetaIdToSchema = null;
        }
        this.indexMetaIdToShortKey = job.indexMetaIdToShortKey == null ? null : Maps.newHashMap(job.indexMetaIdToShortKey);
        this.hasBfChange = job.hasBfChange;
        this.bfColumns = job.bfColumns == null ? null : Sets.newHashSet(job.bfColumns);
        this.bfFpp = job.bfFpp;
        this.indexChange = job.indexChange;
        this.indexes = job.indexes == null ? null : new ArrayList<>(job.indexes);
        this.startTime = job.startTime;
        if (job.commitVersionMap != null) {
            this.commitVersionMap = Maps.newHashMap();
            this.commitVersionMap.putAll(job.commitVersionMap);
        } else {
            this.commitVersionMap = null;
        }
        this.sortKeyIdxes = job.sortKeyIdxes == null ? null : new ArrayList<>(job.sortKeyIdxes);
        this.sortKeyUniqueIds = job.sortKeyUniqueIds == null ? null : new ArrayList<>(job.sortKeyUniqueIds);
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

    void addTabletIdMap(long physicalPartitionId, long shadowIdxMetaId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = physicalPartitionIndexTabletMap.get(physicalPartitionId, shadowIdxMetaId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            physicalPartitionIndexTabletMap.put(physicalPartitionId, shadowIdxMetaId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    void addPartitionShadowIndex(long physicalPartitionId, long shadowIdxMetaId, MaterializedIndex shadowIdx) {
        physicalPartitionIndexMap.put(physicalPartitionId, shadowIdxMetaId, shadowIdx);
    }

    void addIndexSchema(long shadowIdxMetaId, long originIdxMetaId, @NotNull String shadowIndexName,
                        short shadowIdxShortKeyCount,
                        @NotNull List<Column> shadowIdxSchema) {
        indexMetaIdMap.put(shadowIdxMetaId, originIdxMetaId);
        indexMetaIdToName.put(shadowIdxMetaId, shadowIndexName);
        indexMetaIdToShortKey.put(shadowIdxMetaId, shadowIdxShortKeyCount);
        indexMetaIdToSchema.put(shadowIdxMetaId, shadowIdxSchema);
    }

    // REQUIRE: has acquired the exclusive lock of database
    void addShadowIndexToCatalog(@NotNull OlapTable table, long visibleTxnId) {
        Preconditions.checkState(visibleTxnId != -1);
        for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            Preconditions.checkState(physicalPartition != null);
            Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                shadowIndex.setVisibleTxnId(visibleTxnId);
                Preconditions.checkState(shadowIndex.getState() == MaterializedIndex.IndexState.SHADOW,
                        shadowIndex.getState());
                physicalPartition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxMetaId : indexMetaIdMap.keySet()) {
            List<Integer> sortKeyColumnIndexes = null;
            List<Integer> sortKeyColumnUniqueIds = null;

            long orgIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);
            if (orgIndexMetaId == table.getBaseIndexMetaId()) {
                sortKeyColumnIndexes = sortKeyIdxes;
                sortKeyColumnUniqueIds = sortKeyUniqueIds;
            }

            // If upgraded from an old version and do schema change,
            // the schema saved in indexSchemaMap is the schema in the old version, whose uniqueId is -1,
            // so here we initialize column uniqueId here.
            List<Column> columns = indexMetaIdToSchema.get(shadowIdxMetaId);
            boolean restored = LakeTableHelper.restoreColumnUniqueId(columns);
            if (restored) {
                LOG.info("Columns of index {} in table {} has reset all unique ids, column size: {}", shadowIdxMetaId,
                        tableName, columns.size());
            }

            table.setIndexMeta(shadowIdxMetaId, indexMetaIdToName.get(shadowIdxMetaId), columns, 0, 0,
                    indexMetaIdToShortKey.get(shadowIdxMetaId), TStorageType.COLUMN,
                    table.getKeysTypeByIndexMetaId(indexMetaIdMap.get(shadowIdxMetaId)), null, sortKeyColumnIndexes,
                    sortKeyColumnUniqueIds);
            MaterializedIndexMeta orgIndexMeta = table.getIndexMetaByMetaId(orgIndexMetaId);
            Preconditions.checkNotNull(orgIndexMeta);
            MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(shadowIdxMetaId);
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

            // disable tablet creation optimaization to avoid overwriting files with the same name.
            if (table.isFileBundling()) {
                enableTabletCreationOptimization = false;
            }
            if (enableTabletCreationOptimization) {
                numTablets = physicalPartitionIndexMap.size();
            } else {
                numTablets = physicalPartitionIndexMap.values().stream().map(MaterializedIndex::getTablets)
                        .mapToLong(List::size).sum();
            }
            countDownLatch = new MarkedCountDownLatch<>((int) numTablets);
            createReplicaLatch = countDownLatch;
            long baseIndexMetaId = table.getBaseIndexMetaId();
            long gtid = getNextGtid();
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkState(physicalPartition != null);
                TStorageMedium storageMedium = table.getPartitionInfo()
                        .getDataProperty(physicalPartition.getParentId()).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxMetaId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexMetaIdToShortKey.get(shadowIdxMetaId);
                    List<Column> shadowSchema = indexMetaIdToSchema.get(shadowIdxMetaId);
                    long originIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);
                    KeysType originKeysType = table.getKeysTypeByIndexMetaId(originIndexMetaId);
                    TTabletSchema tabletSchema = SchemaInfo.newBuilder()
                            .setId(shadowIdxMetaId) // For newly create materialized index, schema id equals to index meta id
                            .setKeysType(originKeysType)
                            .setShortKeyColumnCount(shadowShortKeyColumnCount)
                            .setSortKeyIndexes(originIndexMetaId == baseIndexMetaId ? sortKeyIdxes : null)
                            .setSortKeyUniqueIds(originIndexMetaId == baseIndexMetaId ? sortKeyUniqueIds : null)
                            .setIndexes(originIndexMetaId == baseIndexMetaId ?
                                        indexes : OlapTable.getIndexesBySchema(indexes, shadowSchema))
                            .setBloomFilterColumnNames(bfColumns)
                            .setBloomFilterFpp(bfFpp)
                            .setStorageType(TStorageType.COLUMN)
                            .addColumns(shadowSchema)
                            .setSchemaHash(0)
                            .build().toTabletSchema();

                    boolean createSchemaFile = true;
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource,
                                shadowTabletId);
                        if (computeNode == null) {
                            //todo: fix the error message.
                            throw new AlterCancelException("No alive backend");
                        }
                        countDownLatch.addMark(computeNode.getId(), shadowTabletId);

                        CreateReplicaTask task = CreateReplicaTask.newBuilder()
                                .setNodeId(computeNode.getId())
                                .setDbId(dbId)
                                .setTableId(tableId)
                                .setPartitionId(physicalPartitionId)
                                .setIndexId(shadowIdxMetaId)
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
                                .setGtid(gtid)
                                .setCompactionStrategy(table.getCompactionStrategy())
                                .setRange(table.isRangeDistribution() ? shadowTablet.getRange() : null)
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
            watershedGtid = getNextGtid();
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

        if (span != null) {
            span.setAttribute("watershedTxnId", this.watershedTxnId);
            span.addEvent("setWaitingTxn");
        }

        // can't add addRollIndexToCatalog into the applier, because of the nextTxnId check.
        // But addRollIndexToCatalog is idempotent, so it's ok to re-add if Leader transferred.
        persistStateChange(this, JobState.WAITING_TXN);

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
            Map<Long, TTabletSchema> indexToBaseTabletReadSchema = new HashMap<>();
            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = physicalPartition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxMetaId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();
                    long originIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);

                    boolean hasNewGeneratedColumn = false;
                    List<Column> diffGeneratedColumnSchema = Lists.newArrayList();
                    if (originIndexMetaId == table.getBaseIndexMetaId()) {
                        List<String> originSchema = table.getSchemaByIndexMetaId(originIndexMetaId).stream().map(col ->
                                new String(col.getName())).collect(Collectors.toList());
                        List<String> newSchema = table.getSchemaByIndexMetaId(shadowIdxMetaId).stream().map(col ->
                                new String(col.getName())).collect(Collectors.toList());

                        if (originSchema.size() != 0 && newSchema.size() != 0) {
                            for (String colNameInNewSchema : newSchema) {
                                if (!originSchema.contains(colNameInNewSchema) &&
                                        table.getColumn(colNameInNewSchema).isGeneratedColumn()) {
                                    diffGeneratedColumnSchema.add(table.getColumn(colNameInNewSchema));
                                }
                            }
                        }

                        if (diffGeneratedColumnSchema.size() != 0) {
                            hasNewGeneratedColumn = true;
                        }
                    }
                    Map<Integer, TExpr> mcExprs = new HashMap<>();
                    TAlterTabletMaterializedColumnReq generatedColumnReq = null;
                    if (hasNewGeneratedColumn) {
                        DescriptorTable descTbl = new DescriptorTable();
                        TupleDescriptor tupleDesc = descTbl.createTupleDescriptor();
                        Map<String, SlotDescriptor> slotDescByName = new HashMap<>();

                        /*
                         * The expression substitution is needed here, because all slotRefs in
                         * GeneratedColumnExpr are still is unAnalyzed. slotRefs get isAnalyzed == true
                         * if it is init by SlotDescriptor. The slot information will be used by be to indentify
                         * the column location in a chunk.
                         */
                        for (Column col : table.getFullSchema()) {
                            SlotDescriptor slotDesc = descTbl.addSlotDescriptor(tupleDesc);
                            slotDesc.setType(col.getType());
                            slotDesc.setColumn(new Column(col));
                            slotDesc.setIsMaterialized(true);
                            slotDesc.setIsNullable(col.isAllowNull());

                            slotDescByName.put(col.getName(), slotDesc);
                        }

                        for (Column generatedColumn : diffGeneratedColumnSchema) {
                            Expr expr = generatedColumn.getGeneratedColumnExpr(table.getIdToColumn());
                            List<Expr> outputExprs = Lists.newArrayList();

                            for (Column col : table.getBaseSchema()) {
                                SlotDescriptor slotDesc = slotDescByName.get(col.getName());

                                if (slotDesc == null) {
                                    throw new AlterCancelException("Expression for generated column can not find " +
                                            "the ref column");
                                }

                                SlotRef slotRef = new SlotRef(slotDesc);
                                slotRef.setColumnName(col.getName());
                                outputExprs.add(slotRef);
                            }

                            TableName tableName = new TableName(db.getFullName(), table.getName());

                            // sourceScope must be set null tableName for its Field in RelationFields
                            // because we hope slotRef can not be resolved in sourceScope but can be
                            // resolved in outputScope to force to replace the node using outputExprs.
                            Scope sourceScope = new Scope(RelationId.anonymous(),
                                    new RelationFields(table.getBaseSchema().stream().map(col ->
                                                    new Field(col.getName(), col.getType(), null, null))
                                            .collect(Collectors.toList())));

                            Scope outputScope = new Scope(RelationId.anonymous(),
                                    new RelationFields(table.getBaseSchema().stream().map(col ->
                                                    new Field(col.getName(), col.getType(), tableName, null))
                                            .collect(Collectors.toList())));

                            if (ConnectContext.get() == null) {
                                LOG.warn("Connect Context is null when add/modify generated column");
                            } else {
                                ConnectContext.get().setDatabase(db.getFullName());
                            }

                            RewriteAliasVisitor visitor =
                                    new RewriteAliasVisitor(sourceScope, outputScope,
                                            outputExprs, ConnectContext.get());

                            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                                            new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(),
                                                    col.getType(), tableName, null)).collect(Collectors.toList()))),
                                    ConnectContext.get());

                            Expr generatedColumnExpr = expr.accept(visitor, null);

                            generatedColumnExpr = ExprUtils.analyzeAndCastFold(generatedColumnExpr);

                            int columnIndex = -1;
                            if (generatedColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                                String originName = Column.removeNamePrefix(generatedColumn.getName());
                                columnIndex = table.getFullSchema().indexOf(table.getColumn(originName));
                            } else {
                                columnIndex = table.getFullSchema().indexOf(generatedColumn);
                            }

                            mcExprs.put(columnIndex, ExprToThrift.treeToThrift(generatedColumnExpr));
                        }
                        // we need this thing, otherwise some expr evalution will fail in BE
                        TQueryGlobals queryGlobals = new TQueryGlobals();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        queryGlobals.setNow_string(dateFormat.format(new Date()));
                        queryGlobals.setTimestamp_ms(new Date().getTime());
                        queryGlobals.setTime_zone(TimeUtils.DEFAULT_TIME_ZONE);

                        TQueryOptions queryOptions = new TQueryOptions();

                        generatedColumnReq = new TAlterTabletMaterializedColumnReq();
                        generatedColumnReq.setQuery_globals(queryGlobals);
                        generatedColumnReq.setQuery_options(queryOptions);
                        generatedColumnReq.setMc_exprs(mcExprs);
                    }

                    TTabletSchema baseTabletReadSchema = indexToBaseTabletReadSchema.get(originIndexMetaId);
                    if (baseTabletReadSchema == null) {
                        baseTabletReadSchema = SchemaInfo.fromMaterializedIndex(
                                table, originIndexMetaId, table.getIndexMetaByMetaId(originIndexMetaId)).toTabletSchema();
                        indexToBaseTabletReadSchema.put(originIndexMetaId, baseTabletReadSchema);
                    }

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource,
                                shadowTablet.getId());
                        if (computeNode == null) {
                            throw new AlterCancelException("No alive backend");
                        }

                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId =
                                physicalPartitionIndexTabletMap.row(physicalPartitionId).get(shadowIdxMetaId).get(shadowTabletId);
                        AlterReplicaTask alterTask =
                                AlterReplicaTask.alterLakeTablet(computeNode.getId(), dbId, tableId, physicalPartitionId,
                                        shadowIdxMetaId, shadowTabletId, originTabletId, visibleVersion, jobId,
                                        watershedTxnId, generatedColumnReq, baseTabletReadSchema);
                        getOrCreateSchemaChangeBatchTask().addTask(alterTask);
                    }
                }

                physicalPartition.setMinRetainVersion(visibleVersion);

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
            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);
                physicalPartition.setMinRetainVersion(0);
                long commitVersion = physicalPartition.getNextVersion();
                commitVersionMap.put(physicalPartitionId, commitVersion);
                LOG.debug("commit version of partition {} is {}. jobId={}", physicalPartitionId, commitVersion, jobId);
            }
            this.finishedTimeMs = System.currentTimeMillis();

            persistStateChange(this, JobState.FINISHED_REWRITING, () -> {
                // NOTE: !!! below this point, this schema change job must success unless the database or table been dropped. !!!
                updateNextVersion(table);
            });
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
            return;
        }

        // Replace the current index with shadow index.
        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            OlapTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                LOG.info("database or table been dropped while doing schema change job {}", jobId);
                return;
            }
            // collect modified columns for inactivating mv
            // Note: should collect before visualiseShadowIndex
            Set<String> modifiedColumns = collectModifiedColumnsForRelatedMVs(table);
            this.finishedTimeMs = System.currentTimeMillis();

            persistStateChange(this, JobState.FINISHED, () -> {
                // Below this point, all query and load jobs will use the new schema.
                visualiseShadowIndex(table);
                // inactivate related mv
                inactiveRelatedMv(modifiedColumns, table);
                table.onReload();
            });
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
            isFileBundling = table.isFileBundling();
            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkState(physicalPartition != null, physicalPartitionId);
                long commitVersion = commitVersionMap.get(physicalPartitionId);
                if (commitVersion != physicalPartition.getVisibleVersion() + 1) {
                    Preconditions.checkState(physicalPartition.getVisibleVersion() < commitVersion,
                            "partition=" + physicalPartitionId + " visibleVersion=" + physicalPartition.getVisibleVersion() +
                                    " commitVersion=" + commitVersion);
                    return false;
                }
            }
        }
        return true;
    }

    protected boolean lakePublishVersion() {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.txnType = TxnTypePB.TXN_NORMAL;
            txnInfo.commitTime = finishedTimeMs / 1000;
            txnInfo.gtid = watershedGtid;

            // txnId is -1 means that BE do nothing, just upgrade the tablet_meta version
            TxnInfoPB originTxnInfo = new TxnInfoPB();
            originTxnInfo.txnId = -1L;
            originTxnInfo.combinedTxnLog = false;
            originTxnInfo.commitTime = finishedTimeMs / 1000;
            originTxnInfo.txnType = TxnTypePB.TXN_EMPTY;
            originTxnInfo.gtid = watershedGtid;

            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                AggregatePublishVersionRequest request = new AggregatePublishVersionRequest();
                long commitVersion = commitVersionMap.get(physicalPartitionId);
                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                    if (!isFileBundling) {
                        Utils.publishVersion(shadowIndex.getTablets(), txnInfo, 1, commitVersion, computeResource,
                                isFileBundling);
                    } else {
                        Utils.createSubRequestForAggregatePublish(shadowIndex.getTablets(),
                                Lists.newArrayList(txnInfo), 1, commitVersion, null, computeResource, request);
                    }
                }

                // For indexes whose schema have not changed, we still need to upgrade the version
                List<MaterializedIndex> originMaterializedIndices;
                List<Tablet> allOtherPartitionTablets = new ArrayList<>();
                try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
                    OlapTable table = getTableOrThrow(db, tableId);
                    PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                    originMaterializedIndices = physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
                }

                for (MaterializedIndex index : originMaterializedIndices) {
                    allOtherPartitionTablets.addAll(index.getTablets());
                }

                if (!isFileBundling) {
                    Utils.publishVersion(allOtherPartitionTablets, originTxnInfo, commitVersion - 1, commitVersion, 
                            computeResource, isFileBundling);
                } else {
                    Utils.createSubRequestForAggregatePublish(allOtherPartitionTablets, Lists.newArrayList(originTxnInfo),
                            commitVersion - 1, commitVersion, null, computeResource, request);
                }

                if (isFileBundling) {
                    Utils.sendAggregatePublishVersionRequest(request, 1, computeResource, null, null);
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
        for (Map.Entry<Long, List<Column>> entry : indexMetaIdToSchema.entrySet()) {
            Long shadowIdxMetaId = entry.getKey();
            long originIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);
            List<Column> shadowSchema = entry.getValue();
            List<Column> originSchema = tbl.getSchemaByIndexMetaId(originIndexMetaId);
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
                    // clear version map to make sure the MV will be refreshed
                    mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
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
        for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            long commitVersion = commitVersionMap.get(physicalPartitionId);
            Preconditions.checkState(physicalPartition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + physicalPartition.getNextVersion() + " commitVersion=" + commitVersion);
            physicalPartition.setNextVersion(commitVersion + 1);
        }
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
            this.indexMetaIdMap = other.indexMetaIdMap;
            this.indexMetaIdToName = other.indexMetaIdToName;
            this.indexMetaIdToSchema = other.indexMetaIdToSchema;
            this.indexMetaIdToShortKey = other.indexMetaIdToShortKey;
            this.hasBfChange = other.hasBfChange;
            this.bfColumns = other.bfColumns;
            this.bfFpp = other.bfFpp;
            this.indexChange = other.indexChange;
            this.indexes = other.indexes;
            this.watershedTxnId = other.watershedTxnId;
            this.watershedGtid = other.watershedGtid;
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
            Long shadowIndexMetaId = cell.getColumnKey();
            MaterializedIndex shadowIndex = cell.getValue();
            assert partitionId != null;
            assert shadowIndexMetaId != null;
            assert shadowIndex != null;
            TStorageMedium medium = table.getPartitionInfo().getDataProperty(physicalPartition.getParentId()).getStorageMedium();
            long shadowIndexId = shadowIndexMetaId;
            TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId, medium, true);
            for (Tablet shadowTablet : shadowIndex.getTablets()) {
                invertedIndex.addTablet(shadowTablet.getId(), shadowTabletMeta);
            }
        }
    }

    void removeShadowIndex(@NotNull OlapTable table) {
        for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
            Preconditions.checkNotNull(partition, physicalPartitionId);
            partition.setMinRetainVersion(0);
            for (MaterializedIndex shadowIdx : physicalPartitionIndexMap.row(physicalPartitionId).values()) {
                partition.deleteMaterializedIndexByMetaId(shadowIdx.getMetaId());
            }
        }
        for (String shadowIndexName : indexMetaIdToName.values()) {
            table.deleteIndexInfo(shadowIndexName);
        }
        // Delete tablet from TabletInvertedIndex
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
            for (MaterializedIndex shadowIdx : physicalPartitionIndexMap.row(physicalPartitionId).values()) {
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    @NotNull
    List<MaterializedIndex> visualiseShadowIndex(@NotNull OlapTable table) {
        List<MaterializedIndex> droppedIndices = new ArrayList<>();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        for (Column column : table.getColumns()) {
            if (VarcharType.VARCHAR.equals(column.getType())) {
                IDictManager.getInstance().removeGlobalDict(table, column.getColumnId());
            }
        }

        // replace the origin index with shadow index, set index state as NORMAL
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
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
            TStorageMedium medium = table.getPartitionInfo().getDataProperty(physicalPartition.getParentId()).getStorageMedium();
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexMetaIdMap.entrySet()) {
                long shadowIdxMetaId = entry.getKey();
                long originIdxMetaId = entry.getValue();
                // get index from globalStateMgr, not from 'partitionIdToRollupIndex'.
                // because if this alter job is recovered from edit log, index in 'physicalPartitionIndexMap'
                // is not the same object in globalStateMgr. So modification on that index can not reflect to the index
                // in globalStateMgr.
                MaterializedIndex shadowIdx = physicalPartition.getLatestIndex(shadowIdxMetaId);
                Preconditions.checkNotNull(shadowIdx, shadowIdxMetaId);
                List<MaterializedIndex> partDroppedIndices = physicalPartition.deleteMaterializedIndexByMetaId(originIdxMetaId);
                Preconditions.checkState(!partDroppedIndices.isEmpty(), originIdxMetaId + " vs. " + shadowIdxMetaId);

                // Add Tablet to TabletInvertedIndex.
                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, tableId, physicalPartition.getId(), shadowIdx.getId(), medium, true);
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
                }

                physicalPartition.visualiseShadowIndex(shadowIdx.getId(), originIdxMetaId == table.getBaseIndexMetaId());

                // the origin tablet created by old schema can be deleted from FE metadata
                for (MaterializedIndex droppedIdx : partDroppedIndices) {
                    for (Tablet originTablet : droppedIdx.getTablets()) {
                        invertedIndex.deleteTablet(originTablet.getId());
                    }
                }

                droppedIndices.addAll(partDroppedIndices);
            }
        }

        // update index schema info of each index
        for (Map.Entry<Long, Long> entry : indexMetaIdMap.entrySet()) {
            long shadowIdxMetaId = entry.getKey();
            long originIdxMetaId = entry.getValue();
            String shadowIdxName = table.getIndexNameByMetaId(shadowIdxMetaId);
            String originIdxName = table.getIndexNameByMetaId(originIdxMetaId);
            table.deleteIndexInfo(originIdxName);
            // the shadow index name is '__starrocks_shadow_xxx', rename it to origin name 'xxx'
            // this will also remove the prefix of columns
            table.renameIndexForSchemaChange(shadowIdxName, originIdxName);
            table.renameColumnNamePrefix(shadowIdxMetaId);

            if (originIdxMetaId == table.getBaseIndexMetaId()) {
                table.setBaseIndexMetaId(shadowIdxMetaId);
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

        return droppedIndices;
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
                return cancelInternal(errMsg);
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

        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();

        persistStateChange(this, JobState.CANCELLED, () -> {
            try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
                OlapTable table = (db != null) ? db.getTable(tableId) : null;
                if (table != null) {
                    removeShadowIndex(table);
                }
            }
        });

        if (span != null) {
            span.setStatus(StatusCode.ERROR, errMsg);
            span.end();
        }

        LOG.info("Lake schema change job canceled, jobId: {}, error: {}", jobId, errMsg);

        return true;
    }

    AgentBatchTask getOrCreateSchemaChangeBatchTask() {
        if (schemaChangeBatchTask == null) { // This would happen after FE restarted and this object was deserialized from Json.
            schemaChangeBatchTask = new AgentBatchTask();
        }
        return schemaChangeBatchTask;
    }

    @Override
    public AlterJobV2 copyForPersist() {
        return new LakeTableSchemaChangeJob(this);
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && schemaChangeBatchTask.getTaskNum() > 0) {
            progress = schemaChangeBatchTask.getFinishedTaskNum() + "/" + schemaChangeBatchTask.getTaskNum();
        }

        // one line for one shadow index
        for (Map.Entry<Long, Long> entry : indexMetaIdMap.entrySet()) {
            long shadowIndexMetaId = entry.getKey();
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            // only show the origin index name
            info.add(Column.removeNamePrefix(indexMetaIdToName.get(shadowIndexMetaId)));
            info.add(shadowIndexMetaId);
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



}
