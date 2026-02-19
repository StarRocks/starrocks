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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/SchemaChangeJobV2.java

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

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.Status;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
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
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TAlterTabletMaterializedColumnReq;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/*
 * Version 2 of SchemaChangeJob.
 * This is for replacing the old SchemaChangeJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class SchemaChangeJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2.class);

    // initially, shadow index id and shadow index meta id are the same.
    // not change the SerializedName for compatibility

    // physical partition id -> (shadow index meta id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    protected Table<Long, Long, Map<Long, Long>> physicalPartitionIndexTabletMap = HashBasedTable.create();
    // physical partition id -> (shadow index meta id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    protected Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap = HashBasedTable.create();
    // shadow index meta id -> origin index meta id
    @SerializedName(value = "indexIdMap")
    protected Map<Long, Long> indexMetaIdMap = Maps.newHashMap();
    // shadow index meta id -> shadow index name(__starrocks_shadow_xxx)
    @SerializedName(value = "indexIdToName")
    protected Map<Long, String> indexMetaIdToName = Maps.newHashMap();
    // shadow index meta id -> index schema
    @SerializedName(value = "indexSchemaMap")
    protected Map<Long, List<Column>> indexMetaIdToSchema = Maps.newHashMap();
    // shadow index meta id -> (shadow index schema version : schema hash)
    @SerializedName(value = "indexSchemaVersionAndHashMap")
    protected Map<Long, SchemaVersionAndHash> indexMetaIdToSchemaVersionAndHash = Maps.newHashMap();
    // shadow index meta id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    protected Map<Long, Short> indexMetaIdToShortKey = Maps.newHashMap();

    // bloom filter info
    @SerializedName(value = "hasBfChange")
    private boolean hasBfChange;
    @SerializedName(value = "bfColumns")
    protected Set<ColumnId> bfColumns = null;
    @SerializedName(value = "bfFpp")
    private double bfFpp = 0;

    // alter index info
    @SerializedName(value = "indexChange")
    private boolean indexChange = false;
    @SerializedName(value = "indexes")
    protected List<Index> indexes = null;

    // The schema change job will wait all transactions before this txn id finished, then send the schema change tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "startTime")
    private long startTime;
    @SerializedName(value = "sortKeyIdxes")
    private List<Integer> sortKeyIdxes;
    @SerializedName(value = "sortKeyUniqueIds")
    private List<Integer> sortKeyUniqueIds;
    // If disableReplicatedStorageForGIN is true, which means this job is adding gin index and table's replicated_storage is true,
    // and we need to disable it.
    @SerializedName(value = "disableReplicatedStorageForGIN")
    private boolean disableReplicatedStorageForGIN = false;
    @SerializedName(value = "historySchema")
    private OlapTableHistorySchema historySchema;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();

    // runtime variable for synchronization between cancel and runPendingJob
    private MarkedCountDownLatch<Long, Long> createReplicaLatch = null;
    private AtomicBoolean waitingCreatingReplica = new AtomicBoolean(false);
    private AtomicBoolean isCancelling = new AtomicBoolean(false);

    public SchemaChangeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    // for deserialization
    private SchemaChangeJobV2() {
        super(JobType.SCHEMA_CHANGE);
    }

    protected SchemaChangeJobV2(SchemaChangeJobV2 job) {
        super(job);
        if (job.physicalPartitionIndexTabletMap != null) {
            this.physicalPartitionIndexTabletMap = HashBasedTable.create();
            for (Cell<Long, Long, Map<Long, Long>> cell : job.physicalPartitionIndexTabletMap.cellSet()) {
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
        this.indexMetaIdToSchemaVersionAndHash = job.indexMetaIdToSchemaVersionAndHash == null ? null
                : Maps.newHashMap(job.indexMetaIdToSchemaVersionAndHash);
        this.indexMetaIdToShortKey = job.indexMetaIdToShortKey == null ? null : Maps.newHashMap(job.indexMetaIdToShortKey);
        this.hasBfChange = job.hasBfChange;
        this.bfColumns = job.bfColumns == null ? null : Sets.newHashSet(job.bfColumns);
        this.bfFpp = job.bfFpp;
        this.indexChange = job.indexChange;
        this.indexes = job.indexes == null ? null : new ArrayList<>(job.indexes);
        this.watershedTxnId = job.watershedTxnId;
        this.startTime = job.startTime;
        this.sortKeyIdxes = job.sortKeyIdxes == null ? null : new ArrayList<>(job.sortKeyIdxes);
        this.sortKeyUniqueIds = job.sortKeyUniqueIds == null ? null : new ArrayList<>(job.sortKeyUniqueIds);
        this.disableReplicatedStorageForGIN = job.disableReplicatedStorageForGIN;
        this.historySchema = job.historySchema;
    }

    public void addTabletIdMap(long physicalPartitionId, long shadowIdxMetaId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = physicalPartitionIndexTabletMap.get(physicalPartitionId, shadowIdxMetaId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            physicalPartitionIndexTabletMap.put(physicalPartitionId, shadowIdxMetaId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    public void addPartitionShadowIndex(long physicalPartitionId, long shadowIdxMetaId, MaterializedIndex shadowIdx) {
        physicalPartitionIndexMap.put(physicalPartitionId, shadowIdxMetaId, shadowIdx);
    }

    public void addIndexSchema(long shadowIdxMetaId, long originIdxMetaId, String shadowIndexName, int shadowSchemaVersion,
                               int shadowSchemaHash, short shadowIdxShortKeyCount, List<Column> shadowIdxSchema) {
        indexMetaIdMap.put(shadowIdxMetaId, originIdxMetaId);
        indexMetaIdToName.put(shadowIdxMetaId, shadowIndexName);
        indexMetaIdToSchemaVersionAndHash.put(shadowIdxMetaId, new SchemaVersionAndHash(shadowSchemaVersion, shadowSchemaHash));
        indexMetaIdToShortKey.put(shadowIdxMetaId, shadowIdxShortKeyCount);
        indexMetaIdToSchema.put(shadowIdxMetaId, shadowIdxSchema);
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

    public void setBloomFilterInfo(boolean hasBfChange, Set<ColumnId> bfColumns, double bfFpp) {
        this.hasBfChange = hasBfChange;
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public void setAlterIndexInfo(boolean indexChange, List<Index> indexes) {
        this.indexChange = indexChange;
        this.indexes = indexes;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setSortKeyIdxes(List<Integer> sortKeyIdxes) {
        this.sortKeyIdxes = sortKeyIdxes;
    }

    public void setSortKeyUniqueIds(List<Integer> sortKeyUniqueIds) {
        this.sortKeyUniqueIds = sortKeyUniqueIds;
    }

    public void setWatershedTxnId(long txnId) {
        this.watershedTxnId = txnId;
    }

    public void setDisableReplicatedStorageForGIN(boolean disableReplicatedStorageForGIN) {
        this.disableReplicatedStorageForGIN = disableReplicatedStorageForGIN;
    }

    public void setHistorySchema(OlapTableHistorySchema historySchema) {
        this.historySchema = historySchema;
    }

    public Optional<OlapTableHistorySchema> getHistorySchema() {
        return Optional.ofNullable(historySchema);
    }

    @Override
    public boolean isExpire() {
        boolean expiredByTime = super.isExpire();
        boolean expiredByHistorySchema = true;
        if (historySchema != null && !historySchema.isExpired()) {
            try {
                expiredByHistorySchema = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().
                    isPreviousTransactionsFinished(historySchema.getHistoryTxnIdThreshold(), dbId, Lists.newArrayList(tableId));
            } catch (Exception e) {
                // As isPreviousTransactionsFinished said, exception happens only when db does not exist,
                // so could clean the history schema safely
            }
            if (expiredByHistorySchema) {
                historySchema.setExpire();
                LOG.info("Expire the history schema, jobId: {}, tableName: {}, expireTxnIdThreshold: {}",
                        jobId, tableName, historySchema.getHistoryTxnIdThreshold());
            }
        }
        return expiredByTime && expiredByHistorySchema;
    }

    /**
     * clear some date structure in this job to save memory
     * these data structures must not used in getInfo method
     */
    @Override
    public void pruneMeta() {
        physicalPartitionIndexTabletMap.clear();
        physicalPartitionIndexMap.clear();
        indexMetaIdToSchema.clear();
        indexMetaIdToShortKey.clear();
    }

    /**
     * runPendingJob():
     * 1. Create all replicas of all shadow indexes and wait them finished.
     * 2. After creating done, add the shadow indexes to globalStateMgr, user can not see this
     * shadow index, but internal load process will generate data for these indexes.
     * 3. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        LOG.info("begin to send create replica tasks. job: {}", jobId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        // 1. create replicas
        AgentBatchTask batchTask = new AgentBatchTask();
        // count total replica num
        int totalReplicaNum = 0;
        for (MaterializedIndex shadowIdx : physicalPartitionIndexMap.values()) {
            for (Tablet tablet : shadowIdx.getTablets()) {
                totalReplicaNum += ((LocalTablet) tablet).getImmutableReplicas().size();
            }
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(totalReplicaNum);
        createReplicaLatch = countDownLatch;

        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            long baseIndexMetaId = tbl.getBaseIndexMetaId();
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            MaterializedIndexMeta index = tbl.getIndexMetaByMetaId(tbl.getBaseIndexMetaId());
            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = tbl.getPhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo()
                        .getDataProperty(physicalPartition.getParentId()).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxMetaId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexMetaIdToShortKey.get(shadowIdxMetaId);
                    List<Column> shadowSchema = indexMetaIdToSchema.get(shadowIdxMetaId);
                    int shadowSchemaHash = indexMetaIdToSchemaVersionAndHash.get(shadowIdxMetaId).schemaHash;
                    int shadowSchemaVersion = indexMetaIdToSchemaVersionAndHash.get(shadowIdxMetaId).schemaVersion;
                    long originIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);
                    KeysType originKeysType = tbl.getKeysTypeByIndexMetaId(originIndexMetaId);
                    TTabletSchema tabletSchema = SchemaInfo.newBuilder()
                            .setId(shadowIdxMetaId) // For newly created materialized, schema id equals to index meta id
                            .setKeysType(originKeysType)
                            .setShortKeyColumnCount(shadowShortKeyColumnCount)
                            .setSchemaHash(shadowSchemaHash)
                            .setVersion(shadowSchemaVersion)
                            .setStorageType(tbl.getStorageType())
                            .setBloomFilterColumnNames(bfColumns)
                            .setBloomFilterFpp(bfFpp)
                            .setIndexes(originIndexMetaId == baseIndexMetaId ?
                                        indexes : OlapTable.getIndexesBySchema(indexes, shadowSchema))
                            .setSortKeyIndexes(originIndexMetaId == baseIndexMetaId ? sortKeyIdxes : null)
                            .setSortKeyUniqueIds(originIndexMetaId == baseIndexMetaId ? sortKeyUniqueIds : null)
                            .addColumns(shadowSchema)
                            .setPrimaryKeyEncodingType(tbl.getPrimaryKeyEncodingType())
                            .build().toTabletSchema();
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        List<Replica> shadowReplicas = ((LocalTablet) shadowTablet).getImmutableReplicas();
                        long baseTabletId = physicalPartitionIndexTabletMap.get(physicalPartitionId, shadowIdxMetaId)
                                .get(shadowTabletId);
                        for (Replica shadowReplica : shadowReplicas) {
                            long backendId = shadowReplica.getBackendId();
                            countDownLatch.addMark(backendId, shadowTabletId);
                            CreateReplicaTask task = CreateReplicaTask.newBuilder()
                                    .setNodeId(backendId)
                                    .setDbId(dbId)
                                    .setTableId(tableId)
                                    .setPartitionId(physicalPartitionId)
                                    .setIndexId(shadowIdxMetaId)
                                    .setTabletId(shadowTabletId)
                                    .setVersion(Partition.PARTITION_INIT_VERSION)
                                    .setStorageMedium(storageMedium)
                                    .setLatch(countDownLatch)
                                    .setEnablePersistentIndex(tbl.enablePersistentIndex())
                                    .setPrimaryIndexCacheExpireSec(tbl.primaryIndexCacheExpireSec())
                                    .setTabletType(TTabletType.TABLET_TYPE_DISK)
                                    .setCompressionType(tbl.getCompressionType())
                                    .setCompressionLevel(tbl.getCompressionLevel())
                                    .setBaseTabletId(baseTabletId)
                                    .setTabletSchema(tabletSchema)
                                    .build();
                            batchTask.addTask(task);
                        } // end for rollupReplicas
                    } // end for rollupTablets
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            long timeout = Math.min(Config.tablet_create_timeout_second * 1000L * totalReplicaNum,
                    Config.max_create_table_timeout_second * 1000L);
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
                // create replicas failed. just cancel the job
                // clear tasks and show the failed replicas to user
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
                String errMsg = null;
                if (!countDownLatch.getStatus().ok()) {
                    errMsg = countDownLatch.getStatus().getErrorMsg();
                } else {
                    List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                    errMsg = "Error replicas:" + Joiner.on(", ").join(subList);
                }
                LOG.warn("failed to create replicas for job: {}, {}", jobId, errMsg);
                throw new AlterCancelException("Create replicas failed. Error: " + errMsg);
            }
        }

        // create all replicas success.
        // add all shadow indexes to globalStateMgr
        tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            final OlapTable finalTbl = tbl;
            this.watershedTxnId =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            persistStateChange(this, JobState.WAITING_TXN, () -> {
                addShadowIndexToCatalog(finalTbl);
                if (disableReplicatedStorageForGIN) {
                    finalTbl.setEnableReplicatedStorage(false);
                }
            });
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        }

        span.setAttribute("watershedTxnId", this.watershedTxnId);
        span.addEvent("setWaitingTxn");
        LOG.info("transfer schema change job {} state to {}, watershed txn_id: {}", jobId, this.jobState,
                watershedTxnId);
    }

    private void addShadowIndexToCatalog(OlapTable tbl) {
        for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition physicalPartition = tbl.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                continue;
            }
            Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                Preconditions.checkState(shadowIndex.getState() == IndexState.SHADOW, shadowIndex.getState());
                physicalPartition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxMetaId : indexMetaIdMap.keySet()) {
            List<Integer> sortKeyColumnIndexes = null;
            List<Integer> sortKeyColumnUniqueIds = null;

            long orgIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);
            if (orgIndexMetaId == tbl.getBaseIndexMetaId()) {
                sortKeyColumnIndexes = sortKeyIdxes;
                sortKeyColumnUniqueIds = sortKeyUniqueIds;
            }

            tbl.setIndexMeta(shadowIdxMetaId, indexMetaIdToName.get(shadowIdxMetaId),
                    indexMetaIdToSchema.get(shadowIdxMetaId),
                    indexMetaIdToSchemaVersionAndHash.get(shadowIdxMetaId).schemaVersion,
                    indexMetaIdToSchemaVersionAndHash.get(shadowIdxMetaId).schemaHash,
                    indexMetaIdToShortKey.get(shadowIdxMetaId), TStorageType.COLUMN,
                    tbl.getKeysTypeByIndexMetaId(orgIndexMetaId), null, sortKeyColumnIndexes,
                    sortKeyColumnUniqueIds);
            MaterializedIndexMeta orgIndexMeta = tbl.getIndexMetaByMetaId(orgIndexMetaId);
            Preconditions.checkNotNull(orgIndexMeta);
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByMetaId(shadowIdxMetaId);
            Preconditions.checkNotNull(indexMeta);
            rebuildMaterializedIndexMeta(orgIndexMeta, indexMeta);
        }

        tbl.rebuildFullSchema();
    }

    /**
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, send schema change tasks to BE.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, schema change job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send schema change tasks. job: {}", jobId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        Map<Long, List<TColumn>> indexToThriftColumns = new HashMap<>();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = tbl.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = physicalPartition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxMetaId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    long originIdxMetaId = indexMetaIdMap.get(shadowIdxMetaId);

                    boolean hasNewGeneratedColumn = false;
                    List<Column> diffGeneratedColumnSchema = Lists.newArrayList();
                    if (originIdxMetaId == tbl.getBaseIndexMetaId()) {
                        List<String> originSchema = tbl.getSchemaByIndexMetaId(originIdxMetaId).stream().map(col ->
                                new String(col.getName())).collect(Collectors.toList());
                        List<String> newSchema = tbl.getSchemaByIndexMetaId(shadowIdxMetaId).stream().map(col ->
                                new String(col.getName())).collect(Collectors.toList());

                        if (originSchema.size() != 0 && newSchema.size() != 0) {
                            for (String colNameInNewSchema : newSchema) {
                                if (!originSchema.contains(colNameInNewSchema) &&
                                        tbl.getColumn(colNameInNewSchema).isGeneratedColumn()) {
                                    diffGeneratedColumnSchema.add(tbl.getColumn(colNameInNewSchema));
                                }
                            }
                        }

                        if (diffGeneratedColumnSchema.size() != 0) {
                            hasNewGeneratedColumn = true;
                        }
                    }
                    Map<Integer, TExpr> mcExprs = new HashMap<>();
                    TAlterTabletMaterializedColumnReq generatedColumnReq = new TAlterTabletMaterializedColumnReq();
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
                        for (Column col : tbl.getFullSchema()) {
                            SlotDescriptor slotDesc = descTbl.addSlotDescriptor(tupleDesc);
                            slotDesc.setType(col.getType());
                            slotDesc.setColumn(new Column(col));
                            slotDesc.setIsMaterialized(true);
                            slotDesc.setIsNullable(col.isAllowNull());

                            slotDescByName.put(col.getName(), slotDesc);
                        }

                        for (Column generatedColumn : diffGeneratedColumnSchema) {
                            Expr expr = generatedColumn.getGeneratedColumnExpr(tbl.getIdToColumn());
                            List<Expr> outputExprs = Lists.newArrayList();

                            for (Column col : tbl.getBaseSchema()) {
                                SlotDescriptor slotDesc = slotDescByName.get(col.getName());

                                if (slotDesc == null) {
                                    throw new AlterCancelException("Expression for generated column can not find " +
                                            "the ref column");
                                }

                                SlotRef slotRef = new SlotRef(slotDesc);
                                slotRef.setColumnName(col.getName());
                                outputExprs.add(slotRef);
                            }

                            TableName tableName = new TableName(db.getFullName(), tbl.getName());

                            // sourceScope must be set null tableName for its Field in RelationFields
                            // because we hope slotRef can not be resolved in sourceScope but can be
                            // resolved in outputScope to force to replace the node using outputExprs.
                            Scope sourceScope = new Scope(RelationId.anonymous(),
                                    new RelationFields(tbl.getBaseSchema().stream().map(col ->
                                                    new Field(col.getName(), col.getType(), null, null))
                                            .collect(Collectors.toList())));

                            Scope outputScope = new Scope(RelationId.anonymous(),
                                    new RelationFields(tbl.getBaseSchema().stream().map(col ->
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
                                            new RelationFields(tbl.getBaseSchema().stream().map(col -> new Field(col.getName(),
                                                    col.getType(), tableName, null)).collect(Collectors.toList()))),
                                    ConnectContext.get());

                            Expr generatedColumnExpr = expr.accept(visitor, null);

                            generatedColumnExpr = ExprUtils.analyzeAndCastFold(generatedColumnExpr);

                            int columnIndex = -1;
                            if (generatedColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                                String originName = Column.removeNamePrefix(generatedColumn.getName());
                                columnIndex = tbl.getFullSchema().indexOf(tbl.getColumn(originName));
                            } else {
                                columnIndex = tbl.getFullSchema().indexOf(generatedColumn);
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

                        generatedColumnReq.setQuery_globals(queryGlobals);
                        generatedColumnReq.setQuery_options(queryOptions);
                        generatedColumnReq.setMc_exprs(mcExprs);
                    }
                    int shadowSchemaHash = indexMetaIdToSchemaVersionAndHash.get(shadowIdxMetaId).schemaHash;
                    int originSchemaHash = tbl.getSchemaHashByIndexMetaId(indexMetaIdMap.get(shadowIdxMetaId));
                    List<TColumn> originSchemaTColumns = indexToThriftColumns.get(originIdxMetaId);
                    if (originSchemaTColumns == null) {
                        originSchemaTColumns = tbl.getSchemaByIndexMetaId(originIdxMetaId).stream()
                                .map(Column::toThrift)
                                .collect(Collectors.toList());
                        indexToThriftColumns.put(originIdxMetaId, originSchemaTColumns);
                    }

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId =
                                physicalPartitionIndexTabletMap.get(physicalPartitionId, shadowIdxMetaId).get(shadowTabletId);
                        for (Replica shadowReplica : ((LocalTablet) shadowTablet).getImmutableReplicas()) {
                            AlterReplicaTask rollupTask = AlterReplicaTask.alterLocalTablet(
                                    shadowReplica.getBackendId(), dbId, tableId, physicalPartitionId,
                                    shadowIdxMetaId, shadowTabletId, originTabletId, shadowReplica.getId(),
                                    shadowSchemaHash, originSchemaHash, visibleVersion, jobId,
                                    generatedColumnReq, originSchemaTColumns);
                            schemaChangeBatchTask.addTask(rollupTask);
                        }
                    }
                }
            } // end for partitions
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        AgentTaskQueue.addBatchTask(schemaChangeBatchTask);
        AgentTaskExecutor.submit(schemaChangeBatchTask);

        this.jobState = JobState.RUNNING;
        span.addEvent("setRunning");

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer schema change job {} state to {}", jobId, this.jobState);
    }

    /**
     * runRunningJob()
     * 1. Wait all schema change tasks to be finished.
     * 2. Check the integrity of the newly created shadow indexes.
     * 3. Replace the origin index with shadow index, and set shadow index's state as NORMAL to be visible to user.
     * 4. Set job'state as FINISHED.
     */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        if (!schemaChangeBatchTask.isFinished()) {
            LOG.info("schema change tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = schemaChangeBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.isFailed() || task.getFailedTimes() >= 3) {
                    throw new AlterCancelException("schema change task failed: " + task.getErrorMsg());
                }
            }
            return;
        }

        /*
         * all tasks are finished. check the integrity.
         * we just check whether all new replicas are healthy.
         */
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        try {
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            // Before schema change, collect modified columns for related mvs.
            Set<String> modifiedColumns = collectModifiedColumnsForRelatedMVs(tbl);

            for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition physicalPartition = tbl.getPhysicalPartition(physicalPartitionId);
                Preconditions.checkNotNull(physicalPartition, physicalPartitionId);

                long visibleVersion = physicalPartition.getVisibleVersion();
                short expectReplicationNum = tbl.getPartitionInfo().getReplicationNum(physicalPartition.getParentId());

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxMetaId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        // Mark schema changed tablet not to move to trash.
                        long baseTabletId = physicalPartitionIndexTabletMap.get(
                                physicalPartitionId, shadowIdxMetaId).get(shadowTablet.getId());
                        // NOTE: known for sure that only LocalTablet uses this SchemaChangeJobV2 class
                        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().
                                markTabletForceDelete(baseTabletId, shadowTablet.getBackendIds());
                        List<Replica> replicas = ((LocalTablet) shadowTablet).getImmutableReplicas();
                        int healthyReplicaNum = 0;
                        for (Replica replica : replicas) {
                            if (replica.getLastFailedVersion() < 0
                                    && replica.checkVersionCatchUp(visibleVersion, false)) {
                                healthyReplicaNum++;
                            }
                        }

                        if (healthyReplicaNum < expectReplicationNum / 2 + 1) {
                            LOG.warn("shadow tablet {} has few healthy replicas: {}, schema change job: {}",
                                    shadowTablet.getId(), replicas, jobId);
                            throw new AlterCancelException(
                                    "shadow tablet " + shadowTablet.getId() + " has few healthy replicas");
                        }
                    } // end for tablets
                }
            } // end for partitions

            this.finishedTimeMs = System.currentTimeMillis();
            // all partitions are good
            persistStateChange(this, JobState.FINISHED, true, () -> {
                onFinished(tbl);
                // If schema changes include fields which defined in related mv, set those mv state to inactive.
                AlterMVJobExecutor.inactiveRelatedMaterializedViewsRecursive(tbl, modifiedColumns);

                pruneMeta();
                tbl.onReload();
            });

        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        }

        LOG.info("schema change job finished: {}", jobId);
        this.span.end();
    }

    private Set<String> collectModifiedColumnsForRelatedMVs(OlapTable tbl) {
        if (tbl.getRelatedMaterializedViews().isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        for (Entry<Long, List<Column>> entry : indexMetaIdToSchema.entrySet()) {
            Long shadowIdxMetaId = entry.getKey();
            long originIndexMetaId = indexMetaIdMap.get(shadowIdxMetaId);
            List<Column> shadowSchema = entry.getValue();
            List<Column> originSchema = tbl.getSchemaByIndexMetaId(originIndexMetaId);
            if (shadowSchema.size() == originSchema.size()) {
                // modify column
                for (Column col : shadowSchema) {
                    if (col.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                        modifiedColumns.add(col.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX, col.getName()));
                    }
                }
            } else if (shadowSchema.size() < originSchema.size()) {
                // drop column
                List<Column> differences = originSchema.stream().filter(element ->
                        !shadowSchema.contains(element)).collect(Collectors.toList());
                // can just drop one column one time, so just one element in differences
                Integer dropIdx = Integer.valueOf(originSchema.indexOf(differences.get(0)));
                modifiedColumns.add(originSchema.get(dropIdx).getName());
            } else {
                // add column should not affect old mv, just ignore.
            }
        }
        return modifiedColumns;
    }

    @Override
    protected void runFinishedRewritingJob() {
        // nothing to do
    }

    private void onFinished(OlapTable tbl) {
        tbl.setState(OlapTableState.UPDATING_META);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        // 
        // partition visible version won't update in schema change, so we need make global
        // dictionary invalid after schema change.
        for (Column column : tbl.getColumns()) {
            if (column.getType().isVarchar()) {
                IDictManager.getInstance().removeGlobalDict(tbl, column.getColumnId());
            }
        }
        // replace the origin index with shadow index, set index state as NORMAL
        for (Partition partition : tbl.getPartitions()) {
            TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium();
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexMetaIdMap.entrySet()) {
                long shadowIdxMetaId = entry.getKey();
                long originIdxMetaId = entry.getValue();
                // initially, shadow index id and shadow index meta id are the same
                long shadowIdxId = shadowIdxMetaId;

                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    // get index from globalStateMgr, not from 'partitionIdToRollupIndex'.
                    // because if this alter job is recovered from edit log, index in 'physicalPartitionIndexMap'
                    // is not the same object in globalStateMgr. So modification on that index can not reflect to the index
                    // in globalStateMgr.
                    MaterializedIndex shadowIdx = physicalPartition.getLatestIndex(shadowIdxMetaId);
                    Preconditions.checkNotNull(shadowIdx, shadowIdxMetaId);
                    List<MaterializedIndex> droppedIndices = physicalPartition.deleteMaterializedIndexByMetaId(originIdxMetaId);
                    Preconditions.checkState(!droppedIndices.isEmpty(), originIdxMetaId + " vs. " + shadowIdxMetaId);

                    // Add to TabletInvertedIndex.
                    // Even thought we have added the tablet to TabletInvertedIndex on pending state, but the pending state
                    // log may be replayed to the image, and the image will not persist the TabletInvertedIndex. So we
                    // should add the tablet to TabletInvertedIndex again on finish state.
                    TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), shadowIdxId,
                            medium);
                    for (Tablet tablet : shadowIdx.getTablets()) {
                        invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            // set the replica state from ReplicaState.ALTER to ReplicaState.NORMAL since the schema change is done.
                            replica.setState(ReplicaState.NORMAL);
                            invertedIndex.addReplica(tablet.getId(), replica);
                        }
                    }

                    physicalPartition.visualiseShadowIndex(shadowIdx.getId(), originIdxMetaId == tbl.getBaseIndexMetaId());

                    // the origin tablet created by old schema can be deleted from FE meta data
                    for (MaterializedIndex droppedIdx : droppedIndices) {
                        for (Tablet originTablet : droppedIdx.getTablets()) {
                            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(originTablet.getId());
                        }
                    }
                }
            }
        }

        // update index schema info of each index
        for (Map.Entry<Long, Long> entry : indexMetaIdMap.entrySet()) {
            long shadowIdxMetaId = entry.getKey();
            long originIdxMetaId = entry.getValue();
            String shadowIdxName = tbl.getIndexNameByMetaId(shadowIdxMetaId);
            String originIdxName = tbl.getIndexNameByMetaId(originIdxMetaId);
            tbl.deleteIndexInfo(originIdxName);
            // the shadow index name is '__starrocks_shadow_xxx', rename it to origin name 'xxx'
            // this will also remove the prefix of columns
            tbl.renameIndexForSchemaChange(shadowIdxName, originIdxName);
            tbl.renameColumnNamePrefix(shadowIdxMetaId);

            if (originIdxMetaId == tbl.getBaseIndexMetaId()) {
                // set base index
                tbl.setBaseIndexMetaId(shadowIdxMetaId);
            }
        }
        // rebuild table's full schema
        tbl.rebuildFullSchema();

        // update bloom filter
        if (hasBfChange) {
            tbl.setBloomFilterInfo(bfColumns, bfFpp);
        }
        // update index
        if (indexChange) {
            tbl.setIndexes(indexes);
        }

        LOG.debug("fullSchema:{}, maxColUniqueId:{}", tbl.getFullSchema(), tbl.getMaxColUniqueId());

        tbl.setState(OlapTableState.NORMAL);
        tbl.lastSchemaUpdateTime.set(System.nanoTime());
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

    /*
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected synchronized boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }

        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();

        persistStateChange(this, JobState.CANCELLED, true, this::cancelInternal);

        pruneMeta();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        span.setStatus(StatusCode.ERROR, errMsg);
        span.end();
        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER);
        // remove all shadow indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {
            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (tbl != null) {
                Locker locker = new Locker();
                locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
                try {
                    for (long physicalPartitionId : physicalPartitionIndexMap.rowKeySet()) {
                        PhysicalPartition physicalPartition = tbl.getPhysicalPartition(physicalPartitionId);
                        Preconditions.checkNotNull(physicalPartition, physicalPartitionId);

                        Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(physicalPartitionId);
                        for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                            MaterializedIndex shadowIdx = entry.getValue();
                            for (Tablet shadowTablet : shadowIdx.getTablets()) {
                                invertedIndex.deleteTablet(shadowTablet.getId());
                            }
                            physicalPartition.deleteMaterializedIndexByMetaId(shadowIdx.getMetaId());
                        }
                    }
                    for (String shadowIndexName : indexMetaIdToName.values()) {
                        tbl.deleteIndexInfo(shadowIndexName);
                    }
                    tbl.setState(OlapTableState.NORMAL);
                    if (disableReplicatedStorageForGIN) {
                        tbl.setEnableReplicatedStorage(true);
                    }
                } finally {
                    locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
                }
            }
        }

        // Job state is updated after WAL is persisted.
    }

    @Override
    public AlterJobV2 copyForPersist() {
        return new SchemaChangeJobV2(this);
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in SchemaChangeHandler.createJob()
     */
    private void replayPending(SchemaChangeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            // table may be dropped before replaying this log. just return
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        try {
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
            for (Cell<Long, Long, MaterializedIndex> cell : physicalPartitionIndexMap.cellSet()) {
                long physicalPartitionId = cell.getRowKey();
                long shadowIndexMetaId = cell.getColumnKey();
                MaterializedIndex shadowIndex = cell.getValue();
                PhysicalPartition partition = tbl.getPhysicalPartition(physicalPartitionId);

                TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partition.getParentId()).getStorageMedium();
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, shadowIndexMetaId,
                        medium);

                for (Tablet shadownTablet : shadowIndex.getTablets()) {
                    invertedIndex.addTablet(shadownTablet.getId(), shadowTabletMeta);
                    for (Replica shadowReplica : ((LocalTablet) shadownTablet).getImmutableReplicas()) {
                        invertedIndex.addReplica(shadownTablet.getId(), shadowReplica);
                    }
                }
            }

            // set table state
            tbl.setState(OlapTableState.SCHEMA_CHANGE);
            if (disableReplicatedStorageForGIN) {
                tbl.setEnableReplicatedStorage(false);
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        }

        // to make sure that this job will run runPendingJob() again to create the shadow index replicas
        this.jobState = JobState.PENDING;
        this.watershedTxnId = replayedJob.watershedTxnId;
        LOG.info("replay pending schema change job: {}", jobId);
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(SchemaChangeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            // table may be dropped before replaying this log. just return
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        try {
            addShadowIndexToCatalog(tbl);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;
        LOG.info("replay waiting txn schema change job: {}", jobId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(SchemaChangeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {

            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (tbl != null) {
                Locker locker = new Locker();
                locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
                try {
                    onFinished(tbl);
                    tbl.onReload();
                } finally {
                    locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
                }
            }
        }
        jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        LOG.info("replay finished schema change job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(SchemaChangeJobV2 replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled schema change job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        SchemaChangeJobV2 replayedSchemaChangeJob = (SchemaChangeJobV2) replayedJob;
        switch (replayedJob.jobState) {
            case PENDING:
                replayPending(replayedSchemaChangeJob);
                break;
            case WAITING_TXN:
                replayWaitingTxn(replayedSchemaChangeJob);
                break;
            case FINISHED:
                replayFinished(replayedSchemaChangeJob);
                break;
            case CANCELLED:
                replayCancelled(replayedSchemaChangeJob);
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && schemaChangeBatchTask.getTaskNum() > 0) {
            progress = schemaChangeBatchTask.getFinishedTaskNum() + "/" + schemaChangeBatchTask.getTaskNum();
        }

        // one line for one shadow index
        boolean isSharedData = RunMode.isSharedDataMode();
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
            if (isSharedData) {
                // schema hash is useless for shared-data, so always set 0
                info.add(String.format("%d:0", indexMetaIdToSchemaVersionAndHash.get(shadowIndexMetaId).schemaVersion));
            } else {
                info.add(indexMetaIdToSchemaVersionAndHash.get(shadowIndexMetaId).toString());
            }
            info.add(watershedTxnId);
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
            if (isSharedData) {
                // although fast schema evolution is not executed on CN,
                // assume it uses default warehouse
                info.add("default_warehouse");
            }
            infos.add(info);
        }
    }

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = schemaChangeBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                AlterReplicaTask alterTask = (AlterReplicaTask) agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(alterTask.getBackendId()));
                info.add(String.valueOf(alterTask.getBaseTabletId()));
                info.add(String.valueOf(alterTask.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
