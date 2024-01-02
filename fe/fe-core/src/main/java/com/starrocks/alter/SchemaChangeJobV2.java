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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
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
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SelectAnalyzer.RewriteAliasVisitor;
import com.starrocks.sql.optimizer.statistics.IDictManager;
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
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.inactiveRelatedMaterializedViews;

/*
 * Version 2 of SchemaChangeJob.
 * This is for replacing the old SchemaChangeJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class SchemaChangeJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2.class);

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
    // shadow index id -> (shadow index schema version : schema hash)
    @SerializedName(value = "indexSchemaVersionAndHashMap")
    private Map<Long, SchemaVersionAndHash> indexSchemaVersionAndHashMap = Maps.newHashMap();
    // shadow index id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    private Map<Long, Short> indexShortKeyMap = Maps.newHashMap();

    // bloom filter info
    @SerializedName(value = "hasBfChange")
    private boolean hasBfChange;
    @SerializedName(value = "bfColumns")
    private Set<String> bfColumns = null;
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
    @SerializedName(value = "sortKeyIdxes")
    private List<Integer> sortKeyIdxes;
    @SerializedName(value = "sortKeyUniqueIds")
    private List<Integer> sortKeyUniqueIds;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();

    public SchemaChangeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    private SchemaChangeJobV2() {
        super(JobType.SCHEMA_CHANGE);
    }

    public void addTabletIdMap(long partitionId, long shadowIdxId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = physicalPartitionIndexTabletMap.get(partitionId, shadowIdxId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            physicalPartitionIndexTabletMap.put(partitionId, shadowIdxId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    public void addPartitionShadowIndex(long partitionId, long shadowIdxId, MaterializedIndex shadowIdx) {
        physicalPartitionIndexMap.put(partitionId, shadowIdxId, shadowIdx);
    }

    public void addIndexSchema(long shadowIdxId, long originIdxId, String shadowIndexName, int shadowSchemaVersion,
                               int shadowSchemaHash, short shadowIdxShortKeyCount, List<Column> shadowIdxSchema) {
        indexIdMap.put(shadowIdxId, originIdxId);
        indexIdToName.put(shadowIdxId, shadowIndexName);
        indexSchemaVersionAndHashMap.put(shadowIdxId, new SchemaVersionAndHash(shadowSchemaVersion, shadowSchemaHash));
        indexShortKeyMap.put(shadowIdxId, shadowIdxShortKeyCount);
        indexSchemaMap.put(shadowIdxId, shadowIdxSchema);
    }

    public void setBloomFilterInfo(boolean hasBfChange, Set<String> bfColumns, double bfFpp) {
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

    /**
     * clear some date structure in this job to save memory
     * these data structures must not used in getInfo method
     */
    private void pruneMeta() {
        physicalPartitionIndexTabletMap.clear();
        physicalPartitionIndexMap.clear();
        indexSchemaMap.clear();
        indexShortKeyMap.clear();
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
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
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }

            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            MaterializedIndexMeta index = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo()
                        .getDataProperty(partition.getParentId()).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                    List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    int shadowSchemaVersion = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaVersion;
                    long originIndexId = indexIdMap.get(shadowIdxId);
                    int originSchemaHash = tbl.getSchemaHashByIndexId(originIndexId);
                    KeysType originKeysType = tbl.getKeysTypeByIndexId(originIndexId);
                    List<Column> originSchema = tbl.getSchemaByIndexId(originIndexId);
                    List<Integer> copiedSortKeyIdxes = index.getSortKeyIdxes();
                    List<Integer> copiedSortKeyUniqueIds = index.getSortKeyUniqueIds();
                    // TODO
                    // the following code appears to be deletable 
                    if (index.getSortKeyIdxes() != null) {
                        if (originSchema.size() > shadowSchema.size()) {
                            List<Column> differences = originSchema.stream().filter(element ->
                                    !shadowSchema.contains(element)).collect(Collectors.toList());
                            // can just drop one column one time, so just one element in differences
                            Integer dropIdx = new Integer(originSchema.indexOf(differences.get(0)));
                            for (int i = 0; i < copiedSortKeyIdxes.size(); ++i) {
                                Integer sortKeyIdx = copiedSortKeyIdxes.get(i);
                                if (dropIdx < sortKeyIdx) {
                                    copiedSortKeyIdxes.set(i, sortKeyIdx - 1);
                                }
                            }
                        } else if (originSchema.size() < shadowSchema.size()) {
                            List<Column> differences = shadowSchema.stream().filter(element ->
                                    !originSchema.contains(element)).collect(Collectors.toList());
                            for (Column difference : differences) {
                                int addColumnIdx = shadowSchema.indexOf(difference);
                                for (int i = 0; i < copiedSortKeyIdxes.size(); ++i) {
                                    Integer sortKeyIdx = copiedSortKeyIdxes.get(i);
                                    int shadowSortKeyIdx = shadowSchema.indexOf(originSchema.get(index.getSortKeyIdxes().get(i)));
                                    if (addColumnIdx < shadowSortKeyIdx) {
                                        copiedSortKeyIdxes.set(i, sortKeyIdx + 1);
                                    }
                                }
                            }
                        }
                    }
                    if (sortKeyIdxes != null) {
                        copiedSortKeyIdxes = sortKeyIdxes;
                    } else if (copiedSortKeyIdxes != null && !copiedSortKeyIdxes.isEmpty()) {
                        sortKeyIdxes = copiedSortKeyIdxes;
                    }
                    // reorder, change sort key columns
                    if (sortKeyUniqueIds != null) {
                        copiedSortKeyUniqueIds = sortKeyUniqueIds;
                    }
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        List<Replica> shadowReplicas = ((LocalTablet) shadowTablet).getImmutableReplicas();
                        for (Replica shadowReplica : shadowReplicas) {
                            long backendId = shadowReplica.getBackendId();
                            countDownLatch.addMark(backendId, shadowTabletId);
                            CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                    backendId, dbId, tableId, partitionId, shadowIdxId, shadowTabletId,
                                    shadowShortKeyColumnCount, shadowSchemaHash, shadowSchemaVersion,
                                    Partition.PARTITION_INIT_VERSION,
                                    originKeysType, TStorageType.COLUMN, storageMedium,
                                    shadowSchema, bfColumns, bfFpp, countDownLatch, indexes,
                                    tbl.isInMemory(),
                                    tbl.enablePersistentIndex(),
                                    tbl.primaryIndexCacheExpireSec(),
                                    tbl.getPartitionInfo().getTabletType(partition.getParentId()),
                                    tbl.getCompressionType(), copiedSortKeyIdxes,
                                    sortKeyUniqueIds);
                            createReplicaTask.setBaseTablet(
                                    physicalPartitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId),
                                    originSchemaHash);
                            batchTask.addTask(createReplicaTask);
                        } // end for rollupReplicas
                    } // end for rollupTablets
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            long timeout = Math.min(Config.tablet_create_timeout_second * 1000L * totalReplicaNum,
                    Config.max_create_table_timeout_second * 1000L);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS) && countDownLatch.getStatus().ok();
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
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
        locker.lockDatabase(db, LockType.WRITE);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            addShadowIndexToCatalog(tbl);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        this.watershedTxnId =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        this.jobState = JobState.WAITING_TXN;
        span.setAttribute("watershedTxnId", this.watershedTxnId);
        span.addEvent("setWaitingTxn");

        // write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        LOG.info("transfer schema change job {} state to {}, watershed txn_id: {}", jobId, this.jobState,
                watershedTxnId);
    }

    private void addShadowIndexToCatalog(OlapTable tbl) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
            if (partition == null) {
                continue;
            }
            Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                Preconditions.checkState(shadowIndex.getState() == IndexState.SHADOW, shadowIndex.getState());
                partition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxId : indexIdMap.keySet()) {
            long orgIndexId = indexIdMap.get(shadowIdxId);
            tbl.setIndexMeta(shadowIdxId, indexIdToName.get(shadowIdxId),
                    indexSchemaMap.get(shadowIdxId),
                    indexSchemaVersionAndHashMap.get(shadowIdxId).schemaVersion,
                    indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash,
                    indexShortKeyMap.get(shadowIdxId), TStorageType.COLUMN,
                    tbl.getKeysTypeByIndexId(orgIndexId), null, sortKeyIdxes,
                    sortKeyUniqueIds);
            MaterializedIndexMeta orgIndexMeta = tbl.getIndexMetaByIndexId(orgIndexId);
            Preconditions.checkNotNull(orgIndexMeta);
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(shadowIdxId);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    long originIdxId = indexIdMap.get(shadowIdxId);

                    boolean hasNewGeneratedColumn = false;
                    List<Column> diffGeneratedColumnSchema = Lists.newArrayList();
                    if (originIdxId == tbl.getBaseIndexId()) {
                        List<String> originSchema = tbl.getSchemaByIndexId(originIdxId).stream().map(col ->
                                new String(col.getName())).collect(Collectors.toList());
                        List<String> newSchema = tbl.getSchemaByIndexId(shadowIdxId).stream().map(col ->
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
                            Column column = generatedColumn;
                            Expr expr = column.generatedColumnExpr();
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

                            RewriteAliasVisitor visitor =
                                    new RewriteAliasVisitor(sourceScope, outputScope,
                                            outputExprs, ConnectContext.get());

                            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                                            new RelationFields(tbl.getBaseSchema().stream().map(col -> new Field(col.getName(),
                                                    col.getType(), tableName, null)).collect(Collectors.toList()))),
                                    ConnectContext.get());

                            Expr generatedColumnExpr = expr.accept(visitor, null);

                            generatedColumnExpr = Expr.analyzeAndCastFold(generatedColumnExpr);

                            int columnIndex = -1;
                            if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX) ||
                                    column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
                                String originName = Column.removeNamePrefix(column.getName());
                                columnIndex = tbl.getFullSchema().indexOf(tbl.getColumn(originName));
                            } else {
                                columnIndex = tbl.getFullSchema().indexOf(column);
                            }

                            mcExprs.put(columnIndex, generatedColumnExpr.treeToThrift());
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
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    int originSchemaHash = tbl.getSchemaHashByIndexId(indexIdMap.get(shadowIdxId));
                    List<Column> originSchemaColumns = tbl.getSchemaByIndexId(originIdxId);

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId = physicalPartitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId);
                        for (Replica shadowReplica : ((LocalTablet) shadowTablet).getImmutableReplicas()) {
                            AlterReplicaTask rollupTask = AlterReplicaTask.alterLocalTablet(
                                    shadowReplica.getBackendId(), dbId, tableId, partitionId,
                                    shadowIdxId, shadowTabletId, originTabletId, shadowReplica.getId(),
                                    shadowSchemaHash, originSchemaHash, visibleVersion, jobId,
                                    generatedColumnReq, originSchemaColumns);
                            schemaChangeBatchTask.addTask(rollupTask);
                        }
                    }
                }
            } // end for partitions
        } finally {
            locker.unLockDatabase(db, LockType.READ);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
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
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        Future<Boolean> future;
        long start;
        locker.lockDatabase(db, LockType.WRITE);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            // Before schema change, collect modified columns for related mvs.
            Set<String> modifiedColumns = collectModifiedColumnsForRelatedMVs(tbl);

            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                long visiableVersion = partition.getVisibleVersion();
                short expectReplicationNum = tbl.getPartitionInfo().getReplicationNum(partition.getParentId());

                Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        // Mark schema changed tablet not to move to trash.
                        long baseTabletId = physicalPartitionIndexTabletMap.get(
                                partitionId, shadowIdxId).get(shadowTablet.getId());
                        // NOTE: known for sure that only LocalTablet uses this SchemaChangeJobV2 class
                        GlobalStateMgr.getCurrentInvertedIndex().
                                markTabletForceDelete(baseTabletId, shadowTablet.getBackendIds());
                        List<Replica> replicas = ((LocalTablet) shadowTablet).getImmutableReplicas();
                        int healthyReplicaNum = 0;
                        for (Replica replica : replicas) {
                            if (replica.getLastFailedVersion() < 0
                                    && replica.checkVersionCatchUp(visiableVersion, false)) {
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

            // all partitions are good
            onFinished(tbl);

            // If schema changes include fields which defined in related mv, set those mv state to inactive.
            inactiveRelatedMaterializedViews(db, tbl, modifiedColumns);

            pruneMeta();
            tbl.onReload();
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();

            start = System.nanoTime();
            future = editLog.logAlterJobNoWait(this);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        EditLog.waitInfinity(start, future);

        LOG.info("schema change job finished: {}", jobId);
        this.span.end();
    }

    private Set<String> collectModifiedColumnsForRelatedMVs(OlapTable tbl) {
        if (tbl.getRelatedMaterializedViews().isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        for (Entry<Long, List<Column>> entry : indexSchemaMap.entrySet()) {
            Long shadowIdxId = entry.getKey();
            long originIndexId = indexIdMap.get(shadowIdxId);
            List<Column> shadowSchema = entry.getValue();
            List<Column> originSchema = tbl.getSchemaByIndexId(originIndexId);
            if (shadowSchema.size() == originSchema.size()) {
                // modify column
                for (Column col : shadowSchema) {
                    if (col.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                        modifiedColumns.add(col.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX, col.getName()));
                    }
                }
            } else if (shadowSchema.size() < originSchema.size()) {
                // drop column
                List<Column> differences = originSchema.stream().filter(element ->
                        !shadowSchema.contains(element)).collect(Collectors.toList());
                // can just drop one column one time, so just one element in differences
                Integer dropIdx = new Integer(originSchema.indexOf(differences.get(0)));
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
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        // 
        // partition visible version won't update in schema change, so we need make global
        // dictionary invalid after schema change.
        for (Column column : tbl.getColumns()) {
            if (column.getType().isVarchar()) {
                IDictManager.getInstance().removeGlobalDict(tbl.getId(), column.getName());
            }
        }
        // replace the origin index with shadow index, set index state as NORMAL
        for (Partition partition : tbl.getPartitions()) {
            TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partition.getParentId()).getStorageMedium();
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                long shadowIdxId = entry.getKey();
                long originIdxId = entry.getValue();

                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    // get index from globalStateMgr, not from 'partitionIdToRollupIndex'.
                    // because if this alter job is recovered from edit log, index in 'physicalPartitionIndexMap'
                    // is not the same object in globalStateMgr. So modification on that index can not reflect to the index
                    // in globalStateMgr.
                    MaterializedIndex shadowIdx = physicalPartition.getIndex(shadowIdxId);
                    Preconditions.checkNotNull(shadowIdx, shadowIdxId);
                    MaterializedIndex droppedIdx = null;
                    if (originIdxId == physicalPartition.getBaseIndex().getId()) {
                        droppedIdx = physicalPartition.getBaseIndex();
                    } else {
                        droppedIdx = physicalPartition.deleteRollupIndex(originIdxId);
                    }
                    Preconditions.checkNotNull(droppedIdx, originIdxId + " vs. " + shadowIdxId);

                    // Add to TabletInvertedIndex.
                    // Even thought we have added the tablet to TabletInvertedIndex on pending state, but the pending state
                    // log may be replayed to the image, and the image will not persist the TabletInvertedIndex. So we
                    // should add the tablet to TabletInvertedIndex again on finish state.
                    TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), shadowIdxId,
                            indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash, medium);
                    for (Tablet tablet : shadowIdx.getTablets()) {
                        invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            // set the replica state from ReplicaState.ALTER to ReplicaState.NORMAL since the schema change is done.
                            replica.setState(ReplicaState.NORMAL);
                            invertedIndex.addReplica(tablet.getId(), replica);
                        }
                    }

                    physicalPartition.visualiseShadowIndex(shadowIdxId, originIdxId == physicalPartition.getBaseIndex().getId());

                    // the origin tablet created by old schema can be deleted from FE meta data
                    for (Tablet originTablet : droppedIdx.getTablets()) {
                        GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(originTablet.getId());
                    }
                }
            }
        }

        // update index schema info of each index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIdxId = entry.getKey();
            long originIdxId = entry.getValue();
            String shadowIdxName = tbl.getIndexNameById(shadowIdxId);
            String originIdxName = tbl.getIndexNameById(originIdxId);
            tbl.deleteIndexInfo(originIdxName);
            // the shadow index name is '__starrocks_shadow_xxx', rename it to origin name 'xxx'
            // this will also remove the prefix of columns
            tbl.renameIndexForSchemaChange(shadowIdxName, originIdxName);
            tbl.renameColumnNamePrefix(shadowIdxId);

            if (originIdxId == tbl.getBaseIndexId()) {
                // set base index
                tbl.setBaseIndexId(shadowIdxId);
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

        //update max column unique id
        int maxColUniqueId = tbl.getMaxColUniqueId();
        for (Column column : tbl.getFullSchema()) {
            if (column.getUniqueId() > maxColUniqueId) {
                maxColUniqueId = column.getUniqueId();
            }
        }
        tbl.setMaxColUniqueId(maxColUniqueId);
        LOG.debug("fullSchema:{}, maxColUniqueId:{}", tbl.getFullSchema(), maxColUniqueId);


        tbl.setState(OlapTableState.NORMAL);
        tbl.lastSchemaUpdateTime.set(System.nanoTime());
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

        cancelInternal();

        pruneMeta();
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        LOG.info("cancel {} job {}, err: {}", this.type, jobId, errMsg);
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        span.setStatus(StatusCode.ERROR, errMsg);
        span.end();
        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER);
        // remove all shadow indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.WRITE);
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                        PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                        Preconditions.checkNotNull(partition, partitionId);

                        Map<Long, MaterializedIndex> shadowIndexMap = physicalPartitionIndexMap.row(partitionId);
                        for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                            MaterializedIndex shadowIdx = entry.getValue();
                            for (Tablet shadowTablet : shadowIdx.getTablets()) {
                                invertedIndex.deleteTablet(shadowTablet.getId());
                            }
                            partition.deleteRollupIndex(shadowIdx.getId());
                        }
                    }
                    for (String shadowIndexName : indexIdToName.values()) {
                        tbl.deleteIndexInfo(shadowIndexName);
                    }
                    tbl.setState(OlapTableState.NORMAL);
                }
            } finally {
                locker.unLockDatabase(db, LockType.WRITE);
            }
        }

        jobState = JobState.CANCELLED;
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in SchemaChangeHandler.createJob()
     */
    private void replayPending(SchemaChangeJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }

            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
            for (Cell<Long, Long, MaterializedIndex> cell : physicalPartitionIndexMap.cellSet()) {
                long partitionId = cell.getRowKey();
                long shadowIndexId = cell.getColumnKey();
                MaterializedIndex shadowIndex = cell.getValue();
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);

                TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partition.getParentId()).getStorageMedium();
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId,
                        indexSchemaVersionAndHashMap.get(shadowIndexId).schemaHash, medium);

                for (Tablet shadownTablet : shadowIndex.getTablets()) {
                    invertedIndex.addTablet(shadownTablet.getId(), shadowTabletMeta);
                    for (Replica shadowReplica : ((LocalTablet) shadownTablet).getImmutableReplicas()) {
                        invertedIndex.addReplica(shadownTablet.getId(), shadowReplica);
                    }
                }
            }

            // set table state
            tbl.setState(OlapTableState.SCHEMA_CHANGE);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            addShadowIndexToCatalog(tbl);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.WRITE);
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    onFinished(tbl);
                    tbl.onReload();
                }
            } finally {
                locker.unLockDatabase(db, LockType.WRITE);
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
            info.add(indexSchemaVersionAndHashMap.get(shadowIndexId).toString());
            info.add(watershedTxnId);
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
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

    /**
     * read data need to persist when job not finish
     */
    private void readJobNotFinishData(DataInput in) throws IOException {
        int partitionNum = in.readInt();
        for (int i = 0; i < partitionNum; i++) {
            long partitionId = in.readLong();
            int indexNum = in.readInt();
            for (int j = 0; j < indexNum; j++) {
                long shadowIndexId = in.readLong();
                int tabletNum = in.readInt();
                Map<Long, Long> tabletMap = Maps.newHashMapWithExpectedSize(tabletNum);
                for (int k = 0; k < tabletNum; k++) {
                    long shadowTabletId = in.readLong();
                    long originTabletId = in.readLong();
                    tabletMap.put(shadowTabletId, originTabletId);
                }
                physicalPartitionIndexTabletMap.put(partitionId, shadowIndexId, tabletMap);
                // shadow index
                MaterializedIndex shadowIndex = MaterializedIndex.read(in);
                physicalPartitionIndexMap.put(partitionId, shadowIndexId, shadowIndex);
            }
        }

        // shadow index info
        int indexNum = in.readInt();
        for (int i = 0; i < indexNum; i++) {
            long shadowIndexId = in.readLong();
            long originIndexId = in.readLong();
            String indexName = Text.readString(in);
            // index schema
            int colNum = in.readInt();
            List<Column> schema = Lists.newArrayListWithCapacity(colNum);
            for (int j = 0; j < colNum; j++) {
                schema.add(Column.read(in));
            }
            int schemaVersion = in.readInt();
            int schemaVersionHash = in.readInt();
            SchemaVersionAndHash schemaVersionAndHash = new SchemaVersionAndHash(schemaVersion, schemaVersionHash);
            short shortKeyCount = in.readShort();

            indexIdMap.put(shadowIndexId, originIndexId);
            indexIdToName.put(shadowIndexId, indexName);
            indexSchemaMap.put(shadowIndexId, schema);
            indexSchemaVersionAndHashMap.put(shadowIndexId, schemaVersionAndHash);
            indexShortKeyMap.put(shadowIndexId, shortKeyCount);
        }

        // bloom filter
        hasBfChange = in.readBoolean();
        if (hasBfChange) {
            int bfNum = in.readInt();
            bfColumns = Sets.newHashSetWithExpectedSize(bfNum);
            for (int i = 0; i < bfNum; i++) {
                bfColumns.add(Text.readString(in));
            }
            bfFpp = in.readDouble();
        }

        watershedTxnId = in.readLong();

        // index
        indexChange = in.readBoolean();
        if (indexChange) {
            if (in.readBoolean()) {
                int indexCount = in.readInt();
                this.indexes = new ArrayList<>();
                for (int i = 0; i < indexCount; ++i) {
                    this.indexes.add(Index.read(in));
                }
            } else {
                this.indexes = null;
            }
        }

        Text.readString(in); //placeholder
    }

    /**
     * read data need to persist when job finished
     */
    private void readJobFinishedData(DataInput in) throws IOException {
        // shadow index info
        int indexNum = in.readInt();
        for (int i = 0; i < indexNum; i++) {
            long shadowIndexId = in.readLong();
            long originIndexId = in.readLong();
            String indexName = Text.readString(in);
            int schemaVersion = in.readInt();
            int schemaVersionHash = in.readInt();
            SchemaVersionAndHash schemaVersionAndHash = new SchemaVersionAndHash(schemaVersion, schemaVersionHash);

            indexIdMap.put(shadowIndexId, originIndexId);
            indexIdToName.put(shadowIndexId, indexName);
            indexSchemaVersionAndHashMap.put(shadowIndexId, schemaVersionAndHash);
        }

        // bloom filter
        hasBfChange = in.readBoolean();
        if (hasBfChange) {
            int bfNum = in.readInt();
            bfColumns = Sets.newHashSetWithExpectedSize(bfNum);
            for (int i = 0; i < bfNum; i++) {
                bfColumns.add(Text.readString(in));
            }
            bfFpp = in.readDouble();
        }

        watershedTxnId = in.readLong();

        // index
        indexChange = in.readBoolean();
        if (indexChange) {
            if (in.readBoolean()) {
                int indexCount = in.readInt();
                this.indexes = new ArrayList<>();
                for (int i = 0; i < indexCount; ++i) {
                    this.indexes.add(Index.read(in));
                }
            } else {
                this.indexes = null;
            }
        }

        Text.readString(in); //placeholder
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        boolean isMetaPruned = in.readBoolean();
        if (isMetaPruned) {
            readJobFinishedData(in);
        } else {
            readJobNotFinishData(in);
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
