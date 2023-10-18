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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/RollupJobV2.java

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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
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
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SelectAnalyzer;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Version 2 of RollupJob.
 * This is for replacing the old RollupJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class RollupJobV2 extends AlterJobV2 implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(RollupJobV2.class);

    // partition id -> (rollup tablet id -> base tablet id)
    @SerializedName(value = "partitionIdToBaseRollupTabletIdMap")
    private Map<Long, Map<Long, Long>> partitionIdToBaseRollupTabletIdMap = Maps.newHashMap();
    @SerializedName(value = "partitionIdToRollupIndex")
    private Map<Long, MaterializedIndex> partitionIdToRollupIndex = Maps.newHashMap();

    // rollup and base schema info
    @SerializedName(value = "baseIndexId")
    private long baseIndexId;
    @SerializedName(value = "rollupIndexId")
    private long rollupIndexId;
    @SerializedName(value = "baseIndexName")
    private String baseIndexName;
    @SerializedName(value = "rollupIndexName")
    private String rollupIndexName;

    @SerializedName(value = "rollupSchema")
    private List<Column> rollupSchema = Lists.newArrayList();
    @SerializedName(value = "baseSchemaHash")
    private int baseSchemaHash;
    @SerializedName(value = "rollupSchemaHash")
    private int rollupSchemaHash;

    @SerializedName(value = "rollupKeysType")
    private KeysType rollupKeysType;
    @SerializedName(value = "rollupShortKeyColumnCount")
    private short rollupShortKeyColumnCount;
    @SerializedName(value = "origStmt")
    private OriginStatement origStmt;

    // The rollup job will wait all transactions before this txn id finished, then send the rollup tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;
    @SerializedName(value = "isColocateMVIndex")
    protected boolean isColocateMVIndex = false;

    // save all create rollup tasks
    private AgentBatchTask rollupBatchTask = new AgentBatchTask();

    public RollupJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs,
                       long baseIndexId, long rollupIndexId, String baseIndexName, String rollupIndexName,
                       List<Column> rollupSchema, int baseSchemaHash, int rollupSchemaHash, KeysType rollupKeysType,
                       short rollupShortKeyColumnCount, OriginStatement origStmt, String viewDefineSql,
                       boolean isColocateMVIndex) {
        super(jobId, JobType.ROLLUP, dbId, tableId, tableName, timeoutMs);

        this.baseIndexId = baseIndexId;
        this.rollupIndexId = rollupIndexId;
        this.baseIndexName = baseIndexName;
        this.rollupIndexName = rollupIndexName;

        this.rollupSchema = rollupSchema;
        this.baseSchemaHash = baseSchemaHash;
        this.rollupSchemaHash = rollupSchemaHash;
        this.rollupKeysType = rollupKeysType;
        this.rollupShortKeyColumnCount = rollupShortKeyColumnCount;

        this.origStmt = origStmt;
        this.viewDefineSql = viewDefineSql;
        this.isColocateMVIndex = isColocateMVIndex;
    }

    private RollupJobV2() {
        super(JobType.ROLLUP);
    }

    public void addTabletIdMap(long partitionId, long rollupTabletId, long baseTabletId) {
        Map<Long, Long> tabletIdMap =
                partitionIdToBaseRollupTabletIdMap.computeIfAbsent(partitionId, k -> Maps.newHashMap());
        tabletIdMap.put(rollupTabletId, baseTabletId);
    }

    public void addMVIndex(long partitionId, MaterializedIndex mvIndex) {
        this.partitionIdToRollupIndex.put(partitionId, mvIndex);
    }

    public String getRollupIndexName() {
        return rollupIndexName;
    }

    /**
     * runPendingJob():
     * 1. Create all rollup replicas and wait them finished.
     * 2. After creating done, add this shadow rollup index to globalStateMgr, user can not see this
     * rollup, but internal load process will generate data for this rollup index.
     * 3. Get a new transaction id, then set job's state to WAITING_TXN
     */
    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);

        LOG.info("begin to send create rollup replica tasks. job: {}", jobId);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        // 1. create rollup replicas
        AgentBatchTask batchTask = new AgentBatchTask();
        // count total replica num
        int totalReplicaNum = 0;
        for (MaterializedIndex rollupIdx : partitionIdToRollupIndex.values()) {
            for (Tablet tablet : rollupIdx.getTablets()) {
                totalReplicaNum += ((LocalTablet) tablet).getImmutableReplicas().size();
            }
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(totalReplicaNum);
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            MaterializedIndexMeta index = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                TTabletType tabletType = tbl.getPartitionInfo().getTabletType(partitionId);
                MaterializedIndex rollupIndex = entry.getValue();

                Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    List<Replica> rollupReplicas = ((LocalTablet) rollupTablet).getImmutableReplicas();
                    for (Replica rollupReplica : rollupReplicas) {
                        long backendId = rollupReplica.getBackendId();
                        Preconditions.checkNotNull(tabletIdMap.get(rollupTabletId)); // baseTabletId
                        countDownLatch.addMark(backendId, rollupTabletId);
                        // create replica with version 1.
                        // version will be updated by following load process, or when rollup task finished.
                        CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                backendId, dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                                rollupShortKeyColumnCount, rollupSchemaHash,
                                Partition.PARTITION_INIT_VERSION,
                                rollupKeysType, TStorageType.COLUMN, storageMedium,
                                rollupSchema, tbl.getCopiedBfColumns(), tbl.getBfFpp(), countDownLatch,
                                tbl.getCopiedIndexes(),
                                tbl.isInMemory(),
                                tbl.enablePersistentIndex(),
                                tbl.primaryIndexCacheExpireSec(),
                                tabletType, tbl.getCompressionType(), index.getSortKeyIdxes(),
                                index.getSortKeyUniqueIds());
                        createReplicaTask.setBaseTablet(tabletIdMap.get(rollupTabletId), baseSchemaHash);
                        batchTask.addTask(createReplicaTask);
                    } // end for rollupReplicas
                } // end for rollupTablets
            }
        } finally {
            db.readUnlock();
        }

        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            long timeout = Math.min(Config.tablet_create_timeout_second * 1000L * totalReplicaNum,
                    Config.max_create_table_timeout_second * 1000L);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                // create rollup replicas failed. just cancel the job
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
                LOG.warn("failed to create rollup replicas for job: {}, {}", jobId, errMsg);
                throw new AlterCancelException("Create rollup replicas failed. Error: " + errMsg);
            }
        }

        // create all rollup replicas success.
        // add rollup index to globalStateMgr
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            addRollupIndexToCatalog(tbl);
        } finally {
            db.writeUnlock();
        }

        this.watershedTxnId =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        this.jobState = JobState.WAITING_TXN;
        span.setAttribute("watershedTxnId", this.watershedTxnId);
        span.addEvent("setWaitingTxn");

        // write edit log
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        LOG.info("transfer rollup job {} state to {}, watershed txn_id: {}", jobId, this.jobState, watershedTxnId);
    }

    private void addRollupIndexToCatalog(OlapTable tbl) {
        for (Partition partition : tbl.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long partitionId = physicalPartition.getId();
                MaterializedIndex rollupIndex = this.partitionIdToRollupIndex.get(partitionId);
                Preconditions.checkNotNull(rollupIndex);
                Preconditions.checkState(rollupIndex.getState() == IndexState.SHADOW, rollupIndex.getState());
                physicalPartition.createRollupIndex(rollupIndex);
            }
        }

        tbl.setIndexMeta(rollupIndexId, rollupIndexName, rollupSchema, 0 /* initial schema version */,
                rollupSchemaHash, rollupShortKeyColumnCount, TStorageType.COLUMN, rollupKeysType, origStmt);
        MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(rollupIndexId);
        Preconditions.checkNotNull(indexMeta);
        indexMeta.setDbId(dbId);
        indexMeta.setViewDefineSql(viewDefineSql);
        indexMeta.setColocateMVIndex(isColocateMVIndex);
        tbl.rebuildFullSchema();
    }

    /**
     * runWaitingTxnJob():
     * 1. Wait the transactions before the watershedTxnId to be finished.
     * 2. If all previous transactions finished, send create rollup tasks to BE.
     * 3. Change job state to RUNNING.
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished()) {
                LOG.info("wait transactions before {} to be finished, rollup job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send rollup tasks. job: {}", jobId);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the rollup task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();

                MaterializedIndex rollupIndex = entry.getValue();
                Map<Long, Long> tabletIdMap = this.partitionIdToBaseRollupTabletIdMap.get(partitionId);
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    long rollupTabletId = rollupTablet.getId();
                    long baseTabletId = tabletIdMap.get(rollupTabletId);

                    DescriptorTable descTable = new DescriptorTable();
                    TupleDescriptor tupleDesc = descTable.createTupleDescriptor();
                    Map<String, SlotDescriptor> slotDescByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    List<Column> rollupColumns = new ArrayList<Column>();
                    Set<String> columnNames = new HashSet<String>();
                    for (Column column : tbl.getBaseSchema()) {
                        rollupColumns.add(column);
                        columnNames.add(column.getName());
                    }
                    for (Column column : rollupSchema) {
                        if (columnNames.contains(column.getName())) {
                            continue;
                        }
                        rollupColumns.add(column);
                    }

                    /*
                     * The expression substitution is needed here, because all slotRefs in
                     * definedExpr are still is unAnalyzed. slotRefs get isAnalyzed == true
                     * if it is init by SlotDescriptor. The slot information will be used by be to identify
                     * the column location in a chunk.
                     */
                    for (Column column : rollupColumns) {
                        SlotDescriptor destSlotDesc = descTable.addSlotDescriptor(tupleDesc);
                        destSlotDesc.setIsMaterialized(true);
                        destSlotDesc.setColumn(column);
                        destSlotDesc.setIsNullable(column.isAllowNull());

                        slotDescByName.put(column.getName(), destSlotDesc);
                    }

                    List<Expr> outputExprs = Lists.newArrayList();
                    for (Column col : tbl.getBaseSchema()) {
                        SlotDescriptor slotDesc = slotDescByName.get(col.getName());
                        if (slotDesc == null) {
                            throw new AlterCancelException("Expression for materialized view column can not find " +
                                    "the ref column");
                        }
                        SlotRef slotRef = new SlotRef(slotDesc);
                        slotRef.setColumnName(col.getName());
                        outputExprs.add(slotRef);
                    }

                    TableName tableName = new TableName(db.getFullName(), tbl.getName());
                    Map<String, Expr> defineExprs = Maps.newHashMap();
                    for (Column column : rollupColumns) {
                        if (column.getDefineExpr() == null) {
                            continue;
                        }

                        List<SlotRef> slots = new ArrayList<>();
                        column.getDefineExpr().collect(SlotRef.class, slots);
                        for (SlotRef slot : slots) {
                            SlotDescriptor slotDesc = slotDescByName.get(slot.getColumnName());
                            if (slotDesc == null) {
                                slotDesc = slotDescByName.get(column.getName());
                            }
                            if (slotDesc == null) {
                                throw new AlterCancelException("slotDesc is null, slot=" + slot.getColumnName()
                                        + ", column=" + column.getName());
                            }
                            slot.setDesc(slotDesc);
                        }

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
                        SelectAnalyzer.RewriteAliasVisitor visitor =
                                new SelectAnalyzer.RewriteAliasVisitor(sourceScope, outputScope,
                                        outputExprs, new ConnectContext());
                        Expr definedExpr = column.getDefineExpr().clone();
                        definedExpr = definedExpr.accept(visitor, null);
                        definedExpr = Expr.analyzeAndCastFold(definedExpr);
                        if (!definedExpr.getType().equals(column.getType())) {
                            definedExpr = new CastExpr(column.getType(), definedExpr);
                        }
                        defineExprs.put(column.getName(), definedExpr);
                    }

                    List<Replica> rollupReplicas = ((LocalTablet) rollupTablet).getImmutableReplicas();
                    for (Replica rollupReplica : rollupReplicas) {
                        AlterReplicaTask rollupTask = AlterReplicaTask.rollupLocalTablet(
                                rollupReplica.getBackendId(), dbId, tableId, partitionId, rollupIndexId, rollupTabletId,
                                baseTabletId, rollupReplica.getId(), rollupSchemaHash, baseSchemaHash, visibleVersion, jobId,
                                defineExprs);
                        rollupBatchTask.addTask(rollupTask);
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        AgentTaskQueue.addBatchTask(rollupBatchTask);
        AgentTaskExecutor.submit(rollupBatchTask);
        this.jobState = JobState.RUNNING;
        span.addEvent("setRunning");

        // DO NOT write edit log here, tasks will be send again if FE restart or master changed.
        LOG.info("transfer rollup job {} state to {}", jobId, this.jobState);
    }

    /**
     * runRunningJob()
     * 1. Wait all create rollup tasks to be finished.
     * 2. Check the integrity of the newly created rollup index.
     * 3. Set rollup index's state to NORMAL to let it visible to query.
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
            throw new AlterCancelException("Databasee " + dbId + " does not exist");
        }

        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
        } finally {
            db.readUnlock();
        }

        if (!rollupBatchTask.isFinished()) {
            LOG.info("rollup tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = rollupBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.getFailedTimes() >= 3) {
                    throw new AlterCancelException("rollup task failed after try three times: " + task.getErrorMsg());
                }
            }
            return;
        }

        /*
         * all tasks are finished. check the integrity.
         * we just check whether all rollup replicas are healthy.
         */
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
            for (Map.Entry<Long, MaterializedIndex> entry : this.partitionIdToRollupIndex.entrySet()) {
                long partitionId = entry.getKey();
                PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                if (partition == null) {
                    continue;
                }

                long visiableVersion = partition.getVisibleVersion();
                short expectReplicationNum = tbl.getPartitionInfo().getReplicationNum(partition.getId());

                MaterializedIndex rollupIndex = entry.getValue();
                for (Tablet rollupTablet : rollupIndex.getTablets()) {
                    List<Replica> replicas = ((LocalTablet) rollupTablet).getImmutableReplicas();
                    int healthyReplicaNum = 0;
                    for (Replica replica : replicas) {
                        if (replica.getLastFailedVersion() < 0
                                && replica.checkVersionCatchUp(visiableVersion, false)) {
                            healthyReplicaNum++;
                        }
                    }

                    if (healthyReplicaNum < expectReplicationNum / 2 + 1) {
                        LOG.warn("rollup tablet {} has few healthy replicas: {}, rollup job: {}",
                                rollupTablet.getId(), replicas, jobId);
                        throw new AlterCancelException(
                                "rollup tablet " + rollupTablet.getId() + " has few healthy replicas");
                    }
                } // end for tablets
            } // end for partitions

            onFinished(tbl);
        } finally {
            db.writeUnlock();
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        LOG.info("rollup job finished: {}", jobId);
        this.span.end();
    }

    @Override
    protected void runFinishedRewritingJob() {
        // nothing to do
    }

    private void onFinished(OlapTable tbl) {
        for (Partition partition : tbl.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                MaterializedIndex rollupIndex = physicalPartition.getIndex(rollupIndexId);
                Preconditions.checkNotNull(rollupIndex, rollupIndexId);
                for (Tablet tablet : rollupIndex.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                        replica.setState(ReplicaState.NORMAL);
                    }
                }
                physicalPartition.visualiseShadowIndex(rollupIndexId, false);
            }
        }
        tbl.rebuildFullSchema();
        tbl.lastSchemaUpdateTime.set(System.currentTimeMillis());
    }

    /**
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }
        cancelInternal();

        jobState = JobState.CANCELLED;
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
        AgentTaskQueue.removeBatchTask(rollupBatchTask, TTaskType.ALTER);
        // remove all rollup indexes, and set state to NORMAL
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    for (Long partitionId : partitionIdToRollupIndex.keySet()) {
                        MaterializedIndex rollupIndex = partitionIdToRollupIndex.get(partitionId);
                        for (Tablet rollupTablet : rollupIndex.getTablets()) {
                            invertedIndex.deleteTablet(rollupTablet.getId());
                        }
                        PhysicalPartition partition = tbl.getPhysicalPartition(partitionId);
                        partition.deleteRollupIndex(rollupIndexId);
                    }
                    tbl.deleteIndexInfo(rollupIndexName);
                }
            } finally {
                db.writeUnlock();
            }
        }
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    protected boolean isPreviousLoadFinished() throws AnalysisException {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .isPreviousTransactionsFinished(watershedTxnId, dbId, Lists.newArrayList(tableId));
    }

    /**
     * Replay job in PENDING state.
     * Should replay all changes before this job's state transfer to PENDING.
     * These changes should be same as changes in RollupHander.processAddRollup()
     */
    private void replayPending(RollupJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            addTabletToInvertedIndex(tbl);
        } finally {
            db.writeUnlock();
        }

        // to make sure that this job will run runPendingJob() again to create the rollup replicas
        this.jobState = JobState.PENDING;
        this.watershedTxnId = replayedJob.watershedTxnId;

        LOG.info("replay pending rollup job: {}", jobId);
    }

    private void addTabletToInvertedIndex(OlapTable tbl) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        // add all rollup replicas to tablet inverted index
        for (Long partitionId : partitionIdToRollupIndex.keySet()) {
            MaterializedIndex rollupIndex = partitionIdToRollupIndex.get(partitionId);
            TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
            TabletMeta rollupTabletMeta = new TabletMeta(dbId, tableId, partitionId, rollupIndexId,
                    rollupSchemaHash, medium);

            for (Tablet rollupTablet : rollupIndex.getTablets()) {
                invertedIndex.addTablet(rollupTablet.getId(), rollupTabletMeta);
                for (Replica rollupReplica : ((LocalTablet) rollupTablet).getImmutableReplicas()) {
                    invertedIndex.addReplica(rollupTablet.getId(), rollupReplica);
                }
            }
        }
    }

    /**
     * Replay job in WAITING_TXN state.
     * Should replay all changes in runPendingJob()
     */
    private void replayWaitingTxn(RollupJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database may be dropped before replaying this log. just return
            return;
        }

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            addRollupIndexToCatalog(tbl);
        } finally {
            db.writeUnlock();
        }

        // should still be in WAITING_TXN state, so that the alter tasks will be resend again
        this.jobState = JobState.WAITING_TXN;
        this.watershedTxnId = replayedJob.watershedTxnId;

        LOG.info("replay waiting txn rollup job: {}", jobId);
    }

    /**
     * Replay job in FINISHED state.
     * Should replay all changes in runRuningJob()
     */
    private void replayFinished(RollupJobV2 replayedJob) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    Preconditions.checkState(tbl.getState() == OlapTableState.ROLLUP);
                    onFinished(tbl);
                }
            } finally {
                db.writeUnlock();
            }
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;

        LOG.info("replay finished rollup job: {}", jobId);
    }

    /**
     * Replay job in CANCELLED state.
     */
    private void replayCancelled(RollupJobV2 replayedJob) {
        cancelInternal();
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
        this.errMsg = replayedJob.errMsg;
        LOG.info("replay cancelled rollup job: {}", jobId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        RollupJobV2 replayedRollupJob = (RollupJobV2) replayedJob;
        switch (replayedJob.jobState) {
            case PENDING:
                replayPending(replayedRollupJob);
                break;
            case WAITING_TXN:
                replayWaitingTxn(replayedRollupJob);
                break;
            case FINISHED:
                replayFinished(replayedRollupJob);
                break;
            case CANCELLED:
                replayCancelled(replayedRollupJob);
                break;
            default:
                break;
        }
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

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = rollupBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                AlterReplicaTask rollupTask = (AlterReplicaTask) agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(rollupTask.getBackendId()));
                info.add(String.valueOf(rollupTask.getBaseTabletId()));
                info.add(String.valueOf(rollupTask.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }

    public Map<Long, MaterializedIndex> getPartitionIdToRollupIndex() {
        return partitionIdToRollupIndex;
    }

    private void setColumnsDefineExpr(Map<String, Expr> columnNameToDefineExpr) {
        for (Map.Entry<String, Expr> entry : columnNameToDefineExpr.entrySet()) {
            for (Column column : rollupSchema) {
                if (column.getName().equalsIgnoreCase(entry.getKey())) {
                    column.setDefineExpr(entry.getValue());
                    break;
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long partitionId = in.readLong();
            int size2 = in.readInt();
            Map<Long, Long> tabletIdMap =
                    partitionIdToBaseRollupTabletIdMap.computeIfAbsent(partitionId, k -> Maps.newHashMap());
            for (int j = 0; j < size2; j++) {
                long rollupTabletId = in.readLong();
                long baseTabletId = in.readLong();
                tabletIdMap.put(rollupTabletId, baseTabletId);
            }

            partitionIdToRollupIndex.put(partitionId, MaterializedIndex.read(in));
        }

        baseIndexId = in.readLong();
        rollupIndexId = in.readLong();
        baseIndexName = Text.readString(in);
        rollupIndexName = Text.readString(in);

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            Column column = Column.read(in);
            rollupSchema.add(column);
        }
        baseSchemaHash = in.readInt();
        rollupSchemaHash = in.readInt();

        rollupKeysType = KeysType.valueOf(Text.readString(in));
        rollupShortKeyColumnCount = in.readShort();

        watershedTxnId = in.readLong();
        Text.readString(in); // placeholder
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // analyze define stmt
        if (origStmt == null) {
            return;
        }

        if (jobState != JobState.PENDING) {
            return;
        }

        Map<String, Expr> columnNameToDefineExpr = MetaUtils.parseColumnNameToDefineExpr(origStmt);
        setColumnsDefineExpr(columnNameToDefineExpr);
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
