// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.journal.JournalTask;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * Version 2 of SchemaChangeJob.
 * This is for replacing the old SchemaChangeJob
 * https://github.com/apache/incubator-doris/issues/1429
 */
public class SchemaChangeJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2.class);

    // partition id -> (shadow index id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    private Table<Long, Long, Map<Long, Long>> partitionIndexTabletMap = HashBasedTable.create();
    // partition id -> (shadow index id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
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
    @SerializedName(value = "storageFormat")
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;
    @SerializedName(value = "startTime")
    private long startTime;
    @SerializedName(value = "sortKeyIdxes")
    private List<Integer> sortKeyIdxes;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();

    public SchemaChangeJobV2(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    private SchemaChangeJobV2() {
        super(JobType.SCHEMA_CHANGE);
    }

    public void addTabletIdMap(long partitionId, long shadowIdxId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = partitionIndexTabletMap.get(partitionId, shadowIdxId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            partitionIndexTabletMap.put(partitionId, shadowIdxId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    public void addPartitionShadowIndex(long partitionId, long shadowIdxId, MaterializedIndex shadowIdx) {
        partitionIndexMap.put(partitionId, shadowIdxId, shadowIdx);
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

    public void setStorageFormat(TStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    public void setSortKeyIdxes(List<Integer> sortKeyIdxes) {
        this.sortKeyIdxes = sortKeyIdxes;
    }

    /**
     * clear some date structure in this job to save memory
     * these data structures must not used in getInfo method
     */
    private void pruneMeta() {
        partitionIndexTabletMap.clear();
        partitionIndexMap.clear();
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
        for (MaterializedIndex shadowIdx : partitionIndexMap.values()) {
            for (Tablet tablet : shadowIdx.getTablets()) {
                totalReplicaNum += ((LocalTablet) tablet).getImmutableReplicas().size();
            }
        }
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(totalReplicaNum);
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }

            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            MaterializedIndexMeta index = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                TStorageMedium storageMedium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                    List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    long originIndexId = indexIdMap.get(shadowIdxId);
                    int originSchemaHash = tbl.getSchemaHashByIndexId(originIndexId);
                    KeysType originKeysType = tbl.getKeysTypeByIndexId(originIndexId);
                    List<Column> originSchema = tbl.getSchemaByIndexId(originIndexId);

                    // copy for generate some const default value
                    List<Column> copiedShadowSchema = Lists.newArrayList();
                    for (Column column : shadowSchema) {
                        Column.DefaultValueType defaultValueType = column.getDefaultValueType();
                        if (defaultValueType == Column.DefaultValueType.CONST) {
                            Column copiedColumn = new Column(column);
                            copiedColumn.setDefaultValue(column.calculatedDefaultValueWithTime(startTime));
                            copiedShadowSchema.add(copiedColumn);
                        } else {
                            copiedShadowSchema.add(column);
                        }
                    }

                    List<Integer> copiedSortKeyIdxes = index.getSortKeyIdxes();
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
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        List<Replica> shadowReplicas = ((LocalTablet) shadowTablet).getImmutableReplicas();
                        for (Replica shadowReplica : shadowReplicas) {
                            long backendId = shadowReplica.getBackendId();
                            countDownLatch.addMark(backendId, shadowTabletId);
                            CreateReplicaTask createReplicaTask = new CreateReplicaTask(
                                    backendId, dbId, tableId, partitionId, shadowIdxId, shadowTabletId,
                                    shadowShortKeyColumnCount, shadowSchemaHash,
                                    Partition.PARTITION_INIT_VERSION,
                                    originKeysType, TStorageType.COLUMN, storageMedium,
                                    copiedShadowSchema, bfColumns, bfFpp, countDownLatch, indexes,
                                    tbl.isInMemory(),
                                    tbl.enablePersistentIndex(),
                                    tbl.getPartitionInfo().getTabletType(partitionId),
                                    tbl.getCompressionType(), copiedSortKeyIdxes);
                            createReplicaTask.setBaseTablet(
                                    partitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId),
                                    originSchemaHash);
                            if (this.storageFormat != null) {
                                createReplicaTask.setStorageFormat(this.storageFormat);
                            }

                            batchTask.addTask(createReplicaTask);
                        } // end for rollupReplicas
                    } // end for rollupTablets
                }
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
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);
            addShadowIndexToCatalog(tbl);
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
        LOG.info("transfer schema change job {} state to {}, watershed txn_id: {}", jobId, this.jobState,
                watershedTxnId);
    }

    private void addShadowIndexToCatalog(OlapTable tbl) {
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = tbl.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
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
                    tbl.getKeysTypeByIndexId(indexIdMap.get(shadowIdxId)), null, sortKeyIdxes);
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

        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    long originIdxId = indexIdMap.get(shadowIdxId);
                    int shadowSchemaHash = indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash;
                    int originSchemaHash = tbl.getSchemaHashByIndexId(indexIdMap.get(shadowIdxId));

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId = partitionIndexTabletMap.get(partitionId, shadowIdxId).get(shadowTabletId);
                        for (Replica shadowReplica : ((LocalTablet) shadowTablet).getImmutableReplicas()) {
                            AlterReplicaTask rollupTask = AlterReplicaTask.alterLocalTablet(
                                    shadowReplica.getBackendId(), dbId, tableId, partitionId,
                                    shadowIdxId, shadowTabletId, originTabletId, shadowReplica.getId(),
                                    shadowSchemaHash, originSchemaHash, visibleVersion, jobId);
                            schemaChangeBatchTask.addTask(rollupTask);
                        }
                    }
                }
            } // end for partitions
        } finally {
            db.readUnlock();
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

        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
        } finally {
            db.readUnlock();
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
        JournalTask journalTask;
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
            Preconditions.checkState(tbl.getState() == OlapTableState.SCHEMA_CHANGE);

            Set<String> modifiedColumns = getModifiedColumns(tbl);

            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = tbl.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                long visibleVersion = partition.getVisibleVersion();
                short expectReplicationNum = tbl.getPartitionInfo().getReplicationNum(partition.getId());

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        // Mark schema changed tablet not to move to trash.
                        long baseTabletId = partitionIndexTabletMap.get(
                                                    partitionId, shadowIdxId).get(shadowTablet.getId());
                        GlobalStateMgr.getCurrentInvertedIndex().
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

            // all partitions are good
            onFinished(tbl);

            // If schema changes include fields which defined in related mv, set those mv state to inactive.
            inactiveRelatedMv(modifiedColumns, tbl);

            pruneMeta();
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();

            journalTask = editLog.logAlterJobNoWait(this);
        } finally {
            db.writeUnlock();
        }

        EditLog.waitInfinity(journalTask);

        LOG.info("schema change job finished: {}", jobId);
        this.span.end();
    }

    private Set<String> getModifiedColumns(OlapTable tbl) {
        if (tbl.getRelatedMaterializedViews().isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Entry<Long, List<Column>> entry : indexSchemaMap.entrySet()) {
            for (Column col : entry.getValue()) {
                if (col.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                    modifiedColumns.add(col.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX));
                }
            }
        }
        return modifiedColumns;
    }

    private void inactiveRelatedMv(Set<String> modifiedColumns, OlapTable tbl) {
        if (modifiedColumns.isEmpty()) {
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        for (MvId mvId : tbl.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) db.getTable(mvId.getId());
            if (mv == null) {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
                continue;
            }
            for (Column mvColumn : mv.getColumns()) {
                if (modifiedColumns.contains(mvColumn.getName())) {
                    LOG.warn("Setting the materialized view {}({}) to invalid because " +
                            "the column {} of the table {} was modified.", mv.getName(), mv.getId(),
                            tbl.getName(), mvColumn.getName());
                    mv.setInactiveAndReason(String.format("column changed: %s.%s", tbl.getName(), mvColumn.getName()));
                }
            }
        }
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
            TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium();
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                long shadowIdxId = entry.getKey();
                long originIdxId = entry.getValue();
                // get index from globalStateMgr, not from 'partitionIdToRollupIndex'.
                // because if this alter job is recovered from edit log, index in 'partitionIndexMap'
                // is not the same object in globalStateMgr. So modification on that index can not reflect to the index
                // in globalStateMgr.
                MaterializedIndex shadowIdx = partition.getIndex(shadowIdxId);
                Preconditions.checkNotNull(shadowIdx, shadowIdxId);
                MaterializedIndex droppedIdx = null;
                if (originIdxId == partition.getBaseIndex().getId()) {
                    droppedIdx = partition.getBaseIndex();
                } else {
                    droppedIdx = partition.deleteRollupIndex(originIdxId);
                }
                Preconditions.checkNotNull(droppedIdx, originIdxId + " vs. " + shadowIdxId);

                // Add to TabletInvertedIndex.
                // Even thought we have added the tablet to TabletInvertedIndex on pending state, but the pending state
                // log may be replayed to the image, and the image will not persist the TabletInvertedIndex. So we
                // should add the tablet to TabletInvertedIndex again on finish state.
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partition.getId(), shadowIdxId,
                        indexSchemaVersionAndHashMap.get(shadowIdxId).schemaHash, medium);
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                        // set the replica state from ReplicaState.ALTER to ReplicaState.NORMAL since the schema change is done.
                        replica.setState(ReplicaState.NORMAL);
                        invertedIndex.addReplica(tablet.getId(), replica);
                    }
                }

                partition.visualiseShadowIndex(shadowIdxId, originIdxId == partition.getBaseIndex().getId());

                // the origin tablet created by old schema can be deleted from FE meta data
                for (Tablet originTablet : droppedIdx.getTablets()) {
                    GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(originTablet.getId());
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

        // set storage format of table, only set if format is v2
        if (storageFormat == TStorageFormat.V2) {
            tbl.setStorageFormat(storageFormat);
        }

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
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    for (long partitionId : partitionIndexMap.rowKeySet()) {
                        Partition partition = tbl.getPartition(partitionId);
                        Preconditions.checkNotNull(partition, partitionId);

                        Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
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
                db.writeUnlock();
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

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }

            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
            for (Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
                long partitionId = cell.getRowKey();
                long shadowIndexId = cell.getColumnKey();
                MaterializedIndex shadowIndex = cell.getValue();

                TStorageMedium medium = tbl.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
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
            db.writeUnlock();
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

        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // table may be dropped before replaying this log. just return
                return;
            }
            addShadowIndexToCatalog(tbl);
        } finally {
            db.writeUnlock();
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
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(tableId);
                if (tbl != null) {
                    onFinished(tbl);
                }
            } finally {
                db.writeUnlock();
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
        String progress = FeConstants.null_string;
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
                partitionIndexTabletMap.put(partitionId, shadowIndexId, tabletMap);
                // shadow index
                MaterializedIndex shadowIndex = MaterializedIndex.read(in);
                partitionIndexMap.put(partitionId, shadowIndexId, shadowIndex);
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
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_70) {
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
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_84) {
            storageFormat = TStorageFormat.valueOf(Text.readString(in));
        }
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

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_84) {
            storageFormat = TStorageFormat.valueOf(Text.readString(in));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    /**
     * This method is only used to deserialize the text mate which version is less then 86.
     * If the meta version >=86, it will be deserialized by the `read` of AlterJobV2 rather then here.
     */
    public static SchemaChangeJobV2 read(DataInput in) throws IOException {
        Preconditions.checkState(GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_86);
        SchemaChangeJobV2 schemaChangeJob = new SchemaChangeJobV2();
        schemaChangeJob.readFields(in);
        return schemaChangeJob;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_80) {
            boolean isMetaPruned = in.readBoolean();
            if (isMetaPruned) {
                readJobFinishedData(in);
            } else {
                readJobNotFinishData(in);
            }
        } else {
            readJobNotFinishData(in);
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
