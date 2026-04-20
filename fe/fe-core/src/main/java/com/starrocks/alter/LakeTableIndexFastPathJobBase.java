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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for the lake-only ADD INDEX / DROP INDEX fast-path jobs.
 *
 * <p>These jobs skip shadow-index creation entirely: they dispatch an
 * {@link AlterReplicaTask} per live tablet carrying a fast-path flag
 * (only_add_index / only_drop_index) that BE's
 * {@code SchemaChangeHandler::do_process_add_index_only} /
 * {@code do_process_drop_index_only} consume. Once all replicas finish, the
 * job publishes a new tablet metadata version via the standard lake
 * publish path and flips the FE catalog state (index list + bloom-filter
 * column flags) in a single write lock.
 *
 * <p>Subclasses plug in two concrete pieces via the hook methods
 * {@link #populateAlterRequest(AlterReplicaTask)} and
 * {@link #applyCatalogMutation(OlapTable)}. Everything else — txn watershed,
 * replica task dispatch, timeout, publish, persist/replay — is shared.
 */
public abstract class LakeTableIndexFastPathJobBase extends AlterJobV2 {

    private static final Logger LOG = LogManager.getLogger(LakeTableIndexFastPathJobBase.class);

    /**
     * Txn id watershed used for {@code runWaitingTxnJob}: loads with a
     * smaller txn id must complete before the index change is visible.
     */
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;

    /**
     * Tablet IDs grouped by physicalPartitionId. Populated at runPendingJob
     * and consumed at runRunningJob so the dispatch is driven by a stable
     * snapshot even if the table is concurrently modified.
     */
    @SerializedName(value = "partitionToTablets")
    protected Map<Long, List<Long>> partitionToTablets = new HashMap<>();

    /**
     * indexMetaId (materialized index) per tablet id. Needed when building
     * {@link AlterReplicaTask#alterLakeTablet(long, long, long, long, long,
     * long, long, long, long, long,
     * com.starrocks.thrift.TAlterTabletMaterializedColumnReq,
     * com.starrocks.thrift.TTabletSchema)}.
     */
    @SerializedName(value = "tabletToIndexMetaId")
    protected Map<Long, Long> tabletToIndexMetaId = new HashMap<>();

    /**
     * Commit version per physical partition; populated at the transition to
     * FINISHED_REWRITING and consumed by lakePublishVersion.
     */
    @SerializedName(value = "commitVersionMap")
    protected Map<Long, Long> commitVersionMap = new HashMap<>();

    /** AgentBatchTask holding all in-flight AlterReplicaTasks. */
    protected transient AgentBatchTask batchTask;

    protected LakeTableIndexFastPathJobBase(JobType type) {
        super(type);
    }

    protected LakeTableIndexFastPathJobBase(long jobId, JobType jobType, long dbId, long tableId, String tableName,
                                            long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    protected LakeTableIndexFastPathJobBase(LakeTableIndexFastPathJobBase other) {
        super(other);
        this.watershedTxnId = other.watershedTxnId;
        this.partitionToTablets = other.partitionToTablets == null ? null : new HashMap<>(other.partitionToTablets);
        this.tabletToIndexMetaId = other.tabletToIndexMetaId == null ? null : new HashMap<>(other.tabletToIndexMetaId);
        this.commitVersionMap = other.commitVersionMap == null ? null : new HashMap<>(other.commitVersionMap);
    }

    // ---------------------------------------------------------------------
    // Hooks for subclasses
    // ---------------------------------------------------------------------

    /**
     * Flag the task with the appropriate fast-path payload. Called once per
     * AlterReplicaTask before the task is added to the batch.
     */
    protected abstract void populateAlterRequest(AlterReplicaTask task);

    /**
     * Mutate the FE catalog to reflect the applied index change. Runs
     * under the table write lock at the FINISHED_REWRITING transition.
     */
    protected abstract void applyCatalogMutation(OlapTable table);

    /**
     * Copy any subclass-specific fields into {@code copy}. Invoked from
     * {@link #copyForPersist()} after the base fields are copied.
     */
    protected abstract void copySubclassFields(LakeTableIndexFastPathJobBase copy);

    // ---------------------------------------------------------------------
    // AlterJobV2 lifecycle
    // ---------------------------------------------------------------------

    @Override
    protected void runPendingJob() throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database does not exist, dbId: " + dbId);
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), tableId);
            if (table == null) {
                throw new AlterCancelException("table does not exist, tableId: " + tableId);
            }
            // Collect every live tablet under every physical partition.
            // Snapshot is immutable for the remainder of the job.
            for (PhysicalPartition pp : table.getAllPhysicalPartitions()) {
                List<Long> tabletIds = new ArrayList<>();
                for (MaterializedIndex idx : pp.getAllMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                    long indexMetaId = idx.getId();
                    for (Tablet tablet : idx.getTablets()) {
                        tabletIds.add(tablet.getId());
                        tabletToIndexMetaId.put(tablet.getId(), indexMetaId);
                    }
                }
                if (!tabletIds.isEmpty()) {
                    partitionToTablets.put(pp.getId(), tabletIds);
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        }

        // Allocate a watershed txn id so we only start dispatching after all
        // earlier in-flight loads against this table have finished. Reusing
        // the standard allocator keeps us consistent with
        // LakeTableAlterMetaJobBase's semantics.
        if (this.watershedTxnId == -1) {
            this.watershedTxnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTransactionIDGenerator().getNextTransactionId();
            persistStateChange(this, this.jobState);
        }
        this.jobState = JobState.WAITING_TXN;
        LOG.info("index fast-path job {} moved to WAITING_TXN, watershed={}", jobId, watershedTxnId);
    }

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        // Poll the txn manager: all txn ids <= watershed must be closed
        // before we flip tablet state. Same guarantee as
        // LakeTableSchemaChangeJob uses before dispatching tasks.
        try {
            if (!GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().isPreviousTransactionsFinished(
                    watershedTxnId, dbId, Lists.newArrayList(tableId))) {
                return;
            }
        } catch (Exception e) {
            throw new AlterCancelException(e.getMessage());
        }
        this.jobState = JobState.RUNNING;
        LOG.info("index fast-path job {} moved to RUNNING", jobId);
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        if (batchTask == null) {
            batchTask = new AgentBatchTask();
            dispatchAllTasks();
        }
        if (!batchTask.isFinished()) {
            // Still in flight; wait for the next scheduler tick.
            if (isTimeout()) {
                throw new AlterCancelException("alter timeout after "
                        + timeoutMs + " ms, still " + batchTask.getTaskNum() + " task(s) in flight");
            }
            return;
        }
        // All tasks finished. Check for failures.
        if (batchTask.getFinishedTaskNum() != batchTask.getTaskNum()) {
            throw new AlterCancelException(
                    "some tablets failed the fast-path index alter; see BE logs for details");
        }
        // Compute commit versions for every physical partition; persist the
        // state transition so replay picks up after a crash.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database does not exist, dbId: " + dbId);
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.WRITE);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), tableId);
            if (table == null) {
                throw new AlterCancelException("table does not exist, tableId: " + tableId);
            }
            commitVersionMap.clear();
            for (Long ppId : partitionToTablets.keySet()) {
                PhysicalPartition pp = table.getPhysicalPartition(ppId);
                Preconditions.checkNotNull(pp, ppId);
                long commitVersion = pp.getNextVersion();
                commitVersionMap.put(ppId, commitVersion);
            }
            this.finishedTimeMs = System.currentTimeMillis();
            persistStateChange(this, JobState.FINISHED_REWRITING, () -> updateNextVersion(table));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.WRITE);
        }
    }

    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        // Publish the new tablet metadata version per partition (lake
        // semantics) then mutate the FE catalog.
        if (!publishVersion()) {
            // publishVersion() returns false while the async task hasn't
            // completed yet; come back on the next scheduler tick.
            if (isTimeout()) {
                throw new AlterCancelException("publish timeout after " + timeoutMs + " ms");
            }
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database does not exist, dbId: " + dbId);
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.WRITE);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), tableId);
            if (table == null) {
                throw new AlterCancelException("table does not exist, tableId: " + tableId);
            }
            applyCatalogMutation(table);
            table.setState(OlapTable.OlapTableState.NORMAL);
            this.finishedTimeMs = System.currentTimeMillis();
            persistStateChange(this, JobState.FINISHED);
            LOG.info("index fast-path job {} finished", jobId);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.WRITE);
        }
    }

    @Override
    protected boolean lakePublishVersion() {
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                return false;
            }
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), tableId);
            if (table == null) {
                return false;
            }
            // Publish each physical partition at its commit version. Utils
            // publishVersion handles the per-tablet PublishVersionTask fan-out
            // and collects results; a single boolean return drives the
            // AlterJobV2 state machine.
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = System.currentTimeMillis() / 1000;
            txnInfo.txnType = TxnTypePB.TXN_NORMAL;
            for (Map.Entry<Long, Long> e : commitVersionMap.entrySet()) {
                PhysicalPartition pp = table.getPhysicalPartition(e.getKey());
                if (pp == null) {
                    LOG.warn("partition {} disappeared during publish; job {}", e.getKey(), jobId);
                    return false;
                }
                long commitVersion = e.getValue();
                Utils.publishVersion(pp.getLatestBaseIndex().getTablets(), txnInfo, commitVersion - 1,
                        commitVersion, computeResource, false);
            }
            return true;
        } catch (Exception e) {
            LOG.warn("publish failed for index fast-path job {}: {}", jobId, e.getMessage(), e);
            return false;
        }
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }
        if (batchTask != null) {
            for (com.starrocks.task.AgentTask task : batchTask.getAllTasks()) {
                AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.ALTER, task.getSignature());
            }
        }
        this.errMsg = errMsg == null ? "" : errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        // Reset table state so subsequent alters are not blocked.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.WRITE);
            try {
                OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), tableId);
                if (table != null && table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                    table.setState(OlapTable.OlapTableState.NORMAL);
                }
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.WRITE);
            }
        }
        persistStateChange(this, JobState.CANCELLED);
        LOG.info("index fast-path job {} cancelled: {}", jobId, errMsg);
        return true;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // Column layout must match SchemaChangeProcDir.TITLE_NAMES. There is
        // no shadow index in this fast path, so IndexId / OriginIndexId are
        // emitted as -1 and the schema-version placeholder is "0:0".
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        info.add(""); // IndexName
        info.add(-1L); // IndexId (no shadow index)
        info.add(-1L); // OriginIndexId
        info.add("0:0"); // SchemaVersion
        info.add(watershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        info.add(FeConstants.NULL_STRING); // Progress
        info.add(timeoutMs / 1000);
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getWarehouseAllowNull(warehouseId);
            info.add(warehouse == null ? "null" : warehouse.getName());
        }
        infos.add(info);
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId == -1 ? Optional.empty() : Optional.of(watershedTxnId);
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        LakeTableIndexFastPathJobBase other = (LakeTableIndexFastPathJobBase) replayedJob;
        this.jobState = other.jobState;
        this.watershedTxnId = other.watershedTxnId;
        this.partitionToTablets = other.partitionToTablets;
        this.tabletToIndexMetaId = other.tabletToIndexMetaId;
        this.commitVersionMap = other.commitVersionMap;
        this.errMsg = other.errMsg;
        this.finishedTimeMs = other.finishedTimeMs;
        if (jobState == JobState.FINISHED) {
            // Reapply catalog mutation at replay so a cold-started FE sees
            // the post-alter table state. The mutation is idempotent by
            // design (add checks presence, drop is no-op on missing).
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db != null) {
                OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), tableId);
                if (table != null) {
                    applyCatalogMutation(table);
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private void dispatchAllTasks() throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("database does not exist, dbId: " + dbId);
        }
        WarehouseManager wm = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), tableId);
            if (table == null) {
                throw new AlterCancelException("table does not exist, tableId: " + tableId);
            }
            // Cache one TTabletSchema per index meta id; alterLakeTablet's
            // constructor checkNotNull-s it, and recomputing per tablet would
            // churn protobuf serialization for no reason.
            Map<Long, TTabletSchema> schemaCache = new HashMap<>();
            for (Map.Entry<Long, List<Long>> e : partitionToTablets.entrySet()) {
                long ppId = e.getKey();
                PhysicalPartition pp = table.getPhysicalPartition(ppId);
                if (pp == null) {
                    throw new AlterCancelException("partition disappeared: " + ppId);
                }
                long visibleVersion = pp.getVisibleVersion();
                for (Long tabletId : e.getValue()) {
                    ComputeNode cn = wm.getComputeNodeAssignedToTablet(computeResource, tabletId);
                    if (cn == null) {
                        throw new AlterCancelException("no alive backend for tablet " + tabletId);
                    }
                    Long indexMetaId = tabletToIndexMetaId.get(tabletId);
                    Preconditions.checkNotNull(indexMetaId, tabletId);
                    TTabletSchema readSchema = schemaCache.computeIfAbsent(indexMetaId, id ->
                            SchemaInfo.fromMaterializedIndex(table, id, table.getIndexMetaByMetaId(id))
                                    .toTabletSchema());
                    // For the fast path, shadow tablet == origin tablet and
                    // shadow index == origin index (no shadow created).
                    AlterReplicaTask task = AlterReplicaTask.alterLakeTablet(cn.getId(), dbId, tableId, ppId,
                            indexMetaId, tabletId, tabletId, visibleVersion, jobId, watershedTxnId,
                            /*generatedColumnReq=*/ null, readSchema);
                    populateAlterRequest(task);
                    batchTask.addTask(task);
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tableId), LockType.READ);
        }
        // Register tasks in AgentTaskQueue *before* submitting so that
        // LeaderImpl.finishTask() can find them when CNs report back.
        // Without this, CN reports arrive as "cannot find task" warnings and
        // the batchTask never transitions to finished.
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
        LOG.info("index fast-path job {} dispatched {} AlterReplicaTasks", jobId, batchTask.getTaskNum());
    }

    private void updateNextVersion(OlapTable table) {
        for (Map.Entry<Long, Long> e : commitVersionMap.entrySet()) {
            PhysicalPartition pp = table.getPhysicalPartition(e.getKey());
            if (pp != null) {
                pp.setNextVersion(e.getValue() + 1);
            }
        }
    }

    // Per-column is_bf_column is no longer a first-class Column attribute;
    // bloom-filter columns are tracked at the OlapTable level via bfColumns,
    // so add/drop subclasses do not need a shared helper here today.
}
