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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockInterruptException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.task.RemoteSnapshotTask;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TTableReplicationRequest;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationMgr extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ReplicationMgr.class);

    @SerializedName(value = "runningJobs")
    private final Map<Long, ReplicationJob> runningJobs = Maps.newConcurrentMap(); // Running jobs

    @SerializedName(value = "committedJobs")
    private final Map<Long, ReplicationJob> committedJobs = Maps.newConcurrentMap(); // Committed jobs

    @SerializedName(value = "abortedJobs")
    private final Map<Long, ReplicationJob> abortedJobs = Maps.newConcurrentMap(); // Aborted jobs, will retry later

    private final transient Set<Long> tablesInConstruction = ConcurrentHashMap.newKeySet();

    public ReplicationMgr() {
        super("replication-mgr", Config.replication_interval_ms);
    }

    @Override
    protected void runAfterLeaseValid() {
        runRunningJobs();
        clearExpiredJobs();
    }

    // runningJobs / committedJobs / abortedJobs are persistent state (saved/loaded via image,
    // updated on followers via editlog replay). They must NOT be cleared on demotion - the
    // next leader resumes those jobs from the same maps. So no onStopped() override is needed.

    public void addReplicationJob(TTableReplicationRequest request) throws StarRocksException {
        LOG.debug("Add replication job, database id: {}, table id: {}, job id: {}",
                request.getDatabase_id(), request.getTable_id(), request.getJob_id());
        long dbId = request.getDatabase_id();
        long tableId = request.getTable_id();
        OlapTable table = getTargetTable(dbId, tableId);
        boolean reservationOwned = false;
        Throwable primaryFailure = null;
        try {
            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
            try {
                table = getTargetTable(dbId, tableId);
                checkLeaderAdmissionOpen();
                checkTableNormal(table);
                if (isTableUnderReplication(dbId, tableId)) {
                    throw new AlreadyExistsException("Replication job of table " + tableId + " is already running");
                }
                if (!tablesInConstruction.add(tableId)) {
                    throw new AlreadyExistsException("Replication job of table " + tableId + " is being constructed");
                }
                reservationOwned = true;
            } finally {
                locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
            }

            ReplicationJob job = createReplicationJob(request);

            locker = new Locker();
            locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
            try {
                table = getTargetTable(dbId, tableId);
                checkLeaderAdmissionOpen();
                checkTableNormal(table);
                if (runningJobs.containsKey(tableId) || hasActiveReplicationTransaction(dbId, tableId)) {
                    throw new AlreadyExistsException("Replication job of table " + tableId + " is already running");
                }
                checkParallelismLimits();
                if (runningJobs.putIfAbsent(tableId, job) != null) {
                    throw new AlreadyExistsException("Replication job of table " + tableId + " is already running");
                }
                tablesInConstruction.remove(tableId);
                reservationOwned = false;
                removeHistoricalJobs(tableId);
                logAddedJob(job);
            } finally {
                locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
            }
        } catch (StarRocksException | RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            if (reservationOwned) {
                try {
                    releaseConstructionReservation(dbId, tableId);
                } catch (RuntimeException | Error cleanupFailure) {
                    if (primaryFailure != null) {
                        primaryFailure.addSuppressed(cleanupFailure);
                    } else {
                        throw cleanupFailure;
                    }
                }
            }
        }
    }

    @VisibleForTesting
    protected ReplicationJob createReplicationJob(TTableReplicationRequest request) throws StarRocksException {
        return isLakeReplicationJob(request) ? new LakeReplicationJob(request) : new ReplicationJob(request);
    }

    private boolean isLakeReplicationJob(TTableReplicationRequest request) {
        return request != null && request.src_cluster_run_mode != null
                && request.src_cluster_run_mode == RunMode.toTRunMode(RunMode.SHARED_DATA);
    }

    public void addReplicationJob(ReplicationJob job) throws AlreadyExistsException {
        long dbId = job.getDatabaseId();
        long tableId = job.getTableId();
        OlapTable table;
        try {
            table = getTargetTable(dbId, tableId);
        } catch (StarRocksException e) {
            throw new AlreadyExistsException(e.getMessage());
        }
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
        try {
            try {
                table = getTargetTable(dbId, tableId);
            } catch (StarRocksException e) {
                throw new AlreadyExistsException(e.getMessage());
            }
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            if (!globalStateMgr.isLeader() || !globalStateMgr.isLeaderWorkAdmissionOpen()) {
                throw new AlreadyExistsException("Leader work admission is closed");
            }
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new AlreadyExistsException(
                        "Table " + table.getName() + " state is not NORMAL: " + table.getState());
            }
            if (isTableUnderReplication(dbId, tableId)) {
                throw new AlreadyExistsException("Replication job of table " + tableId + " is already running");
            }
            checkParallelismLimits();
            if (runningJobs.putIfAbsent(tableId, job) != null) {
                throw new AlreadyExistsException("Replication job of table " + tableId + " is already running");
            }
            removeHistoricalJobs(tableId);
            logAddedJob(job);
        } finally {
            locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
        }
    }

    public boolean isTableUnderReplication(long dbId, long tableId) {
        return tablesInConstruction.contains(tableId)
                || runningJobs.containsKey(tableId)
                || hasActiveReplicationTransaction(dbId, tableId);
    }

    private void checkParallelismLimits() {
        // Limit replication job size
        if (runningJobs.size() >= Config.replication_max_parallel_table_count) {
            throw new RuntimeException(
                    "The replication jobs exceeds the replication_max_parallel_table_count: "
                            + Config.replication_max_parallel_table_count);
        }

        // Limit replication replica count
        long replicationReplicaCount = getReplicatingReplicaCount();
        if (replicationReplicaCount >= Config.replication_max_parallel_replica_count) {
            throw new RuntimeException("The replicating replica count in all running replication jobs "
                    + replicationReplicaCount
                    + " exceeds replication_max_parallel_replica_count: "
                    + Config.replication_max_parallel_replica_count);
        }

        // Limit replication data size
        long replicatingDataSizeMB = getReplicatingDataSize() / 1048576;
        if (replicatingDataSizeMB >= Config.replication_max_parallel_data_size_mb) {
            throw new RuntimeException("The replicating data size in all running replication jobs "
                    + replicatingDataSizeMB
                    + "(MB) exceeds replication_max_parallel_data_size_mb: "
                    + Config.replication_max_parallel_data_size_mb);
        }

    }

    private void removeHistoricalJobs(long tableId) {
        committedJobs.remove(tableId);
        abortedJobs.remove(tableId);
    }

    private void logAddedJob(ReplicationJob job) {
        long replicatingDataSizeMB = getReplicatingDataSize() / 1048576;
        LOG.info("Added replication job, database id: {}, table id: {}, "
                + "replication data size: {}, current replicating data size: {}(MB)",
                job.getDatabaseId(), job.getTableId(), job.getReplicationDataSize(), replicatingDataSizeMB);
    }

    private OlapTable getTargetTable(long dbId, long tableId) throws StarRocksException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (database == null) {
            throw new StarRocksException("Database " + dbId + " does not exist");
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (!(table instanceof OlapTable)) {
            throw new StarRocksException("OLAP table " + tableId + " does not exist");
        }
        return (OlapTable) table;
    }

    private void checkLeaderAdmissionOpen() throws StarRocksException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (!globalStateMgr.isLeader() || !globalStateMgr.isLeaderWorkAdmissionOpen()) {
            throw new StarRocksException("Leader work admission is closed");
        }
    }

    private void checkTableNormal(OlapTable table) throws StarRocksException {
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new StarRocksException(
                    "Table " + table.getName() + " state is not NORMAL: " + table.getState());
        }
    }

    private boolean hasActiveReplicationTransaction(long dbId, long tableId) {
        try {
            return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getDatabaseTransactionMgr(dbId)
                    .hasActiveTransaction(tableId, TransactionState.LoadJobSourceType.REPLICATION);
        } catch (AnalysisException e) {
            LOG.warn("Cannot inspect replication transactions for database {}, reject table {} admission",
                    dbId, tableId, e);
            return true;
        }
    }

    private void releaseConstructionReservation(long dbId, long tableId) {
        boolean restoreInterrupt = Thread.interrupted();
        Locker locker = new Locker();
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        boolean dbLockHeld = false;
        boolean tableLockHeld = false;
        Throwable cleanupFailure = null;
        try {
            while (!dbLockHeld || !tableLockHeld) {
                try {
                    if (!dbLockHeld) {
                        locker.lock(dbId, LockType.INTENTION_EXCLUSIVE);
                        dbLockHeld = true;
                    }
                    if (!tableLockHeld) {
                        locker.lock(tableId, LockType.WRITE);
                        tableLockHeld = true;
                    }
                } catch (LockInterruptException e) {
                    restoreInterrupt = true;
                    Thread.interrupted();
                    dbLockHeld = dbLockHeld
                            || lockManager.isOwner(dbId, locker, LockType.INTENTION_EXCLUSIVE);
                    tableLockHeld = tableLockHeld
                            || lockManager.isOwner(tableId, locker, LockType.WRITE);
                } catch (LockException e) {
                    throw new IllegalStateException(
                            "Failed to acquire lock while releasing replication construction reservation", e);
                }
            }

            tablesInConstruction.remove(tableId);
        } catch (RuntimeException | Error failure) {
            cleanupFailure = failure;
            throw failure;
        } finally {
            try {
                releaseConstructionReservationLocks(
                        locker, dbId, tableId, dbLockHeld, tableLockHeld);
            } catch (RuntimeException | Error unlockFailure) {
                if (cleanupFailure != null) {
                    cleanupFailure.addSuppressed(unlockFailure);
                } else {
                    throw unlockFailure;
                }
            } finally {
                if (restoreInterrupt) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void releaseConstructionReservationLocks(
            Locker locker, long dbId, long tableId, boolean dbLockHeld, boolean tableLockHeld) {
        Throwable unlockFailure = null;
        if (tableLockHeld) {
            try {
                locker.release(tableId, LockType.WRITE);
            } catch (RuntimeException | Error failure) {
                unlockFailure = failure;
            }
        }
        if (dbLockHeld) {
            try {
                locker.release(dbId, LockType.INTENTION_EXCLUSIVE);
            } catch (RuntimeException | Error failure) {
                if (unlockFailure == null) {
                    unlockFailure = failure;
                } else {
                    unlockFailure.addSuppressed(failure);
                }
            }
        }
        if (unlockFailure instanceof RuntimeException) {
            throw (RuntimeException) unlockFailure;
        }
        if (unlockFailure instanceof Error) {
            throw (Error) unlockFailure;
        }
    }

    public Collection<ReplicationJob> getRunningJobs() {
        return runningJobs.values();
    }

    public Collection<ReplicationJob> getCommittedJobs() {
        return committedJobs.values();
    }

    public Collection<ReplicationJob> getAbortedJobs() {
        return abortedJobs.values();
    }

    public void cancelRunningJobs() {
        List<ReplicationJob> toRemovedJobs = Lists.newArrayList();
        for (ReplicationJob job : runningJobs.values()) {
            job.cancel();

            if (job.getState().equals(ReplicationJobState.ABORTED)) {
                toRemovedJobs.add(job);
                abortedJobs.put(job.getTableId(), job);
            }
        }

        for (ReplicationJob job : toRemovedJobs) {
            runningJobs.remove(job.getTableId(), job);
        }
    }

    @VisibleForTesting
    public void removeRunningJob(ReplicationJob job) {
        runningJobs.remove(job.getTableId(), job);
    }

    public void finishRemoteSnapshotTask(RemoteSnapshotTask task, TFinishTaskRequest request) {
        ReplicationJob job = runningJobs.get(task.getTableId());
        if (job == null) {
            LOG.warn("Remote snapshot task {} is finished, but cannot find it in replication jobs", task);
            return;
        }

        job.finishRemoteSnapshotTask(task, request);
    }

    public void finishReplicateSnapshotTask(ReplicateSnapshotTask task, TFinishTaskRequest request) {
        ReplicationJob job = runningJobs.get(task.getTableId());
        if (job == null) {
            LOG.warn("Replicate snapshot task {} is finished, but cannot find it in replication jobs", task);
            return;
        }

        job.finishReplicateSnapshotTask(task, request);
    }

    public void replayReplicationJob(ReplicationJob replicationJob) {
        if (replicationJob.getState().equals(ReplicationJobState.COMMITTED)) {
            committedJobs.put(replicationJob.getTableId(), replicationJob);
            runningJobs.remove(replicationJob.getTableId());
        } else if (replicationJob.getState().equals(ReplicationJobState.ABORTED)) {
            abortedJobs.put(replicationJob.getTableId(), replicationJob);
            runningJobs.remove(replicationJob.getTableId());
        } else {
            runningJobs.put(replicationJob.getTableId(), replicationJob);
        }
    }

    public void replayDeleteReplicationJob(ReplicationJob replicationJob) {
        if (replicationJob.getState().equals(ReplicationJobState.COMMITTED)) {
            committedJobs.remove(replicationJob.getTableId());
        } else if (replicationJob.getState().equals(ReplicationJobState.ABORTED)) {
            abortedJobs.remove(replicationJob.getTableId());
        } else {
            runningJobs.remove(replicationJob.getTableId());
        }
    }

    private long getReplicatingReplicaCount() {
        long replicatingReplicaCount = 0;
        for (ReplicationJob job : runningJobs.values()) {
            replicatingReplicaCount += job.getReplicationReplicaCount();
        }
        return replicatingReplicaCount;
    }

    private long getReplicatingDataSize() {
        long replicatingDataSize = 0;
        for (ReplicationJob job : runningJobs.values()) {
            replicatingDataSize += job.getReplicationDataSize();
        }
        return replicatingDataSize;
    }

    private void runRunningJobs() {
        List<ReplicationJob> toRemovedJobs = Lists.newArrayList();
        for (ReplicationJob job : runningJobs.values()) {
            job.run();

            ReplicationJobState state = job.getState();
            if (state.equals(ReplicationJobState.COMMITTED)) {
                toRemovedJobs.add(job);
                committedJobs.put(job.getTableId(), job);
            } else if (state.equals(ReplicationJobState.ABORTED)) {
                toRemovedJobs.add(job);
                abortedJobs.put(job.getTableId(), job);
            }
        }

        for (ReplicationJob job : toRemovedJobs) {
            runningJobs.remove(job.getTableId(), job);
        }
    }

    protected void clearExpiredJobs() {
        for (Iterator<Map.Entry<Long, ReplicationJob>> it = committedJobs.entrySet().iterator(); it.hasNext();) {
            ReplicationJob job = it.next().getValue();
            if (!job.isExpired()) {
                continue;
            }

            GlobalStateMgr.getServingState().getEditLog().logDeleteReplicationJob(job, wal -> {
                it.remove();
            });
        }

        for (Iterator<Map.Entry<Long, ReplicationJob>> it = abortedJobs.entrySet().iterator(); it.hasNext();) {
            ReplicationJob job = it.next().getValue();
            if (!job.isExpired()) {
                continue;
            }

            GlobalStateMgr.getServingState().getEditLog().logDeleteReplicationJob(job, wal -> {
                it.remove();
            });
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.REPLICATION_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        ReplicationMgr replicationMgr = reader.readJson(ReplicationMgr.class);
        runningJobs.putAll(replicationMgr.runningJobs);
        committedJobs.putAll(replicationMgr.committedJobs);
        abortedJobs.putAll(replicationMgr.abortedJobs);
    }
}
