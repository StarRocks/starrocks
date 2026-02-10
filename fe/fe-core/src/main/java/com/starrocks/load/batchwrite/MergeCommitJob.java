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

package com.starrocks.load.batchwrite;

import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Responsible for managing load requests with the isomorphic parameters. It will
 * create load tasks to merge these requests, and monitor the status of these tasks.
 */
public class MergeCommitJob implements MergeCommitTaskCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitJob.class);

    private static final String LABEL_PREFIX = "merge_commit_";

    private final long id;
    private final TableId tableId;
    private final String warehouseName;
    private final StreamLoadInfo streamLoadInfo;
    private final int batchWriteIntervalMs;
    private final int batchWriteParallel;
    private final boolean asyncMode;
    private final StreamLoadKvParams loadParameters;
    private final ComputeResource computeResource;

    /**
     * The assigner for coordinator backends.
     */
    private final CoordinatorBackendAssigner coordinatorBackendAssigner;

    /**
     * The executor to run batch write tasks.
     */
    private final Executor executor;

    /** Update the transaction state of the backend if this is a sync mode. */
    private final TxnStateDispatcher txnUpdateDispatch;

    /**
     * The factory to create query coordinators.
     */
    private final Coordinator.Factory queryCoordinatorFactory;

    /**
     * The lock to manage concurrent access to the load executor map.
     */
    private final ReentrantReadWriteLock lock;

    /**
     * The map to store load executors, keyed by their labels.
     */
    private final ConcurrentHashMap<String, MergeCommitTask> mergeCommitTasks;

    private final AtomicLong lastLoadCreateTimeMs;

    public MergeCommitJob(
            long id,
            TableId tableId,
            String warehouseName,
            StreamLoadInfo streamLoadInfo,
            int batchWriteIntervalMs,
            int batchWriteParallel,
            StreamLoadKvParams loadParameters,
            CoordinatorBackendAssigner coordinatorBackendAssigner,
            Executor executor,
            TxnStateDispatcher txnUpdateDispatch) {
        this.id = id;
        this.tableId = tableId;
        this.warehouseName = warehouseName;
        this.streamLoadInfo = streamLoadInfo;
        this.batchWriteIntervalMs = batchWriteIntervalMs;
        this.batchWriteParallel = batchWriteParallel;
        this.asyncMode = loadParameters.getBatchWriteAsync().orElse(false);
        this.loadParameters = loadParameters;
        this.coordinatorBackendAssigner = coordinatorBackendAssigner;
        this.executor = executor;
        this.txnUpdateDispatch = txnUpdateDispatch;
        this.queryCoordinatorFactory = new DefaultCoordinator.Factory();
        this.mergeCommitTasks = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.lastLoadCreateTimeMs = new AtomicLong(System.currentTimeMillis());
        this.computeResource = streamLoadInfo.getComputeResource();
    }

    public long getId() {
        return id;
    }

    public TableId getTableId() {
        return tableId;
    }

    public long getWarehouseId() {
        return streamLoadInfo.getWarehouseId();
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public int getBatchWriteParallel() {
        return batchWriteParallel;
    }

    public int numRunningLoads() {
        return mergeCommitTasks.size();
    }

    public ComputeResource getComputeResource() {
        return computeResource;
    }

    /**
     * Requests coordinator backends for the batch write operation.
     * The backend can accept write operations for the specified table.
     *
     * @return The result of the request for coordinator backends.
     */
    public RequestCoordinatorBackendResult requestCoordinatorBackends() {
        TStatus status = new TStatus();
        List<ComputeNode> backends = null;
        try {
            Optional<List<ComputeNode>> ret = coordinatorBackendAssigner.getBackends(id);
            if (ret.isPresent() && !ret.get().isEmpty()) {
                backends = ret.get();
                status.setStatus_code(TStatusCode.OK);
            } else {
                status.setStatus_code(TStatusCode.SERVICE_UNAVAILABLE);
                String errMsg = String.format(
                        "Can't find available backends, db: %s, table: %s, warehouse: %s, load id: %s",
                        tableId.getDbName(), tableId.getTableName(), warehouseName, id);
                status.setError_msgs(Collections.singletonList(errMsg));
                LOG.error(errMsg);
            }
        } catch (Exception exception) {
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            String msg = String.format("Unexpected exception happen when getting backends, db: %s, table: %s, " +
                        "warehouse: %s, load id: %s, error: %s", tableId.getDbName(), tableId.getTableName(),
                                warehouseName, id, exception.getMessage());
            status.setError_msgs(Collections.singletonList(msg));
            LOG.error("Failed to get backends, db: {}, table: {}, warehouse: {}, load id: {}",
                    tableId.getDbName(), tableId.getTableName(), warehouseName, id, exception);
        }
        return new RequestCoordinatorBackendResult(status, backends);
    }

    /**
     * Requests a load for the write operation from the specified backend.
     *
     * <p>This method first checks if there is an active task that can accept the backend. If found,
     * it returns the existing task's label. Otherwise, it creates a new {@link MergeCommitTask}
     * and registers it with {@link StreamLoadMgr} for tracking and management.</p>
     *
     * @param user the user who initiated the load request
     * @param backendId the id of the backend requesting the load
     * @param backendHost the host of the backend requesting the load
     * @return the result containing the status and the load label if successful
     */
    public RequestLoadResult requestLoad(String user, long backendId, String backendHost) {
        TStatus status = new TStatus();
        lock.readLock().lock();
        try {
            for (MergeCommitTask mergeCommitTask : mergeCommitTasks.values()) {
                if (mergeCommitTask.isActive() && mergeCommitTask.containsBackend(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, mergeCommitTask.getLabel());
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        MergeCommitTask newTask = null;
        lock.writeLock().lock();
        try {
            for (MergeCommitTask mergeCommitTask : mergeCommitTasks.values()) {
                if (mergeCommitTask.isActive() && mergeCommitTask.containsBackend(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, mergeCommitTask.getLabel());
                }
            }

            RequestCoordinatorBackendResult requestCoordinatorBackendResult = requestCoordinatorBackends();
            if (!requestCoordinatorBackendResult.isOk()) {
                return new RequestLoadResult(requestCoordinatorBackendResult.getStatus(), null);
            }

            Set<Long> backendIds = requestCoordinatorBackendResult.getValue().stream()
                    .map(ComputeNode::getId).collect(Collectors.toSet());
            if (!backendIds.contains(backendId)) {
                ComputeNode backend = GlobalStateMgr.getCurrentState()
                        .getNodeMgr().getClusterInfo().getBackendOrComputeNode(backendId);
                if (backend == null || !backend.isAvailable()) {
                    status.setStatus_code(TStatusCode.SERVICE_UNAVAILABLE);
                    status.setError_msgs(Collections.singletonList(
                            String.format("Backend [%s, %s] is not available", backendId, backendHost)));
                    return new RequestLoadResult(status, null);
                }
                backendIds.add(backendId);
            }

            Optional<Long> dbId = getDbId();
            if (dbId.isEmpty()) {
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Collections.singletonList(
                        String.format("Database '%s' does not exist", tableId.getDbName())));
                return new RequestLoadResult(status, null);
            }
            TUniqueId loadId = UUIDUtil.genTUniqueId();
            String label = LABEL_PREFIX + DebugUtil.printId(loadId);
            newTask = new MergeCommitTask(
                    GlobalStateMgr.getCurrentState().getNextId(),
                    dbId.get(), tableId, label, loadId, streamLoadInfo, batchWriteIntervalMs, loadParameters,
                    user, warehouseName, backendIds, queryCoordinatorFactory, this);
            mergeCommitTasks.put(label, newTask);
            try {
                executor.execute(newTask);
            } catch (Exception e) {
                mergeCommitTasks.remove(label);
                String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                newTask.cancel(String.format("Failed to submit task: %s", errorMsg));
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Collections.singletonList(errorMsg));
                return new RequestLoadResult(status, null);
            }
            MergeCommitMetricRegistry.getInstance().updateRunningTask(1L);
            status.setStatus_code(TStatusCode.OK);
            lastLoadCreateTimeMs.set(System.currentTimeMillis());
            return new RequestLoadResult(status, label);
        } finally {
            lock.writeLock().unlock();
            if (newTask != null) {
                // Register the task with StreamLoadMgr for observing (e.g., information_schema.loads).
                // 1. Pass false for addTxnCallback because MergeCommitTask registers itself in run().
                // 2. StreamLoadMgr will expire the task automatically, so no need to remove it explicitly.
                // 3. StreamLoadMgr.addLoadTask may do some cleanup, so register it after unlock
                try {
                    GlobalStateMgr.getCurrentState().getStreamLoadMgr().addLoadTask(newTask, false);
                } catch (Exception e) {
                    LOG.debug("Failed to register task in StreamLoadMgr, db={}, table={}, label={}",
                            tableId.getDbName(), tableId.getTableName(), newTask.getLabel(), e);
                }
            }
        }
    }

    /**
     * Checks if the batch write operation is active.
     *
     * @return true if there are active load executors or the idle time is less than
     *                  the configured threshold, false otherwise.
     */
    public boolean isActive() {
        long idleTime = System.currentTimeMillis() - lastLoadCreateTimeMs.get();
        return !mergeCommitTasks.isEmpty() || idleTime < Config.merge_commit_idle_ms;
    }

    @Override
    public void finish(MergeCommitTask executor) {
        lock.writeLock().lock();
        try {
            mergeCommitTasks.remove(executor.getLabel());
        } finally {
            lock.writeLock().unlock();
        }

        long txnId = executor.getTxnId();
        if (!asyncMode && txnId > 0) {
            for (long backendId : executor.getBackendIds()) {
                try {
                    txnUpdateDispatch.submitTask(tableId.getDbName(), txnId, backendId);
                } catch (Exception e) {
                    LOG.error("Fail to submit transaction state update task, db: {}, txn_id: {}, backend id: {}",
                            tableId.getDbName(), txnId, backendId, e);
                }
            }
        }
    }

    /**
     * Resolves the database ID from the database name in {@link #tableId}.
     *
     * @return the database ID if the database exists, otherwise an empty Optional
     */
    private Optional<Long> getDbId() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        return db == null ? Optional.empty() : Optional.of(db.getId());
    }

    @VisibleForTesting
    MergeCommitTask getTask(String label) {
        return mergeCommitTasks.get(label);
    }
}
