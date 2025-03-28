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

import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Responsible for managing batch write operations with the isomorphic parameters.
 * It will allocate a load for each write operation, and monitor the status of the load.
 */
public class IsomorphicBatchWrite implements LoadExecuteCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IsomorphicBatchWrite.class);

    private static final String LABEL_PREFIX = "merge_commit_";

    private final long id;
    private final TableId tableId;
    private final String warehouseName;
    private final StreamLoadInfo streamLoadInfo;
    private final int batchWriteIntervalMs;
    private final int batchWriteParallel;
    private final boolean asyncMode;
    private final StreamLoadKvParams loadParameters;

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
    private final ConcurrentHashMap<String, LoadExecutor> loadExecutorMap;

    private final AtomicLong lastLoadCreateTimeMs;

    public IsomorphicBatchWrite(
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
        this.loadExecutorMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.lastLoadCreateTimeMs = new AtomicLong(System.currentTimeMillis());
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

    public int getBatchWriteParallel() {
        return batchWriteParallel;
    }

    public int numRunningLoads() {
        return loadExecutorMap.size();
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
     * @param backendId The id of the backend.
     * @param backendHost The host of the backend.
     * @return The result of the request for the load.
     */
    public RequestLoadResult requestLoad(long backendId, String backendHost) {
        TStatus status = new TStatus();
        lock.readLock().lock();
        try {
            for (LoadExecutor loadExecutor : loadExecutorMap.values()) {
                if (loadExecutor.isActive() && loadExecutor.containCoordinatorBackend(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, loadExecutor.getLabel());
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            for (LoadExecutor loadExecutor : loadExecutorMap.values()) {
                if (loadExecutor.isActive() && loadExecutor.containCoordinatorBackend(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, loadExecutor.getLabel());
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

            String label = LABEL_PREFIX + DebugUtil.printId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
            TUniqueId loadId = UUIDUtil.toTUniqueId(UUID.randomUUID());
            LoadExecutor loadExecutor = new LoadExecutor(
                    tableId, label, loadId, streamLoadInfo, batchWriteIntervalMs, loadParameters,
                    backendIds, queryCoordinatorFactory, this);
            loadExecutorMap.put(label, loadExecutor);
            try {
                executor.execute(loadExecutor);
            } catch (Exception e) {
                loadExecutorMap.remove(label);
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Collections.singletonList(e.getMessage()));
                return new RequestLoadResult(status, null);
            }
            status.setStatus_code(TStatusCode.OK);
            lastLoadCreateTimeMs.set(System.currentTimeMillis());
            return new RequestLoadResult(status, label);
        } finally {
            lock.writeLock().unlock();
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
        return !loadExecutorMap.isEmpty() || idleTime < Config.merge_commit_idle_ms;
    }

    @Override
    public void finishLoad(LoadExecutor executor) {
        lock.writeLock().lock();
        try {
            loadExecutorMap.remove(executor.getLabel());
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

    @VisibleForTesting
    LoadExecutor getLoadExecutor(String label) {
        return loadExecutorMap.get(label);
    }
}
