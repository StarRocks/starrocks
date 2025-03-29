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
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;

/**
 * Manages batch write operations.
 */
public class BatchWriteMgr extends FrontendDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(BatchWriteMgr.class);

    // An atomic counter used to generate unique ids for isomorphic batch writes.
    private final AtomicLong idGenerator;

    // A read-write lock to ensure thread-safe access to the loadMap.
    private final ReentrantReadWriteLock lock;

    // A concurrent map that stores IsomorphicBatchWrite instances, keyed by BatchWriteId.
    private final ConcurrentHashMap<BatchWriteId, IsomorphicBatchWrite> isomorphicBatchWriteMap;

    // An assigner that manages the assignment of coordinator backends.
    private final CoordinatorBackendAssigner coordinatorBackendAssigner;

    // A thread pool executor for executing batch write tasks.
    private final ThreadPoolExecutor threadPoolExecutor;

    private final TxnStateDispatcher txnStateDispatcher;

    public BatchWriteMgr() {
        super("merge-commit-mgr", Config.merge_commit_gc_check_interval_ms);
        this.idGenerator = new AtomicLong(0L);
        this.isomorphicBatchWriteMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.coordinatorBackendAssigner = new CoordinatorBackendAssignerImpl();
        this.threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                        Config.merge_commit_executor_threads_num, "batch-write-load", true);
        this.txnStateDispatcher = new TxnStateDispatcher(threadPoolExecutor);
    }

    @Override
    public synchronized void start() {
        super.start();
        this.coordinatorBackendAssigner.start();
        LOG.info("Start batch write manager");
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(Config.merge_commit_gc_check_interval_ms);
        cleanupInactiveBatchWrite();
    }

    /**
     * Requests coordinator backends for the specified table and load parameters.
     *
     * @param tableId The ID of the table for which the coordinator backends are requested.
     * @param params The parameters for the stream load.
     * @return A RequestCoordinatorBackendResult containing the status of the operation and the coordinator backends.
     */
    public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
        lock.readLock().lock();
        try {
            Pair<TStatus, IsomorphicBatchWrite> result = getOrCreateTableBatchWrite(tableId, params);
            if (result.first.getStatus_code() != TStatusCode.OK) {
                return new RequestCoordinatorBackendResult(result.first, null);
            }
            return result.second.requestCoordinatorBackends();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Requests a load operation for the specified table and load parameters.
     *
     * @param tableId The ID of the table for which the load is requested.
     * @param params The parameters for the stream load.
     * @param backendId The id of the backend where the request is from.
     * @param backendHost The host of the backend where the request is from.
     * @return A RequestLoadResult containing the status of the operation and the load result.
     */
    public RequestLoadResult requestLoad(
            TableId tableId, StreamLoadKvParams params, long backendId, String backendHost) {
        lock.readLock().lock();
        try {
            Pair<TStatus, IsomorphicBatchWrite> result = getOrCreateTableBatchWrite(tableId, params);
            if (result.first.getStatus_code() != TStatusCode.OK) {
                return new RequestLoadResult(result.first, null);
            }
            return result.second.requestLoad(backendId, backendHost);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Cleans up inactive batch writes to release resources.
     */
    @VisibleForTesting
    void cleanupInactiveBatchWrite() {
        lock.writeLock().lock();
        try {
            List<Map.Entry<BatchWriteId, IsomorphicBatchWrite>> loads = isomorphicBatchWriteMap.entrySet().stream()
                            .filter(entry -> !entry.getValue().isActive())
                            .collect(Collectors.toList());
            for (Map.Entry<BatchWriteId, IsomorphicBatchWrite> entry : loads) {
                isomorphicBatchWriteMap.remove(entry.getKey());
                coordinatorBackendAssigner.unregisterBatchWrite(entry.getValue().getId());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves or creates an IsomorphicBatchWrite instance for the specified table and parameters.
     *
     * @param tableId The ID of the table for which the batch write is requested.
     * @param params The parameters for the stream load.
     * @return A Pair containing the status of the operation and the IsomorphicBatchWrite instance.
     */
    private Pair<TStatus, IsomorphicBatchWrite> getOrCreateTableBatchWrite(TableId tableId, StreamLoadKvParams params) {
        BatchWriteId uniqueId = new BatchWriteId(tableId, params);
        IsomorphicBatchWrite load = isomorphicBatchWriteMap.get(uniqueId);
        if (load != null) {
            return new Pair<>(new TStatus(TStatusCode.OK), load);
        }

        String warehouseName = params.getWarehouse().orElse(DEFAULT_WAREHOUSE_NAME);
        StreamLoadInfo streamLoadInfo;
        try {
            streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), params);
        } catch (Exception e) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    String.format("Failed to build stream load info, error: %s", e.getMessage())));
            return new Pair<>(status, null);
        }

        Integer batchWriteIntervalMs = params.getBatchWriteIntervalMs().orElse(null);
        if (batchWriteIntervalMs == null || batchWriteIntervalMs <= 0) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    "Batch write interval must be set positive, but is " + batchWriteIntervalMs));
            return new Pair<>(status, null);
        }

        Integer batchWriteParallel = params.getBatchWriteParallel().orElse(null);
        if (batchWriteParallel == null || batchWriteParallel <= 0) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    "Batch write parallel must be set positive, but is " + batchWriteParallel));
            return new Pair<>(status, null);
        }

        try {
            load = isomorphicBatchWriteMap.computeIfAbsent(uniqueId, uid -> {
                long id = idGenerator.getAndIncrement();
                IsomorphicBatchWrite newLoad = new IsomorphicBatchWrite(
                        id, tableId, warehouseName, streamLoadInfo, batchWriteIntervalMs, batchWriteParallel,
                        params, coordinatorBackendAssigner, threadPoolExecutor, txnStateDispatcher);
                coordinatorBackendAssigner.registerBatchWrite(id, newLoad.getWarehouseId(), tableId,
                        newLoad.getBatchWriteParallel());
                return newLoad;
            });
            LOG.info("Create batch write, id: {}, {}, {}", load.getId(), tableId, params);
        } catch (Exception e) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Collections.singletonList(e.getMessage()));
            LOG.error("Failed to create batch write for {}, params: {}", tableId, params, e);
            return new Pair<>(status, null);
        }

        return new Pair<>(new TStatus(TStatusCode.OK), load);
    }

    /**
     * Returns the number of batch writes currently managed.
     *
     * @return The number of batch writes.
     */
    public int numBatchWrites() {
        return isomorphicBatchWriteMap.size();
    }

    public CoordinatorBackendAssigner getCoordinatorBackendAssigner() {
        return coordinatorBackendAssigner;
    }
}
