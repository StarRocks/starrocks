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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TEnvelopeType;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Utils;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;

/**
 * Manages batch write operations.
 */
public class BatchWriteMgr extends LeaderDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(BatchWriteMgr.class);

    // An atomic counter used to generate unique ids for isomorphic batch writes.
    private final AtomicLong idGenerator;

    // A read-write lock to ensure thread-safe access to the loadMap.
    private final ReentrantReadWriteLock lock;

    // A concurrent map that stores MergeCommitJob instances, keyed by BatchWriteId.
    private final ConcurrentHashMap<BatchWriteId, MergeCommitJob> mergeCommitJobs;

    // An assigner that manages the assignment of coordinator backends.
    private final CoordinatorBackendAssigner coordinatorBackendAssigner;

    // A thread pool executor for executing batch write tasks. Not final: shutdownNow() in
    // onStopped() and rebuilt by start() so leader-side worker threads exit promptly on
    // demotion and a fresh pool is available after re-election.
    private volatile ThreadPoolExecutor threadPoolExecutor;

    // Rebuilt together with threadPoolExecutor so it never holds a reference to a shut down pool.
    private volatile TxnStateDispatcher txnStateDispatcher;

    public BatchWriteMgr() {
        super("merge-commit-mgr", Config.merge_commit_gc_check_interval_ms);
        this.idGenerator = new AtomicLong(0L);
        this.mergeCommitJobs = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.coordinatorBackendAssigner = new CoordinatorBackendAssignerImpl();
        this.threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                        Config.merge_commit_executor_threads_num, "merge-commit", true);
        this.txnStateDispatcher = new TxnStateDispatcher(threadPoolExecutor);
    }

    @Override
    public synchronized void start() {
        // The re-activation cleanliness gate verifies the previous pool terminated before start() runs
        // (onStopped awaits its termination and only then clears isRunning), so there is no restart
        // guard here - just rebuild the pool if a previous demotion shut it down (or on first start).
        if (threadPoolExecutor == null || threadPoolExecutor.isShutdown()) {
            threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                    Config.merge_commit_executor_threads_num, "merge-commit", true);
            txnStateDispatcher = new TxnStateDispatcher(threadPoolExecutor);
        }
        super.start();
        this.coordinatorBackendAssigner.start();
        LOG.info("Start batch write manager");
    }

    @Override
    protected void runAfterLeaseValid() {
        setInterval(Config.merge_commit_gc_check_interval_ms);
        cleanupInactiveJobs();
    }

    @Override
    protected void onStopped() {
        // mergeCommitJobs and the per-table backend-assigner registrations are leader-session
        // bookkeeping: BE coordinators are picked again when the next leader sees the next
        // request. Drop the registrations so a new leader does not inherit assignments made
        // against a sealed editlog. The owned thread pool and the coordinator assigner are
        // shut down so their worker threads exit promptly during demotion drain; both are
        // rebuilt by start() on re-election.
        lock.writeLock().lock();
        try {
            for (MergeCommitJob job : mergeCommitJobs.values()) {
                try {
                    coordinatorBackendAssigner.unregisterBatchWrite(job.getId());
                } catch (Throwable t) {
                    LOG.warn("unregister batch write {} failed", job.getId(), t);
                }
            }
            mergeCommitJobs.clear();
            MergeCommitMetricRegistry.getInstance().setJobNum(0);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            coordinatorBackendAssigner.stop();
        } catch (Throwable t) {
            LOG.warn("stop coordinatorBackendAssigner failed", t);
        }
        // shutdownNow() interrupts in-flight merge-commit submissions; wait until the pool actually
        // terminates so this worker does not clear isRunning while a merge-commit task is still running
        // (the re-activation gate reads isRunning as the single quiescence signal). start() rebuilds it.
        shutdownNowAndAwaitTermination("BatchWriteMgr.threadPoolExecutor", threadPoolExecutor);
    }

    /**
     * Requests coordinator backends for the specified table and load parameters.
     *
     * @param tableId The ID of the table for which the coordinator backends are requested.
     * @param params The parameters for the stream load.
     * @param userIdentity the user who initiated the load request
     * @return A RequestCoordinatorBackendResult containing the status of the operation and the coordinator backends.
     */
    public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params,
                                                                      UserIdentity userIdentity) {
        lock.readLock().lock();
        try {
            Pair<TStatus, MergeCommitJob> result = getOrCreateJob(tableId, params, userIdentity);
            if (result.first.getStatus_code() != TStatusCode.OK) {
                return new RequestCoordinatorBackendResult(result.first, null);
            }
            RequestCoordinatorBackendResult backendResult = result.second.requestCoordinatorBackends();
            if (backendResult.getStatus().getStatus_code() == TStatusCode.SERVICE_UNAVAILABLE) {
                MergeCommitJob newJob =
                        switchCnGroupIfUnavailable(new BatchWriteId(tableId, params), result.second, null);
                if (newJob != null) {
                    backendResult = newJob.requestCoordinatorBackends();
                }
            }
            return backendResult;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Requests a load operation for the specified table and load parameters.
     *
     * @param tableId the ID of the table for which the load is requested
     * @param params the parameters for the stream load
     * @param userIdentity the user who initiated the load request
     * @param backendId the id of the backend where the request is from
     * @param backendHost the host of the backend where the request is from
     * @return a RequestLoadResult containing the status of the operation and the load label if successful
     */
    public RequestLoadResult requestLoad(
            TableId tableId, StreamLoadKvParams params, UserIdentity userIdentity, long backendId, String backendHost) {
        lock.readLock().lock();
        try {
            Pair<TStatus, MergeCommitJob> result = getOrCreateJob(tableId, params, userIdentity);
            if (result.first.getStatus_code() != TStatusCode.OK) {
                return new RequestLoadResult(result.first, null);
            }

            String user = userIdentity != null ? userIdentity.getUser() : "";
            RequestLoadResult loadResult = result.second.requestLoad(user, backendId, backendHost);
            if (loadResult.getStatus().getStatus_code() == TStatusCode.SERVICE_UNAVAILABLE) {
                MergeCommitJob newJob =
                        switchCnGroupIfUnavailable(new BatchWriteId(tableId, params), result.second, backendId);
                // Retry only when the requesting node is in the new job's CN group. A task cannot run on a node
                // outside the group of its job, so the new job turns down a request from any other node, and such a
                // request returns the original failure.
                if (newJob != null && isInCnGroup(backendId, newJob.getComputeResource())) {
                    loadResult = newJob.requestLoad(user, backendId, backendHost);
                }
            }
            return loadResult;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Cleans up inactive jobs to release resources.
     */
    @VisibleForTesting
    void cleanupInactiveJobs() {
        lock.writeLock().lock();
        try {
            List<Map.Entry<BatchWriteId, MergeCommitJob>> loads = mergeCommitJobs.entrySet().stream()
                            .filter(entry -> !entry.getValue().isActive())
                            .collect(Collectors.toList());
            for (Map.Entry<BatchWriteId, MergeCommitJob> entry : loads) {
                mergeCommitJobs.remove(entry.getKey());
                coordinatorBackendAssigner.unregisterBatchWrite(entry.getValue().getId());
            }
            MergeCommitMetricRegistry.getInstance().setJobNum(mergeCommitJobs.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves or creates an MergeCommitJob instance for the specified table and parameters.
     *
     * @param tableId The ID of the table for which the batch write is requested.
     * @param params The parameters for the stream load.
     * @param userIdentity the user who initiated the load request
     * @return A Pair containing the status of the operation and the MergeCommitJob instance.
     */
    private Pair<TStatus, MergeCommitJob> getOrCreateJob(TableId tableId, StreamLoadKvParams params, UserIdentity userIdentity) {
        BatchWriteId uniqueId = new BatchWriteId(tableId, params);
        MergeCommitJob load = mergeCommitJobs.get(uniqueId);

        String warehouseName = params.getWarehouse().orElse(null);
        if (warehouseName == null) {
            // Try to use `session.warehouse` in user property if warehouse is not specified
            Optional<String> userWarehouseName = Utils.getUserDefaultWarehouse(userIdentity);
            if (userWarehouseName.isPresent() &&
                    GlobalStateMgr.getCurrentState().getWarehouseMgr().warehouseExists(userWarehouseName.get())) {
                warehouseName = userWarehouseName.get();
                // Check job warehouse name and user default warehouse name
                if (load != null && !warehouseName.equals(load.getWarehouseName())) {
                    TStatus status = new TStatus();
                    status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
                    status.setError_msgs(Collections.singletonList(String.format(
                            "Job warehouse %s does not match the request user default warehouse %s",
                            load.getWarehouseName(), warehouseName)));
                    return new Pair<>(status, null);
                }
            }
        }
        if (warehouseName == null) {
            warehouseName = DEFAULT_WAREHOUSE_NAME;
        }

        if (load != null) {
            return new Pair<>(new TStatus(TStatusCode.OK), load);
        }

        StreamLoadInfo streamLoadInfo;
        try {
            streamLoadInfo = buildStreamLoadInfo(warehouseName, params);
        } catch (Exception e) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    String.format("Failed to build stream load info, error: %s", e.getMessage())));
            return new Pair<>(status, null);
        }

        if (streamLoadInfo.getEnvelope() == TEnvelopeType.DEBEZIUM) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(tableId.getDbName(), tableId.getTableName());
            if (table instanceof OlapTable && ((OlapTable) table).getKeysType() != KeysType.PRIMARY_KEYS) {
                TStatus status = new TStatus();
                status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
                status.setError_msgs(Collections.singletonList(
                        "envelope=debezium is only supported on PRIMARY KEY tables"));
                return new Pair<>(status, null);
            }
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
            String finalWarehouseName = warehouseName;
            load = mergeCommitJobs.computeIfAbsent(uniqueId, uid -> createJob(
                    tableId, finalWarehouseName, streamLoadInfo, batchWriteIntervalMs, batchWriteParallel, params));

            String user = userIdentity != null ? userIdentity.getUser() : "";
            LOG.info("Create merge commit job, user: {}, id: {}, {}, {}", user, load.getId(), tableId, params);
        } catch (Exception e) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Collections.singletonList(e.getMessage()));
            LOG.error("Failed to create batch write for {}, params: {}", tableId, params, e);
            return new Pair<>(status, null);
        }
        MergeCommitMetricRegistry.getInstance().setJobNum(mergeCommitJobs.size());
        return new Pair<>(new TStatus(TStatusCode.OK), load);
    }

    private static StreamLoadInfo buildStreamLoadInfo(String warehouseName, StreamLoadKvParams params)
            throws Exception {
        CRAcquireContext acquireContext = CRAcquireContext.of(warehouseName);
        return StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), params, acquireContext);
    }

    // Creates a job and registers it with the assigner, which gives it coordinator nodes of the job's CN group.
    private MergeCommitJob createJob(TableId tableId, String warehouseName, StreamLoadInfo streamLoadInfo,
                                     int batchWriteIntervalMs, int batchWriteParallel, StreamLoadKvParams params) {
        long id = idGenerator.getAndIncrement();
        MergeCommitJob job = new MergeCommitJob(
                id, tableId, warehouseName, streamLoadInfo, batchWriteIntervalMs, batchWriteParallel,
                params, coordinatorBackendAssigner, threadPoolExecutor, txnStateDispatcher);
        coordinatorBackendAssigner.registerBatchWrite(id, job.getComputeResource(), tableId, job.getBatchWriteParallel());
        return job;
    }

    /**
     * Moves a job to another CN group of its warehouse when no compute node of its own CN group can coordinate
     * loads any more: all of them are dead, decommissioned or in the blocklist, as when the availability zone of
     * the group is down. A job keeps the CN group it got at creation, so without this every request for the table
     * fails with "Can't find available backends" until a node of that group is back or the job has been idle for
     * {@link Config#merge_commit_idle_ms}, while the other CN groups of the warehouse could take the load.
     *
     * <p>Called only after a request on the job failed as unavailable, because it found no coordinator backend or
     * came from a node outside the job's CN group, so requests that succeed pay nothing, and the check runs at most
     * once per {@link MergeCommitJob#CN_GROUP_CHECK_INTERVAL_MS} for a job. The new job is registered with the
     * assigner before it replaces the old one in {@link #mergeCommitJobs}, and the old job is unregistered only
     * after that, so a request never finds a job the assigner does not know. Tasks of the old job keep running and
     * report to it. A job is not moved back when its old CN group recovers.
     *
     * @param uniqueId the key of the job
     * @param job the job a request failed on as unavailable
     * @param requestBackendId the compute node that asked for a load on the BE RPC path, null on the HTTP path
     * @return the job to retry the request on, or null to return the original failure
     */
    @VisibleForTesting
    MergeCommitJob switchCnGroupIfUnavailable(BatchWriteId uniqueId, MergeCommitJob job, Long requestBackendId) {
        MergeCommitJob current = mergeCommitJobs.get(uniqueId);
        if (current != job) {
            // Another request has moved the job already
            return current;
        }
        if (!job.tryClaimCnGroupCheck(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()))) {
            return null;
        }
        ComputeResource oldResource = job.getComputeResource();
        if (hasAvailableNodes(oldResource)) {
            // Other nodes of the group can coordinate the job, and the assigner moves it to them
            return null;
        }
        StreamLoadInfo streamLoadInfo = buildStreamLoadInfoOnAnotherCnGroup(job, requestBackendId);
        if (streamLoadInfo == null) {
            LOG.warn("No compute node of CN group {} can coordinate merge commit job {} of {}, and no other enabled " +
                    "CN group of warehouse {} has one", oldResource, job.getId(), job.getTableId(), job.getWarehouseName());
            return null;
        }
        MergeCommitJob newJob;
        try {
            newJob = createJob(job.getTableId(), job.getWarehouseName(), streamLoadInfo, job.getBatchWriteIntervalMs(),
                    job.getBatchWriteParallel(), job.getLoadParameters());
        } catch (Exception e) {
            LOG.warn("Failed to move merge commit job {} of {} from CN group {} to {}", job.getId(), job.getTableId(),
                    oldResource, streamLoadInfo.getComputeResource(), e);
            return null;
        }
        if (!mergeCommitJobs.replace(uniqueId, job, newJob)) {
            // Another request moved the job while this one was registering its replacement
            coordinatorBackendAssigner.unregisterBatchWrite(newJob.getId());
            return mergeCommitJobs.get(uniqueId);
        }
        coordinatorBackendAssigner.unregisterBatchWrite(job.getId());
        MergeCommitMetricRegistry.getInstance().incJobCnGroupSwitch();
        LOG.warn("Move merge commit job of {} from CN group {} to {} because no compute node of the group can " +
                "coordinate loads, old job id: {}, new job id: {}, params: {}", job.getTableId(), oldResource,
                newJob.getComputeResource(), job.getId(), newJob.getId(), job.getLoadParameters());
        return newJob;
    }

    /**
     * Builds the stream load info for a replacement of the job on another CN group of the job's warehouse that is
     * enabled and has a node to coordinate loads. DISABLE CNGROUP leaves the nodes of a group alive, so a disabled
     * group is skipped wherever it comes from. On the BE RPC path the group of the requesting compute node comes
     * first, because a task cannot run on a node outside the CN group of its job, so any other group would fail that
     * request. Otherwise the group is acquired the same way as for a new job. If that gives the job's own group again
     * (its nodes still count as alive while they are in the blocklist or decommissioned), a disabled group or a group
     * without such a node, one of the other usable groups of the warehouse is taken at random.
     *
     * @return null if no other enabled CN group of the warehouse can coordinate loads
     */
    private StreamLoadInfo buildStreamLoadInfoOnAnotherCnGroup(MergeCommitJob job, Long requestBackendId) {
        ComputeResource oldResource = job.getComputeResource();
        StreamLoadInfo streamLoadInfo;
        try {
            streamLoadInfo = buildStreamLoadInfo(job.getWarehouseName(), job.getLoadParameters());
        } catch (Exception e) {
            LOG.warn("Failed to acquire a CN group of warehouse {} for merge commit job {} of {}",
                    job.getWarehouseName(), job.getId(), job.getTableId(), e);
            return null;
        }
        ComputeResource requestNodeResource = requestBackendId == null ? null : getCnGroupOfNode(requestBackendId);
        if (requestNodeResource != null && requestNodeResource.getWarehouseId() == oldResource.getWarehouseId()
                && isUsableReplacement(requestNodeResource, oldResource)) {
            streamLoadInfo.setComputeResource(requestNodeResource);
            return streamLoadInfo;
        }
        if (isUsableReplacement(streamLoadInfo.getComputeResource(), oldResource)) {
            return streamLoadInfo;
        }
        List<ComputeResource> candidates;
        try {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = warehouseManager.getWarehouse(job.getWarehouseName());
            candidates = new ArrayList<>(warehouseManager.getComputeResourceProvider().getComputeResources(warehouse));
        } catch (Exception e) {
            LOG.warn("Failed to list the CN groups of warehouse {}", job.getWarehouseName(), e);
            return null;
        }
        Collections.shuffle(candidates);
        for (ComputeResource candidate : candidates) {
            if (isUsableReplacement(candidate, oldResource)) {
                streamLoadInfo.setComputeResource(candidate);
                return streamLoadInfo;
            }
        }
        return null;
    }

    // The CN group of a compute node, or null if the node is unknown
    private static ComputeResource getCnGroupOfNode(long nodeId) {
        try {
            ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
            if (node == null) {
                return null;
            }
            return GlobalStateMgr.getCurrentState().getWarehouseMgr().getComputeResourceProvider()
                    .ofComputeResource(node.getWarehouseId(), node.getWorkerGroupId());
        } catch (Exception e) {
            LOG.warn("Failed to get the CN group of compute node {}", nodeId, e);
            return null;
        }
    }

    private static boolean isInCnGroup(long nodeId, ComputeResource computeResource) {
        ComputeResource nodeResource = getCnGroupOfNode(nodeId);
        return nodeResource != null && isSameCnGroup(nodeResource, computeResource);
    }

    private static boolean isUsableReplacement(ComputeResource candidate, ComputeResource oldResource) {
        return candidate != null && !isSameCnGroup(candidate, oldResource)
                && !GlobalStateMgr.getCurrentState().getWarehouseMgr().isComputeResourceDisabled(candidate)
                && hasAvailableNodes(candidate);
    }

    private static boolean isSameCnGroup(ComputeResource left, ComputeResource right) {
        return left.getWarehouseId() == right.getWarehouseId() && left.getWorkerGroupId() == right.getWorkerGroupId();
    }

    // Whether a compute node of the CN group can coordinate loads. The assigner picks coordinators the same way.
    private static boolean hasAvailableNodes(ComputeResource computeResource) {
        try {
            return !LoadScanNode.getAvailableComputeNodes(computeResource).isEmpty();
        } catch (Exception e) {
            LOG.warn("Failed to get the available compute nodes of CN group {}", computeResource, e);
            return false;
        }
    }

    @VisibleForTesting
    MergeCommitJob getJob(TableId tableId, StreamLoadKvParams params) {
        return mergeCommitJobs.get(new BatchWriteId(tableId, params));
    }

    /**
     * Returns the number of jobs currently managed.
     *
     * @return The number of jobs.
     */
    public int numJobs() {
        return mergeCommitJobs.size();
    }

    public CoordinatorBackendAssigner getCoordinatorBackendAssigner() {
        return coordinatorBackendAssigner;
    }
}
