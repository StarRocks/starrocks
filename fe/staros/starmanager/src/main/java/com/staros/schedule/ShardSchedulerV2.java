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


package com.staros.schedule;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.staros.exception.ExceptionCode;
import com.staros.exception.NoAliveWorkersException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.ScheduleConflictStarException;
import com.staros.exception.StarException;
import com.staros.exception.WorkerNotHealthyStarException;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.AddShardInfo;
import com.staros.proto.AddShardRequest;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.ReplicaState;
import com.staros.replica.Replica;
import com.staros.schedule.select.FirstNSelector;
import com.staros.schedule.select.Selector;
import com.staros.service.ServiceManager;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.shard.ShardManager;
import com.staros.shard.ShardPolicyFilter;
import com.staros.util.AbstractServer;
import com.staros.util.Config;
import com.staros.util.LockCloseable;
import com.staros.util.Utils;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.staros.util.Utils.namedThreadFactory;

/**
 * Designed to be multi-services shared scheduler
 * Assumptions:
 *    Uniqueness:
 *      * serviceId + shardId, globally unique
 *      * shardGroupId, globally unique
 *      * workerGroupId, globally unique
 *      * workerId, globally unique
 *  Two phases scheduling:
 *   - phase 1: calculation
 *     * select worker as target to add/remove shard replica
 *   - phase 2: dispatch
 *     * send RPC call to worker to make the change of the adding/removing shard replica, and update shard info upon success.
 *   Some of the calls can skip phase 1 and directly go to phase 2, e.g. to re-add replica back to worker if the worker is
 *   restarted.
 *  TODO: handle following cases
 *     1. handle shard migration
 *     2. handle workerGroup balance (could be caused by worker online, or triggered by monitoring system)
 *     3. thread safety refactor of serviceManager/shardManager/workerManager components for multi-threads scheduling
 */
public class ShardSchedulerV2 extends AbstractServer implements Scheduler {

    private static final Logger LOG = LogManager.getLogger(ShardSchedulerV2.class);
    private static final int PRIORITY_LOW = 0;
    private static final int PRIORITY_MEDIUM = 10;
    private static final int PRIORITY_HIGH = 20;
    private static final int ADJUST_THREAD_INTERVAL_SECS = 5 * 60; // unit: SECONDS
    private static final int INIT_CHECK_THREAD_DELAY_SECS = 30; // unit: SECONDS

    private static final int SHORT_NAP = 100; // unit: us

    // locker to ensure conflict requests are processed in sequential.
    private final ExclusiveLocker requestLocker = new ExclusiveLocker();
    // TODO: Configurable policy based Selector
    private final Selector scoreSelector = new FirstNSelector();

    private static final List<PlacementPolicy> CONFLICT_POLICIES =
            Arrays.asList(PlacementPolicy.EXCLUDE, PlacementPolicy.PACK, PlacementPolicy.SPREAD);

    // Phase 1 executors, mainly focus on selecting target workers for the shard replica. CPU intensive operation.
    private ScheduledThreadPoolExecutor calculateExecutors;
    // Phase 2 executors, make RPC calls to worker to finalize the selection. Network IO intensive operation.
    private ThreadPoolExecutor dispatchExecutors;
    // ThreadPool Executors that periodically adjust the `calculateExecutors` and `dispatchExecutors` thread pool
    // size according to the total number of shards
    private ScheduledThreadPoolExecutor adjustPoolExecutors;

    private final ServiceManager serviceManager;
    private final WorkerManager workerManager;

    private static final Counter METRIC_WORKER_SHARD_COUNT =
            MetricsSystem.registerCounter("starmgr_schedule_shard_ops",
                    "count of operations by adding/remove shards to/from worker",
                    Lists.newArrayList("op"));
    private final CounterDataPoint addShardOpCounter = METRIC_WORKER_SHARD_COUNT.labelValues("add");
    private final CounterDataPoint removeShardOpCounter = METRIC_WORKER_SHARD_COUNT.labelValues("remove");

    /**
     * Cancellable close object. If cancelled before calling close(), the runnable object will not be actually run.
     */
    private static class DeferOp implements Closeable {
        private final Runnable runnable;
        private boolean done;

        public DeferOp(Runnable runnable) {
            this.runnable = runnable;
            done = false;
        }

        public void cancel() {
            this.done = true;
        }

        @Override
        public void close() {
            if (!done) {
                done = true;
                runnable.run();
            }
        }
    }

    /**
     * Helper class to manage schedule request exclusive relationship
     * 1. at any time, only one ScheduleRequestContext with the same tuple (serviceId, shardId, workerGroupId)
     *  can be entered into phase 2.
     * 2. at any time, only one shard of the same ShardGroup (whose placement policy is in conflictPolicies) can
     *  be entered the phase 2 execution.
     * NOTE:
     *  1. the 1st conflict doesn't necessarily imply the 2nd conflict. E.g. the shard doesn't have any shardGroup or
     *   the shard doesn't belong to any shard groups with conflict placement policies.
     *  2. the 2nd conflict doesn't necessarily imply the 1st conflict. E.g. it is conflict due to two different shards
     *   from the same shard group are request to schedule at the same time.
     */
    private static class ExclusiveLocker {
        // Lock-free Set, no need to protect by lock
        protected Set<ScheduleRequestContext> exclusiveContexts = ConcurrentHashMap.newKeySet();
        // protect exclusiveShardGroups
        protected final ReentrantLock exclusiveMapLock = new ReentrantLock();
        // workerGroup -> (shardGroup, serviceId + shardId), check conflicts of a workerGroup shard scheduling in the same shardGroup
        protected final Map<Long, Set<Long>> exclusiveShardGroups = new HashMap<>();

        /**
         * try to set exclusive marker for schedule request with given context.
         * @param ctx request context
         * @param manager shard manager
         * @return true - if the exclusive markers are all set
         *         false - fail to set the exclusive marker, conflicts detected.
         */
        public boolean tryLock(ScheduleRequestContext ctx, ShardManager manager) {
            // exclusiveContexts ensure the same request is executed sequentially.
            if (exclusiveContexts.add(ctx)) {
                // exclusiveShardGroups ensure only one request in progress in the specific shardGroup
                if (checkAndUpdateExclusiveShardGroups(ctx, manager)) {
                    // Set closeable callback
                    ctx.setRunnable(() -> tryUnlock(ctx));
                    return true;
                } else {
                    // rollback adding operation
                    exclusiveContexts.remove(ctx);
                }
            }
            return false;
        }

        private void tryUnlock(ScheduleRequestContext ctx) {
            // reverse order of tryLock()
            cleanExclusiveShardGroup(ctx);
            exclusiveContexts.remove(ctx);
        }

        /**
         * Clean exclusive shardGroup ids from exclusiveShardGroups
         * @param ctx ScheduleRequestContext to be processed
         */
        private void cleanExclusiveShardGroup(ScheduleRequestContext ctx) {
            // clean shardGroup exclusive marker.
            // can't use Shard.getShardGroupIds() from Shard since the shard's group list can be updated in between.
            Collection<Long> exclusiveGroups = ctx.getExclusiveGroupIds();
            if (exclusiveGroups != null && !exclusiveGroups.isEmpty()) {
                try (LockCloseable ignored = new LockCloseable(exclusiveMapLock)) {
                    Collection<Long> groupMarker = exclusiveShardGroups.get(ctx.getWorkerGroupId());
                    if (groupMarker != null) {
                        groupMarker.removeAll(exclusiveGroups);
                        if (groupMarker.isEmpty()) {
                            exclusiveShardGroups.remove(ctx.getWorkerGroupId());
                        }
                    }
                }
            }
        }

        /**
         * Check whether there is any conflict shard that belongs to the same shard group that is in processing.
         * @return true: check pass, no conflict. false: check fail, has conflict.
         */
        private boolean checkAndUpdateExclusiveShardGroups(ScheduleRequestContext ctx, ShardManager shardManager) {
            Shard shard = shardManager.getShard(ctx.getShardId());
            if (shard == null) {
                return true;
            }
            Set<Long> exclusiveIds = shard.getGroupIds().stream()
                    .map(shardManager::getShardGroup)
                    .filter(y -> y != null && CONFLICT_POLICIES.contains(y.getPlacementPolicy()))
                    .map(ShardGroup::getGroupId)
                    .collect(Collectors.toSet());
            if (exclusiveIds.isEmpty()) {
                return true;
            }
            try (LockCloseable ignored = new LockCloseable(exclusiveMapLock)) {
                Set<Long> groupMarker = exclusiveShardGroups.get(ctx.getWorkerGroupId());
                if (groupMarker == null) {
                    groupMarker = new HashSet<>();
                    exclusiveShardGroups.put(ctx.getWorkerGroupId(), groupMarker);
                } else {
                    // check if there is any conflict shard group in scheduling
                    if (exclusiveIds.stream().anyMatch(groupMarker::contains)) {
                        // has conflict, need to do the schedule later.
                        LOG.debug("Has conflict shardgroup running, retry later. {}", ctx);
                        return false;
                    }
                }
                // add all groupIds into exclusiveShardGroup to prevent shards in the same group to be scheduled.
                groupMarker.addAll(exclusiveIds);
                // save the exclusiveGroupIds and add into exclusiveShardGroups
                ctx.setExclusiveGroupIds(exclusiveIds);
            }
            return true;
        }
    }

    /**
     * Abstraction of phase 2 tasks.
     * Wrap of FutureTask with Comparable implementation, so it can be prioritized.
     */
    private static class DispatchTask<T> extends FutureTask<T> implements Comparable<DispatchTask<T>> {
        private final int priority;
        private final String description;

        public DispatchTask(Runnable runnable, T result, int priority, String description) {
            super(runnable, result);
            this.priority = priority;
            this.description = description;
        }

        public DispatchTask(Callable<T> callable, int priority, String description) {
            super(callable);
            this.priority = priority;
            this.description = description;
        }

        @Override
        public int compareTo(DispatchTask o) {
            // reverse order of Integer, because PriorityQueue peaks element from least to most
            return Integer.compare(o.priority, this.priority);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private DispatchTask<StarException> dispatchTaskForAddToWorker(
            String serviceId, List<Long> shardIds, long workerId, int priority) {
        Callable<StarException> callable = () -> {
            try {
                // NOTE: Replicas added to the worker directly should not be considered as temp replica
                // E.g., the worker is restarted, the replicas will be sent to the worker again, or
                // in ShardBalance scenario, the replica will be moved between workers.
                // TODO: what if the temp replica is balanced to a different worker??
                executeAddToWorker(serviceId, shardIds, false, workerId);
                return null;
            } catch (StarException e) {
                return e;
            } catch (Exception e) {
                return new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
        };
        String description = String.format("[AddToWorker Task] serviceId: %s, workerId: %s, priority: %d",
                serviceId, workerId, priority);
        return new DispatchTask<>(callable, priority, description);
    }

    private DispatchTask<Boolean> dispatchTaskForAddToGroup(
            ScheduleRequestContext ctx, int priority) {
        String description = String.format("[AddToGroup Task] %s, priority: %d", ctx, priority);
        return new DispatchTask<>(() -> executeAddToGroupPhase2(ctx), true, priority, description);
    }

    private DispatchTask<Boolean> dispatchTaskForRemoveFromGroup(ScheduleRequestContext ctx, int priority) {
        String description = String.format("[RemoveFromGroup Task] %s, priority: %d", ctx, priority);
        return new DispatchTask<>(() -> executeRemoveFromGroupPhase2(ctx), true, priority, description);
    }

    private DispatchTask<StarException> dispatchTaskForRemoveFromWorker(
            String serviceId, List<Long> shardIds, long workerId, int priority) {
        Callable<StarException> callable = () -> {
            try {
                executeRemoveFromWorker(serviceId, shardIds, workerId);
                return null;
            } catch (StarException e) {
                return e;
            } catch (Exception e) {
                return new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
        };
        String description = String.format("[RemoveFromWorker Task] serviceId: %s, workerId: %s, priority: %d",
                serviceId, workerId, priority);
        return new DispatchTask<>(callable, priority, description);
    }

    public ShardSchedulerV2(ServiceManager serviceManager, WorkerManager workerManager) {
        this.serviceManager = serviceManager;
        this.workerManager = workerManager;
    }

    /**
     * BLOCKING interface to schedule single shard (shardId) in service (serviceId) in workerGroup (wgId).
     * @param serviceId service id
     * @param shardId shard id
     * @param wgId worker group id
     * @throws StarException exception if fails
     */
    @Override
    public void scheduleAddToGroup(String serviceId, long shardId, long wgId) throws StarException {
        scheduleAddToGroup(serviceId, Collections.nCopies(1, shardId), wgId);
    }

    /**
     * BLOCKING interface of scheduling a list of shards to a WorkerGroup. These shards are not necessarily
     * to be in the same shard group.
     * @param serviceId  shard serviceId
     * @param shardIds  shard id list to be scheduled
     * @param wgId  workerGroup id to be scheduled
     * @throws StarException starException with ExceptionCode
     */
    @Override
    public void scheduleAddToGroup(String serviceId, List<Long> shardIds, long wgId) throws StarException {
        CountDownLatch latch = new CountDownLatch(shardIds.size());
        List<ScheduleRequestContext> ctxs = new ArrayList<>();
        for (Long id : shardIds) {
            ScheduleRequestContext ctx = new ScheduleRequestContext(serviceId, id, wgId, latch);
            ctxs.add(ctx);
            submitCalcTaskInternal(() -> executeAddToGroupPhase1(ctx), 0);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new StarException(ExceptionCode.SCHEDULE, e.getMessage());
        }
        for (ScheduleRequestContext ctx : ctxs) {
            if (ctx.getException() != null) {
                // If any shard schedule fails with exception, propagate it to the caller
                throw ctx.getException();
            }
        }
    }

    /**
     * Async schedule single shard (shardId) in service (serviceId) in wgId workerGroup (wgId), don't wait for result.
     * @param serviceId service identity
     * @param shardId shard identity
     * @param wgId worker group identity
     * @throws StarException exception if fails
     */
    @Override
    public void scheduleAsyncAddToGroup(String serviceId, long shardId, long wgId) throws StarException {
        ScheduleRequestContext ctx = new ScheduleRequestContext(serviceId, shardId, wgId, null);
        submitCalcTaskInternal(() -> executeAddToGroupPhase1(ctx), 0);
    }

    /**
     * Request to schedule a list of shards in service (serviceId), let schedule to choose the default worker group.
     *   Wait for the scheduling done.
     * @param serviceId service identity
     * @param shardIds list of shard identities
     * @throws StarException exception if fails
     */
    @Override
    public void scheduleAddToDefaultGroup(String serviceId, List<Long> shardIds) throws StarException {
        WorkerGroup group = workerManager.getDefaultWorkerGroup(serviceId);
        if (group == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("DefaultWorkerGroup not exist for service %s", serviceId));
        }
        // TODO: it is still possible that the worker group is deleted after this scheduleShards() called.
        scheduleAddToGroup(serviceId, shardIds, group.getGroupId());
    }

    /**
     * Remove redundant replicas for the shard, running:
     *   1. with low priority
     *   2. as background task
     *   3. remove one replica at most each time
     * @param serviceId target service id shard belongs to
     * @param shardId shard id
     * @param workerGroupId target worker group id
     * @throws StarException throws star exception if the task can't be added to thread pool.
     */
    @Override
    public void scheduleAsyncRemoveFromGroup(String serviceId, long shardId, long workerGroupId)
            throws StarException {
        ScheduleRequestContext ctx = new ScheduleRequestContext(serviceId, shardId, workerGroupId, null);
        submitCalcTaskInternal(() -> executeRemoveFromGroupPhase1(ctx), 0);
    }

    /**
     * Adding shard to group. Phase 1, choose workers from the worker group.
     */
    private void executeAddToGroupPhase1(ScheduleRequestContext ctx) {
        try {
            executeAddToGroupPhase1Detail(ctx);
        } catch (ScheduleConflictStarException e) {
            // A specific exception that schedule understands and retries the request
            // TODO: add RetryPolicy inside RequestCtx to achieve fine controlled retry behavior
            submitCalcTaskInternal(() -> executeAddToGroupPhase1(ctx), SHORT_NAP);
        } catch (StarException exception) {
            ctx.done(exception);
        } catch (Throwable throwable) {
            ctx.done(new StarException(ExceptionCode.SCHEDULE, throwable.getMessage()));
        }
    }

    /**
     * submit phase 1 task to calculateExecutors thread pool, optionally with delay
     * @param run runnable object
     * @param delay: delay time before execute, usually for retry
     * @exception StarException fail to submit the task
     */
    private void submitCalcTaskInternal(Runnable run, long delay) throws StarException {
        try {
            if (delay == 0) {
                calculateExecutors.execute(run);
            } else {
                calculateExecutors.schedule(run, delay, TimeUnit.MICROSECONDS);
            }
        } catch (RejectedExecutionException e) {
            if (!isRunning()) {
                throw new StarException(ExceptionCode.SCHEDULE, "Scheduling shutdown!");
            } else {
                throw new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
        }
    }

    /**
     * Phase2: add a replica of a shard to a worker without
     * @param serviceId service Id
     * @param shardId shard id to be added
     * @param workerId target worker Id to be added
     * @throws StarException Schedule Error
     * TODO: refactor with Priority Queue
     */
    @Override
    public void scheduleAddToWorker(String serviceId, long shardId, long workerId) throws StarException {
        scheduleAddToWorker(serviceId, Collections.nCopies(1, shardId), workerId);
    }

    /**
     * Phase2: batch add list of shards to a worker
     * @param serviceId service Id
     * @param shardIds list of shard ids to be added
     * @param workerId target worker Id to be added
     * @throws StarException Schedule Error
     */
    @Override
    public void scheduleAddToWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        DispatchTask<StarException> task = dispatchTaskForAddToWorker(serviceId, shardIds, workerId, PRIORITY_HIGH);
        submitDispatchTask(task, true /* wait */);
    }

    /**
     * Phase2: Async interface to schedule a list of shards to the target worker, don't wait for the result.
     * @param serviceId service id
     * @param shardIds list of shard ids
     * @param workerId target worker id
     * @throws StarException schedule error
     */
    @Override
    public void scheduleAsyncAddToWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        DispatchTask<StarException> task = dispatchTaskForAddToWorker(serviceId, shardIds, workerId, PRIORITY_MEDIUM);
        submitDispatchTask(task, false /* wait */);
    }

    /**
     * Phase2: Remove single shard replica from target worker. worker should belong to the service (serviceId)
     * @param serviceId  shard service Id
     * @param shardId  shard id to be removed
     * @param workerId  worker id
     */
    @Override
    public void scheduleRemoveFromWorker(String serviceId, long shardId, long workerId) throws StarException {
        scheduleRemoveFromWorker(serviceId, Collections.nCopies(1, shardId), workerId);
    }

    /**
     * Phase2: Remove list of shards from target worker, wait for the result back.
     * @param serviceId  shard service Id
     * @param shardIds  list of shard ids to be removed
     * @param workerId  target worker id
     * @throws StarException error out if the request can't be fulfilled. ExceptionCode will represent the error category.
     */
    @Override
    public void scheduleRemoveFromWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        DispatchTask<StarException> task = dispatchTaskForRemoveFromWorker(serviceId, shardIds, workerId, PRIORITY_MEDIUM);
        submitDispatchTask(task, true /* wait */);
    }

    /**
     * Phase2: Remove list of shards from target worker, don't wait for the result.
     * @param serviceId  shard service Id
     * @param shardIds  list of shard ids to be removed
     * @param workerId  target worker id
     */
    @Override
    public void scheduleAsyncRemoveFromWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        DispatchTask<StarException> task = dispatchTaskForRemoveFromWorker(serviceId, shardIds, workerId, PRIORITY_LOW);
        submitDispatchTask(task, false /* wait */);
    }

    private <T extends Exception>  void submitDispatchTask(DispatchTask<T> task, boolean wait) throws StarException {
        try {
            dispatchExecutors.execute(task);
        } catch (Exception e) {
            LOG.error("Fail to submit schedule task {}", task, e);
            throw new StarException(ExceptionCode.SCHEDULE, e.getMessage());
        }
        if (wait) {
            Exception exception = null;
            try {
                if (task.get() != null) {
                    exception = task.get();
                }
            } catch (Throwable e) {
                LOG.error("Fail to get task result. task: {}", task, e);
                throw new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
            if (exception != null) {
                if (exception instanceof StarException) {
                    throw (StarException) exception;
                } else {
                    // convert to StarException
                    throw new StarException(ExceptionCode.SCHEDULE, exception.getMessage());
                }
            }
        }
    }

    /**
     * Process a single shard schedule request.
     * @param ctx Request context
     * @throws ScheduleConflictStarException, schedule is conflicts, caller can retry the ctx.
     *         StarException, other error cases
     */
    private void executeAddToGroupPhase1Detail(ScheduleRequestContext ctx) {
        // TODO: to be multi-threads safe, shard/shardGroup/workerGroup accessing should be all thread-safe.
        ShardManager shardManager = serviceManager.getShardManager(ctx.getServiceId());
        if (shardManager == null) {
            ctx.done(new NotExistStarException("Service {} Not Exist", ctx.getServiceId()));
            return;
        }
        Shard shard = shardManager.getShard(ctx.getShardId());
        if (shard == null) {
            ctx.done(new NotExistStarException("Shard {} Not Exist", ctx.getShardId()));
            return;
        }
        WorkerGroup wg = workerManager.getWorkerGroupNoException(shard.getServiceId(), ctx.getWorkerGroupId());
        if (wg == null) {
            ctx.done(new NotExistStarException("WorkerGroup {} doesn't exist!", ctx.getWorkerGroupId()));
            return;
        }

        // check replica numbers
        int replicaNum = wg.getReplicaNumber();
        // existReplicas contains all the replicas including the ones in SCALE_IN state
        List<Long> existReplicas = new ArrayList<>();
        // validNumReplicas counts the replicas without the ones in SCALE_IN state.
        // dead replicas will be counted as long as they are not expired.
        int validNumReplicas = 0;
        int deadReplicas = 0;
        List<Long> tempReplica = new ArrayList<>();
        List<Replica> scaleInReplica = new ArrayList<>();
        for (Replica replica : shard.getReplica()) {
            Worker w = workerManager.getWorker(replica.getWorkerId());
            if (w == null || w.getGroupId() != ctx.getWorkerGroupId()) {
                continue;
            }
            if (!w.isAlive()) {
                if (!w.replicaExpired()) {
                    ++deadReplicas;
                }
            } else {
                if (replica.getState() != ReplicaState.REPLICA_SCALE_IN) {
                    // Don't count the replica in SCALE_IN state, it will be decommissioned soon.
                    validNumReplicas++;
                    if (replica.getTempFlag()) {
                        tempReplica.add(replica.getWorkerId());
                    }
                } else {
                    scaleInReplica.add(replica);
                }
            }
            existReplicas.add(replica.getWorkerId());
        }
        if (validNumReplicas + deadReplicas <= replicaNum) {
            // valid replicas + dead replicas less or equal to expected replicas
            // all the temp replicas should be converted to normal replicas
            // If OkReplica + TempReplica > replicaNum, let schedulerAsyncRemoveFromGroup to remove temp replicas first,
            // and then convert temp replicas to normal replicas here.
            for (long workerId : tempReplica) {
                LOG.debug("Convert shard:{} temp replica:{} to normal replica.", shard.getShardId(), workerId);
                shardManager.convertTempShardReplicaToNormal(Collections.nCopies(1, shard.getShardId()), workerId);
            }
        }
        if (validNumReplicas > 0 && validNumReplicas + deadReplicas >= replicaNum) {
            // has live replicas and live replicas + deadReplicas exceeds the expected number, don't do anything
            // All good, nothing to do. Let ShardChecker takes care of the removal.
            LOG.debug("shard:{} replica stats in workerGroup:{}, validNumReplicas:{}, deadReplica:{}, replicaNum:{}",
                    shard.getShardId(), ctx.getWorkerGroupId(), validNumReplicas, deadReplicas, replicaNum);
            ctx.done();
            return;
        }

        int desired = replicaNum - validNumReplicas - deadReplicas;
        // set if the replica should be a temp replica
        // `desired` will be <= 0 only when validNumReplica = 0 && deadReplicas >= replicaNum
        // NOTE: set the tempReplica flag every time in case the context get reused in retry scenario
        ctx.setGenerateTempReplica(desired <= 0);
        if (desired < 1) {
            // e.g. validNumReplicas = 0, deadReplicas = 4, replicaNum = 3
            // need to make sure at least one replica available for serving
            desired = 1;
        }
        int priority = PRIORITY_MEDIUM;
        if (ctx.isWaited()) {
            // someone is waiting for the request result
            priority = PRIORITY_HIGH;
        } else {
            if (desired <= replicaNum / 2) {
                // already has more than half of replicas, set its priority to LOW
                priority = PRIORITY_LOW;
            }
        }

        // get all alive worker Ids in the workerGroup
        Set<Long> wIds = new HashSet<>(wg.getAllWorkerIds(true /*onlyAlive*/));
        if (wIds.isEmpty()) {
            ctx.done(new NoAliveWorkersException("WorkerGroup {} doesn't have alive workers", ctx.getWorkerGroupId()));
            return;
        }
        // exclude workers that already has a copy of the shard
        existReplicas.forEach(wIds::remove);

        if (wIds.isEmpty() && !scaleInReplica.isEmpty()) {
            // After removing existing replicas, no workerId available, try to convert SCALE_IN replicas back to NORMAL if any
            Replica cancelReplica = scaleInReplica.get(0);
            LOG.debug("Cancel shard:{} replica:{} from SCALE_IN ...", shard.getShardId(), cancelReplica.getWorkerId());
            List<Long> shardIds = Collections.singletonList(shard.getShardId());
            shardManager.cancelScaleInShardReplica(shardIds, cancelReplica.getWorkerId());
            ctx.done();
            return;
        }

        // IMPORTANT: add exclusiveShardGroup marker so there is only one shard
        // from a shardGroup can be scheduled. This should be put before
        // pulling any shard replicas who co-exists in the same shard group as the processing one.
        if (!requestLocker.tryLock(ctx, shardManager)) {
            ctx.reset();
            throw new ScheduleConflictStarException();
        }

        try (DeferOp cleanOp = new DeferOp(ctx.getRunnable())) { // enters critical area
            // get shard's all 1st-degree shards categorized by shard group type
            Map<PlacementPolicy, Collection<Long>> ppMap = new HashMap<>();
            for (Long gid : shard.getGroupIds()) {
                ShardGroup g = shardManager.getShardGroup(gid);
                PlacementPolicy policy = g.getPlacementPolicy();
                List<Long> workerIds = new ArrayList<>();
                for (Long id : g.getShardIds()) {
                    if (id == shard.getShardId()) {
                        continue;
                    }
                    Shard firstDegreeShard = shardManager.getShard(id);
                    if (firstDegreeShard == null) {
                        continue;
                    }
                    workerIds.addAll(firstDegreeShard.getReplicaWorkerIds()
                            .stream()
                            .filter(wIds::contains)
                            .collect(Collectors.toList()));
                }
                if (workerIds.isEmpty()) {
                    continue;
                }
                if (ppMap.containsKey(policy)) {
                    ppMap.get(policy).addAll(workerIds);
                } else {
                    ppMap.put(policy, workerIds);
                }
            }

            ShardPolicyFilter.filter(ppMap, wIds);

            // Final: Select valid workerIds
            if (wIds.isEmpty()) {
                ctx.done(new StarException(ExceptionCode.SCHEDULE,
                        String.format("Can't find worker for request: %s", ctx)));
                return;
            }

            List<Long> workerIds;
            if (wIds.size() < desired) {
                LOG.debug("Schedule requests {} workers, but only {} available. {}", desired, wIds.size(), ctx);
                workerIds = new ArrayList<>(wIds);
            } else {
                ScheduleScorer scorer = new ScheduleScorer(wIds);
                // apply shardgroup policy scoring
                ppMap.forEach(scorer::apply);
                // apply worker status scoring
                scorer.apply(workerManager);
                LOG.debug("final scores for selection: {}, for request {}", scorer.getScores(), ctx);
                // Select the worker with the HIGHEST score
                workerIds = scorer.selectHighEnd(scoreSelector, desired);
            }
            ctx.setWorkerIds(workerIds);

            // submit to next thread pool to do the execution.
            LOG.debug("Schedule request {}, pending schedule to workerList: {}", ctx, ctx.getWorkerIds());
            DispatchTask<Boolean> task = dispatchTaskForAddToGroup(ctx, priority);
            try {
                dispatchExecutors.execute(task);
                // submit success, don't clean the exclusive group marker
                cleanOp.cancel();
            } catch (Throwable e) {
                LOG.error("Fail to add task {} into dispatchWorkerExecutors", task, e);
                // error out
                ctx.done(new StarException(ExceptionCode.SCHEDULE, e.getMessage()));
            }
        }
    }

    /**
     * Phase2: Process tasks submitted by phase1, talk to workers to assign shards.
     *  NOTE: the ctx will not be removed from pendingFinishRequests until its execution is done.
     * @param ctx ScheduleRequestContext to be processed
     */
    private void executeAddToGroupPhase2(ScheduleRequestContext ctx) {
        try (DeferOp ignored = new DeferOp(ctx.getRunnable())) {
            if (isRunning()) {
                executeAddToWorker(ctx);
            } else {
                ctx.done(new StarException(ExceptionCode.SCHEDULE, "Schedule shutdown in progress"));
            }
        }
    }

    private void executeAddToWorker(ScheduleRequestContext ctx) {
        StarException exception = null;
        for (long workerId : ctx.getWorkerIds()) {
            try {
                executeAddToWorker(ctx.getServiceId(), Collections.nCopies(1, ctx.getShardId()),
                        ctx.isGenerateTempReplica(), workerId);
            } catch (StarException e) {
                exception = e;
            } catch (Exception e) {
                exception = new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
        }
        ctx.done(exception);
    }

    /**
     * Do execution of adding shards to worker
     * @param serviceId service Id to be operated
     * @param shardIds  list of shards to be removed
     * @param workerId  target worker id
     */
    private void executeAddToWorker(String serviceId, List<Long> shardIds, boolean isTempReplica, long workerId) {
        ShardManager shardManager = serviceManager.getShardManager(serviceId);
        if (shardManager == null) {
            throw new StarException(ExceptionCode.NOT_EXIST, String.format("service %s not exists", serviceId));
        }

        Worker worker = workerManager.getWorker(workerId);
        if (worker == null) {
            throw new StarException(ExceptionCode.NOT_EXIST, String.format("worker %d not exists", workerId));
        }

        if (!worker.getServiceId().equals(serviceId)) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                    String.format("worker %d doesn't belong to service: %s", workerId, serviceId));
        }

        WorkerGroup wg = workerManager.getWorkerGroup(serviceId, worker.getGroupId());
        if (wg == null) {
            throw new NotExistStarException("workerGroup {} not exists", worker.getGroupId());
        }

        // Generate the AddShardInfo based on the workerGroup properties.
        List<Long> workerIds = wg.replicationEnabled() ? wg.getAllWorkerIds(false) : Lists.newArrayList(workerId);
        boolean includeReplicaHash = wg.replicationEnabled();
        Replica newReplica = new Replica(workerId);
        newReplica.setReplicaState(wg.warmupEnabled() ? ReplicaState.REPLICA_SCALE_OUT : ReplicaState.REPLICA_OK);

        List<List<AddShardInfo>> batches = new ArrayList<>();
        int remainShardSize = shardIds.size();
        List<AddShardInfo> miniBatch = new ArrayList<>(Integer.min(remainShardSize, Config.SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE));
        for (Long id : shardIds) {
            Shard shard = shardManager.getShard(id);
            --remainShardSize;
            if (shard == null) {
                if (shardIds.size() == 1) {
                    throw new StarException(ExceptionCode.NOT_EXIST, String.format("shard %d not exists", id));
                }
            } else {
                Replica suppReplica = shard.hasReplica(workerId) ? null : newReplica;
                miniBatch.add(shard.getAddShardInfo(workerIds, suppReplica, includeReplicaHash));
                if (miniBatch.size() >= Config.SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE) {
                    batches.add(miniBatch);
                    miniBatch = new ArrayList<>(Integer.min(remainShardSize, Config.SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE));
                }
            }
        }
        if (!miniBatch.isEmpty()) {
            batches.add(miniBatch);
        }

        for (List<AddShardInfo> info : batches) {
            AddShardRequest request = AddShardRequest.newBuilder()
                    .setServiceId(serviceId)
                    .setWorkerId(workerId)
                    .addAllShardInfo(info)
                    .build();

            addShardOpCounter.inc();
            try {
                worker.addShard(request);
                // rpc succeed, do meta stuff
                List<Long> currentBatchShardIdList =
                        request.getShardInfoList().stream().map(AddShardInfo::getShardId).collect(Collectors.toList());
                if (wg.warmupEnabled()) {
                    shardManager.scaleOutShardReplicas(currentBatchShardIdList, workerId, isTempReplica);
                } else {
                    shardManager.addShardReplicas(currentBatchShardIdList, workerId, isTempReplica);
                }
            } catch (WorkerNotHealthyStarException e) {
                throw e;
            } catch (StarException e) {
                throw new StarException(ExceptionCode.SCHEDULE, String.format(
                        "Schedule add shard task execution failed serviceId: %s, workerId: %d, shardIds: %s",
                        serviceId, workerId, shardIds));
            }
        }
    }

    /**
     * Do execution of removing shards
     * @param serviceId service Id to be operated
     * @param shardIds  list of shards to be removed
     * @param workerId  target worker id
     */
    private void executeRemoveFromWorker(String serviceId, List<Long> shardIds, long workerId) {
        ShardManager shardManager = serviceManager.getShardManager(serviceId);
        if (shardManager == null) {
            throw new StarException(ExceptionCode.NOT_EXIST, String.format("service %s not exist!", serviceId));
        }

        Worker worker = workerManager.getWorker(workerId);
        if (worker == null) {
            LOG.debug("worker {} not exist when execute remove shards for service {}!", workerId, serviceId);
        } else if (!worker.isAlive()) {
            LOG.debug("worker {} dead when execute remove shards for service {}.", workerId, serviceId);
        } else if (!worker.getServiceId().equals(serviceId)) {
            LOG.debug("worker {} doesn't belong to service {}", workerId, serviceId);
        } else {
            RemoveShardRequest request = RemoveShardRequest.newBuilder()
                    .setServiceId(serviceId)
                    .setWorkerId(workerId)
                    .addAllShardIds(shardIds).build();
            removeShardOpCounter.inc();
            try {
                worker.removeShard(request);
            } catch (WorkerNotHealthyStarException e) {
                throw e;
            } catch (StarException e) {
                // mark this as schedule exception when clean failed
                throw new StarException(ExceptionCode.SCHEDULE, String.format(
                        "Schedule remove shard task execution failed serviceId: %s, workerId: %d, shardIds: %s",
                        serviceId, workerId, shardIds));
            }
        }
        // worker mismatch or rpc succeed, do meta stuff
        shardManager.removeShardReplicas(shardIds, workerId);
    }

    /**
     * Try to select a replica to remove for given shard from given worker Group
     * @param ctx request context
     */
    private void executeRemoveFromGroupPhase1(ScheduleRequestContext ctx) {
        if (!isRunning()) {
            ctx.done(new StarException(ExceptionCode.SCHEDULE, "Schedule in shutdown progress"));
            return;
        }
        try {
            executeRemoveFromGroupPhase1Detail(ctx);
        } catch (ScheduleConflictStarException exception) {
            // TODO: add RetryPolicy inside RequestCtx to achieve fine controlled retry behavior
            submitCalcTaskInternal(() -> executeRemoveFromGroupPhase1(ctx), SHORT_NAP);
        } catch (StarException exception) {
            ctx.done(exception);
        } catch (Throwable throwable) {
            ctx.done(new StarException(ExceptionCode.SCHEDULE, throwable.getMessage()));
        }
    }

    /**
     * execute the task of selecting a replica of the shard from the worker group to remove.
     *
     * @param ctx Request context
     * @throws ScheduleConflictStarException - schedule is conflicts, caller can retry the ctx.
     *         StarException - other error cases
     */
    private void executeRemoveFromGroupPhase1Detail(ScheduleRequestContext ctx) {
        ShardManager shardManager = serviceManager.getShardManager(ctx.getServiceId());
        if (shardManager == null) {
            ctx.done(new StarException(ExceptionCode.NOT_EXIST, String.format("Service %s Not Exist", ctx.getServiceId())));
            return;
        }

        Shard shard = shardManager.getShard(ctx.getShardId());
        if (shard == null) {
            ctx.done(new StarException(ExceptionCode.NOT_EXIST, String.format("Shard %d Not Exist", ctx.getShardId())));
            return;
        }

        WorkerGroup wg = workerManager.getWorkerGroup(ctx.getServiceId(), ctx.getWorkerGroupId());
        if (wg == null) {
            ctx.done(new NotExistStarException("WorkerGroup {} Not Exist", ctx.getWorkerGroupId()));
            return;
        }

        int replicaNum = wg.getReplicaNumber();
        List<Long> okWorkerIds = new ArrayList<>();
        List<Long> scaleInWorkerIds = new ArrayList<>();
        List<Long> expiredReplicas = new ArrayList<>();
        List<Long> tempReplicas = new ArrayList<>();
        for (Replica replica : shard.getReplica()) {
            long workerId = replica.getWorkerId();
            Worker worker = workerManager.getWorker(workerId);
            if (worker == null) {
                expiredReplicas.add(workerId);
            } else if (worker.getGroupId() == ctx.getWorkerGroupId()) {
                // Our interested worker group
                if (worker.replicaExpired()) {
                    expiredReplicas.add(workerId);
                } else if (worker.isAlive()) {
                    // based on the removal priority
                    // SCALE_IN > TEMP > OK, SCALE_OUT is not considered, TEMP + SCALE_OUT will be considered as TEMP
                    if (replica.getState() == ReplicaState.REPLICA_SCALE_IN) {
                        scaleInWorkerIds.add(workerId);
                    } else if (replica.getTempFlag()) {
                        tempReplicas.add(workerId);
                    } else if (replica.getState() == ReplicaState.REPLICA_OK) {
                        okWorkerIds.add(workerId);
                    }
                }
            }
        }
        for (long workerId : expiredReplicas) {
            // remove the replicas directly without sending the RPC to the worker.
            shardManager.removeShardReplicas(Collections.nCopies(1, ctx.getShardId()), workerId);
        }

        // dead replicas are not taking into consideration here.
        if (okWorkerIds.size() + scaleInWorkerIds.size() + tempReplicas.size() <= replicaNum) {
            // Don't do anything, because right now the total replicas are less or equal to the expected number.
            // Wish dead replicas can be back some time in the future. Finger crossed.
            LOG.debug("{}, Number of replicas (include dead ones) are less than expected replica. Skip it.", ctx);
            ctx.done();
            return;
        }

        List<Long> workerIds;
        if (!requestLocker.tryLock(ctx, shardManager)) {
            throw new ScheduleConflictStarException();
        }
        // tryLock will call ctx.setCloseable() to register a closeable to clean the exclusive markers
        try (DeferOp cleanOp = new DeferOp(ctx.getRunnable())) {
            // When removing replicas, the replicas are preferred in the following order:
            // 1. workers with scale-in replica
            // 2. workers with ok replica
            // NOTE:
            //   - scale-out replicas will not be touched.
            //   - dead replicas will not be touched
            // TODO: Is it a good idea to prefer scale-out replica over OK replica here?

            // scoring all the replicas
            ScheduleScorer scorer;
            if (!scaleInWorkerIds.isEmpty()) {
                scorer = new ScheduleScorer(scaleInWorkerIds);
            } else if (!tempReplicas.isEmpty()) {
                scorer = new ScheduleScorer(tempReplicas);
            } else {
                scorer = new ScheduleScorer(okWorkerIds);
            }
            for (long groupId : shard.getGroupIds()) {
                ShardGroup group = shardManager.getShardGroup(groupId);
                if (group == null) {
                    continue;
                }

                List<Long> allReplicaWorkerIds = new ArrayList<>();
                for (long sid : group.getShardIds()) {
                    if (sid == shard.getShardId()) {
                        continue;
                    }
                    Shard firstDegreeShard = shardManager.getShard(sid);
                    if (firstDegreeShard == null) {
                        continue;
                    }
                    allReplicaWorkerIds.addAll(firstDegreeShard.getReplicaWorkerIds());
                }
                scorer.apply(group.getPlacementPolicy(), allReplicaWorkerIds);
            }
            scorer.apply(workerManager);
            LOG.debug("final scores for selection: {}, for request {}", scorer.getScores(), ctx);
            // select replica with the LOWEST score to remove
            workerIds = scorer.selectLowEnd(scoreSelector, 1);
            LOG.debug("Final selection for remove-healthy shard, request:{} selection: {}", ctx, workerIds);

            Preconditions.checkState(workerIds.size() == 1L, "Should only have one replica to remove!");

            ctx.setWorkerIds(workerIds);
            LOG.debug("Schedule request {}, pending schedule to workerList: {}", ctx, ctx.getWorkerIds());
            DispatchTask<Boolean> task = dispatchTaskForRemoveFromGroup(ctx, ctx.isWaited() ? PRIORITY_MEDIUM : PRIORITY_LOW);
            try {
                dispatchExecutors.execute(task);
                // submit success, don't clean the exclusive group marker
                cleanOp.cancel();
            } catch (Throwable e) {
                LOG.error("Fail to add task {} into dispatchWorkerExecutors", task, e);
                // error out
                ctx.done(new StarException(ExceptionCode.SCHEDULE, e.getMessage()));
            }
        }
    }

    /**
     * Select the worker whose lastSeenTime is oldest.
     * @param workers workers to choose
     * @return the worker with smallest lastSeenTime
     */
    private Worker selectOldestWorkerLastSeen(List<Worker> workers) {
        Worker targetWorker = workers.get(0);
        for (Worker worker : workers) {
            if (worker.getLastSeenTime() < targetWorker.getLastSeenTime()) {
                targetWorker = worker;
            }
        }
        return targetWorker;
    }

    private void executeRemoveFromGroupPhase2(ScheduleRequestContext ctx) {
        try (DeferOp ignored = new DeferOp(ctx.getRunnable())) {
            if (isRunning()) {
                executeRemoveFromWorker(ctx);
            } else {
                ctx.done(new StarException(ExceptionCode.SCHEDULE, "Schedule shutdown in progress"));
            }
        }
    }

    private void executeRemoveFromWorker(ScheduleRequestContext ctx) {
        StarException exception = null;
        for (long workerId : ctx.getWorkerIds()) {
            try {
                executeRemoveFromWorker(ctx.getServiceId(), Collections.nCopies(1, ctx.getShardId()), workerId);
            } catch (StarException e) {
                exception = e;
            } catch (Exception e) {
                exception = new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
        }
        ctx.done(exception);
    }

    @Override
    public boolean isIdle() {
        if (!isRunning()) {
            return true;
        }
        return calculateExecutors.getCompletedTaskCount() == calculateExecutors.getTaskCount() &&
                dispatchExecutors.getCompletedTaskCount() == dispatchExecutors.getTaskCount();
    }

    @Override
    public void doStart() {
        calculateExecutors = new ScheduledThreadPoolExecutor(2, namedThreadFactory("scheduler-calc-pool"));
        calculateExecutors.setMaximumPoolSize(2);

        dispatchExecutors = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<>(),
                namedThreadFactory("scheduler-dispatch-pool"));

        adjustPoolExecutors = new ScheduledThreadPoolExecutor(1);
        adjustPoolExecutors.setMaximumPoolSize(1);

        adjustPoolExecutors.scheduleAtFixedRate(this::adjustScheduleThreadPoolSize, INIT_CHECK_THREAD_DELAY_SECS,
                ADJUST_THREAD_INTERVAL_SECS, TimeUnit.SECONDS);
    }

    // automatically adjust thread pool size according to the number of shards
    void adjustScheduleThreadPoolSize() {
        if (!isRunning()) {
            return;
        }
        long totalShards = 0;
        for (String serviceId : serviceManager.getServiceIdSet()) {
            ShardManager shardManager = serviceManager.getShardManager(serviceId);
            if (shardManager == null) {
                continue;
            }
            totalShards += shardManager.getShardCount();
        }
        int numOfThreads = estimateNumOfCoreThreads(totalShards);
        if (calculateExecutors.getCorePoolSize() == numOfThreads) {
            return;
        }

        LOG.info("Total number of shards in memory: {}, adjust scheduler core thread pool size from {} to {}",
                totalShards, calculateExecutors.getCorePoolSize(), numOfThreads);

        // `dispatchExecutors` always has the double threads number of the `calculateExecutors`
        if (calculateExecutors.getCorePoolSize() < numOfThreads) {
            // scale out
            calculateExecutors.setMaximumPoolSize(numOfThreads);
            calculateExecutors.setCorePoolSize(numOfThreads);
            dispatchExecutors.setMaximumPoolSize(numOfThreads * 2);
            dispatchExecutors.setCorePoolSize(numOfThreads * 2);
        } else {
            // scale in
            calculateExecutors.setCorePoolSize(numOfThreads);
            calculateExecutors.setMaximumPoolSize(numOfThreads);
            dispatchExecutors.setCorePoolSize(numOfThreads * 2);
            dispatchExecutors.setMaximumPoolSize(numOfThreads * 2);
        }
    }

    /**
     * This is just a rough estimation of the threads based on experiences
     *
     * @param numOfShards the total number of shards in the service managers
     */
    static int estimateNumOfCoreThreads(long numOfShards) {
        if (numOfShards < 5 * 1000) { // 5K
            return 1;
        } else if (numOfShards < 10 * 1000) { // 10K
            return 2;
        } else if (numOfShards < 100 * 1000) { // 100K
            return 4;
        } else if (numOfShards < 1000 * 1000) { // 1M
            return 8;
        } else {
            // at most 16 threads for calculateExecutors and 32 threads for dispatchExecutors
            return 16;
        }
    }

    @Override
    public void doStop() {
        Utils.shutdownExecutorService(calculateExecutors);
        Utils.shutdownExecutorService(dispatchExecutors);
        Utils.shutdownExecutorService(adjustPoolExecutors);
    }
}
