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


package com.staros.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.staros.exception.AlreadyExistsStarException;
import com.staros.exception.FailedPreconditionStarException;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.StarException;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.staros.journal.StarMgrJournal;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.ReplicationType;
import com.staros.proto.SectionType;
import com.staros.proto.UpdateWorkerGroupInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerManagerImageMetaFooter;
import com.staros.proto.WorkerManagerImageMetaHeader;
import com.staros.proto.WorkerManagerServiceWorkerGroup;
import com.staros.proto.WorkerManagerWorkers;
import com.staros.section.Section;
import com.staros.section.SectionReader;
import com.staros.section.SectionWriter;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.IdGenerator;
import com.staros.util.LockCloseable;
import com.staros.util.LogUtils;
import com.staros.util.Utils;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.staros.util.Utils.executeNoExceptionOrDie;
import static com.staros.util.Utils.namedThreadFactory;

/**
 * Worker Manager handles all operations that are related
 * to worker, such as add worker and remove worker.
 * <p>
 * Assumptions:
 * - WorkerGroupId is globally unique
 * Running Mode: WorkerManager can run in two different modes based on whether supporting zero worker group id.
 * - Strict Mode:
 * Zero group id is not supported. Client must create a worker group before adding a worker
 * - Compatible Mode:
 * Zero group id is allowed, and is created ahead by starManager, client can add worker to group-0 without
 * creating it first. This mode is mostly used when doing process level integration with starRocks FE and support manual
 * management of BE with `ALTER SYSTEM ADD/DROP BACKEND XXX`.
 */
public class WorkerManager {
    private static final Logger LOG = LogManager.getLogger(WorkerManager.class);

    private static final long NO_WORKER_GROUP_ID = -1;
    // global variables for prometheus metrics
    private static ThreadPoolExecutor TPE_FOR_METRIC;

    private static int ADJUST_THREAD_POOL_INTERVAL_MS = 60 * 1000; // unit: MILLISECONDS
    private static int ADJUST_THREAD_POOL_INIT_DELAY_MS = 30 * 1000; // unit: MILLISECONDS

    // workerId -> Worker
    private final Map<Long, Worker> workers;
    // serviceId -> DefaultWorkerGroupId
    private final Map<String, ServiceWorkerGroup> serviceWorkerGroups;

    private final JournalSystem journalSystem;
    private final IdGenerator idGenerator;
    private final ReentrantReadWriteLock lock;
    private final ResourceManager resourceManager;
    // Workload ThreadPool
    private ThreadPoolExecutor executor;
    // Single-Thread for adjusting the size of executor
    private ScheduledExecutorService adjustExecutor;

    static {
        // Initialize the global GaugeCallback.
        MetricsSystem.registerGaugeCallback("starmgr_workpool_pending_tasks",
                "count of pending tasks in starmgr work pool",
                WorkerManager::getExecutorPendingTasks);
    }

    private static void getExecutorPendingTasks(GaugeWithCallback.Callback cb) {
        long len = 0;
        ThreadPoolExecutor tmpExecutor = TPE_FOR_METRIC;
        if (tmpExecutor != null) {
            len = tmpExecutor.getTaskCount() - tmpExecutor.getCompletedTaskCount();
        }
        if (len < 0) {
            len = 0;
        }
        cb.call(len);
    }

    /**
     * Helper data structure that implements map interface, can be used as a map object.
     * - manage worker groups by service
     * - tracking default worker group id
     * - be able to provide COMPATIBLE mode (allows 0 group id)
     * - No integrity check or lock is assured, all should be done in WorkerManager.
     */
    private static class ServiceWorkerGroup implements Map<Long, WorkerGroup> {
        private final String serviceId;
        // Don't expect too many WorkerGroups in a service, but use MAP in order to index workerGroups by worker group id.
        private final Map<Long, WorkerGroup> workerGroups;
        private long defaultWorkerGroupId;

        public ServiceWorkerGroup(String serviceId) {
            this.serviceId = serviceId;
            this.workerGroups = new HashMap<>();
            this.defaultWorkerGroupId = NO_WORKER_GROUP_ID;
            if (Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
                this.workerGroups.put(Constant.DEFAULT_ID, new WorkerGroup(serviceId, Constant.DEFAULT_ID));
                this.defaultWorkerGroupId = Constant.DEFAULT_ID;
            }
        }

        public String getServiceId() {
            return serviceId;
        }

        public WorkerGroup getDefaultWorkerGroup() {
            return workerGroups.get(defaultWorkerGroupId);
        }

        @Override
        public int size() {
            return workerGroups.size();
        }

        @Override
        public boolean isEmpty() {
            return workerGroups.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return workerGroups.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return workerGroups.containsValue(value);
        }

        @Override
        public WorkerGroup get(Object key) {
            return workerGroups.get(key);
        }

        @Override
        public WorkerGroup put(Long key, WorkerGroup value) {
            WorkerGroup result = workerGroups.put(key, value);
            updateDefaultWorkerGroup();
            return result;
        }

        @Override
        public WorkerGroup remove(Object key) {
            WorkerGroup result = workerGroups.remove(key);
            updateDefaultWorkerGroup();
            return result;
        }

        @Override
        public void putAll(Map<? extends Long, ? extends WorkerGroup> m) {
            workerGroups.putAll(m);
            updateDefaultWorkerGroup();
        }

        @Override
        public void clear() {
            workerGroups.clear();
            defaultWorkerGroupId = NO_WORKER_GROUP_ID;
        }

        @Override
        public Set<Long> keySet() {
            return workerGroups.keySet();
        }

        @Override
        public Collection<WorkerGroup> values() {
            return workerGroups.values();
        }

        @Override
        public Set<Entry<Long, WorkerGroup>> entrySet() {
            return workerGroups.entrySet();
        }

        private void updateDefaultWorkerGroup() {
            if (workerGroups.containsKey(defaultWorkerGroupId)) {
                return;
            }
            defaultWorkerGroupId = workerGroups.keySet()
                    .stream()
                    .min(Comparator.naturalOrder())
                    .orElse(NO_WORKER_GROUP_ID);
        }
    }

    public WorkerManager(JournalSystem journalSystem, IdGenerator idGenerator) {
        this.workers = new HashMap<>();
        this.serviceWorkerGroups = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.journalSystem = journalSystem;
        this.idGenerator = idGenerator;
        this.resourceManager = ResourceManagerFactory.createResourceManager(this);
        this.executor = null;
    }

    public void bootstrapService(String serviceId) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (serviceWorkerGroups.containsKey(serviceId)) {
                return;
            }
            serviceWorkerGroups.put(serviceId, new ServiceWorkerGroup(serviceId));
        }
    }

    public long createWorkerGroup(String serviceId, String owner, WorkerGroupSpec spec, Map<String, String> labels,
                                  Map<String, String> properties, int replicaNumber, ReplicationType replicationType,
                                  WarmupLevel warmupLevel) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                throw new NotExistStarException("ServiceId:{} not exist!", serviceId);
            }
            if (!resourceManager.isValidSpec(spec)) {
                throw new InvalidArgumentStarException("Invalid WorkerGroupSpec:{}", spec.getSize());
            }
            if (replicaNumber < 0) {
                throw new InvalidArgumentStarException("worker group replica number should be larger than 0, now is {}.",
                        replicaNumber);
            }
            if (replicaNumber == 0) {
                replicaNumber = 1; // default to 1
            }
            if (replicationType == ReplicationType.NO_SET) {
                replicationType = ReplicationType.NO_REPLICATION;
            }
            long groupId = idGenerator.getNextId();
            Preconditions.checkState(!serviceWorkerGroups.get(serviceId).containsKey(groupId));
            WorkerGroup group = new WorkerGroup(serviceId, groupId, owner, spec, labels, properties, replicaNumber,
                    replicationType, warmupLevel);

            Journal journal = StarMgrJournal.logCreateWorkerGroup(group);
            journalSystem.write(journal);

            executeNoExceptionOrDie(() -> serviceWorkerGroups.get(serviceId).put(groupId, group));
            submitAsyncTask(() -> resourceManager.provisionResource(serviceId, groupId, spec, owner));
            return groupId;
        }
    }

    /**
     * Add a worker for worker group in service
     *
     * @return worker id
     * @throws StarException - ALREADY_EXIST if the ipPort already used by other worker
     *                       - NOT_EXIST if the given groupId or serviceId not exist yet
     */
    public long addWorker(String serviceId, long groupId, String ipPort) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            WorkerGroup group = getWorkerGroupInternalNoLock(serviceId, groupId);
            if (group == null) {
                throw new NotExistStarException("worker group {} or service {} not exist.", groupId, serviceId);
            }
            if (workers.values().stream().anyMatch(x -> x.getIpPort().equals(ipPort))) {
                throw new AlreadyExistsStarException("worker address {} already exist.", ipPort);
            }
            return addWorkerInternal(serviceId, groupId, ipPort);
        }
    }

    public List<Long> addWorkers(String serviceId, long groupId, List<String> ipPorts) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            WorkerGroup group = getWorkerGroupInternalNoLock(serviceId, groupId);
            if (group == null) {
                throw new NotExistStarException("worker group {} or service {} not exist.", groupId, serviceId);
            }
            if (workers.values().stream().anyMatch(x -> ipPorts.contains(x.getIpPort()))) {
                throw new AlreadyExistsStarException("duplicated worker address detected.");
            }

            List<Long> result = new ArrayList<>();
            for (String ipPort : ipPorts) {
                result.add(addWorkerInternal(serviceId, groupId, ipPort));
            }
            return result;
        }
    }

    private long addWorkerInternal(String serviceId, long groupId, String ipPort) throws StarException {
        WorkerGroup group = getWorkerGroupInternalNoLock(serviceId, groupId);
        Preconditions.checkState(group != null);
        long workerId = idGenerator.getNextId();
        Preconditions.checkState(!workers.containsKey(workerId));
        Worker worker = new Worker(serviceId, groupId, workerId, ipPort);

        Journal journal = StarMgrJournal.logAddWorker(worker);
        journalSystem.write(journal);
        executeNoExceptionOrDie(() -> {
            // Add the worker into workers and also corresponding serviceWorkerGroup
            workers.put(workerId, worker);
            group.addWorker(worker);
        });
        LOG.info("worker {} added to group {} in service {}.", workerId, groupId, serviceId);

        submitAsyncTask(() -> doWorkerHeartbeat(workerId));
        return workerId;
    }

    public void doWorkerHeartbeat(long workerId) {
        Worker worker = getWorker(workerId);
        if (worker != null) {
            Pair<Boolean, Boolean> pair = worker.heartbeat();
            if (pair.getValue()) { // state has changed
                persistWorkerInfo(workerId);
            }
        }
    }

    private void persistWorkerInfo(long workerId) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            Worker worker = workers.get(workerId);
            if (worker != null) {
                try {
                    Journal journal = StarMgrJournal.logUpdateWorker(worker.getServiceId(),
                            Collections.nCopies(1, worker));
                    journalSystem.write(journal);
                } catch (StarException e) {
                    // TODO: writes journal failed, need another mechanism to make sure
                    //       follower gets the right worker state
                    LOG.warn("fail to persist worker {} info.", workerId);
                }
            }
        }
    }

    /**
     * Remove a worker for worker group in service
     *
     * @throws StarException if group id or worker id not exist
     */
    public void removeWorker(String serviceId, long groupId, long workerId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            WorkerGroup group = getWorkerGroupInternalNoLock(serviceId, groupId);
            if (group == null) {
                throw new NotExistStarException("worker group {} or service {} not exist.", groupId, serviceId);
            }
            if (!workers.containsKey(workerId)) {
                throw new NotExistStarException("worker:{} not exist in worker group.", workerId);
            }
            Worker worker = workers.get(workerId);
            if (!worker.getServiceId().equals(serviceId) || worker.getGroupId() != groupId) {
                throw new InvalidArgumentStarException("inconsistent group or service info of worker:{}", workerId);
            }

            Journal journal = StarMgrJournal.logRemoveWorker(serviceId, groupId, workerId);
            journalSystem.write(journal);

            executeNoExceptionOrDie(() -> {
                group.removeWorker(worker);
                deleteWorkerInternal(workerId);
            });
            LOG.info("worker {} removed from group {} in service {}.", workerId, groupId, serviceId);
        }
    }

    /**
     * Get worker info by id
     */
    public WorkerInfo getWorkerInfo(long workerId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            if (!workers.containsKey(workerId)) {
                throw new NotExistStarException("worker:{} not exist.", workerId);
            }
            return workers.get(workerId).toProtobuf();
        }
    }

    /**
     * Get worker info by ip port
     */
    public WorkerInfo getWorkerInfo(String ipPort) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            for (Worker worker : workers.values()) {
                if (worker.getIpPort().equals(ipPort)) {
                    return worker.toProtobuf();
                }
            }
            throw new NotExistStarException("worker:{} not exist.", ipPort);
        }
    }

    /**
     * Returns a default worker group of the service.
     *
     * @param serviceId service identity
     * @return default WorkerGroup. the workerGroup with the smallest id returns or exception if no worker group available.
     * @throws StarException NOT_EXIST if the service doesn't have any workerGroup yet.
     */
    public WorkerGroup getDefaultWorkerGroup(String serviceId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                throw new NotExistStarException("service:{} has no default worker group!", serviceId);
            }
            WorkerGroup group = serviceWorkerGroups.get(serviceId).getDefaultWorkerGroup();
            if (group == null) {
                throw new NotExistStarException("service:{} has no default worker group!", serviceId);
            }
            return group;
        }
    }

    public WorkerGroup getWorkerGroup(String serviceId, long groupId) throws StarException {
        WorkerGroup group = getWorkerGroupNoException(serviceId, groupId);
        if (group == null) {
            throw new NotExistStarException("worker group {} or service {} not exist.", groupId, serviceId);
        }
        return group;
    }

    public WorkerGroup getWorkerGroupNoException(String serviceId, long groupId) {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return getWorkerGroupInternalNoLock(serviceId, groupId);
        }
    }

    public Worker getWorker(long workerId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return workers.get(workerId);
        }
    }

    /**
     * Get a snapshot of all worker ids
     * @return list of worker ids
     */
    public List<Long> getAllWorkerIds() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return new ArrayList<>(workers.keySet());
        }
    }

    private WorkerGroup getWorkerGroupInternalNoLock(String serviceId, long groupId) {
        ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
        if (swg == null) {
            return null;
        }
        return swg.get(groupId);
    }

    /**
     * process worker heartbeat, update worker's info
     */
    public boolean processWorkerHeartbeat(String serviceId, long workerId, long startTime, long numOfShards,
                                          Map<String, String> workerProperties, long lastSeenTime) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!workers.containsKey(workerId)) {
                throw new NotExistStarException("worker:{} not exist.", workerId);
            }
            Worker worker = workers.get(workerId);
            if (!worker.getServiceId().equals(serviceId)) {
                throw new InvalidArgumentStarException("worker:{} does not belong to service:{}.", workerId, serviceId);
            }
            if (lastSeenTime < worker.getLastSeenTime()) {
                throw new FailedPreconditionStarException("Expired heartbeat update for worker:{} at {}. Reject it!",
                        workerId, lastSeenTime);
            }
            worker.updateLastSeenTime(lastSeenTime);
            // <isRestart, needPersist>
            Pair<Boolean, Boolean> pair = worker.updateInfo(startTime, workerProperties, numOfShards);
            // NOTE: no need to update the worker in workerGroups, because the two places are referring the same object.
            if (pair.getValue()) {
                persistWorkerInfo(workerId);
            }
            return pair.getKey();
        }
    }

    public void replayCreateWorkerGroup(String serviceId, WorkerGroup group) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                LogUtils.fatal(LOG, "service:{} not exist, should not happen!", serviceId);
            }
            long groupId = group.getGroupId();
            if (serviceWorkerGroups.get(serviceId).containsKey(groupId)) {
                LogUtils.fatal(LOG, "workerGroup:{} already exist when replay add worker group, should not happen!", groupId);
            }
            serviceWorkerGroups.get(serviceId).put(groupId, group);
        }
    }

    public void replayAddWorker(String serviceId, Worker worker) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            WorkerGroup group = getWorkerGroupInternalNoLock(serviceId, worker.getGroupId());
            if (group == null) {
                LogUtils.fatal(LOG, "service:{} or group:{} not exist, should not happen!", serviceId, worker.getGroupId());
                return;
            }
            if (workers.containsKey(worker.getWorkerId())) {
                LogUtils.fatal(LOG, "worker {} already exist when replay add worker, should not happen!", worker.getWorkerId());
                return;
            }
            workers.put(worker.getWorkerId(), worker);
            group.addWorker(worker);
        }
    }

    public void replayRemoveWorker(String serviceId, long groupId, long workerId) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!workers.containsKey(workerId)) {
                LOG.warn("worker:{} should exist before replay remove operation. Ignore it for now!", workerId);
                return;
            }
            Worker worker = workers.get(workerId);
            Preconditions.checkState(worker.getServiceId().equals(serviceId),
                    String.format("worker:%d service id mismatch!", workerId));
            Preconditions.checkState(worker.getGroupId() == groupId,
                    String.format("worker:%d group id mismatch!", workerId));

            serviceWorkerGroups.get(serviceId).get(groupId).removeWorker(worker);
            workers.remove(workerId);
        }
    }

    public void replayUpdateWorker(String serviceId, List<Worker> workers) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                LogUtils.fatal(LOG, "service:{} not exist when replayUpdateWorker, should never happen!", serviceId);
                return;
            }
            for (Worker worker : workers) {
                if (!this.workers.containsKey(worker.getWorkerId()) ||
                        getWorkerGroupInternalNoLock(serviceId, worker.getGroupId()) == null) {
                    LogUtils.fatal(LOG, "worker {} not exist when replay update worker, should not happen!",
                            worker.getWorkerId());
                    return;
                }
            }
            for (Worker worker : workers) {
                // No need to update workers in workerGroup, they are referring to the same object instance.
                this.workers.get(worker.getWorkerId()).update(worker);
            }
        }
    }

    public int getWorkerCount() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return workers.size();
        }
    }

    public List<Long> getAllWorkerGroupIds(String serviceId) {
        List<Long> workerGroupIds = new ArrayList<>();
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            if (serviceWorkerGroups.containsKey(serviceId)) {
                workerGroupIds.addAll(serviceWorkerGroups.get(serviceId).keySet());
            }
        }
        return workerGroupIds;
    }

    public List<WorkerGroupDetailInfo> listWorkerGroupsById(String serviceId, List<Long> groupIds, boolean includeWorkersInfo) {
        if (groupIds.isEmpty()) {
            throw new InvalidArgumentStarException("Empty worker group id provided!");
        }
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                throw new NotExistStarException("service:{} not registered yet!", serviceId);
            }
            ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
            List<WorkerGroupDetailInfo> result = new ArrayList<>();
            for (long id : groupIds) {
                if (swg.containsKey(id)) {
                    result.add(buildWorkerGroupDetailInfo(swg.get(id), includeWorkersInfo));
                } else {
                    throw new NotExistStarException("group:{} not exist in service:{}", id, serviceId);
                }
            }
            return result;
        }
    }

    public List<WorkerGroupDetailInfo> listWorkerGroups(String serviceId, Map<String, String> filterLabels,
                                                        boolean includeWorkersInfo) {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                throw new NotExistStarException("service:{} not registered yet!", serviceId);
            }
            ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
            List<WorkerGroupDetailInfo> result = new ArrayList<>();
            if (filterLabels.isEmpty()) {
                result = swg.values().stream()
                        .map(x -> buildWorkerGroupDetailInfo(x, includeWorkersInfo))
                        .collect(Collectors.toList());
            } else {
                for (WorkerGroup group : swg.values()) {
                    if (allLabelMatching(group.getLabels(), filterLabels)) {
                        result.add(buildWorkerGroupDetailInfo(group, includeWorkersInfo));
                    }
                }
            }
            if (result.isEmpty()) {
                throw new NotExistStarException("No worker group matching criteria in service:{}", serviceId);
            } else {
                return result;
            }
        }
    }

    private WorkerGroupDetailInfo buildWorkerGroupDetailInfo(WorkerGroup group, boolean includeWorkersInfo) {
        WorkerGroupDetailInfo.Builder builder = WorkerGroupDetailInfo.newBuilder().mergeFrom(group.toProtobuf());
        if (includeWorkersInfo) {
            builder.addAllWorkersInfo(group.getAllWorkerIds(false).stream()
                            .map(workers::get)
                            .map(Worker::toProtobuf)
                            .collect(Collectors.toList()));
        }
        return builder.build();
    }

    private static boolean allLabelMatching(Map<String, String> sourceLabels, Map<String, String> expectLabels) {
        return expectLabels.entrySet()
                .stream()
                .allMatch(x -> Objects.equals(sourceLabels.get(x.getKey()), x.getValue()));
    }

    public void updateWorkerGroup(String serviceId, long groupId, WorkerGroupSpec spec, Map<String, String> labels,
                                  Map<String, String> props, int replicaNumber, ReplicationType replicationType,
                                  WarmupLevel warmupLevel)
            throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                throw new NotExistStarException("service:{} not registered yet!", serviceId);
            }
            ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
            if (!swg.containsKey(groupId)) {
                throw new NotExistStarException("worker group:{} not exist in service:{}!", groupId, serviceId);
            }
            if (spec != null && !resourceManager.isValidSpec(spec)) {
                throw new InvalidArgumentStarException("Invalid WorkerGroupSpec:{}", spec.getSize());
            }
            UpdateWorkerGroupInfo.Builder builder = UpdateWorkerGroupInfo.newBuilder().setGroupId(groupId);
            if (spec != null) {
                builder.setSpec(spec);
            }
            if (labels != null) {
                builder.putAllLabels(labels);
            }
            if (props != null) {
                builder.putAllProperties(props);
            }
            if (spec != null) {
                builder.setSpec(spec);
            }
            if (replicaNumber > 0) {
                builder.setReplicaNumber(replicaNumber);
            }
            if (replicationType != ReplicationType.NO_SET) {
                builder.setReplicationType(replicationType);
            }
            if (warmupLevel != WarmupLevel.WARMUP_NOT_SET) {
                builder.setWarmupLevel(warmupLevel);
            }

            Journal journal = StarMgrJournal.logUpdateWorkerGroup(serviceId, builder.build());
            journalSystem.write(journal);
            executeNoExceptionOrDie(() -> {
                WorkerGroup group = swg.get(groupId);

                if (spec != null) {
                    group.updateSpec(spec);
                    submitAsyncTask(() -> resourceManager.alterResourceSpec(serviceId, groupId, spec));
                }
                if (props != null && !props.isEmpty()) {
                    group.setProperties(props);
                }
                if (labels != null && !labels.isEmpty()) {
                    group.setLabels(labels);
                }
                if (replicaNumber > 0) {
                    group.setReplicaNumber(replicaNumber);
                }
                if (replicationType != ReplicationType.NO_SET) {
                    group.setReplicationType(replicationType);
                }
                if (warmupLevel != WarmupLevel.WARMUP_NOT_SET) {
                    group.setWarmupLevel(warmupLevel);
                }
            });
        }
    }

    public void replayUpdateWorkerGroup(String serviceId, UpdateWorkerGroupInfo info) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                LogUtils.fatal(LOG, "service:{} not registered yet during replay!", serviceId);
            }
            long groupId = info.getGroupId();
            ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
            if (!swg.containsKey(groupId)) {
                LogUtils.fatal(LOG, "worker group:{} not exist in service:{}, during replay updateWorkerGroup!",
                        groupId, serviceId);
            }
            WorkerGroup group = swg.get(groupId);
            if (info.hasSpec()) {
                // TODO: just set spec, don't trigger resource change
                group.updateSpec(info.getSpec());
            }
            if (!info.getLabelsMap().isEmpty()) {
                group.setLabels(info.getLabelsMap());
            }
            if (!info.getPropertiesMap().isEmpty()) {
                group.setProperties(info.getPropertiesMap());
            }
            if (info.getReplicaNumber() > 0) {
                group.setReplicaNumber(info.getReplicaNumber());
            }
            if (info.getReplicationType() != ReplicationType.NO_SET) {
                group.setReplicationType(info.getReplicationType());
            }
            if (info.getWarmupLevel() != WarmupLevel.WARMUP_NOT_SET) {
                group.setWarmupLevel(info.getWarmupLevel());
            }
        }
    }

    public void deleteWorkerGroup(String serviceId, long groupId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                throw new NotExistStarException("service:{} not registered yet!", serviceId);
            }
            ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
            if (!swg.containsKey(groupId)) {
                throw new NotExistStarException("worker group:{} not exist in service:{}!", groupId, serviceId);
            }
            Journal journal = StarMgrJournal.logDeleteWorkerGroup(serviceId, groupId);
            journalSystem.write(journal);
            executeNoExceptionOrDie(() -> deleteWorkerGroupInternalNoLock(swg, groupId));
            submitAsyncTask(() -> resourceManager.releaseResource(serviceId, groupId));
        }
    }

    public void replayDeleteWorkerGroup(String serviceId, long groupId) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (!serviceWorkerGroups.containsKey(serviceId)) {
                LogUtils.fatal(LOG, String.format("service:%s not registered yet! Should not happen!", serviceId));
            }
            ServiceWorkerGroup swg = serviceWorkerGroups.get(serviceId);
            if (!swg.containsKey(groupId)) {
                LogUtils.fatal(LOG, String.format("worker group:%d not exist in service:%s, should not happen!",
                        groupId, serviceId));
            }
            deleteWorkerGroupInternalNoLock(swg, groupId);
        }
    }

    private void deleteWorkerGroupInternalNoLock(ServiceWorkerGroup serviceWorkerGroup, long groupId) {
        WorkerGroup group = serviceWorkerGroup.get(groupId);
        List<Long> allIds = group.getAllWorkerIds(false);
        allIds.forEach(this::deleteWorkerInternal);
        serviceWorkerGroup.remove(groupId);
    }

    private void deleteWorkerInternal(long workerId) {
        Worker todo = workers.remove(workerId);
        if (todo != null) {
            submitAsyncTask(todo::decommission);
        }
    }

    public void submitAsyncTask(Runnable runnable) {
        try {
            if (executor != null) {
                executor.execute(runnable);
            }
        } catch (Exception exception) {
            LOG.info("Fail to add async task for new added worker, ignore it for now. ", exception);
        }
    }

    /**
     * dump workerManager's meta
     *
     * @param out Output stream
     * @throws IOException I/O exception
     * <pre>
     *  +--------------------+
     *  | WORKER_MGR_HEADER  | (batch_size, number of workers, number of service worker groups)
     *  +--------------------+
     *  | WORKER_MANAGER_SWG |
     *  +------------------- +
     *  |      SWG-1         |
     *  |      SWG-2         |
     *  |       ...          |
     *  |      SWG-N         |
     *  +--------------------+
     *  | WORKER_MGR_WORKER  |
     *  +--------------------+
     *  |  WORKERS_BATCH_1   |
     *  |  WORKERS_BATCH_2   |
     *  |       ...          |
     *  |  WORKERS_BATCH_N   |
     *  +--------------------+
     *  | WORKER_MGR_FOOTER  | (checksum)
     *  +--------------------+
     * </pre>
     */
    public void dumpMeta(OutputStream out) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            LOG.debug("start dump worker manager meta data ...");
            final int NUM_PER_BATCH = 100;
            WorkerManagerImageMetaHeader header = WorkerManagerImageMetaHeader.newBuilder()
                    .setNumServiceWorkerGroup(serviceWorkerGroups.size())
                    .setNumWorker(workers.size())
                    .setBatchSize(NUM_PER_BATCH)
                    .setDigestAlgorithm(Constant.DEFAULT_DIGEST_ALGORITHM)
                    .build();
            header.writeDelimitedTo(out);

            DigestOutputStream mdStream = Utils.getDigestOutputStream(out, Constant.DEFAULT_DIGEST_ALGORITHM);
            try (SectionWriter writer = new SectionWriter(mdStream)) {
                // Write WorkerGroups
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_WORKERMGR_SWG)) {
                    dumpServiceWorkerGroups(stream);
                }
                // Write Workers
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_WORKERMGR_WORKER)) {
                    dumpWorkers(stream, NUM_PER_BATCH);
                }
            }
            // flush the mdStream because we are going to write to `out` directly.
            mdStream.flush();
            WorkerManagerImageMetaFooter.Builder footerBuilder = WorkerManagerImageMetaFooter.newBuilder();
            if (mdStream.getMessageDigest() != null) {
                footerBuilder.setChecksum(ByteString.copyFrom(mdStream.getMessageDigest().digest()));
            }
            footerBuilder.build().writeDelimitedTo(out);
            LOG.debug("end dump worker manager meta data.");
        }
    }

    public void loadMeta(InputStream in) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            LOG.debug("start load worker manager meta data ...");
            WorkerManagerImageMetaHeader header = WorkerManagerImageMetaHeader.parseDelimitedFrom(in);
            if (header == null) {
                throw new EOFException();
            }
            int numSwg = header.getNumServiceWorkerGroup();
            int numWorkers = header.getNumWorker();

            DigestInputStream digestInput = Utils.getDigestInputStream(in, header.getDigestAlgorithm());

            try (SectionReader reader = new SectionReader(digestInput)) {
                reader.forEach(x -> loadSection(x, header));
            }

            WorkerManagerImageMetaFooter footer = WorkerManagerImageMetaFooter.parseDelimitedFrom(in);
            if (footer == null) {
                throw new EOFException();
            }
            Utils.validateChecksum(digestInput.getMessageDigest(), footer.getChecksum());
            LOG.debug("end load worker manager meta data, loaded serviceWorkerGroup:{}, workers:{}",
                    numSwg, numWorkers);
        }
    }

    private void loadSection(Section section, WorkerManagerImageMetaHeader header) throws IOException {
        InputStream stream = section.getStream();
        switch (section.getHeader().getSectionType()) {
            case SECTION_WORKERMGR_SWG:
                loadServiceWorkerGroups(stream, header.getNumServiceWorkerGroup());
                break;
            case SECTION_WORKERMGR_WORKER:
                loadWorkers(stream, header.getNumWorker(), header.getBatchSize());
                break;
            default:
                LOG.warn("Unknown section type:{} when loadMeta in WorkerManager, ignore it!",
                        section.getHeader().getSectionType());
                break;
        }
    }

    private void dumpServiceWorkerGroups(OutputStream stream) throws IOException {
        for (ServiceWorkerGroup swg : serviceWorkerGroups.values()) {
            // single service worker group serialization
            WorkerManagerServiceWorkerGroup.Builder builder = WorkerManagerServiceWorkerGroup.newBuilder()
                    .setServiceId(swg.getServiceId())
                    .setNumWorkerGroup(swg.workerGroups.size());
            WorkerGroup defaultWorkerGroup = swg.getDefaultWorkerGroup();
            // ensure the default worker group is the first worker group to serialize
            if (defaultWorkerGroup != null) {
                builder.addWorkerGroupDetails(defaultWorkerGroup.toProtobuf().toByteString());
            }
            for (WorkerGroup group : swg.values()) {
                if (group == defaultWorkerGroup) {
                    continue;
                }
                builder.addWorkerGroupDetails(group.toProtobuf().toByteString());
            }
            builder.build().writeDelimitedTo(stream);
        }
    }

    private void loadServiceWorkerGroups(InputStream stream, int numServiceWorkerGroup) throws IOException {
        for (int i = 0; i < numServiceWorkerGroup; ++i) {
            WorkerManagerServiceWorkerGroup swg = WorkerManagerServiceWorkerGroup.parseDelimitedFrom(stream);
            if (swg == null) {
                throw new EOFException();
            }
            Preconditions.checkState(swg.getNumWorkerGroup() == swg.getWorkerGroupDetailsCount());
            bootstrapService(swg.getServiceId());
            for (ByteString bs : swg.getWorkerGroupDetailsList()) {
                WorkerGroup group = WorkerGroup.fromProtobuf(WorkerGroupDetailInfo.parseFrom(bs));
                serviceWorkerGroups.get(group.getServiceId()).put(group.getGroupId(), group);
            }
        }
    }

    private void dumpWorkers(OutputStream stream, int batchSize) throws IOException {
        // Batch serialize workers to avoid a huge portion of protobuf message
        WorkerManagerWorkers.Builder builder = WorkerManagerWorkers.newBuilder();
        int totalCount = 0;
        for (Worker worker : workers.values()) {
            builder.addWorker(worker.toProtobuf().toByteString());
            if (builder.getWorkerCount() >= batchSize) {
                totalCount += builder.getWorkerCount();
                builder.build().writeDelimitedTo(stream);
                builder = WorkerManagerWorkers.newBuilder();
            }
        }
        if (builder.getWorkerCount() > 0) {
            totalCount += builder.getWorkerCount();
            builder.build().writeDelimitedTo(stream);
        }
        Preconditions.checkState(totalCount == workers.size());
    }

    private void loadWorkers(InputStream stream, int numWorkers, int batchSize) throws IOException {
        int numBatches = numWorkers / batchSize;
        if (numWorkers % batchSize > 0) {
            ++numBatches;
        }
        for (int i = 0; i < numBatches; ++i) {
            WorkerManagerWorkers batchWorkers = WorkerManagerWorkers.parseDelimitedFrom(stream);
            if (batchWorkers == null) {
                throw new EOFException();
            }
            for (ByteString bs : batchWorkers.getWorkerList()) {
                Worker worker = Worker.fromProtobuf(WorkerInfo.parseFrom(bs));
                WorkerGroup group = getWorkerGroupInternalNoLock(worker.getServiceId(), worker.getGroupId());
                if (group != null) {
                    workers.put(worker.getWorkerId(), worker);
                    group.addWorker(worker);
                } else {
                    LOG.warn("Can't find worker group for worker: {}", worker);
                }
            }
        }
    }

    public void start() {
        if (executor != null) {
            Utils.shutdownExecutorService(adjustExecutor);
            Utils.shutdownExecutorService(executor);
            executor = null;
            adjustExecutor = null;
        }
        executor = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                namedThreadFactory("worker-generic-pool"));
        TPE_FOR_METRIC = executor;

        adjustExecutor = Executors.newScheduledThreadPool(1, namedThreadFactory("worker-pool-adjust"));
        adjustExecutor.scheduleAtFixedRate(this::adjustGenericPoolSize, ADJUST_THREAD_POOL_INTERVAL_MS,
                ADJUST_THREAD_POOL_INIT_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (executor != null) {
            Utils.shutdownExecutorService(adjustExecutor);
            Utils.shutdownExecutorService(executor);
            executor = null;
            TPE_FOR_METRIC = null;
            adjustExecutor = null;
        }
        resourceManager.stop();
    }

    @VisibleForTesting
    static int calculateThreadPoolSize(int currentSize, int numWorkers, long pendingTasks) {
        final int MIN_CORE_SIZE = 2;
        final int MAX_CORE_SIZE = 64;
        final int ADJUST_STEP = 2;

        int newSize = currentSize;
        if (pendingTasks <= numWorkers) {
            if (currentSize > 4) { // slowly scale-in
                newSize = Math.max(newSize - 1, MIN_CORE_SIZE);
            }
            // else (pendingTasks < numWorkers && currentSize <= 4): DO NOTHING
            // Not that many pending tasks, and not that many threads, don't bother to adjust the threads.
        } else { // scale-out
            // multiplex = log10(pendingTasks)
            // 1: pendingTasks <= 10
            // 2: pendingTasks -> (10, 100]
            // 3: pendingTasks -> (100, 1000]
            // ...
            int multiplex = (int) Math.ceil(Math.log10(pendingTasks));
            newSize += multiplex * ADJUST_STEP;
            newSize = Math.min(newSize, MAX_CORE_SIZE);
        }
        return newSize;
    }

    private void adjustGenericPoolSize() {
        ThreadPoolExecutor tmpExecutor = executor;
        if (tmpExecutor == null) {
            return;
        }
        int currentSize = tmpExecutor.getCorePoolSize();
        int expectedPoolSize =
                calculateThreadPoolSize(currentSize, workers.size(), tmpExecutor.getQueue().size());
        if (expectedPoolSize != currentSize) {
            LOG.info("Adjust WorkerManager worker-generic-pool size from {} to {}", currentSize, expectedPoolSize);
            Utils.adjustFixedThreadPoolExecutors(tmpExecutor, expectedPoolSize);
        }
    }

    public void dump(DataOutputStream out) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            for (ServiceWorkerGroup swg : serviceWorkerGroups.values()) {
                for (WorkerGroup workerGroup : swg.values()) {
                    String s = JsonFormat.printer().print(workerGroup.toProtobuf()) + "\n";
                    out.writeBytes(s);
                }
            }
            for (Worker worker : workers.values()) {
                String s = JsonFormat.printer().print(worker.toProtobuf()) + "\n";
                out.writeBytes(s);
            }
        }
    }

    public static WorkerManager createWorkerManagerForTest(JournalSystem journalSystem) {
        return new WorkerManager(journalSystem == null ? new DummyJournalSystem() : journalSystem, new IdGenerator(null));
    }
}
