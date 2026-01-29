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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.staros.exception.AlreadyExistsStarException;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.StarException;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.util.LockCloseable;
import com.staros.util.Text;
import com.staros.util.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Worker Group manages a certain number of worker in one service.
 * NOTE:
 *  `replicationType` affects the replicas sent to the worker, specifically,
 *   - NO_SYNC: only the single replica will be pushed to the worker, and the replicas will not be included
 *       in the Shard object hashCode() calculation.
 *   - SYNC/ASYNC: not only the single replica assigned to the worker will be pushed to the worker, but also the replicas
 *        in the same workerGroup will be pushed to the worker as well. The hashCode sent to the worker includes the
 *        replica changes in the same worker group.
 *  `warmupLevel` affects the replica state.
 *   - WARMUP_NOTHING: the new replica will be directly turn to REPLICA_STATE_OK
 *   - WARMUP_META/WARMUP_FOOTER/WARMUP_ALL: the new replica will be in REPLICA_SCALE_OUT first and then to REPLICA_OK.
 *      offline replica will be in REPLICA_SCALE_IN first and wait for the counterpart replica to be ready and then
 *      removed from the system.
 */
public class WorkerGroup implements Writable {
    private static final Logger LOG = LogManager.getLogger(WorkerGroup.class);
    private static final String BUILTIN_OWNER = "OwnByStarManager";
    private static final WorkerGroupSpec BUILTIN_SPEC = WorkerGroupSpec.newBuilder().setSize("L").build();

    private final String serviceId;
    private final long groupId;
    private final String owner;
    private WorkerGroupSpec spec;
    private WorkerGroupState state;
    private Map<String, String> labels;
    private Map<String, String> properties;
    private int replicaNumber;
    private ReplicationType replicationType;
    private WarmupLevel warmupLevel;

    // can be rebuilt by replaying all workers
    private final Map<Long, Worker> workers;
    private final ReentrantReadWriteLock readWriteLock;

    public WorkerGroup(String serviceId, long groupId) {
        this(serviceId, groupId, BUILTIN_OWNER, BUILTIN_SPEC, null, null, 1 /* replicaNumber */,
                ReplicationType.NO_REPLICATION, WarmupLevel.WARMUP_NOTHING);
    }

    public WorkerGroup(String serviceId, long groupId, String owner, WorkerGroupSpec spec, Map<String, String> labels,
                       Map<String, String> properties, int replicaNumber, ReplicationType replicationType,
                       WarmupLevel warmupLevel) {
        this.serviceId = serviceId;
        this.groupId = groupId;
        this.owner = owner;
        // make a copy of the spec
        this.spec = WorkerGroupSpec.newBuilder(spec).build();
        if (labels == null) {
            this.labels = ImmutableMap.of();
        } else {
            this.labels = ImmutableMap.copyOf(labels);
        }
        if (properties == null) {
            this.properties = ImmutableMap.of();
        } else {
            this.properties = ImmutableMap.copyOf(properties);
        }
        this.replicaNumber = replicaNumber;
        this.replicationType = replicationType;
        this.state = WorkerGroupState.PENDING;
        // handle backwards compatibility, default to warmup nothing
        if (warmupLevel == WarmupLevel.WARMUP_NOT_SET) {
            this.warmupLevel = WarmupLevel.WARMUP_NOTHING;
        } else {
            this.warmupLevel = warmupLevel;
        }

        this.workers = new HashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public String getServiceId() {
        return serviceId;
    }

    public long getGroupId() {
        return groupId;
    }

    public String getOwner() {
        return owner;
    }

    public WorkerGroupSpec getSpec() {
        return spec;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void setReplicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
    }

    public ReplicationType getReplicationType() {
        return replicationType;
    }

    public boolean replicationEnabled() {
        Preconditions.checkState(replicationType != ReplicationType.NO_SET);
        return replicationType.ordinal() > ReplicationType.NO_REPLICATION.ordinal();
    }

    public void setWarmupLevel(WarmupLevel warmupLevel) {
        Preconditions.checkState(warmupLevel != WarmupLevel.WARMUP_NOT_SET);
        this.warmupLevel = warmupLevel;
    }

    public WarmupLevel getWarmupLevel() {
        return warmupLevel;
    }

    public boolean warmupEnabled() {
        Preconditions.checkState(warmupLevel != WarmupLevel.WARMUP_NOT_SET);
        return warmupLevel.ordinal() > WarmupLevel.WARMUP_NOTHING.ordinal();
    }

    public WorkerGroupState getState() {
        return state;
    }

    public void updateState(WorkerGroupState state) {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            this.state = state;
        }
    }

    public void updateSpec(WorkerGroupSpec spec) {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            this.spec = WorkerGroupSpec.newBuilder(spec).build();
            this.state = WorkerGroupState.PENDING;
        }
    }

    public void setLabels(Map<String, String> labels) {
        if (labels == null) {
            return;
        }
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            this.labels = ImmutableMap.copyOf(labels);
        }
    }

    public void setProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            this.properties = ImmutableMap.copyOf(properties);
        }
    }

    public void setReplicaNumber(int rn) throws StarException {
        if (rn <= 0) {
            throw new InvalidArgumentStarException("worker group {} replica number should be larger than 0, now is {}.",
                    groupId, rn);
        }
        replicaNumber = rn;
    }

    public void addWorker(Worker worker) throws StarException {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            checkWorkerNoLock(worker);
            if (workers.containsKey(worker.getWorkerId())) {
                throw new AlreadyExistsStarException("worker {}({}) already exist in worker group.",
                        worker.getWorkerId(), worker.getIpPort());
            }
            worker.setWorkerGroup(this);
            workers.put(worker.getWorkerId(), worker);
        }
    }

    public boolean removeWorker(Worker worker) throws StarException {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            if (!workers.containsKey(worker.getWorkerId())) {
                return false;
            } else {
                checkWorkerNoLock(worker);
                workers.remove(worker.getWorkerId());
                return true;
            }
        }
    }

    public boolean updateWorker(Worker worker) throws StarException {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.writeLock())) {
            if (!workers.containsKey(worker.getWorkerId())) {
                return false;
            }
            checkWorkerNoLock(worker);
            return workers.get(worker.getWorkerId()).update(worker);
        }
    }

    public List<Long> getAllWorkerIds(boolean onlyAlive) {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.readLock())) {
            if (onlyAlive) {
                return workers.values().stream()
                        .filter(Worker::isAlive)
                        .map(Worker::getWorkerId)
                        .collect(Collectors.toList());
            } else {
                return new ArrayList<>(workers.keySet());
            }
        }
    }

    public int getWorkerCount() {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.readLock())) {
            return workers.size();
        }
    }

    public Worker getWorker(long workerId) {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.readLock())) {
            return workers.get(workerId);
        }
    }

    private void checkWorkerNoLock(Worker worker) throws IllegalStateException {
        Preconditions.checkState(worker.getServiceId().equals(serviceId),
                String.format("worker's service id %s not match service id %s in worker group.",
                        worker.getServiceId(), serviceId));

        Preconditions.checkState(worker.getGroupId() == groupId,
                String.format("worker's group id %d not match group id %d in worker group.",
                        worker.getGroupId(), groupId));
    }

    public WorkerGroupDetailInfo toProtobuf() {
        try (LockCloseable ignored = new LockCloseable(readWriteLock.readLock())) {
            return WorkerGroupDetailInfo.newBuilder()
                    .setServiceId(serviceId)
                    .setGroupId(groupId)
                    .setOwner(owner)
                    .setSpec(spec)
                    .putAllLabels(labels)
                    .putAllProperties(properties)
                    .setState(state)
                    .setReplicaNumber(replicaNumber)
                    .setReplicationType(replicationType)
                    .setWarmupLevel(warmupLevel)
                    .build();
        }
    }

    public static WorkerGroup fromProtobuf(WorkerGroupDetailInfo info) {
        int replicaNumber = info.getReplicaNumber() == 0 ? 1 : info.getReplicaNumber();
        ReplicationType replicationType = (info.getReplicationType() == ReplicationType.NO_SET) ?
                ReplicationType.NO_REPLICATION : info.getReplicationType();
        WorkerGroup group = new WorkerGroup(info.getServiceId(),
                info.getGroupId(),
                info.getOwner(),
                info.getSpec(),
                info.getLabelsMap(),
                info.getPropertiesMap(),
                replicaNumber,
                replicationType,
                info.getWarmupLevel());
        group.updateState(info.getState());
        return group;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static WorkerGroup read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        WorkerGroupDetailInfo info = WorkerGroupDetailInfo.parseFrom(bytes);
        return WorkerGroup.fromProtobuf(info);
    }
}
