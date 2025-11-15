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
import com.staros.exception.StarException;
import com.staros.exception.WorkerNotHealthyStarException;
import com.staros.proto.AddShardRequest;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.staros.starlet.StarletAgent;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.LockCloseable;
import com.staros.util.Text;
import com.staros.util.Writable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Worker implements Writable {
    private static final Logger LOG = LogManager.getLogger(Worker.class);

    private final String serviceId;
    private final long groupId;
    private final long workerId;
    private final String ipPort;
    private long startTime;
    // last healthy heartbeat time, Usually set as System.currentTimeMillis() in ms
    private long lastSeenTime;
    // the latest timestamp the worker is marked as DOWN
    private long lastDownTime;
    private final AtomicReference<WorkerState> state;
    // ImmutableMap, Copy-On-Write when doing updateInfo()
    private Map<String, String> workerProperties;
    private final ReentrantReadWriteLock lock;

    // Approximate number of shards reported by starlet client,
    // no need to persistent, will be updated by worker's heartbeat.
    // TODO: what if allows starlet to report shards in separate batches?
    private final AtomicLong numOfShards;
    private final StarletAgent starletAgent;
    private int heartbeatEpoch;
    private WorkerGroup workerGroup; // not persist to protobuf

    public Worker(String serviceId, long groupId, long workerId, String ipPort) {
        this.serviceId = serviceId;
        this.groupId = groupId;
        this.workerId = workerId;
        this.ipPort = ipPort;
        this.startTime = 0;
        this.lastSeenTime = 0;
        this.lastDownTime = 0;
        this.state = new AtomicReference<>(WorkerState.DOWN);
        this.workerProperties = ImmutableMap.of();
        this.lock = new ReentrantReadWriteLock();
        this.workerGroup = null;

        this.heartbeatEpoch = 0;
        this.numOfShards = new AtomicLong(0);
        this.starletAgent = StarletAgentFactory.newStarletAgent();
        this.starletAgent.setWorker(this);
    }

    public void setWorkerGroup(WorkerGroup wg) {
        this.workerGroup = wg;
    }

    public boolean warmupEnabled() {
        return workerGroup.warmupEnabled();
    }

    public String getServiceId() {
        return serviceId;
    }

    public long getGroupId() {
        return groupId;
    }

    public long getWorkerId() {
        return workerId;
    }

    public String getIpPort() {
        return ipPort;
    }

    public WorkerState getState() {
        return state.get();
    }

    public Map<String, String> getProperties() {
        return workerProperties;
    }

    public long getNumOfShards() {
        long n = numOfShards.get();
        return n < 0 ? 0 : n;
    }

    public boolean setState(WorkerState state) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (this.state.get() == state) {
                return false;
            }
            this.state.set(state);
            if (state == WorkerState.DOWN) {
                // record the timestamp when the worker is marked as down.
                this.lastDownTime = System.currentTimeMillis();
            }
            LOG.info("worker {} state set to {}.", workerId, this.state);
            return true;
        }
    }

    public boolean isAlive() {
        return this.state.get() == WorkerState.ON;
    }

    public boolean isShutdown() {
        return this.state.get() == WorkerState.SHUTTING_DOWN;
    }

    public void updateLastSeenTime(long lastSeenTime) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            if (lastSeenTime > this.lastSeenTime) {
                this.lastSeenTime = lastSeenTime;
            }
        }
    }

    public long getLastSeenTime() {
        return lastSeenTime;
    }

    public ReplicationType getReplicationType() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            Preconditions.checkState(workerGroup != null);
            return workerGroup.getReplicationType();
        }
    }

    public WarmupLevel getWarmupLevel() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            Preconditions.checkState(workerGroup != null);
            return workerGroup.getWarmupLevel();
        }
    }

    public long getLastDownTime() {
        return lastDownTime;
    }

    /**
     * Update worker internal state. Called by worker heartbeat response.
     *
     * @param sTime       worker start time
     * @param workerProps worker properties
     * @param numOfShards number of shards
     * @return Pair<restarted, needPersist>
     */
    public Pair<Boolean, Boolean> updateInfo(long sTime, Map<String, String> workerProps, long numOfShards) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            boolean needPersist = false;
            if (workerProps != null && !workerProperties.equals(workerProps)) {
                LOG.debug("worker {} properties changed, prev:{}, now:{}.", workerId, workerProperties, workerProps);
                workerProperties = ImmutableMap.copyOf(workerProps);
                needPersist = true;
            }

            boolean restarted = false;
            if (sTime == 0 || sTime < startTime) {
                LOG.info("Detect invalid start time of worker {}, reported startTime:{}, latest startTime:{}!",
                        workerId, sTime, startTime);
            } else { // startTime <= sTime
                if (sTime > startTime) {
                    // startTime == 0 means this is the first time to get the worker's heartbeat, don't treat it as restart.
                    if (startTime != 0L) {
                        restarted = true;
                        SimpleDateFormat fmt = new SimpleDateFormat("MM-dd HH:mm:ss.SSS");
                        LOG.info("Detect worker {} start at {}, previous start time: {}.",
                                workerId, fmt.format(new Date(sTime)), fmt.format(new Date(startTime)));
                    }
                    startTime = sTime;
                }
            }
            // update number of shards hosted on the worker
            ++this.heartbeatEpoch;
            this.numOfShards.set(numOfShards);
            needPersist = needPersist || restarted;
            return Pair.of(restarted, needPersist);
        }
    }

    /**
     * Called by Worker::fromProtobuf() to restore its internal state.
     *
     * @param startTime    start time
     * @param state        worker state
     * @param workerProps  worker properties
     * @param lastSeenTime worker last seen time
     * @param lastDownTime worker last downtime
     */
    private void restoreState(long startTime, WorkerState state, Map<String, String> workerProps, long lastSeenTime,
                              long lastDownTime) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            this.startTime = startTime;
            this.state.set(state);
            this.workerProperties = workerProps;
            this.lastSeenTime = lastSeenTime;
            this.lastDownTime = lastDownTime;
        }
    }

    public WorkerInfo toProtobuf() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return WorkerInfo.newBuilder()
                    .setServiceId(serviceId)
                    .setGroupId(groupId)
                    .setWorkerId(workerId)
                    .setIpPort(ipPort)
                    .setWorkerState(getState())
                    .putAllWorkerProperties(workerProperties)
                    .setStartTime(startTime)
                    .setLastDownTime(lastDownTime)
                    .setTabletNum(numOfShards.get())
                    .build();
        }
    }

    public static Worker fromProtobuf(WorkerInfo info) {
        String serviceId = info.getServiceId();
        long groupId = info.getGroupId();
        long workerId = info.getWorkerId();
        String ipPort = info.getIpPort();
        WorkerState state = info.getWorkerState();
        long startTime = info.getStartTime();
        long downtime = info.getLastDownTime();
        Map<String, String> workerProps = info.getWorkerPropertiesMap();

        Worker worker = new Worker(serviceId, groupId, workerId, ipPort);
        // use startTime as its initial lastSeenTime
        worker.restoreState(startTime, state, workerProps, startTime, downtime);
        return worker;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static Worker read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        WorkerInfo info = WorkerInfo.parseFrom(bytes);
        return Worker.fromProtobuf(info);
    }

    /*
     * @return Pair<success, stateChanged>
     */
    public Pair<Boolean, Boolean> heartbeat() {
        Pair<Boolean, Boolean> pair = starletAgent.heartbeat();
        if (pair.getKey()) {
            updateLastSeenTime(System.currentTimeMillis());
        }
        return pair;
    }

    public boolean match(Worker worker) {
        if (worker == null) {
            return false;
        }
        if (this.workerId == worker.workerId) {
            Preconditions.checkState(this.serviceId.equals(worker.serviceId));
            Preconditions.checkState(this.groupId == worker.groupId);
            return true;
        } else {
            return false;
        }
    }

    public boolean update(Worker worker) {
        if (this.workerId != worker.workerId) {
            return false;
        }
        Preconditions.checkState(this.groupId == worker.groupId);
        Preconditions.checkState(this.serviceId.equals(worker.serviceId));

        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            this.startTime = worker.startTime;
            this.state.set(worker.state.get());
            this.workerProperties = ImmutableMap.copyOf(worker.workerProperties);
            return true;
        }
    }

    public void addShard(AddShardRequest request) throws StarException {
        if (!isAlive()) {
            throw new WorkerNotHealthyStarException(String.format(
                    "Worker not healthy when try to add shard replica to serviceId: %s, workerId: %s, worker stat: %s",
                    serviceId, workerId, state.get()));
        }
        long epoch = heartbeatEpoch;
        starletAgent.addShard(request);
        // increase value only if numOfShards matches the number before RPC call, may be still inaccurate.
        if (epoch == heartbeatEpoch) {
            numOfShards.addAndGet(request.getShardInfoCount());
        }
    }

    public void updateShard(AddShardRequest request) throws StarException {
        if (!isAlive()) {
            throw new WorkerNotHealthyStarException(String.format(
                    "Worker not healthy when try to update shard replica to serviceId: %s, workerId: %s, worker stat: %s",
                    serviceId, workerId, state.get()));
        }
        starletAgent.addShard(request);
    }

    public void removeShard(RemoveShardRequest request) throws StarException {
        if (!isAlive()) {
            throw new WorkerNotHealthyStarException(String.format(
                    "Worker not healthy when try to remove shard replica, serviceId: %s, workerId: %s, worker stat: %s",
                    serviceId, workerId, state.get()));
        }
        long epoch = heartbeatEpoch;
        starletAgent.removeShard(request);
        // decrease value only if numOfShards matches the number before RPC call, may be still inaccurate.
        if (epoch == heartbeatEpoch) {
            numOfShards.getAndAdd(-request.getShardIdsCount());
        }
    }

    public void decommission() {
        starletAgent.disconnectWorker();
    }

    public boolean replicaExpired() {
        return state.get() == WorkerState.DOWN &&
                lastDownTime + Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L < System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("[Worker] id:%d, group:%d, service:%s", workerId, groupId, serviceId);
    }

    // FOR TEST ONLY
    public void setLastDownTime(long downtime) {
        this.lastDownTime = downtime;
    }
}
