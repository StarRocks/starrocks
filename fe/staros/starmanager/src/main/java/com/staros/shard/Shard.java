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


package com.staros.shard;

import com.google.common.collect.ImmutableList;
import com.staros.filecache.FileCache;
import com.staros.filestore.FilePath;
import com.staros.filestore.FileStore;
import com.staros.proto.AddShardInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaInfoLite;
import com.staros.proto.ReplicaState;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardState;
import com.staros.replica.Replica;
import com.staros.util.LockCloseable;
import com.staros.util.Text;
import com.staros.util.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Shard implements Writable {
    private static final Logger LOG = LogManager.getLogger(Shard.class);
    private final String serviceId;
    // TODO: use Set to ensure uniqueness of groupIds
    private List<Long> groupIds;
    private final long shardId;
    private ShardState state;
    private FilePath filePath;
    private FileCache fileCache;

    /**
     * Replica Number is moved from Shard property to WorkerGroup property,
     * so in different workerGroup there will be different number of replica.
     */
    @Deprecated
    private int expectedReplicaNum;
    // actual replicas at current time
    private ImmutableList<Replica> replicas;
    private Map<String, String> properties;
    private final ReentrantReadWriteLock lock;

    public Shard(String serviceId,
            List<Long> groupIds,
            long shardId) {
        this(serviceId, groupIds, shardId, null, null);
    }

    public Shard(
            String serviceId,
            List<Long> groupIds,
            long shardId,
            FilePath filePath,
            FileCache fileCache) {
        this.serviceId = serviceId;
        this.groupIds = Collections.unmodifiableList(new ArrayList<>(groupIds));
        this.shardId = shardId;
        this.state = ShardState.NORMAL;
        this.filePath = filePath;
        this.fileCache = fileCache;
        this.expectedReplicaNum = 1;
        this.replicas = ImmutableList.of();
        this.properties = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public String getServiceId() {
        return serviceId;
    }

    public List<Long> getGroupIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            // It is safe to return a read-only list to outside.
            return groupIds;
        }
    }

    public long getShardId() {
        return shardId;
    }

    public boolean joinGroup(long groupId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            if (groupIds.contains(groupId)) {
                return false;
            }
            // Copy-On-Write
            List<Long> newGroup = new ArrayList<>(groupIds);
            newGroup.add(groupId);
            groupIds = Collections.unmodifiableList(newGroup);
            return true;
        }
    }

    public boolean quitGroup(long groupId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            if (!groupIds.contains(groupId)) {
                return false;
            }
            //Copy-On-Write
            List<Long> newGroup = new ArrayList<>(groupIds);
            newGroup.remove(groupId);
            groupIds = Collections.unmodifiableList(newGroup);
            return true;
        }
    }

    public ShardState getState() {
        return state;
    }

    public void setState(ShardState state) {
        this.state = state;
    }

    public FilePath getFilePath() {
        return filePath;
    }

    public void setFileCacheEnable(boolean enableCache) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            this.fileCache.setFileCacheEnable(enableCache);
        }
    }

    public void setFilePath(FilePath path) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            filePath = path;
        }
    }

    public FileCache getFileCache() {
        return fileCache;
    }

    public void setReplicas(List<Replica> replicas) {
        this.replicas = ImmutableList.copyOf(replicas);
    }

    public List<Long> getReplicaWorkerIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return replicas.stream().map(Replica::getWorkerId).collect(Collectors.toList());
        }
    }

    public int getReplicaSize() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return replicas.size();
        }
    }

    public ImmutableList<Replica> getReplica() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return replicas;
        }
    }

    @Deprecated
    public int getExpectedReplicaNum() {
        return expectedReplicaNum;
    }

    @Deprecated
    public void setExpectedReplicaNum(int expectedReplicaNum) {
        this.expectedReplicaNum = expectedReplicaNum;
    }

    public boolean hasReplica(long workerId) {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return replicas.stream().anyMatch(x -> x.getWorkerId() == workerId);
        }
    }

    public boolean addReplica(long workerId) {
        return addReplica(workerId, ReplicaState.REPLICA_OK);
    }

    public boolean addReplica(long workerId, ReplicaState state) {
        return addReplica(workerId, state, false);
    }

    public boolean addTempReplica(long workerId) {
        return addReplica(workerId, ReplicaState.REPLICA_OK, true);
    }

    private boolean addReplica(long workerId, ReplicaState state, boolean isTemp) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            if (replicas.stream().anyMatch(x -> x.getWorkerId() == workerId)) {
                return false;
            }
            // TODO: every Replica is PRIMARY for now as the role is not used yet.
            //  Proper PRIMARY/SECONDARY should take worker group into consideration.
            // COW: rebuild the replica
            replicas = ImmutableList.<Replica>builder().addAll(replicas)
                    .add(new Replica(workerId, state, System.currentTimeMillis(), isTemp)).build();
            return true;
        }
    }

    public boolean removeReplica(long workerId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            // TODO: properly set replica ROLE when multiple worker groups is taken into consideration.
            if (replicas.stream().noneMatch(x -> x.getWorkerId() == workerId)) {
                return false;
            }
            // COW: rebuild the replica
            ImmutableList.Builder<Replica> builder = ImmutableList.builder();
            replicas.forEach(x -> {
                if (x.getWorkerId() != workerId) {
                    builder.add(x);
                }
            });
            replicas = builder.build();
            return true;
        }
    }

    /**
     * Scale out a replica. Adding a new replica and set its state to REPLICA_SCALE_OUT
     * @param workerId the worker to be associated with the replica
     * @return whether the replica is added success or not
     */
    public boolean scaleOutReplica(long workerId) {
        return addReplica(workerId, ReplicaState.REPLICA_SCALE_OUT);
    }

    public boolean scaleOutTempReplica(long workerId) {
        return addReplica(workerId, ReplicaState.REPLICA_SCALE_OUT, true);
    }

    /**
     * Set the scale-out action done for the replica. The replica state turns to REPLICA_OK.
     * @param workerId the worker to be associated with the replica
     * @return whether the replica state is set success or not
     */
    public boolean scaleOutReplicaDone(long workerId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            Optional<Replica> replica = replicas.stream().filter(x -> x.getWorkerId() == workerId).findAny();
            if (!replica.isPresent()) {
                return false;
            }
            if (replica.get().getState() != ReplicaState.REPLICA_SCALE_OUT) {
                return false;
            }
            replica.get().setReplicaState(ReplicaState.REPLICA_OK);
            return true;
        }
    }

    /**
     * Set the corresponding replica from temp replica to normal replica
     * @param workerId the worker to be associated with the replica
     * @return true, only when the replica is a temp replica and the flag is cleared successful
     */
    public boolean convertTempReplicaToNormal(long workerId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            Optional<Replica> replica = replicas.stream().filter(x -> x.getWorkerId() == workerId).findAny();
            if (!replica.isPresent()) {
                return false;
            }

            if (!replica.get().getTempFlag()) {
                return false;
            }
            replica.get().clearTempFlag();
            return true;
        }
    }

    /**
     * Scale in a replica. Mark a new replica as REPLICA_SCALE_IN
     * @param workerId the worker to be associated with the replica
     * @return whether the replica is set to SCALE_IN success or not
     */
    public boolean scaleInReplica(long workerId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            Optional<Replica> replica = replicas.stream().filter(x -> x.getWorkerId() == workerId).findAny();
            if (!replica.isPresent()) {
                return false;
            }
            replica.get().setReplicaState(ReplicaState.REPLICA_SCALE_IN);
            return true;
        }
    }

    public boolean cancelScaleInReplica(long workerId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            Optional<Replica> replica = replicas.stream().filter(x -> x.getWorkerId() == workerId).findAny();
            if (!replica.isPresent()) {
                return false;
            }
            if (replica.get().getState() != ReplicaState.REPLICA_SCALE_IN) {
                return false;
            }
            replica.get().setReplicaState(ReplicaState.REPLICA_OK);
            return true;
        }
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ShardInfo toProtobufInternal(boolean includeSecret, boolean withoutReplicaInfo) {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            ShardInfo.Builder builder = ShardInfo.newBuilder();
            builder.setServiceId(serviceId);
            builder.addAllGroupIds(groupIds);
            builder.setShardId(shardId);
            builder.setShardState(state);
            if (filePath != null) {
                if (includeSecret) {
                    builder.setFilePath(filePath.toProtobuf());
                } else {
                    builder.setFilePath(filePath.toDebugProtobuf());
                }
            }
            if (fileCache != null) {
                builder.setFileCache(fileCache.toProtobuf());
            }
            if (!withoutReplicaInfo) {
                for (Replica replica : replicas) {
                    builder.addReplicaInfo(replica.toProtobuf());
                }
            }
            builder.putAllShardProperties(properties);
            builder.setExpectedReplicaNum(expectedReplicaNum);
            builder.setHashCode(hashCode());
            return builder.build();
        }
    }

    public ShardInfo toProtobuf() {
        return toProtobufInternal(true /* includeSecret */, false /* withoutReplicaInfo */);
    }

    public ShardInfo toDebugProtobuf() {
        return toProtobufInternal(false /* includeSecret */, false /* withoutReplicaInfo */);
    }

    public ShardInfo toProtobuf(boolean withoutReplicaInfo) {
        return toProtobufInternal(true /* includeSecret */, withoutReplicaInfo);
    }

    public AddShardInfo getAddShardInfo() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return AddShardInfo.newBuilder()
                    .setShardId(shardId)
                    .setFileCacheInfo(fileCache.toProtobuf())
                    .setFilePathInfo((filePath.toProtobuf()))
                    .putAllShardProperties(properties)
                    .setHashCode(hashCode())
                    .build();
        }
    }

    public AddShardInfo getAddShardInfo(List<Long> workerIds, boolean includeReplicaHash) {
        if (workerIds == null || workerIds.isEmpty()) {
            // go with the fast track
            return getAddShardInfo();
        } else {
            return getAddShardInfo(workerIds, null, includeReplicaHash);
        }
    }

    // Shall interest only the workerIds in the same workerGroup, but the replica doesn't contain worker group info.
    // So this interface just filters the replicas with the list of worker ids.
    public AddShardInfo getAddShardInfo(List<Long> workerIds, Replica supplReplica, boolean includeReplicaHash) {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            List<ReplicaInfoLite> infoLites = new ArrayList<>();
            if (!workerIds.isEmpty()) {
                for (Replica replica : replicas) {
                    if (workerIds.contains(replica.getWorkerId())) {
                        infoLites.add(replica.toReplicaInfoLite());
                    }
                }
            }

            int hash;
            if (includeReplicaHash) {
                hash = hashCodeWithReplicas(workerIds);
            } else {
                hash = hashCode();
            }
            if (supplReplica != null &&
                    infoLites.stream().noneMatch(l -> l.getWorkerId() == supplReplica.getWorkerId())) {
                infoLites.add(supplReplica.toReplicaInfoLite());
                if (includeReplicaHash) {
                    hash = Objects.hash(hash, supplReplica);
                }
            }

            AddShardInfo.Builder builder = AddShardInfo.newBuilder()
                    .setShardId(shardId)
                    .setFileCacheInfo(fileCache.toProtobuf())
                    .setFilePathInfo((filePath.toProtobuf()))
                    .putAllShardProperties(properties)
                    .setHashCode(hash);
            if (!infoLites.isEmpty()) {
                builder.addAllReplicaInfo(infoLites);
            }
            return builder.build();
        }
    }

    public int hashCodeWithReplicas(List<Long> workerIds) {
        if (workerIds == null || workerIds.isEmpty()) {
            return hashCode();
        }
        int hash = 0;
        List<Replica> tempReplicas;
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            // get hashCode and replica under the same lock
            hash = hashCode();
            tempReplicas = getReplica();
        }
        // calculate the hash with no lock
        for (Replica replica : tempReplicas) {
            if (workerIds.contains(replica.getWorkerId())) {
                hash = Objects.hash(hash, replica);
            }
        }
        return hash;
    }

    public boolean updateFileStore(FileStoreInfo fsInfo) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            FileStore fileStore = filePath.fs;
            if (!fsInfo.getFsKey().equals(fileStore.key())) {
                return false;
            }

            long version = fsInfo.getVersion();
            if (fileStore.getVersion() < version) {
                filePath.fs = FileStore.fromProtobuf(fsInfo);
                return true;
            }
            return false;
        }
    }

    public static Shard fromProtobuf(ShardInfo info) {
        String serviceId = info.getServiceId();
        List<Long> groupIds = info.getGroupIdsList();
        long shardId = info.getShardId();
        FilePath path = null;
        if (info.hasFilePath()) {
            path = FilePath.fromProtobuf(info.getFilePath());
        }
        FileCache cache = null;
        if (info.hasFileCache()) {
            cache = FileCache.fromProtobuf(info.getFileCache());
        }
        Shard shard = new Shard(serviceId, groupIds, shardId, path, cache);
        shard.setState(info.getShardState());
        List<ReplicaInfo> replicaInfos = info.getReplicaInfoList();
        List<Replica> replicas = new ArrayList<>(replicaInfos.size());
        for (ReplicaInfo replicaInfo : replicaInfos) {
            replicas.add(Replica.fromProtobuf(replicaInfo));
        }
        shard.setReplicas(replicas);
        Map<String, String> properties = info.getShardPropertiesMap();
        shard.setProperties(properties);
        int replicaNum = info.getExpectedReplicaNum() == 0 ? 1 : info.getExpectedReplicaNum();
        shard.setExpectedReplicaNum(replicaNum);
        return shard;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static Shard read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        ShardInfo info = ShardInfo.parseFrom(bytes);
        return Shard.fromProtobuf(info);
    }

    @Override
    public int hashCode() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            long fsVersion = 0L;
            if (filePath != null && filePath.fs != null) {
                fsVersion = filePath.fs.getVersion();
            }

            boolean enableCache = false;
            if (fileCache != null) {
                enableCache = fileCache.getFileCacheEnable();
            }
            return Objects.hash(fsVersion, enableCache);
        }
    }
}
