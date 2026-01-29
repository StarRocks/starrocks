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

import com.staros.proto.PlacementPolicy;
import com.staros.proto.ShardGroupInfo;
import com.staros.util.Constant;
import com.staros.util.LockCloseable;
import com.staros.util.Text;
import com.staros.util.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ShardGroup implements Writable {
    // Refer to ShardManagerBenchTest.testBenchListHashSet, it will 100x difference when the number of elements
    // is large than SET_INDEX_THRESHOLD.
    private static final long SET_INDEX_THRESHOLD = 4096;

    private final String serviceId;
    private final long groupId;
    private final PlacementPolicy policy;
    // NOTE: the order of shard in the List matters.
    private List<Long> shardIds;
    // For quick search if the given shard id exists or not
    private Set<Long> shardIdSet;
    private final boolean anonymous;
    private final long metaGroupId; // 0 means not belong to any meta group
    private final Map<String, String> labels;
    private final Map<String, String> properties;
    private final ReentrantReadWriteLock lock;

    public ShardGroup(String serviceId, long groupId) {
        this(serviceId, groupId, PlacementPolicy.NONE, false, Constant.DEFAULT_ID /* metaGroupId */);
    }

    public ShardGroup(String serviceId, long groupId, PlacementPolicy policy, boolean anonymous, long metaGroupId) {
        this(serviceId, groupId, policy, anonymous, metaGroupId, null, null);
    }

    public ShardGroup(String serviceId, long groupId, PlacementPolicy policy, boolean anonymous, long metaGroupId,
            Map<String, String> labels, Map<String, String> properties) {
        this.serviceId = serviceId;
        this.groupId = groupId;
        this.policy = policy;
        this.shardIds = Collections.unmodifiableList(new ArrayList<>());
        this.shardIdSet = null;
        this.anonymous = anonymous;
        this.metaGroupId = metaGroupId;
        this.labels = new HashMap<>();
        this.properties = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();

        if (labels != null) {
            this.labels.putAll(labels);
        }
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }

    public ShardGroup(ShardGroupInfo info) {
        this(info.getServiceId(), info.getGroupId(), info.getPolicy(), info.getAnonymous(), info.getMetaGroupId(),
                info.getLabelsMap(), info.getPropertiesMap());
        this.shardIds = Collections.unmodifiableList(new ArrayList<>(info.getShardIdsList()));
        mayBuildShardIdSet();
    }

    private void mayBuildShardIdSet() {
        if (shardIds.size() > SET_INDEX_THRESHOLD) {
            this.shardIdSet = new HashSet<>(shardIds);
        }
    }

    public String getServiceId() {
        return serviceId;
    }

    public long getGroupId() {
        return groupId;
    }

    private boolean contains(long shardId) {
        if (shardIdSet != null) {
            return shardIdSet.contains(shardId);
        }
        return shardIds.contains(shardId);
    }

    public boolean addShardId(long shardId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            if (contains(shardId)) {
                return false;
            }

            List<Long> newList = new ArrayList<>(shardIds);
            newList.add(shardId);
            shardIds = Collections.unmodifiableList(newList);
            if (shardIdSet != null) {
                shardIdSet.add(shardId);
            } else {
                mayBuildShardIdSet();
            }
            return true;
        }
    }

    public List<Long> batchAddShardId(List<Long> addShardIds) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            List<Long> todoShardIds;
            if (shardIdSet != null) {
                todoShardIds = addShardIds.stream().filter(x -> !shardIdSet.contains(x)).collect(Collectors.toList());
            } else {
                todoShardIds = addShardIds.stream().filter(x -> !shardIds.contains(x)).collect(Collectors.toList());
            }
            if (!todoShardIds.isEmpty()) {
                List<Long> newList = new ArrayList<>(shardIds.size() + todoShardIds.size());
                newList.addAll(shardIds);
                newList.addAll(todoShardIds);
                shardIds = Collections.unmodifiableList(newList);
                if (shardIdSet != null) {
                    shardIdSet.addAll(todoShardIds);
                } else {
                    mayBuildShardIdSet();
                }
            }
            return todoShardIds;
        }
    }

    public boolean removeShardId(long shardId) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            if (!contains(shardId)) {
                return false;
            }
            List<Long> newList = new ArrayList<>(shardIds);
            newList.remove(shardId);
            shardIds = Collections.unmodifiableList(newList);
            if (shardIdSet != null) {
                shardIdSet.remove(shardId);
            }
            return true;
        }
    }

    public List<Long> getShardIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return shardIds;
        }
    }

    public PlacementPolicy getPlacementPolicy() {
        return policy;
    }

    public boolean isAnonymous() {
        return anonymous;
    }

    public long getMetaGroupId() {
        return metaGroupId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getProperty(String propKey) {
        return properties.get(propKey);
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public String getLabel(String labelKey) {
        return labels.get(labelKey);
    }

    public ShardGroupInfo toProtobuf() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            ShardGroupInfo info = ShardGroupInfo.newBuilder()
                    .setServiceId(serviceId)
                    .setGroupId(groupId)
                    .addAllShardIds(shardIds)
                    .setPolicy(policy)
                    .setAnonymous(anonymous)
                    .setMetaGroupId(metaGroupId)
                    .putAllLabels(labels)
                    .putAllProperties(properties)
                    .build();
            return info;
        }
    }

    public static ShardGroup fromProtobuf(ShardGroupInfo info) {
        return new ShardGroup(info);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static ShardGroup read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        ShardGroupInfo info = ShardGroupInfo.parseFrom(bytes);
        return ShardGroup.fromProtobuf(info);
    }
}
