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

import com.staros.proto.MetaGroupInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.util.LockCloseable;
import com.staros.util.Text;
import com.staros.util.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetaGroup implements Writable {
    private final String serviceId;
    private final long metaGroupId;
    private List<Long> shardGroupIds;
    private final PlacementPolicy placementPolicy;
    private final ReentrantReadWriteLock lock;

    public MetaGroup(String serviceId, long metaGroupId, List<Long> shardGroupIds,
            PlacementPolicy placementPolicy) {
        this.serviceId = serviceId;
        this.metaGroupId = metaGroupId;
        this.shardGroupIds = Collections.unmodifiableList(new ArrayList<>(shardGroupIds));
        this.placementPolicy = placementPolicy;
        this.lock = new ReentrantReadWriteLock();
    }

    public String getServiceId() {
        return serviceId;
    }

    public long getMetaGroupId() {
        return metaGroupId;
    }

    public void setShardGroupIds(List<Long> shardGroupIds) {
        try (LockCloseable ignore = new LockCloseable(lock.writeLock())) {
            this.shardGroupIds = Collections.unmodifiableList(new ArrayList<>(shardGroupIds));
        }
    }

    public List<Long> getShardGroupIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return shardGroupIds;
        }
    }

    public PlacementPolicy getPlacementPolicy() {
        return placementPolicy;
    }

    public MetaGroupInfo toProtobuf() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            MetaGroupInfo info = MetaGroupInfo.newBuilder()
                     .setServiceId(serviceId)
                     .setMetaGroupId(metaGroupId)
                     .addAllShardGroupIds(shardGroupIds)
                     .setPlacementPolicy(placementPolicy)
                     .build();
            return info;
        }
    }

    public static MetaGroup fromProtobuf(MetaGroupInfo info) {
        return new MetaGroup(info.getServiceId(),
                info.getMetaGroupId(),
                info.getShardGroupIdsList(),
                info.getPlacementPolicy());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static MetaGroup read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        MetaGroupInfo info = MetaGroupInfo.parseFrom(bytes);
        return MetaGroup.fromProtobuf(info);
    }
}
