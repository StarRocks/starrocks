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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Partition.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Internal representation of partition-related metadata.
 */
public class Partition extends MetaObject implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Partition.class);

    public static final long PARTITION_INIT_VERSION = 1L;

    public enum PartitionState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE
    }

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "state")
    private PartitionState state;

    @SerializedName(value = "dpid")
    private long defaultPhysicalPartitionId;

    @SerializedName(value = "idToSubPartition")
    private Map<Long, PhysicalPartition> idToSubPartition = Maps.newHashMap();
    private Map<String, PhysicalPartition> nameToSubPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    @SerializedName(value = "distributionInfo")
    private DistributionInfo distributionInfo;

    public Partition(long id, String name, DistributionInfo distributionInfo) {
        this.id = id;
        this.name = name;
        this.state = PartitionState.NORMAL;
        this.distributionInfo = distributionInfo;
    }

    private Partition() {
    }

    public Partition(long id,
                     long physicalPartitionId,
                     String name,
                     MaterializedIndex baseIndex,
                     DistributionInfo distributionInfo) {
        this.id = id;
        this.name = name;
        this.state = PartitionState.NORMAL;

        this.baseIndex = baseIndex;

        this.visibleVersion = PARTITION_INIT_VERSION;
        this.visibleVersionTime = System.currentTimeMillis();
        // PARTITION_INIT_VERSION == 1, so the first load version is 2 !!!
        this.nextVersion = this.visibleVersion + 1;
        this.dataVersion = this.visibleVersion;
        this.nextDataVersion = this.nextVersion;
        this.versionEpoch = this.nextVersionEpoch();
        this.versionTxnType = TransactionType.TXN_NORMAL;
        this.distributionInfo = distributionInfo;

        this.defaultPhysicalPartitionId = physicalPartitionId;
        PhysicalPartition physicalPartition = new PhysicalPartition(physicalPartitionId,
                generatePhysicalPartitionName(physicalPartitionId), id, baseIndex);
        this.idToSubPartition.put(physicalPartitionId, physicalPartition);
        this.nameToSubPartition.put(physicalPartition.getName(), physicalPartition);
    }

    public Partition shallowCopy() {
        Partition partition = new Partition();
        partition.id = this.id;
        partition.name = this.name;
        partition.state = this.state;
        partition.baseIndex = this.baseIndex;
        partition.idToVisibleRollupIndex = Maps.newHashMap(this.idToVisibleRollupIndex);
        partition.idToShadowIndex = Maps.newHashMap(this.idToShadowIndex);
        partition.visibleVersion = this.visibleVersion;
        partition.visibleVersionTime = this.visibleVersionTime;
        partition.nextVersion = this.nextVersion;
        partition.dataVersion = this.dataVersion;
        partition.nextDataVersion = this.nextDataVersion;
        partition.versionEpoch = this.versionEpoch;
        partition.versionTxnType = this.versionTxnType;
        partition.distributionInfo = this.distributionInfo;
        partition.shardGroupId = this.shardGroupId;
        partition.defaultPhysicalPartitionId = this.defaultPhysicalPartitionId;
        partition.idToSubPartition = Maps.newHashMap(this.idToSubPartition);
        partition.nameToSubPartition = Maps.newHashMap(this.nameToSubPartition);
        return partition;
    }

    public void setIdForRestore(long id) {
        this.id = id;
    }

    public long getId() {
        return this.id;
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return this.name;
    }

    public void setState(PartitionState state) {
        this.state = state;
    }

    public PartitionState getState() {
        return this.state;
    }

    public DistributionInfo getDistributionInfo() {
        return distributionInfo;
    }

    public void setDistributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    public void addSubPartition(PhysicalPartition subPartition) {
        if (idToSubPartition.size() == 0) {
            defaultPhysicalPartitionId = subPartition.getId();
        }
        if (subPartition.getName() == null) {
            subPartition.setName(generatePhysicalPartitionName(subPartition.getId()));
        }

        idToSubPartition.put(subPartition.getId(), subPartition);
        nameToSubPartition.put(subPartition.getName(), subPartition);
    }

    public void removeSubPartition(long id) {
        PhysicalPartition subPartition = idToSubPartition.remove(id);
        if (subPartition != null) {
            nameToSubPartition.remove(subPartition.getName());
        }
    }

    public Collection<PhysicalPartition> getSubPartitions() {
        return idToSubPartition.values();
    }

    public PhysicalPartition getSubPartition(long id) {
        return idToSubPartition.get(id);
    }

    public PhysicalPartition getSubPartition(String name) {
        return nameToSubPartition.get(name);
    }

    public PhysicalPartition getDefaultPhysicalPartition() {
        return idToSubPartition.get(defaultPhysicalPartitionId);
    }

    public boolean hasData() {
        boolean hasData = false;
        for (PhysicalPartition subPartition : getSubPartitions()) {
            hasData |= subPartition.hasStorageData();
        }
        return hasData;
    }

    public long getDataSize() {
        long dataSize = 0;
        for (PhysicalPartition subPartition : getSubPartitions()) {
            dataSize += subPartition.storageDataSize();
        }
        return dataSize;
    }

    public long getRowCount() {
        long rowCount = 0;
        for (PhysicalPartition subPartition : getSubPartitions()) {
            rowCount += subPartition.storageRowCount();
        }

        return rowCount;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (PhysicalPartition subPartition : getSubPartitions()) {
            replicaCount += subPartition.storageReplicaCount();
        }
        return replicaCount;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, distributionInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Partition)) {
            return false;
        }

        Partition partition = (Partition) obj;
        return (id == partition.id) && distributionInfo.equals(partition.distributionInfo);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("partition_id: ").append(id).append("; ");
        buffer.append("name: ").append(name).append("; ");
        buffer.append("partition_state.name: ").append(state.name()).append("; ");
        buffer.append("distribution_info.type: ").append(distributionInfo.getType().name()).append("; ");
        buffer.append("distribution_info: ").append(distributionInfo.toString());

        return buffer.toString();
    }

    public String generatePhysicalPartitionName(long physicalPartitionId) {
        return this.name + '_' + physicalPartitionId;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (dataVersion == 0) {
            dataVersion = visibleVersion;
        }
        if (nextDataVersion == 0) {
            nextDataVersion = nextVersion;
        }
        if (versionEpoch == 0) {
            versionEpoch = nextVersionEpoch();
        }
        if (versionTxnType == null) {
            versionTxnType = TransactionType.TXN_NORMAL;
        }

        if (defaultPhysicalPartitionId == 0) {
            String partitionJson = GsonUtils.GSON.toJson(this);
            PhysicalPartition physicalPartition = GsonUtils.GSON.fromJson(partitionJson, PhysicalPartition.class);
            physicalPartition.setParentId(id);

            long physicalPartitionId = id;
            defaultPhysicalPartitionId = physicalPartitionId;
            idToSubPartition.put(physicalPartitionId, physicalPartition);
            nameToSubPartition.put(generatePhysicalPartitionName(physicalPartitionId), physicalPartition);
        }

        for (PhysicalPartition subPartition : idToSubPartition.values()) {
            if (subPartition.getName() == null) {
                subPartition.setName(generatePhysicalPartitionName(subPartition.getId()));
            }
            if (subPartition.getBaseIndex().getShardGroupId() == PhysicalPartition.INVALID_SHARD_GROUP_ID) {
                subPartition.getBaseIndex().setShardGroupId(getDefaultPhysicalPartition().getShardGroupId());
            }

            nameToSubPartition.put(subPartition.getName(), subPartition);
        }
    }

    /**************************************PhysicalPartition **********************************************/

    @SerializedName(value = "shardGroupId")
    private long shardGroupId = PhysicalPartition.INVALID_SHARD_GROUP_ID;

    /* Physical Partition Member */
    @SerializedName(value = "isImmutable")
    private AtomicBoolean isImmutable = new AtomicBoolean(false);

    @SerializedName(value = "baseIndex")
    private MaterializedIndex baseIndex;
    /**
     * Visible rollup indexes are indexes which are visible to user.
     * User can do query on them, show them in related 'show' stmt.
     */
    @SerializedName(value = "idToVisibleRollupIndex")
    private Map<Long, MaterializedIndex> idToVisibleRollupIndex = Maps.newHashMap();
    /**
     * Shadow indexes are indexes which are not visible to user.
     * Query will not run on these shadow indexes, and user can not see them neither.
     * But load process will load data into these shadow indexes.
     */
    @SerializedName(value = "idToShadowIndex")
    private Map<Long, MaterializedIndex> idToShadowIndex = Maps.newHashMap();

    /**
     * committed version(hash): after txn is committed, set committed version(hash)
     * visible version(hash): after txn is published, set visible version
     * next version(hash): next version is set after finished committing, it should equal to committed version + 1
     */

    // not have committedVersion because committedVersion = nextVersion - 1
    @SerializedName(value = "visibleVersion")
    private volatile long visibleVersion;
    @SerializedName(value = "visibleVersionTime")
    private volatile long visibleVersionTime;
    @SerializedName(value = "nextVersion")
    private volatile long nextVersion;

    /*
     * in shared-nothing mode, data version is always equals to visible version
     * in shared-data mode, compactions increase visible version but not data version
     */
    @SerializedName(value = "dataVersion")
    private volatile long dataVersion;
    @SerializedName(value = "nextDataVersion")
    private volatile long nextDataVersion;

    /*
     * if the visible version and version epoch are unchanged, the data is unchanged
     */
    @SerializedName(value = "versionEpoch")
    private volatile long versionEpoch;
    @SerializedName(value = "versionTxnType")
    private volatile TransactionType versionTxnType;

    public long nextVersionEpoch() {
        return GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid();
    }
}