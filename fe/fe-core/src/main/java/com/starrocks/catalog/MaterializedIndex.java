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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/MaterializedIndex.java

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.clone.BalanceStat;
import com.starrocks.clone.BalanceStat.BalanceType;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TIndexState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

public class MaterializedIndex extends MetaObject implements Writable, GsonPostProcessable {
    public enum IndexState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE,
        SHADOW; // index in SHADOW state is visible to load process, but invisible to query

        public boolean isVisible() {
            return this == IndexState.NORMAL || this == IndexState.SCHEMA_CHANGE;
        }

        public TIndexState toThrift() {
            switch (this) {
                case NORMAL:
                    return TIndexState.NORMAL;
                case SHADOW:
                    return TIndexState.SHADOW;
                default:
                    return null;
            }
        }

        public static IndexState fromThrift(TIndexState tState) {
            switch (tState) {
                case NORMAL:
                    return IndexState.NORMAL;
                case SHADOW:
                    return IndexState.SHADOW;
                default:
                    return null;
            }
        }
    }

    public enum IndexExtState {
        ALL,
        VISIBLE, // index state in NORMAL and SCHEMA_CHANGE
        SHADOW // index state in SHADOW
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "state")
    private IndexState state;
    @SerializedName(value = "rowCount")
    private long rowCount;

    // Virtual buckets in order.
    // There is a tablet id for each virtual bucket,
    // which means this virtual bucket's data is stored in this tablet.
    // We divide data into virtual buckets and then arrange these virtual buckets
    // into physical buckets, which are tablets.
    // Each virtual bucket is associated with a tablet. Multiple virtual buckets are
    // allowed to be associated with the same tablet.
    @SerializedName(value = "virtualBuckets")
    private List<Long> virtualBuckets;

    private Map<Long, Tablet> idToTablets;

    // Since virtual buckets keeps the order, this can be deprecated if idToTablets persists
    @SerializedName(value = "tablets")
    private List<Tablet> tablets;

    @SerializedName(value = "shardGroupId")
    private long shardGroupId = PhysicalPartition.INVALID_SHARD_GROUP_ID;

    // If this is an index of LakeTable and the index state is SHADOW, all transactions
    // whose txn id is less than 'visibleTxnId' will ignore this index when sending
    // PublishVersionRequest requests to BE nodes.
    private long visibleTxnId;

    // Tablet distribution balance stat
    private AtomicReference<BalanceStat> balanceStat = new AtomicReference<>(BalanceStat.BALANCED_STAT);

    public MaterializedIndex() {
        this(0, IndexState.NORMAL, PhysicalPartition.INVALID_SHARD_GROUP_ID);
    }

    public MaterializedIndex(long id) {
        this(id, IndexState.NORMAL);
    }

    public MaterializedIndex(long id, @Nullable IndexState state) {
        this(id, state, 0, PhysicalPartition.INVALID_SHARD_GROUP_ID);
    }

    public MaterializedIndex(long id, @Nullable IndexState state, long shardGroupId) {
        this(id, state, 0, shardGroupId);
    }

    /**
     * Construct a new instance of {@link MaterializedIndex}.
     * <p>
     * {@code visibleTxnId} will be ignored if {@code state} is not {@code IndexState.SHADOW}
     *
     * @param id           the id of the index
     * @param state        the state of the index
     * @param visibleTxnId the minimum transaction id that can see this index.
     */
    public MaterializedIndex(long id, @Nullable IndexState state, long visibleTxnId, long shardGroupId) {
        this.id = id;
        this.state = state == null ? IndexState.NORMAL : state;
        this.virtualBuckets = new ArrayList<>();
        this.idToTablets = new HashMap<>();
        this.tablets = new ArrayList<>();
        this.rowCount = 0;
        this.visibleTxnId = (this.state == IndexState.SHADOW) ? visibleTxnId : 0;
        this.shardGroupId = shardGroupId;
    }

    /**
     * Checks whether {@code this} {@link MaterializedIndex} is visible to a transaction.
     * <p>
     * If this {@link MaterializedIndex} is not visible to a transaction,
     * {@link com.starrocks.transaction.PublishVersionDaemon} will not send {@link com.starrocks.proto.PublishVersionRequest}
     * to tablets of this index.
     * <p>
     * Only used for {@link com.starrocks.lake.LakeTable} now.
     *
     * @param txnId the id of a transaction created by {@link com.starrocks.transaction.DatabaseTransactionMgr}
     * @return true iff this index is visible to the transaction, false otherwise.
     */
    public boolean visibleForTransaction(long txnId) {
        return state == IndexState.NORMAL || visibleTxnId <= txnId;
    }

    /**
     * Update the value of visibleTxnId.
     *
     * @param visibleTxnId the new value of visibleTxnId.
     */
    public void setVisibleTxnId(long visibleTxnId) {
        Preconditions.checkState(state == IndexState.SHADOW);
        this.visibleTxnId = visibleTxnId;
    }

    public void setShardGroupId(long shardGroupId) {
        this.shardGroupId = shardGroupId;
    }

    public long getShardGroupId() {
        return shardGroupId;
    }

    // The virtual buckets are in order
    public List<Long> getVirtualBuckets() {
        return virtualBuckets;
    }

    public void setVirtualBuckets(List<Long> virtualBuckets) {
        this.virtualBuckets = virtualBuckets;
    }

    public List<Tablet> getTablets() {
        return tablets;
    }

    // With virtual buckets, the order of tablets is irrelevant
    public List<Long> getTabletIds() {
        List<Long> tabletIds = Lists.newArrayListWithCapacity(tablets.size());
        for (Tablet tablet : tablets) {
            tabletIds.add(tablet.getId());
        }
        return tabletIds;
    }

    public Tablet getTablet(long tabletId) {
        return idToTablets.get(tabletId);
    }

    public void clearTabletsForRestore() {
        virtualBuckets.clear();
        idToTablets.clear();
        tablets.clear();
    }

    public void addTablet(Tablet tablet, TabletMeta tabletMeta) {
        addTablet(tablet, tabletMeta, true);
    }

    public void addTablet(Tablet tablet, TabletMeta tabletMeta, boolean updateInvertedIndex) {
        virtualBuckets.add(tablet.getId());
        idToTablets.put(tablet.getId(), tablet);
        tablets.add(tablet);
        if (updateInvertedIndex) {
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tablet.getId(), tabletMeta);
        }
    }

    public void setIdForRestore(long idxId) {
        this.id = idxId;
    }

    public long getId() {
        return id;
    }

    public void setState(IndexState state) {
        this.state = state;
    }

    public IndexState getState() {
        return this.state;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getDataSize() {
        return getDataSize(false);
    }

    public long getDataSize(boolean singleReplica) {
        long dataSize = 0;
        for (Tablet tablet : getTablets()) {
            dataSize += tablet.getDataSize(singleReplica);
        }
        return dataSize;
    }

    public long getTabletMaxDataSize() {
        long maxDataSize = 0;
        for (Tablet tablet : getTablets()) {
            maxDataSize = Math.max(maxDataSize, tablet.getDataSize(true));
        }
        return maxDataSize;
    }

    public long getReplicaCount() {
        if (tablets.isEmpty()) {
            return 0L;
        }

        Tablet t = tablets.get(0);
        if (t instanceof LakeTablet) {
            return tablets.size();
        } else {
            Preconditions.checkState(t instanceof LocalTablet);
            long replicaCount = 0;
            for (Tablet tablet : getTablets()) {
                LocalTablet localTablet = (LocalTablet) tablet;
                replicaCount += localTablet.getImmutableReplicas().size();
            }
            return replicaCount;
        }
    }

    public List<Integer> getVirtualBucketsByTabletId(long tabletId) {
        List<Integer> virtualBucketIndexes = new ArrayList<>();
        for (int i = 0; i < virtualBuckets.size(); ++i) {
            if (virtualBuckets.get(i).longValue() == tabletId) {
                virtualBucketIndexes.add(i);
            }
        }
        return virtualBucketIndexes;
    }

    // With virtual buckets, the order index of tablets is irrelevant.
    // Keep this method only for colocate table in shared-nothing mode,
    // in which we do not implement tablet split and merge and virtual buckets are the same with tabletIds.
    public int getTabletOrderIdx(long tabletId) {
        int idx = 0;
        for (Tablet tablet : tablets) {
            if (tablet.getId() == tabletId) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    public void setBalanceStat(BalanceStat balanceStat) {
        this.balanceStat.set(balanceStat);
    }

    public BalanceStat getBalanceStat() {
        return balanceStat.get();
    }

    public boolean isTabletBalanced() {
        return getBalanceStat().isBalanced();
    }

    public BalanceType getBalanceType() {
        return getBalanceStat().getBalanceType();
    }




    @Override
    public int hashCode() {
        return Objects.hashCode(virtualBuckets, idToTablets);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MaterializedIndex)) {
            return false;
        }
        MaterializedIndex other = (MaterializedIndex) obj;
        return virtualBuckets.equals(other.virtualBuckets) && idToTablets.equals(other.idToTablets)
                && state.equals(other.state) && (rowCount == other.rowCount) && (visibleTxnId == other.visibleTxnId);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("index id: ").append(id).append("; ");
        buffer.append("index state: ").append(state.name()).append("; ");
        buffer.append("shardGroupId: ").append(shardGroupId).append("; ");
        buffer.append("row count: ").append(rowCount).append("; ");
        buffer.append("visibleTxnId: ").append(visibleTxnId).append("; ");
        buffer.append("virtual buckets size: ").append(virtualBuckets.size()).append("; ");
        buffer.append("virtual buckets: ").append(virtualBuckets).append("; ");
        buffer.append("tablets size: ").append(tablets.size()).append("; ");
        buffer.append("tablets: [");
        for (Tablet tablet : tablets) {
            buffer.append("tablet: ").append(tablet.toString()).append(", ");
        }
        buffer.append("]; ");

        return buffer.toString();
    }

    @Override
    public void gsonPostProcess() {
        // build "idToTablets" from "tablets"
        for (Tablet tablet : tablets) {
            idToTablets.put(tablet.getId(), tablet);
        }

        // Build "virtualBuckets" from "tablets" when upgrading from old versions
        // Before StarRocks didn't have virtual buckets, it would be empty when loading
        // from the previous image.
        // In this situation, the virtual bucket is equivalent to the physical tablet.
        // So just fill the virtual buckets with the physical tablets.
        if (virtualBuckets.isEmpty()) {
            for (Tablet tablet : tablets) {
                virtualBuckets.add(tablet.getId());
            }
        }
    }
}
