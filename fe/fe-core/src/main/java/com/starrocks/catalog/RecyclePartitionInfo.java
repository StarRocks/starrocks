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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.JsonWriter;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.server.GlobalStateMgr;

public abstract class RecyclePartitionInfo extends JsonWriter {
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "partition")
    protected Partition partition;
    @SerializedName(value = "dataProperty")
    protected DataProperty dataProperty;
    @SerializedName(value = "replicationNum")
    protected short replicationNum;
    @SerializedName(value = "isInMemory")
    protected boolean isInMemory;
    @SerializedName(value = "recoverable")
    protected boolean recoverable;

    public RecyclePartitionInfo() {
        recoverable = true;
    }

    public RecyclePartitionInfo(long dbId, long tableId, Partition partition,
                                   DataProperty dataProperty, short replicationNum,
                                   boolean isInMemory) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;
        this.dataProperty = dataProperty;
        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.recoverable = true;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Partition getPartition() {
        return partition;
    }

    public DataProperty getDataProperty() {
        return dataProperty;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public boolean isRecoverable() {
        return recoverable;
    }

    public void setRecoverable(boolean recoverable) {
        this.recoverable = recoverable;
    }

    public boolean delete() {
        GlobalStateMgr.getCurrentState().getStarRocksMeta().onErasePartition(partition);
        return true;
    }

    abstract Range<PartitionKey> getRange();

    abstract DataCacheInfo getDataCacheInfo();

    abstract void recover(OlapTable table) throws DdlException;

    protected static void recoverRangePartition(OlapTable table, RecyclePartitionInfo recyclePartitionInfo) throws DdlException {
        Preconditions.checkState(recyclePartitionInfo.isRecoverable());
        // check if range is invalid
        final String partitionName = recyclePartitionInfo.getPartition().getName();
        Range<PartitionKey> recoverRange = recyclePartitionInfo.getRange();
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        if (partitionInfo.getAnyIntersectRange(recoverRange, false) != null) {
            throw new DdlException("Cannot recover partition '" + partitionName + "': Range conflict.");
        }

        // recover partition
        Partition recoverPartition = recyclePartitionInfo.getPartition();
        Preconditions.checkState(recoverPartition.getName().equalsIgnoreCase(partitionName));
        table.addPartition(recoverPartition);

        // recover partition info
        long partitionId = recoverPartition.getId();
        partitionInfo.setRange(partitionId, false, recoverRange);
        partitionInfo.setDataProperty(partitionId, recyclePartitionInfo.getDataProperty());
        partitionInfo.setReplicationNum(partitionId, recyclePartitionInfo.getReplicationNum());
        partitionInfo.setIsInMemory(partitionId, recyclePartitionInfo.isInMemory());
        if (recyclePartitionInfo.getDataCacheInfo() != null) {
            partitionInfo.setDataCacheInfo(partitionId, recyclePartitionInfo.getDataCacheInfo());
        }
    }
}
