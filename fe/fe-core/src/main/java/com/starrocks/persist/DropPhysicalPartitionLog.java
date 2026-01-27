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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

/**
 * Persist info for dropping a physical partition.
 */
public class DropPhysicalPartitionLog implements Writable {

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("partitionId")
    private long partitionId;

    @SerializedName("physicalPartitionId")
    private long physicalPartitionId;

    public DropPhysicalPartitionLog(long dbId, long tableId, long partitionId, long physicalPartitionId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.physicalPartitionId = physicalPartitionId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    @Override
    public String toString() {
        return "DropPhysicalPartitionLog{" +
                "dbId=" + dbId +
                ", tableId=" + tableId +
                ", partitionId=" + partitionId +
                ", physicalPartitionId=" + physicalPartitionId +
                '}';
    }
}
