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

import com.google.gson.annotations.SerializedName;
import com.starrocks.thrift.TPhysicalPartitionTableDbId;

import java.util.Objects;

public class PhysicalPartitionTableDbId {
    @SerializedName(value = "dbId")
    public long dbId;
    @SerializedName(value = "tableId")
    public long tableId;
    @SerializedName(value = "partitionId")
    public long partitionId;

    public PhysicalPartitionTableDbId(long dbId, long tableId, long partitionId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PhysicalPartitionTableDbId)) {
            return false;
        }

        PhysicalPartitionTableDbId other = (PhysicalPartitionTableDbId) obj;
        return this.dbId == other.dbId && this.tableId == other.tableId &&
               this.partitionId == other.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, partitionId);
    }

    public TPhysicalPartitionTableDbId toThrift() {
        TPhysicalPartitionTableDbId tId = new TPhysicalPartitionTableDbId();
        tId.setDb_id(this.dbId);
        tId.setTable_id(this.tableId);
        tId.setPartition_id(partitionId);
        return tId;
    }

    public static PhysicalPartitionTableDbId fromThrift(TPhysicalPartitionTableDbId tId) {
        return new PhysicalPartitionTableDbId(tId.getDb_id(), tId.getTable_id(),
                                              tId.getPartition_id());
    }
}