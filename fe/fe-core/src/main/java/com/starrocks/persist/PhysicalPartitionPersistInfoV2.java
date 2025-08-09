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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.io.Writable;

public class PhysicalPartitionPersistInfoV2 implements Writable {

    @SerializedName("dbId")
    private Long dbId;
    @SerializedName("tableId")
    private Long tableId;
    @SerializedName("partitionId")
    private Long partitionId;
    @SerializedName("physicalPartition")
    private PhysicalPartition partition;

    public PhysicalPartitionPersistInfoV2(Long dbId, Long tableId, Long partitionId, PhysicalPartition partition) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partition = partition;
    }

    public Long getDbId() {
        return this.dbId;
    }

    public Long getTableId() {
        return this.tableId;
    }

    public Long getPartitionId() {
        return this.partitionId;
    }

    public PhysicalPartition getPhysicalPartition() {
        return this.partition;
    }

}