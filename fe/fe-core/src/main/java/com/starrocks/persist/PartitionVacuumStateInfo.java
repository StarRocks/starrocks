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
import com.starrocks.catalog.VacuumState;
import com.starrocks.common.io.Writable;

/**
 * Journal record for {@link OperationType#OP_MODIFY_PARTITION_VACUUM_STATE}: the incremental (bounded,
 * resumable) auto-vacuum state of one physical partition, written each round by {@code AutovacuumDaemon}
 * so an in-flight pass survives FE restart / failover instead of restarting. Replayed by
 * {@code LocalMetastore.replayModifyPartitionVacuumState}. GSON-serialized (like ModifyPartitionInfo);
 * Writable is a marker here, the actual (de)serialization goes through logJsonObject / EditLogDeserializer.
 */
public class PartitionVacuumStateInfo implements Writable {
    @SerializedName(value = "db")
    private long dbId;
    @SerializedName(value = "tbl")
    private long tableId;
    @SerializedName(value = "pid")
    private long physicalPartitionId;
    @SerializedName(value = "vs")
    private VacuumState vacuumState;

    public PartitionVacuumStateInfo() {
        // for persist
    }

    public PartitionVacuumStateInfo(long dbId, long tableId, long physicalPartitionId,
                                    VacuumState vacuumState) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionId = physicalPartitionId;
        this.vacuumState = vacuumState;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public VacuumState getVacuumState() {
        return vacuumState;
    }
}
