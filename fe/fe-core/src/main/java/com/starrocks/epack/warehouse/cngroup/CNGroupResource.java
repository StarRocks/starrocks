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

package com.starrocks.epack.warehouse.cngroup;

import com.google.gson.annotations.SerializedName;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Since a warehouse can contain multi cngroups that each one contains complete compute resources, CNGroupResource is used to
 * identify the CNGroup in a warehouse.
 * </p>
 * During the execution of a job, the CNGroupResource schedule should :
 * - (Scheduling Integrity) The CNGroup assigned for each job’s minimum execution unit must remain consistent:
 *  - It should not happen that within a single execution unit, some tablets are assigned to CNGroup1 while others are
 *  assigned to CNGroup2, as this breaks task integrity and leads to poor cache performance.
 *  - Even worse would be assigning the same tablet to different CNGroups (e.g., CNGroup1 and CNGroup2) during execution,
 *      which would cause significant scheduling and execution issues.
 * - (Schedulability) Ensure that CNGroups within a job can be adjusted with minimal impact on SLA during CNGroup switching:
 *  - if a CNGroup switch occurs and there are still available CNGroups, the job must be able to retry and complete
 *  successfully.
 *  - if the job has a retry mechanism, it should ideally support automatic CNGroup switching awareness at the
 *  execution-unit level.
 */
public class CNGroupResource implements ComputeResource {
    private static final Logger LOG = LogManager.getLogger(CNGroupResource.class);

    public static final CNGroupResource DEFAULT =
            new CNGroupResource(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, -1);

    // The warehouseId is used to identify the warehouse where the CNGroupResource is located.
    @SerializedName("warehouseId")
    private final long warehouseId;
    // The cnGroupResource is used to identify the CNGroup in the warehouse.
    @SerializedName("cnGroupId")
    private final long cnGroupId;
    // The createTimeMs is used to identify the creation time of the CNGroupResource.
    private final long createTimeMs;

    public CNGroupResource(long warehouseId, long cnGroupId, long createTimeMs) {
        this.warehouseId = warehouseId;
        this.cnGroupId = cnGroupId;
        this.createTimeMs = createTimeMs;
    }

    public static CNGroupResource of(long warehouseId, long cnGroupId) {
        return new CNGroupResource(warehouseId, cnGroupId, System.currentTimeMillis());
    }

    @Override
    public long getWarehouseId() {
        return warehouseId;
    }

    @Override
    public long getWorkerGroupId() {
        return cnGroupId;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    @Override
    public String toString() {
        return "{warehouseId=" + warehouseId +
                ", cnGroupId=" + cnGroupId +
                ", createdTime=" + createTimeMs +
                "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(warehouseId, cnGroupId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CNGroupResource)) {
            return false;
        }
        CNGroupResource other = (CNGroupResource) obj;
        return warehouseId == other.warehouseId && cnGroupId == other.cnGroupId;
    }
}
