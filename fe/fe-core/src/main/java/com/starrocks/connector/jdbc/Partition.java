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


package com.starrocks.connector.jdbc;

import com.google.gson.JsonObject;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.persist.gson.GsonUtils;

import java.util.Objects;

/**
 * Partition stores some necessary information used in the planner stage
 * such as in the cbo and building scan range stage. The purpose of caching partition instance
 * is to reduce repeated calls to the hive metastore rpc interface at each stage.
 */
public class Partition implements PartitionInfo {
    private final String partitionName;
    private final long modifiedTime;

    public Partition(String partitionName, long modifiedTime) {
        this.partitionName = partitionName;
        this.modifiedTime = modifiedTime;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public long getModifiedTime() {
        return modifiedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Partition partition = (Partition) o;
        return Objects.equals(partitionName, partition.partitionName) &&
                Objects.equals(modifiedTime, partition.modifiedTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, modifiedTime);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Partition{");
        sb.append("partitionName=").append(partitionName);
        sb.append(", modifiedTime=").append(modifiedTime);
        sb.append('}');
        return sb.toString();
    }

    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.add("partitionName", (GsonUtils.GSON.toJsonTree(partitionName)));
        obj.add("modifiedTime", (GsonUtils.GSON.toJsonTree(modifiedTime)));
        return obj;
    }
}
