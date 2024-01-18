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


package com.starrocks.cloudnative.compaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class PartitionIdentifier {
    @SerializedName(value = "dbId")
    private final long dbId;
    @SerializedName(value = "tableId")
    private final long tableId;
    @SerializedName(value = "partitionId")
    private final long partitionId;

    public PartitionIdentifier(long dbId, long tableId, long partitionId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionIdentifier that = (PartitionIdentifier) o;
        return dbId == that.dbId && tableId == that.tableId && partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, partitionId);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
