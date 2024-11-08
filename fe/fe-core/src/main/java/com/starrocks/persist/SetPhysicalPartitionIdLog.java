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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SetPhysicalPartitionIdLog implements Writable {
    @SerializedName(value = "phId")
    private final Map<PartitionIdentifier, Long> phId;

    public SetPhysicalPartitionIdLog() {
        this.phId = new HashMap<>();
    }

    public void addPhysicalPartitionId(Long dbId, Long tableId, Long partitionId, Long physicalPartitionId) {
        PartitionIdentifier partitionIdentifier = new PartitionIdentifier(dbId, tableId, partitionId);
        phId.put(partitionIdentifier, physicalPartitionId);
    }

    public Map<PartitionIdentifier, Long> getPhId() {
        return phId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
