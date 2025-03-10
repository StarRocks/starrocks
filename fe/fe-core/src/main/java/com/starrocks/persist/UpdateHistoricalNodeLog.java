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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

public class UpdateHistoricalNodeLog implements Writable {
    @SerializedName(value = "warehouse")
    private String warehouse;

    @SerializedName(value = "updateTime")
    private long updateTime;

    @SerializedName(value = "idToBackend")
    private Map<Long, Backend> idToBackend;

    @SerializedName(value = "needUpdateComputeNode")
    private Map<Long, ComputeNode> idToComputeNode;

    public UpdateHistoricalNodeLog(String warehouse, long updateTime, Map<Long, Backend> idToBackend,
                                   Map<Long, ComputeNode> idToComputeNode) {
        this.warehouse = warehouse;
        this.updateTime = updateTime;
        this.idToBackend = idToBackend;
        this.idToComputeNode = idToComputeNode;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public Map<Long, Backend> getIdToBackend() {
        return idToBackend;
    }

    public Map<Long, ComputeNode> getIdToComputeNode() {
        return idToComputeNode;
    }

    public static UpdateHistoricalNodeLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), UpdateHistoricalNodeLog.class);
    }
}