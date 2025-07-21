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

import java.io.DataInput;
import java.io.IOException;

/**
 * A generic wrapper of warehouse internal oplog
 * The payload is an opaque that should be parsed by each implementation details
 */
public class WarehouseInternalOpLog implements Writable {
    @SerializedName(value = "whName")
    private String warehouseName;

    @SerializedName(value = "payload")
    private String payload;

    public WarehouseInternalOpLog(String warehouseName, String payload) {
        this.warehouseName = warehouseName;
        this.payload = payload;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public String getPayload() {
        return payload;
    }

    public static WarehouseInternalOpLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WarehouseInternalOpLog.class);
    }
}
