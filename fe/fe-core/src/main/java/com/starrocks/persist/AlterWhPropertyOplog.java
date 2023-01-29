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
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AlterWhPropertyOplog implements Writable {
    @SerializedName(value = "whName")
    private String warehouseName;

    @SerializedName(value = "properties")
    private Map<String, String> properties = new HashMap<>();

    public AlterWhPropertyOplog(String whName, Map<String, String> properties) {
        this.warehouseName = whName;
        this.properties = properties;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterWhPropertyOplog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterWhPropertyOplog.class);
    }
}
