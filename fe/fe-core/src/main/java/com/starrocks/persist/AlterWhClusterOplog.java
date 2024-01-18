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
import com.starrocks.cloudnative.warehouse.Cluster;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterWhClusterOplog implements Writable {
    @SerializedName(value = "warehouseName")
    private String warehouseName;

    @SerializedName(value = "cluster")
    private Cluster cluster;

    public String getWarehouseName() {
        return warehouseName;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public AlterWhClusterOplog(String warehouseName, Cluster cluster) {
        this.warehouseName = warehouseName;
        this.cluster = cluster;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterWhClusterOplog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterWhClusterOplog.class);
    }

}
