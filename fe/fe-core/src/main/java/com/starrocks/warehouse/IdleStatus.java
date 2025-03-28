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

package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class IdleStatus {

    @SerializedName("isClusterIdle")
    boolean isClusterIdle;

    @SerializedName("clusterIdleTime")
    long clusterIdleTime;

    @SerializedName("warehouses")
    List<WarehouseStatus> warehouses;

    public IdleStatus(boolean isClusterIdle, long clusterIdleTime, List<WarehouseStatus> warehouses) {
        this.isClusterIdle = isClusterIdle;
        this.clusterIdleTime = clusterIdleTime;
        this.warehouses = warehouses;
    }

    public static class WarehouseStatus {
        @SerializedName("id")
        long id;
        @SerializedName("name")
        String name;
        @SerializedName("isIdle")
        boolean isIdle;
        @SerializedName("idleTime")
        long idleTime;

        public WarehouseStatus(long id, String name, boolean isIdle, long idleTime) {
            this.id = id;
            this.name = name;
            this.isIdle = isIdle;
            this.idleTime = idleTime;
        }
    }
}
