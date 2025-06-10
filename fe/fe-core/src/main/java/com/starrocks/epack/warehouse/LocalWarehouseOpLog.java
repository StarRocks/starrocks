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

package com.starrocks.epack.warehouse;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

/**
 * A simple OpLog to encapsulate the details of LocalWarehouse specific EditLog.
 */
public class LocalWarehouseOpLog implements Writable {
    public static final short CREATE_CNGROUP = 1;
    public static final short DROP_CNGROUP = 2;
    public static final short ENABLE_CNGROUP = 3;
    public static final short DISABLE_CNGROUP = 4;

    @SerializedName(value = "op")
    private short op;

    @SerializedName(value = "cgname")
    private String cngroupName;

    @SerializedName(value = "cngroup")
    Cluster cluster;

    // For GSON deserialization only
    private LocalWarehouseOpLog() {
    }

    public LocalWarehouseOpLog(short op) {
        this.op = op;
    }

    public short getOp() {
        return op;
    }

    public String getCNGroupName() {
        return cngroupName;
    }

    public Cluster getCluster() {
        return cluster;
    }

    private void setCNGroupName(String cnGroupName) {
        this.cngroupName = cnGroupName;
    }

    private void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static LocalWarehouseOpLog fromJson(String payload) {
        return GsonUtils.GSON.fromJson(payload, LocalWarehouseOpLog.class);
    }

    public static LocalWarehouseOpLog createCNGroupOpLog(Cluster cluster) {
        LocalWarehouseOpLog log = new LocalWarehouseOpLog(CREATE_CNGROUP);
        log.setCluster(cluster);
        return log;
    }

    public static LocalWarehouseOpLog dropCNGroupOpLog(String cngroupName) {
        LocalWarehouseOpLog log = new LocalWarehouseOpLog(DROP_CNGROUP);
        log.setCNGroupName(cngroupName);
        return log;
    }

    public static LocalWarehouseOpLog enableCNGroupOpLog(String cngroupName) {
        LocalWarehouseOpLog log = new LocalWarehouseOpLog(ENABLE_CNGROUP);
        log.setCNGroupName(cngroupName);
        return log;
    }

    public static LocalWarehouseOpLog disableCNGroupOpLog(String cngroupName) {
        LocalWarehouseOpLog log = new LocalWarehouseOpLog(DISABLE_CNGROUP);
        log.setCNGroupName(cngroupName);
        return log;
    }
}
