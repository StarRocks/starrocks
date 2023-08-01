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
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class Warehouse implements Writable {

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "id")
    private long id;

    public enum WarehouseState {
        INITIALIZING,
        RUNNING,
        SUSPENDED,
        SCALING
    }

    @SerializedName(value = "state")
    protected WarehouseState state = WarehouseState.INITIALIZING;

    private volatile boolean exist = true;

    public Warehouse(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setState(WarehouseState state) {
        this.state = state;
    }

    public WarehouseState getState() {
        return state;
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public abstract void getProcNodeData(BaseProcResult result);

    public abstract Map<Long, Cluster> getClusters() throws DdlException;

    public abstract Cluster getAnyAvailableCluster();

    public abstract void setClusters(Map<Long, Cluster> clusters) throws DdlException;

    public List<List<String>> getClusterInfo() {
        return getClusterProcData().getRows();
    }

    public abstract ProcResult getClusterProcData();

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Warehouse read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Warehouse.class);
    }
}
