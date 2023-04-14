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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import org.jline.utils.Log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Cluster implements Writable {
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;
    @SerializedName(value = "computeNodeList")
    private List<ComputeNode> computeNodeList = new ArrayList<>();

    public Cluster(long id) {
        this.id = id;
        // in Shared_data RunMode, need to create a workerGroup
        if (RunMode.allowCreateLakeTable()) {
            try {
                workerGroupId = GlobalStateMgr.getCurrentStarOSAgent().createWorkerGroup("x0");
            } catch (DdlException e) {
                Log.warn(e);
                System.exit(-1);
            }
        }
    }

    public Cluster(long id, long workerGroupId) {
        this.id = id;
        this.workerGroupId = workerGroupId;
    }

    public void addNodes(ComputeNode cn, String warehouseName) {
        computeNodeList.add(cn);
        if (RunMode.allowCreateLakeTable()) {
            // add worker to group
            cn.setWorkerGroupId(workerGroupId);
            cn.setWarehouseName(warehouseName);
        }
    }

    public List<ComputeNode> getComputeNodeList() {
        return computeNodeList;
    }

    public void clearComputeNodes() {
        computeNodeList.clear();
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public long getId() {
        return id;
    }

    public int getRunningSqls() {
        return -1;
    }
    public int getPendingSqls() {
        return -1;
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(String.valueOf(this.getId()),
                String.valueOf(this.getPendingSqls()),
                String.valueOf(this.getRunningSqls())));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Cluster read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Cluster.class);
    }

}
