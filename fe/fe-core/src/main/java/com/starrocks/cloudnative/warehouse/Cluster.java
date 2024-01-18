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

package com.starrocks.cloudnative.warehouse;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.cloudnative.StarOSAgent;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;

    public Cluster(long id) {
        this.id = id;
        workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
    }

    public long getId() {
        return id;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public int getRunningSqls() {
        return -1;
    }
    public int getPendingSqls() {
        return -1;
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(String.valueOf(this.getId()),
                String.valueOf(this.getWorkerGroupId()),
                String.valueOf(this.getComputeNodeIds()),
                String.valueOf(this.getPendingSqls()),
                String.valueOf(this.getRunningSqls())));
    }

    public List<Long> getComputeNodeIds() {
        List<Long> nodeIds = new ArrayList<>();
        if (RunMode.isSharedNothingMode()) {
            return nodeIds;
        }
        try {
            // ask starMgr for node lists
            nodeIds = GlobalStateMgr.getCurrentStarOSAgent().
                    getWorkersByWorkerGroup(workerGroupId);
        } catch (UserException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
        }
        return nodeIds;
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
