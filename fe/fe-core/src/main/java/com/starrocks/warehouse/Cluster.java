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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;
    @SerializedName(value = "computeNodeIds")
    private Set<Long> computeNodeIds = new HashSet<>();

    public Cluster(long id) {
        this.id = id;
        // use default workerGroupId for community version
        workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
    }

    public long getId() {
        return id;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
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
                String.valueOf(this.getPendingSqls()),
                String.valueOf(this.getRunningSqls())));
    }

    public void addNode(long cnId) {
        computeNodeIds.add(cnId);
    }

    public void dropNode(long cnId) {
        computeNodeIds.remove(cnId);
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
