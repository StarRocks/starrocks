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
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Cluster implements Writable {
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;

    // Note: we only record running sqls number and pending sqls in Warehouse and Cluster
    // We suppose that sql queue tool has nothing to do with  Cluster,
    // Cluster offers related interfaces and sql queue tool will update counter according to the implementation of sqls.
    private AtomicInteger numRunningSqls;

    public Cluster(long id, long workerGroupId) {
        this.id = id;
        this.workerGroupId = workerGroupId;
    }

    // set the associated worker group id when resizing
    /*    public void setWorkerGroupId(long id) {
        this.workerGroupId = id;
    }*/

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
    /*    public int setRunningSqls(int val) {
        return 1;
    }
    public int addAndGetRunningSqls(int delta) {
        return 1;
    }*/

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
