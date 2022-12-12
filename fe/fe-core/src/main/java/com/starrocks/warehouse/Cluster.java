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

import java.util.concurrent.atomic.AtomicInteger;

public class Cluster {
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;

    // 注：Warehouse 和 Cluster 对象只记录了running 和 pending sql 的数量,
    // 我们假设 sql 排队功能 和 Cluster 类完全解耦，Cluster 类提供接口,
    // sql 排队功能根据 sql 执行情况负责调用这些接口更新 counter
    private AtomicInteger numRunningSqls;

    public Cluster(long id, long workerGroupId) {
        this.id = id;
        this.workerGroupId = workerGroupId;
    }

    // set the associated worker group id when resizing
    public void setWorkerGroupId(long id) {}
    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public int getRunningSqls() {
        return 1;
    }
    public int setRunningSqls(int val) {
        return 1;
    }
    public int addAndGetRunningSqls(int delta) {
        return 1;
    }
}
