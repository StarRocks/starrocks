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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Warehouse {
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;

    private enum WarehouseState {
        INITIALIZING,
        RUNNING,
        SUSPENDED,
        SCALING
    }

    @SerializedName(value = "state")
    private WarehouseState state;

    // cluster id -> cluster obj
    @SerializedName(value = "clusters")
    private Map<Long, Cluster> clusters;

    // properties
    @SerializedName(value = "size")
    private String size = "${default_size}";
    @SerializedName(value = "minCluster")
    private int minCluster = 1;
    @SerializedName(value = "maxCluster")
    private int maxCluster = 5;
    // and others ...

    private AtomicInteger numPendingSqls;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public void addCluster() {}
    public void removeCluster() {}
    public void suspend() {}
    public void resume() {}
    public void showClusters() {}

    // property setters and getters
    public void setSize(String size) {}
    public void getSize() {}
    // and others...

    public long numTotalRunningSqls() {
        return 1L;
    }

    public int getPendingSqls() {
        return 1;
    }
    
    public int setPendingSqls(int val) {
        return 1;
    }

    public int addAndGetPendingSqls(int delta) {
        return 1;
    }
}
