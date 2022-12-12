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

package com.starrocks.server;

import com.google.gson.annotations.SerializedName;
import com.starrocks.warehouse.Warehouse;

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager {
    @SerializedName(value = "nmap")
    private Map<String, Warehouse> nameToWarehouse;
    @SerializedName(value = "imap")
    private Map<Long, Warehouse> idToWarehouse;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // these apis need lock protection
    public void createWarehouse() {}
    public void dropWarehouse(String name) {}
    public void alterWarehouse(String name) {}
    public void showWarehouses(String pattern) {}

    // warehouse meta persistence api
    public void loadWarehouses() {}
    public void saveWarehouses() {}
    public void replayCreateWarehouse() {}
    public void replayDropWarehouse() {}
    public void replayAlterWarehouse() {}
}
