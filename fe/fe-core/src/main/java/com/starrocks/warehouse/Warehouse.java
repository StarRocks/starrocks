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
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.ProcResult;

import java.util.List;

public abstract class Warehouse implements Writable {
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "comment")
    protected String comment;

    public Warehouse(long id, String name, String comment) {
        this.id = id;
        this.name = name;
        this.comment = comment;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public abstract long getResumeTime();

    public abstract Long getAnyWorkerGroupId();

    public abstract List<Long> getWorkerGroupIds();

    public abstract List<String> getWarehouseInfo();

    public abstract List<List<String>> getWarehouseNodesInfo();

    public abstract ProcResult fetchResult();
<<<<<<< HEAD
=======

    public abstract void createCNGroup(CreateCnGroupStmt stmt) throws DdlException;

    public abstract void dropCNGroup(DropCnGroupStmt stmt) throws DdlException;

    public abstract void enableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException;

    public abstract void disableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException;

    public abstract void alterCNGroup(AlterCnGroupStmt stmt) throws DdlException;

    public abstract void replayInternalOpLog(String payload);

    public abstract boolean isAvailable();
>>>>>>> afc0438d2b ([Enhancement] Eliminate heartbeat error logs for backends in suspended warehouses (#66733))
}
