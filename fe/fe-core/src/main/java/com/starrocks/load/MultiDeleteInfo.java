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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/MultiDeleteInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MultiDeleteInfo implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;

    @SerializedName(value = "deleteConditions")
    private List<String> deleteConditions;
    @SerializedName(value = "createTimeMs")
    private long createTimeMs;
    @SerializedName(value = "partitionIds")
    private List<Long> partitionIds;
    @SerializedName(value = "partitionNames")
    private List<String> partitionNames;
    @SerializedName(value = "noPartitionSpecified")
    private boolean noPartitionSpecified = false;

    public MultiDeleteInfo(long dbId, long tableId, String tableName, List<String> deleteConditions) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.deleteConditions = deleteConditions;
        this.createTimeMs = System.currentTimeMillis();
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setDeleteConditions(List<String> deleteConditions) {
        this.deleteConditions = deleteConditions;
    }

    public List<String> getDeleteConditions() {
        return deleteConditions;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public boolean isNoPartitionSpecified() {
        return noPartitionSpecified;
    }

    public void setPartitions(boolean noPartitionSpecified, List<Long> partitionIds, List<String> partitionNames) {
        this.noPartitionSpecified = noPartitionSpecified;
        Preconditions.checkState(partitionIds.size() == partitionNames.size(), "partition info error.");
        this.partitionIds = partitionIds;
        this.partitionNames = partitionNames;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public static MultiDeleteInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), MultiDeleteInfo.class);
    }

    public static MultiDeleteInfo upgrade(DeleteInfo deleteInfo) {
        MultiDeleteInfo multiDeleteInfo = new MultiDeleteInfo(deleteInfo.getDbId(), deleteInfo.getTableId(),
                deleteInfo.getTableName(), Lists.newArrayList(deleteInfo.getDeleteConditions()));
        multiDeleteInfo.setPartitions(false, Lists.newArrayList(deleteInfo.getPartitionId()),
                Lists.newArrayList(deleteInfo.getPartitionName()));
        return multiDeleteInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

}