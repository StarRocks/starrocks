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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/DropPartitionInfo.java

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

package com.starrocks.persist;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DropPartitionsInfo implements Writable {
    @SerializedName(value = "dbId")
    private Long dbId;
    @SerializedName(value = "tableId")
    private Long tableId;
    @SerializedName(value = "isTempPartition")
    private boolean isTempPartition = false;
    @SerializedName(value = "forceDrop")
    private boolean forceDrop = false;
    @SerializedName(value = "partitionNames")
    private List<String> partitionNames = new ArrayList<>();

    private DropPartitionsInfo() {
    }

    public DropPartitionsInfo(Long dbId, Long tableId, boolean isTempPartition, boolean forceDrop, List<String> partitionNames) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.partitionNames = partitionNames;
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public static DropPartitionsInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DropPartitionsInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public int hashCode() {
        return Objects.hashCode(dbId, tableId, partitionNames != null ? partitionNames : Collections.emptyList());
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DropPartitionsInfo info = (DropPartitionsInfo) obj;
        return (dbId.equals(info.dbId)) && (tableId.equals(info.tableId)) &&
                ((partitionNames == null && info.partitionNames == null) ||
                        (partitionNames != null && partitionNames.equals(info.partitionNames))) &&
                (isTempPartition == info.isTempPartition) && (forceDrop == info.forceDrop);
    }
}

// [partitionName] -》 旧读[partitionName,partitionNames]  -》 新写[partitionName,partitionNames] -》新读[partitionName,partitionNames]
// [null] -》新写[单个分区][] -》[partitionName,partitionNames] -》 新读[partitionName,partitionNames]
// [null] -》新写[多个分区][] -》[null,partitionNames]          -》 新读[null,partitionNames]
// [null] -》新写[我的版本代码][单个分区][] -》[partitionName,partitionNames] -》 降级[从我的版本切换到之前的版本]         -》旧读[partitionName][可降级]
// [null] -》新写[我的版本代码][多个分区][] -》[null,partitionNames]          -》 降级[从我的版本切换到之前的版本]         -》旧读[无法降级]
// [------------方案-------------]
// JournalEntity
