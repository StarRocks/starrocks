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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    @SerializedName(value = "isDropAll")
    private boolean isDropAll = false;

    private DropPartitionsInfo() {
    }

    public DropPartitionsInfo(Long dbId, Long tableId, boolean isTempPartition, boolean forceDrop, List<String> partitionNames) {
        this(dbId, tableId, isTempPartition, forceDrop, partitionNames, false);
    }

    public DropPartitionsInfo(Long dbId, Long tableId, boolean isTempPartition,
                              boolean forceDrop, List<String> partitionNames, boolean isDropAll) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.partitionNames = partitionNames;
        this.isDropAll = isDropAll;
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

    public boolean isDropAll() {
        return isDropAll;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DropPartitionsInfo that = (DropPartitionsInfo) o;
        return isTempPartition == that.isTempPartition && forceDrop == that.forceDrop && Objects.equals(dbId, that.dbId) &&
                Objects.equals(tableId, that.tableId) && Objects.equals(partitionNames, that.partitionNames) &&
                isDropAll == that.isDropAll;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, isTempPartition, forceDrop, partitionNames, isDropAll);
    }
}
