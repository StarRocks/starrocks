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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/PartitionCommitInfo.java

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

package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColumnId;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

public class PartitionCommitInfo implements Writable {

    @SerializedName(value = "partitionId")
    private long physicalPartitionId;
    @SerializedName(value = "version")
    private long version;

    // For LakeTable, the value of versionTime indicates different circumstances:
    //  = 0 : no publish version task has been executed since process starts
    //  < 0 : last publish version task failed and Math.abs(versionTime) is the last execution time
    //  > 0 : last publish version task succeeded and versionTime is the last execution time
    //
    // For OlapTable, versionTime always greater than 0.
    @SerializedName(value = "versionTime")
    private long versionTime;

    @SerializedName(value = "dataVersion")
    private long dataVersion;

    @SerializedName(value = "versionEpoch")
    private long versionEpoch;

    // For low cardinality string column with global dict
    // TODO(KKS): move invalidDictCacheColumns and validDictCacheColumns to TableCommitInfo
    // Currently, for support FE rollback, we persist the invalidDictCacheColumns in PartitionCommitInfo by json,
    // not TableCommitInfo.

    @SerializedName(value = "invalidColumns")
    private List<ColumnId> invalidDictCacheColumns = Lists.newArrayList();
    @SerializedName(value = "validColumns")
    private List<ColumnId> validDictCacheColumns = Lists.newArrayList();
    @SerializedName(value = "DictCollectedVersion")
    private List<Long> dictCollectedVersions = Lists.newArrayList();

    // compaction score quantiles of lake table
    @SerializedName(value = "compactionScore")
    private Quantiles compactionScore;

    private boolean isDoubleWrite = false;

    public PartitionCommitInfo() {

    }

    public PartitionCommitInfo(long physicalPartitionId, long version, long visibleTime) {
        super();
        this.physicalPartitionId = physicalPartitionId;
        this.version = version;
        this.versionTime = visibleTime;
    }

    public PartitionCommitInfo(long physicalPartitionId, long version, long visibleTime,
                               List<ColumnId> invalidDictCacheColumns,
                               List<ColumnId> validDictCacheColumns,
                               List<Long> dictCollectedVersions) {
        super();
        this.physicalPartitionId = physicalPartitionId;
        this.version = version;
        this.versionTime = visibleTime;
        this.invalidDictCacheColumns = invalidDictCacheColumns;
        this.validDictCacheColumns = validDictCacheColumns;
        this.dictCollectedVersions = dictCollectedVersions;
    }



    public static PartitionCommitInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionCommitInfo.class);
    }

    public void setVersionTime(long time) {
        this.versionTime = time;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(long dataVersion) {
        this.dataVersion = dataVersion;
    }

    public long getVersionEpoch() {
        return versionEpoch;
    }

    public void setVersionEpoch(long versionEpoch) {
        this.versionEpoch = versionEpoch;
    }

    public void setIsDoubleWrite(boolean isDoubleWrite) {
        this.isDoubleWrite = isDoubleWrite;
    }

    public boolean isDoubleWrite() {
        return isDoubleWrite;
    }

    public List<ColumnId> getInvalidDictCacheColumns() {
        return invalidDictCacheColumns;
    }

    public List<ColumnId> getValidDictCacheColumns() {
        return validDictCacheColumns;
    }

    public List<Long> getDictCollectedVersions() {
        return dictCollectedVersions;
    }

    public void setCompactionScore(Quantiles compactionScore) {
        this.compactionScore = compactionScore;
    }

    @Nullable
    public Quantiles getCompactionScore() {
        return compactionScore;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("partitionId=");
        sb.append(physicalPartitionId);
        sb.append(", version=").append(version);
        sb.append(", versionTime=").append(versionTime);
        sb.append(", isDoubleWrite=").append(isDoubleWrite);
        return sb.toString();
    }
}