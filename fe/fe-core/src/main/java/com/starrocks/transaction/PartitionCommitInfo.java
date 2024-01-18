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
import com.starrocks.cloudnative.compaction.Quantiles;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

public class PartitionCommitInfo implements Writable {

    @SerializedName(value = "partitionId")
    private long partitionId;
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

    // For low cardinality string column with global dict
    // TODO(KKS): move invalidDictCacheColumns and validDictCacheColumns to TableCommitInfo
    // Currently, for support FE rollback, we persist the invalidDictCacheColumns in PartitionCommitInfo by json,
    // not TableCommitInfo.

    @SerializedName(value = "invalidColumns")
    private List<String> invalidDictCacheColumns = Lists.newArrayList();
    @SerializedName(value = "validColumns")
    private List<String> validDictCacheColumns = Lists.newArrayList();
    @SerializedName(value = "DictCollectedVersion")
    private List<Long> dictCollectedVersions = Lists.newArrayList();

    // compaction score quantiles of lake table
    @SerializedName(value = "compactionScore")
    private Quantiles compactionScore;

    public PartitionCommitInfo() {

    }

    public PartitionCommitInfo(long partitionId, long version, long visibleTime) {
        super();
        this.partitionId = partitionId;
        this.version = version;
        this.versionTime = visibleTime;
    }

    public PartitionCommitInfo(long partitionId, long version, long visibleTime,
                               List<String> invalidDictCacheColumns,
                               List<String> validDictCacheColumns,
                               List<Long> dictCollectedVersions) {
        super();
        this.partitionId = partitionId;
        this.version = version;
        this.versionTime = visibleTime;
        this.invalidDictCacheColumns = invalidDictCacheColumns;
        this.validDictCacheColumns = validDictCacheColumns;
        this.dictCollectedVersions = dictCollectedVersions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionCommitInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionCommitInfo.class);
    }

    public void setVersionTime(long time) {
        this.versionTime = time;
    }

    public long getPartitionId() {
        return partitionId;
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

    public List<String> getInvalidDictCacheColumns() {
        return invalidDictCacheColumns;
    }

    public List<String> getValidDictCacheColumns() {
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
        StringBuilder sb = new StringBuilder("partitionid=");
        sb.append(partitionId);
        sb.append(", version=").append(version);
        sb.append(", versionHash=").append(0);
        sb.append(", versionTime=").append(versionTime);
        return sb.toString();
    }
}