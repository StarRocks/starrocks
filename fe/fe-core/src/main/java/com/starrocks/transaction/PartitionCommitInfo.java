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
import com.starrocks.common.io.Writable;
import com.starrocks.lake.compaction.Quantiles;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class PartitionCommitInfo implements Writable {

    @SerializedName(value = "partitionId")
    private long physicalPartitionId;
    @SerializedName(value = "version")
    private long version;

    // For LakeTable, the value of versionTime indicates different circumstances:
    //  = 0 : this partition has not published yet
    //  > 0 : last publish version task succeeded and versionTime is the last execution time
    //
    // A failed attempt is recorded in lastPublishFailureTime instead of by negating this field:
    // versionTime is also the timestamp handed to Partition#updateVisibleVersion, so it must never
    // carry a negative value into the image.
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

    private final Map<Long, Long> tabletIdToRowCountForPartitionFirstLoad = new HashMap<>();

    private boolean isDoubleWrite = false;

    // Paces the "fail to publish partition" error log for this partition. Deliberately not
    // serialized: it only throttles logging inside one FE process. Races between publish
    // threads can at worst let one extra line through, which is not worth a lock here.
    private long lastPublishErrorLogTime = 0;

    // When the last publish attempt for this partition failed, used to space out retries.
    // Deliberately not serialized: it is in-process retry state, and a leader that has just taken
    // over should attempt a publish immediately rather than inherit a stale back-off.
    private long lastPublishFailureTime = 0;

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

    public void setVersionTime(long time) {
        this.versionTime = time;
    }

    // Records a failed publish attempt. versionTime is left alone so it keeps meaning
    // "the time this partition became visible", which is what the txn log appliers read.
    public void markPublishFailed(long now) {
        this.lastPublishFailureTime = now;
    }

    public void markPublishSucceeded(long now) {
        this.lastPublishFailureTime = 0;
        this.versionTime = now;
    }

    // 0 when the last attempt did not fail.
    public long getLastPublishFailureTime() {
        return lastPublishFailureTime;
    }

    // Returns true at most once per intervalMs. A partition that keeps failing to publish is
    // retried continuously, so logging every failure turns one stuck partition into a steady
    // stream of identical messages.
    public boolean shouldLogPublishError(long now, long intervalMs) {
        if (now - lastPublishErrorLogTime < intervalMs) {
            return false;
        }
        lastPublishErrorLogTime = now;
        return true;
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

    public Map<Long, Long> getTabletIdToRowCountForPartitionFirstLoad() {
        return tabletIdToRowCountForPartitionFirstLoad;
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