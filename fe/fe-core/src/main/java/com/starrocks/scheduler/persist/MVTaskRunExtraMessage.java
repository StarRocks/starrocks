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


package com.starrocks.scheduler.persist;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MVTaskRunExtraMessage implements Writable {
    @SerializedName("forceRefresh")
    private boolean forceRefresh;
    @SerializedName("partitionStart")
    private String partitionStart;
    @SerializedName("partitionEnd")
    private String partitionEnd;

    // refreshed partitions of materialized view in this task run
    @SerializedName("mvPartitionsToRefresh")
    private Set<String> mvPartitionsToRefresh = Sets.newHashSet();
    // refreshed partitions of the ref base table in this task run which should only have one table for now.
    @SerializedName("refBaseePartitionsToRefreshMap")
    private Map<String, Set<String>> refBasePartitionsToRefreshMap = Maps.newHashMap();
    // refreshed partitions of all the base tables which are optimized by optimizer and the real partitions in executing.
    @SerializedName("basePartitionsToRefreshMap")
    private Map<String, Set<String>> basePartitionsToRefreshMap = Maps.newHashMap();

    public MVTaskRunExtraMessage() {
    }

    public boolean isForceRefresh() {
        return forceRefresh;
    }

    public void setForceRefresh(boolean forceRefresh) {
        this.forceRefresh = forceRefresh;
    }

    public String getPartitionStart() {
        return partitionStart;
    }

    public void setPartitionStart(String basePartitionStart) {
        this.partitionStart = basePartitionStart;
    }

    public String getPartitionEnd() {
        return partitionEnd;
    }

    public void setPartitionEnd(String basePartitionEnd) {
        this.partitionEnd = basePartitionEnd;
    }

    public Set<String> getMvPartitionsToRefresh() {
        return mvPartitionsToRefresh;
    }

    public void setMvPartitionsToRefresh(Set<String> mvPartitionsToRefresh) {
        this.mvPartitionsToRefresh = mvPartitionsToRefresh;
    }

    public Map<String, Set<String>> getBasePartitionsToRefreshMap() {
        return basePartitionsToRefreshMap;
    }

    public Map<String, Set<String>> getRefBasePartitionsToRefreshMap() {
        return refBasePartitionsToRefreshMap;
    }

    public void setRefBasePartitionsToRefreshMap(
            Map<String, Set<String>> refBasePartitionsToRefreshMap) {
        this.refBasePartitionsToRefreshMap = refBasePartitionsToRefreshMap;
    }

    public String getMvPartitionsToRefreshString() {
        if (mvPartitionsToRefresh != null)  {
            String mvPartitionToRefresh = Joiner.on(",").join(mvPartitionsToRefresh);
            return StringUtils.substring(mvPartitionToRefresh, 0, 1024);
        } else {
            return "";
        }
    }

    public String getBasePartitionsToRefreshMapString() {
        if (basePartitionsToRefreshMap != null) {
            String basePartitionToRefresh = basePartitionsToRefreshMap.toString();
            return StringUtils.substring(basePartitionToRefresh, 0, 1024);
        } else {
            return "";
        }
    }

    public void setBasePartitionsToRefreshMap(
            Map<String, Set<String>> basePartitionsToRefreshMap) {
        this.basePartitionsToRefreshMap = basePartitionsToRefreshMap;
    }

    public static MVTaskRunExtraMessage read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MVTaskRunExtraMessage.class);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
