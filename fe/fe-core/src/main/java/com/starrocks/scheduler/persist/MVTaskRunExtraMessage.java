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
import com.starrocks.scheduler.ExecuteOption;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MVTaskRunExtraMessage implements Writable {
    // max reserve size for set/map to avoid too long metadata in fe
    private static final int MAX_RESERVE_SIZE = 32;

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
    @SerializedName("refBasePartitionsToRefreshMap")
    private Map<String, Set<String>> refBasePartitionsToRefreshMap = Maps.newHashMap();
    // refreshed partitions of all the base tables which are optimized by optimizer and the real partitions in executing.
    @SerializedName("basePartitionsToRefreshMap")
    private Map<String, Set<String>> basePartitionsToRefreshMap = Maps.newHashMap();

    @SerializedName("nextPartitionStart")
    private String nextPartitionStart;
    @SerializedName("nextPartitionEnd")
    private String nextPartitionEnd;

    // task run starts to process time
    // NOTE: finishTime - processStartTime = process task run time(exclude pending time)
    @SerializedName("processStartTime")
    private long processStartTime = 0;

    @SerializedName("executeOption")
    private ExecuteOption executeOption = new ExecuteOption(true);

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
        this.mvPartitionsToRefresh = trimHashSet(mvPartitionsToRefresh);
    }

    public Map<String, Set<String>> getBasePartitionsToRefreshMap() {
        return basePartitionsToRefreshMap;
    }

    public Map<String, Set<String>> getRefBasePartitionsToRefreshMap() {
        return refBasePartitionsToRefreshMap;
    }

    private Set<String> trimHashSet(Set<String> set) {
        if (set != null && set.size() > MAX_RESERVE_SIZE) {
            return set.stream().limit(MAX_RESERVE_SIZE).collect(Collectors.toSet());
        }
        return set;
    }

    private Map<String, Set<String>> trimHashMap(Map<String, Set<String>> map) {
        if (map != null && map.size() > MAX_RESERVE_SIZE) {
            return map.entrySet().stream()
                    .limit(MAX_RESERVE_SIZE)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return map;
    }

    public void setRefBasePartitionsToRefreshMap(Map<String, Set<String>> refBasePartitionsToRefreshMap) {
        this.refBasePartitionsToRefreshMap = trimHashMap(refBasePartitionsToRefreshMap);
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

    public void setBasePartitionsToRefreshMap(Map<String, Set<String>> basePartitionsToRefreshMap) {
        this.basePartitionsToRefreshMap = trimHashMap(basePartitionsToRefreshMap);
    }

    public static MVTaskRunExtraMessage read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MVTaskRunExtraMessage.class);
    }

    public ExecuteOption getExecuteOption() {
        return executeOption;
    }

    public void setExecuteOption(ExecuteOption executeOption) {
        this.executeOption = executeOption;
    }

    public String getNextPartitionStart() {
        return nextPartitionStart;
    }

    public void setNextPartitionStart(String nextPartitionStart) {
        this.nextPartitionStart = nextPartitionStart;
    }

    public String getNextPartitionEnd() {
        return nextPartitionEnd;
    }

    public void setNextPartitionEnd(String nextPartitionEnd) {
        this.nextPartitionEnd = nextPartitionEnd;
    }

    public long getProcessStartTime() {
        return processStartTime;
    }

    public void setProcessStartTime(long processStartTime) {
        this.processStartTime = processStartTime;
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
