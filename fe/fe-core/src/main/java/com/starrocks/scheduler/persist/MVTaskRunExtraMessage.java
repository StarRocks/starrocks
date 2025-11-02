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
import com.starrocks.common.Config;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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
    // NOTE: This is set in mv's plan schdduler stage and are partitions which should be refreshed.
    // refreshed partitions of the ref base table in this task run which should only have one table for now.
    @SerializedName("refBasePartitionsToRefreshMap")
    private Map<String, Set<String>> refBasePartitionsToRefreshMap = Maps.newHashMap();
    // NOTE: This is only set after mv's version map has been committed and it's from final task run exec plan.
    // refreshed partitions of all the base tables which are optimized by optimizer and the real partitions in executing.
    @SerializedName("basePartitionsToRefreshMap")
    private Map<String, Set<String>> basePartitionsToRefreshMap = Maps.newHashMap();

    @SerializedName("nextPartitionStart")
    private String nextPartitionStart;
    @SerializedName("nextPartitionEnd")
    private String nextPartitionEnd;
    @SerializedName("nextPartitionValues")
    private String nextPartitionValues;

    // task run starts to process time
    // NOTE: finishTime - processStartTime = process task run time(exclude pending time)
    @SerializedName("processStartTime")
    private long processStartTime = 0;

    @SerializedName("executeOption")
    private ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(),
            false, Maps.newHashMap());

    @SerializedName("planBuilderMessage")
    public Map<String, String> planBuilderMessage = Maps.newHashMap();

    @SerializedName("adaptivePartitionRefreshNumber")
    private int adaptivePartitionRefreshNumber = -1;

    // the refresh mode of this task run
    @SerializedName("refreshMode")
    public String refreshMode = "";

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
        if (CollectionUtils.isEmpty(mvPartitionsToRefresh)) {
            return;
        }
        this.mvPartitionsToRefresh = Sets.newHashSet(MvUtils.shrinkToSize(mvPartitionsToRefresh,
                Config.max_mv_task_run_meta_message_values_length));
    }

    public Map<String, Set<String>> getBasePartitionsToRefreshMap() {
        return basePartitionsToRefreshMap;
    }

    public Map<String, Set<String>> getRefBasePartitionsToRefreshMap() {
        return refBasePartitionsToRefreshMap;
    }

    public void setRefBasePartitionsToRefreshMap(Map<String, Set<String>> refBasePartitionsToRefreshMap) {
        this.refBasePartitionsToRefreshMap = MvUtils.shrinkToSize(refBasePartitionsToRefreshMap,
                Config.max_mv_task_run_meta_message_values_length);
    }

    public String getMvPartitionsToRefreshString() {
        if (mvPartitionsToRefresh != null)  {
            String mvPartitionToRefresh = Joiner.on(",").join(mvPartitionsToRefresh);
            return StringUtils.substring(mvPartitionToRefresh, 0, 1024);
        } else {
            return "";
        }
    }

    public void setBasePartitionsToRefreshMap(Map<String, Set<String>> basePartitionsToRefreshMap) {
        this.basePartitionsToRefreshMap = MvUtils.shrinkToSize(basePartitionsToRefreshMap,
                Config.max_mv_task_run_meta_message_values_length);
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

    public String getNextPartitionValues() {
        return nextPartitionValues;
    }

    public void setNextPartitionValues(String nextPartitionValues) {
        this.nextPartitionValues = nextPartitionValues;
    }

    public long getProcessStartTime() {
        return processStartTime;
    }

    public void setProcessStartTime(long processStartTime) {
        this.processStartTime = processStartTime;
    }

    public void setPlanBuilderMessage(Map<String, String> planBuilderMessage) {
        this.planBuilderMessage = MvUtils.shrinkToSize(planBuilderMessage,
                Config.max_mv_task_run_meta_message_values_length);
    }

    public Map<String, String> getPlanBuilderMessage() {
        return planBuilderMessage;
    }

    public void setRefreshMode(String refreshMode) {
        this.refreshMode = refreshMode;
    }

    public String getRefreshMode() {
        return refreshMode;
    }

    public int getAdaptivePartitionRefreshNumber() {
        return adaptivePartitionRefreshNumber;
    }

    public void setAdaptivePartitionRefreshNumber(int adaptivePartitionRefreshNumber) {
        this.adaptivePartitionRefreshNumber = adaptivePartitionRefreshNumber;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
