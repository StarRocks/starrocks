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
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.MVPartitionMapPB;
import com.starrocks.persist.proto.MVSnapshotMapPB;
import com.starrocks.persist.proto.MVSourceRangeMapPB;
import com.starrocks.persist.proto.MVTaskRunExtraMessagePB;
import com.starrocks.persist.proto.StringListPB;
import com.starrocks.persist.proto.StringMapPB;
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

    // For pinned PCT batches: base-table identifier -> frozen Iceberg snapshot id.
    // Serialized into information_schema.task_runs.EXTRA_MESSAGE for post-mortem debugging.
    @SerializedName("pinnedSnapshotIdMap")
    private Map<String, Long> pinnedSnapshotIdMap = Maps.newHashMap();

    // For IVM refreshes: per base table ("catalog.db.tbl"), the consumed TVR version range
    // {start, end} and the matching snapshot commit times in epoch millis (empty when unresolvable).
    @SerializedName("imvSourceVersionRange")
    private Map<String, Map<String, String>> imvSourceVersionRange = Maps.newHashMap();
    @SerializedName("imvSourceTimestampRange")
    private Map<String, Map<String, String>> imvSourceTimestampRange = Maps.newHashMap();

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

    public Map<String, Long> getPinnedSnapshotIdMap() {
        return pinnedSnapshotIdMap;
    }

    public void setPinnedSnapshotIdMap(Map<String, Long> pinnedSnapshotIdMap) {
        this.pinnedSnapshotIdMap = MvUtils.shrinkToSize(pinnedSnapshotIdMap,
                Config.max_mv_task_run_meta_message_values_length);
    }

    public Map<String, Map<String, String>> getImvSourceVersionRange() {
        return imvSourceVersionRange;
    }

    public void setImvSourceVersionRange(Map<String, Map<String, String>> imvSourceVersionRange) {
        this.imvSourceVersionRange = MvUtils.shrinkToSize(imvSourceVersionRange,
                Config.max_mv_task_run_meta_message_values_length);
    }

    public Map<String, Map<String, String>> getImvSourceTimestampRange() {
        return imvSourceTimestampRange;
    }

    public void setImvSourceTimestampRange(Map<String, Map<String, String>> imvSourceTimestampRange) {
        this.imvSourceTimestampRange = MvUtils.shrinkToSize(imvSourceTimestampRange,
                Config.max_mv_task_run_meta_message_values_length);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public MVTaskRunExtraMessagePB toProto() {
        MVTaskRunExtraMessagePB.Builder builder = MVTaskRunExtraMessagePB.newBuilder()
                .setForceRefresh(forceRefresh)
                .setProcessStartTime(processStartTime)
                .setAdaptivePartitionRefreshNumber(adaptivePartitionRefreshNumber);
        if (partitionStart != null) {
            builder.setPartitionStart(partitionStart);
        }
        if (partitionEnd != null) {
            builder.setPartitionEnd(partitionEnd);
        }
        if (mvPartitionsToRefresh != null) {
            builder.setMvPartitionsToRefresh(StringListPB.newBuilder().addAllValues(mvPartitionsToRefresh));
        }
        if (refBasePartitionsToRefreshMap != null) {
            builder.setRefBasePartitionsToRefreshMap(partitionsToProto(refBasePartitionsToRefreshMap));
        }
        if (basePartitionsToRefreshMap != null) {
            builder.setBasePartitionsToRefreshMap(partitionsToProto(basePartitionsToRefreshMap));
        }
        if (nextPartitionStart != null) {
            builder.setNextPartitionStart(nextPartitionStart);
        }
        if (nextPartitionEnd != null) {
            builder.setNextPartitionEnd(nextPartitionEnd);
        }
        if (nextPartitionValues != null) {
            builder.setNextPartitionValues(nextPartitionValues);
        }
        if (executeOption != null) {
            builder.setExecuteOption(executeOption.toProto());
        }
        if (planBuilderMessage != null) {
            builder.setPlanBuilderMessage(StringMapPB.newBuilder().putAllValues(ProtoMaps.withoutNullValues(planBuilderMessage)));
        }
        if (refreshMode != null) {
            builder.setRefreshMode(refreshMode);
        }
        if (pinnedSnapshotIdMap != null) {
            builder.setPinnedSnapshotIdMap(MVSnapshotMapPB.newBuilder()
                    .putAllValues(ProtoMaps.withoutNullValues(pinnedSnapshotIdMap)));
        }
        if (imvSourceVersionRange != null) {
            builder.setImvSourceVersionRange(rangesToProto(imvSourceVersionRange));
        }
        if (imvSourceTimestampRange != null) {
            builder.setImvSourceTimestampRange(rangesToProto(imvSourceTimestampRange));
        }
        return builder.build();
    }

    public static MVTaskRunExtraMessage fromProto(MVTaskRunExtraMessagePB pb) {
        MVTaskRunExtraMessage result = new MVTaskRunExtraMessage();
        result.forceRefresh = pb.getForceRefresh();
        result.processStartTime = pb.getProcessStartTime();
        if (pb.hasPartitionStart()) {
            result.partitionStart = pb.getPartitionStart();
        }
        if (pb.hasPartitionEnd()) {
            result.partitionEnd = pb.getPartitionEnd();
        }
        if (pb.hasMvPartitionsToRefresh()) {
            result.mvPartitionsToRefresh = Sets.newHashSet(pb.getMvPartitionsToRefresh().getValuesList());
        }
        if (pb.hasRefBasePartitionsToRefreshMap()) {
            result.refBasePartitionsToRefreshMap = partitionsFromProto(pb.getRefBasePartitionsToRefreshMap());
        }
        if (pb.hasBasePartitionsToRefreshMap()) {
            result.basePartitionsToRefreshMap = partitionsFromProto(pb.getBasePartitionsToRefreshMap());
        }
        if (pb.hasNextPartitionStart()) {
            result.nextPartitionStart = pb.getNextPartitionStart();
        }
        if (pb.hasNextPartitionEnd()) {
            result.nextPartitionEnd = pb.getNextPartitionEnd();
        }
        if (pb.hasNextPartitionValues()) {
            result.nextPartitionValues = pb.getNextPartitionValues();
        }
        if (pb.hasExecuteOption()) {
            result.executeOption = ExecuteOption.fromProto(pb.getExecuteOption());
        }
        if (pb.hasPlanBuilderMessage()) {
            result.planBuilderMessage = Maps.newHashMap(pb.getPlanBuilderMessage().getValuesMap());
        }
        if (pb.hasAdaptivePartitionRefreshNumber()) {
            result.adaptivePartitionRefreshNumber = pb.getAdaptivePartitionRefreshNumber();
        }
        if (pb.hasRefreshMode()) {
            result.refreshMode = pb.getRefreshMode();
        }
        if (pb.hasPinnedSnapshotIdMap()) {
            result.pinnedSnapshotIdMap = Maps.newHashMap(pb.getPinnedSnapshotIdMap().getValuesMap());
        }
        if (pb.hasImvSourceVersionRange()) {
            result.imvSourceVersionRange = rangesFromProto(pb.getImvSourceVersionRange());
        }
        if (pb.hasImvSourceTimestampRange()) {
            result.imvSourceTimestampRange = rangesFromProto(pb.getImvSourceTimestampRange());
        }
        return result;
    }

    private static MVPartitionMapPB partitionsToProto(Map<String, Set<String>> partitions) {
        MVPartitionMapPB.Builder builder = MVPartitionMapPB.newBuilder();
        ProtoMaps.withoutNullValues(partitions).forEach((table, names) ->
                builder.putValues(table, StringListPB.newBuilder().addAllValues(names).build()));
        return builder.build();
    }

    private static Map<String, Set<String>> partitionsFromProto(MVPartitionMapPB pb) {
        Map<String, Set<String>> result = Maps.newHashMap();
        pb.getValuesMap().forEach((table, names) -> result.put(table, Sets.newHashSet(names.getValuesList())));
        return result;
    }

    private static MVSourceRangeMapPB rangesToProto(Map<String, Map<String, String>> ranges) {
        MVSourceRangeMapPB.Builder builder = MVSourceRangeMapPB.newBuilder();
        ProtoMaps.withoutNullValues(ranges).forEach((table, range) -> builder.putValues(table,
                StringMapPB.newBuilder().putAllValues(ProtoMaps.withoutNullValues(range)).build()));
        return builder.build();
    }

    private static Map<String, Map<String, String>> rangesFromProto(MVSourceRangeMapPB pb) {
        Map<String, Map<String, String>> result = Maps.newHashMap();
        pb.getValuesMap().forEach((table, range) -> result.put(table, Maps.newHashMap(range.getValuesMap())));
        return result;
    }
}
