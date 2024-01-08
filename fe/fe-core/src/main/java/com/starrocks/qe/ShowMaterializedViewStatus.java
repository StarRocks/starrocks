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

package com.starrocks.qe;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.thrift.TMaterializedViewStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ShowMaterializedViewStatus represents one materialized view's refresh status for ShowMaterializedViews command usage.
 */
public class ShowMaterializedViewStatus {
    public static final String MULTI_TASK_RUN_SEPARATOR = "|";

    private long id;
    private String dbName;
    private String name;
    private String refreshType;
    private boolean isActive;
    private String text;
    private long rows;
    private String partitionType;
    private long lastCheckTime;
    private String inactiveReason;
    private List<TaskRunStatus> lastJobTaskRunStatus;

    /**
     * RefreshJobStatus represents a batch of batch TaskRunStatus because a fresh may trigger more than one task runs.
     */
    public class RefreshJobStatus {
        private long taskId;
        private String taskName;
        private Constants.TaskRunState refreshState;
        private long mvRefreshStartTime;
        private long mvRefreshEndTime;
        private long totalProcessDuration;
        private boolean isForce;
        private List<String> refreshedPartitionStarts;
        private List<String> refreshedPartitionEnds;
        private List<Map<String, Set<String>>> refreshedBasePartitionsToRefreshMaps;
        private List<Set<String>> refreshedMvPartitionsToRefreshs;
        private String errorCode;
        private String errorMsg;
        private boolean isRefreshFinished;
        private ExtraMessage extraMessage;

        public RefreshJobStatus() {
        }

        public long getMvRefreshStartTime() {
            return mvRefreshStartTime;
        }

        public void setMvRefreshStartTime(long mvRefreshStartTime) {
            this.mvRefreshStartTime = mvRefreshStartTime;
        }

        public long getMvRefreshEndTime() {
            return mvRefreshEndTime;
        }

        public void setMvRefreshEndTime(long mvRefreshEndTime) {
            this.mvRefreshEndTime = mvRefreshEndTime;
        }

        public long getTotalProcessDuration() {
            return totalProcessDuration;
        }

        public void setTotalProcessDuration(long totalProcessDuration) {
            this.totalProcessDuration = totalProcessDuration;
        }

        public boolean isForce() {
            return isForce;
        }

        public void setForce(boolean force) {
            isForce = force;
        }

        public List<String> getRefreshedPartitionStarts() {
            return refreshedPartitionStarts == null ? Lists.newArrayList() : refreshedPartitionStarts;
        }

        public void setRefreshedPartitionStarts(List<String> refreshedPartitionStarts) {
            this.refreshedPartitionStarts = refreshedPartitionStarts;
        }

        public List<String> getRefreshedPartitionEnds() {
            return refreshedPartitionEnds == null ? Lists.newArrayList() : refreshedPartitionEnds;
        }

        public void setRefreshedPartitionEnds(List<String> refreshedPartitionEnds) {
            this.refreshedPartitionEnds = refreshedPartitionEnds;
        }

        public List<Map<String, Set<String>>> getRefreshedBasePartitionsToRefreshMaps() {
            return refreshedBasePartitionsToRefreshMaps == null ? Lists.newArrayList() : refreshedBasePartitionsToRefreshMaps;
        }

        public void setRefreshedBasePartitionsToRefreshMaps(List<Map<String, Set<String>>> refreshedBasePartitionsToRefreshMaps) {
            this.refreshedBasePartitionsToRefreshMaps = refreshedBasePartitionsToRefreshMaps;
        }

        public List<Set<String>> getRefreshedMvPartitionsToRefreshs() {
            return refreshedMvPartitionsToRefreshs == null ? Lists.newArrayList() : refreshedMvPartitionsToRefreshs;
        }

        public void setRefreshedMvPartitionsToRefreshs(List<Set<String>> refreshedMvPartitionsToRefreshs) {
            this.refreshedMvPartitionsToRefreshs = refreshedMvPartitionsToRefreshs;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(String errorCode) {
            this.errorCode = errorCode;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        public void setErrorMsg(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        public Constants.TaskRunState getRefreshState() {
            return refreshState;
        }

        public void setRefreshState(Constants.TaskRunState refreshState) {
            this.refreshState = refreshState;
        }

        public boolean isRefreshFinished() {
            return isRefreshFinished;
        }

        public void setRefreshFinished(boolean refreshFinished) {
            isRefreshFinished = refreshFinished;
        }

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public String getTaskName() {
            return taskName;
        }

        public void setTaskName(String taskName) {
            this.taskName = taskName;
        }

        public ExtraMessage getExtraMessage() {
            return extraMessage;
        }

        public void setExtraMessage(ExtraMessage extraMessage) {
            this.extraMessage = extraMessage;
        }
    }

    /**
     * To avoid changing show materialized view's result schema, use this to keep extra message.
     */
    class ExtraMessage {
        @SerializedName("queryIds")
        private List<String> queryIds;
        @SerializedName("isManual")
        private boolean isManual = false;
        @SerializedName("isSync")
        private boolean isSync = false;
        @SerializedName("isReplay")
        private boolean isReplay = false;
        @SerializedName("priority")
        private int priority = Constants.TaskRunPriority.LOWEST.value();

        public boolean isManual() {
            return isManual;
        }

        public void setManual(boolean manual) {
            isManual = manual;
        }

        public boolean isSync() {
            return isSync;
        }

        public void setSync(boolean sync) {
            isSync = sync;
        }

        public boolean isReplay() {
            return isReplay;
        }

        public void setReplay(boolean replay) {
            isReplay = replay;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public List<String> getQueryIds() {
            return queryIds;
        }

        public void setQueryIds(List<String> queryIds) {
            this.queryIds = queryIds;
        }
    }

    public ShowMaterializedViewStatus(long id, String dbName, String name) {
        this.id = id;
        this.dbName = dbName;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRefreshType() {
        return refreshType;
    }

    public void setRefreshType(String refreshType) {
        this.refreshType = refreshType;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public long getLastCheckTime() {
        return lastCheckTime;
    }

    public void setLastCheckTime(long lastCheckTime) {
        this.lastCheckTime = lastCheckTime;
    }

    public String getInactiveReason() {
        return inactiveReason;
    }

    public void setInactiveReason(String inactiveReason) {
        this.inactiveReason = inactiveReason;
    }

    public void setLastJobTaskRunStatus(List<TaskRunStatus> lastJobTaskRunStatus) {
        if (lastJobTaskRunStatus != null) {
            // sort by process start time
            lastJobTaskRunStatus.sort(Comparator.comparing(TaskRunStatus::getProcessStartTime));
            this.lastJobTaskRunStatus = lastJobTaskRunStatus;
        }
    }

    private List<String> applyTaskRunStatusWith(Function<TaskRunStatus, String> func) {
        return lastJobTaskRunStatus.stream()
                .map(x -> func.apply(x))
                .map(x -> Optional.ofNullable(x).orElse("NULL"))
                .collect(Collectors.toList());
    }

    public RefreshJobStatus getRefreshJobStatus() {
        RefreshJobStatus status = new RefreshJobStatus();
        if (lastJobTaskRunStatus == null || lastJobTaskRunStatus.isEmpty()) {
            return status;
        }

        TaskRunStatus firstTaskRunStatus = lastJobTaskRunStatus.get(0);
        TaskRunStatus lastTaskRunStatus = lastJobTaskRunStatus.get(lastJobTaskRunStatus.size() - 1);

        status.setTaskId(firstTaskRunStatus.getTaskId());
        status.setTaskName(firstTaskRunStatus.getTaskName());

        // extra message
        ExtraMessage extraMessage = new ExtraMessage();
        List<String> queryIds = applyTaskRunStatusWith(x -> x.getQueryId());
        // queryIds
        extraMessage.setQueryIds(queryIds);
        MVTaskRunExtraMessage firstTaskRunExtraMessage = firstTaskRunStatus.getMvTaskRunExtraMessage();
        if (firstTaskRunExtraMessage != null && firstTaskRunExtraMessage.getExecuteOption() != null) {
            ExecuteOption executeOption = firstTaskRunExtraMessage.getExecuteOption();
            extraMessage.setManual(executeOption.isManual());
            extraMessage.setSync(executeOption.getIsSync());
            extraMessage.setReplay(executeOption.isReplay());
            extraMessage.setPriority(executeOption.getPriority());
            status.setExtraMessage(extraMessage);
        }

        // start time
        long mvRefreshStartTime = firstTaskRunStatus.getProcessStartTime();
        status.setMvRefreshStartTime(mvRefreshStartTime);

        // last refresh state
        status.setRefreshState(lastTaskRunStatus.getLastRefreshState());

        // is force
        MVTaskRunExtraMessage mvTaskRunExtraMessage = lastTaskRunStatus.getMvTaskRunExtraMessage();
        status.setForce(mvTaskRunExtraMessage.isForceRefresh());

        // getPartitionStart
        List<String> refreshedPartitionStarts = applyTaskRunStatusWith(x ->
                x.getMvTaskRunExtraMessage().getPartitionStart());
        status.setRefreshedPartitionStarts(refreshedPartitionStarts);

        // getPartitionEnd
        List<String> refreshedPartitionEnds = applyTaskRunStatusWith(x ->
                x.getMvTaskRunExtraMessage().getPartitionEnd());
        status.setRefreshedPartitionEnds(refreshedPartitionEnds);

        // getBasePartitionsToRefreshMapString
        List<Map<String, Set<String>>> refreshedBasePartitionsToRefreshMaps = lastJobTaskRunStatus.stream()
                .map(x -> x.getMvTaskRunExtraMessage().getBasePartitionsToRefreshMap())
                .map(x -> Optional.ofNullable(x).orElse(Maps.newHashMap()))
                .collect(Collectors.toList());
        status.setRefreshedBasePartitionsToRefreshMaps(refreshedBasePartitionsToRefreshMaps);

        // getMvPartitionsToRefreshString
        List<Set<String>> refreshedMvPartitionsToRefreshs = lastJobTaskRunStatus.stream()
                .map(x -> x.getMvTaskRunExtraMessage().getMvPartitionsToRefresh())
                .map(x -> Optional.ofNullable(x).orElse(Sets.newHashSet()))
                .collect(Collectors.toList());
        status.setRefreshedMvPartitionsToRefreshs(refreshedMvPartitionsToRefreshs);

        // only updated when refresh is finished
        if (lastTaskRunStatus.isRefreshFinished()) {
            status.setRefreshFinished(true);

            long mvRefreshFinishTime = lastTaskRunStatus.getFinishTime();
            status.setMvRefreshEndTime(mvRefreshFinishTime);

            long totalProcessDuration = lastJobTaskRunStatus.stream()
                    .map(x -> x.calculateRefreshProcessDuration())
                    .collect(Collectors.summingLong(Long::longValue));
            status.setTotalProcessDuration(totalProcessDuration);
            status.setErrorCode(String.valueOf(lastTaskRunStatus.getErrorCode()));
            status.setErrorMsg(Strings.nullToEmpty(lastTaskRunStatus.getErrorMessage()));
        }
        return status;
    }


    /**
     * Return the thrift of show materialized views command from be's request.
     */
    public TMaterializedViewStatus toThrift() {
        TMaterializedViewStatus status = new TMaterializedViewStatus();
        status.setId(String.valueOf(this.id));
        status.setDatabase_name(this.dbName);
        status.setName(this.name);
        status.setRefresh_type(this.refreshType);
        status.setIs_active(this.isActive ? "true" : "false");
        status.setInactive_reason(this.inactiveReason);
        status.setPartition_type(this.partitionType);

        RefreshJobStatus refreshJobStatus = getRefreshJobStatus();
        status.setTask_id(String.valueOf(refreshJobStatus.getTaskId()));
        status.setTask_name(refreshJobStatus.getTaskName());
        // start time
        status.setLast_refresh_start_time(TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshStartTime()));
        // LAST_REFRESH_STATE
        status.setLast_refresh_state(String.valueOf(refreshJobStatus.getRefreshState()));
        // is force
        status.setLast_refresh_force_refresh(refreshJobStatus.isForce() ? "true" : "false");
        // partitionStart
        status.setLast_refresh_start_partition(Joiner.on(MULTI_TASK_RUN_SEPARATOR)
                .join(refreshJobStatus.getRefreshedPartitionStarts()));
        // partitionEnd
        status.setLast_refresh_end_partition(Joiner.on(MULTI_TASK_RUN_SEPARATOR)
                .join(refreshJobStatus.getRefreshedPartitionEnds()));
        // basePartitionsToRefreshMapString
        status.setLast_refresh_base_refresh_partitions(Joiner.on(MULTI_TASK_RUN_SEPARATOR)
                .join(refreshJobStatus.getRefreshedBasePartitionsToRefreshMaps()));
        // mvPartitionsToRefreshString
        status.setLast_refresh_mv_refresh_partitions(Joiner.on(MULTI_TASK_RUN_SEPARATOR)
                .join(refreshJobStatus.getRefreshedMvPartitionsToRefreshs()));

        // only updated when refresh is finished
        if (refreshJobStatus.isRefreshFinished()) {
            status.setLast_refresh_finished_time(TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshEndTime()));
            status.setLast_refresh_duration(formatDuration(refreshJobStatus.getTotalProcessDuration()));
            status.setLast_refresh_error_code(refreshJobStatus.getErrorCode());
            status.setLast_refresh_error_message(refreshJobStatus.getErrorMsg());
        }

        status.setRows(String.valueOf(this.rows));
        status.setText(this.text);

        // extra message
        status.setExtra_message(refreshJobStatus.getExtraMessage() == null ? "" :
                GsonUtils.GSON.toJson(refreshJobStatus.getExtraMessage()));

        return status;
    }

    /**
     * Return show materialized views result set. Note: result set's order should keep same with
     * schema table MaterializedViewsSystemTable's define in the `MaterializedViewsSystemTable` class.
     */
    public List<String> toResultSet() {
        ArrayList<String> resultRow = new ArrayList<>();

        // NOTE: All fields should be ordered by `MaterializedViewsSystemTable`'s definition.
        // Add fields to the result set
        // mv id
        addField(resultRow, id);
        // db name
        addField(resultRow, dbName);
        // mv name
        addField(resultRow, name);
        // mv refresh type
        addField(resultRow, refreshType);
        // mv is active?
        addField(resultRow, isActive);
        // mv inactive reason?
        addField(resultRow, inactiveReason);
        // mv partition type
        addField(resultRow, partitionType);

        RefreshJobStatus refreshJobStatus = getRefreshJobStatus();
        // task id
        addField(resultRow, refreshJobStatus.getTaskId());
        // task name
        addField(resultRow, Strings.nullToEmpty(refreshJobStatus.getTaskName()));
        // process start time
        addField(resultRow, TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshStartTime()));
        // process finish time
        addField(resultRow, TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshEndTime()));
        // process duration
        addField(resultRow, formatDuration(refreshJobStatus.getTotalProcessDuration()));
        // last refresh state
        addField(resultRow, refreshJobStatus.getRefreshState());
        // whether it's force refresh
        addField(resultRow, refreshJobStatus.isForce);
        // partitionStart
        addField(resultRow, (Joiner.on(MULTI_TASK_RUN_SEPARATOR).join(refreshJobStatus.getRefreshedPartitionStarts())));
        // partitionEnd
        addField(resultRow, (Joiner.on(MULTI_TASK_RUN_SEPARATOR).join(refreshJobStatus.getRefreshedPartitionEnds())));
        // basePartitionsToRefreshMapString
        addField(resultRow, (Joiner.on(MULTI_TASK_RUN_SEPARATOR)
                .join(refreshJobStatus.getRefreshedBasePartitionsToRefreshMaps())));
        // mvPartitionsToRefreshString
        addField(resultRow, (Joiner.on(MULTI_TASK_RUN_SEPARATOR)
                .join(refreshJobStatus.getRefreshedMvPartitionsToRefreshs())));
        // error code
        addField(resultRow, refreshJobStatus.getErrorCode());
        // error message
        addField(resultRow, Strings.nullToEmpty(refreshJobStatus.getErrorMsg()));

        // rows
        addField(resultRow, rows);
        // text
        addField(resultRow, text);
        // extra message
        addField(resultRow, refreshJobStatus.getExtraMessage() == null ? "" :
                GsonUtils.GSON.toJson(refreshJobStatus.getExtraMessage()));

        return resultRow;
    }

    // Add a field to the result set
    private void addField(List<String> resultRow, Object field) {
        if (field == null) {
            resultRow.add("");
        } else {
            resultRow.add(String.valueOf(field));
        }
    }

    // Fill with empty fields
    private void addEmptyFields(List<String> resultRow, int count) {
        resultRow.addAll(Collections.nCopies(count, ""));
    }


    private String formatDuration(long duration) {
        return DebugUtil.DECIMAL_FORMAT_SCALE_3.format(duration / 1000D);
    }
}
