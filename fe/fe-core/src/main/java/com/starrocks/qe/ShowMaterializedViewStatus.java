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

import com.google.api.client.util.Sets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TMaterializedViewStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOG = LogManager.getLogger(ShowMaterializedViewStatus.class);

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
    private String queryRewriteStatus;
    private long taskId;
    private String taskName;
    private List<TaskRunStatus> lastJobTaskRunStatus;

    /**
     * RefreshJobStatus represents a batch of batch TaskRunStatus because a fresh may trigger more than one task runs.
     */
    public class RefreshJobStatus {
        private String taskOwner;
        private String jobId;
        private Constants.TaskRunState refreshState;
        private long mvRefreshStartTime;
        private long mvRefreshProcessTime;
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

        public long getMvRefreshProcessTime() {
            return mvRefreshProcessTime;
        }

        public void setMvRefreshProcessTime(long mvRefreshProcessTime) {
            this.mvRefreshProcessTime = mvRefreshProcessTime;
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

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
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

        public ExtraMessage getExtraMessage() {
            return extraMessage;
        }

        public void setExtraMessage(ExtraMessage extraMessage) {
            this.extraMessage = extraMessage;
        }

        public String getTaskOwner() {
            return taskOwner;
        }

        public void setTaskOwner(String taskOwner) {
            this.taskOwner = taskOwner;
        }

        @Override
        public String toString() {
            return "RefreshJobStatus{" +
                    "taskId=" + taskId +
                    ", taskName='" + taskName + '\'' +
                    ", taskOwner='" + taskOwner + '\'' +
                    ", refreshState=" + refreshState +
                    ", mvRefreshStartTime=" + mvRefreshStartTime +
                    ", mvRefreshEndTime=" + mvRefreshEndTime +
                    ", totalProcessDuration=" + totalProcessDuration +
                    ", isForce=" + isForce +
                    ", refreshedPartitionStarts=" + refreshedPartitionStarts +
                    ", refreshedPartitionEnds=" + refreshedPartitionEnds +
                    ", refreshedBasePartitionsToRefreshMaps=" + refreshedBasePartitionsToRefreshMaps +
                    ", refreshedMvPartitionsToRefreshs=" + refreshedMvPartitionsToRefreshs +
                    ", errorCode='" + errorCode + '\'' +
                    ", errorMsg='" + errorMsg + '\'' +
                    ", isRefreshFinished=" + isRefreshFinished +
                    ", extraMessage=" + extraMessage +
                    '}';
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
        @SerializedName("lastTaskRunState")
        private Constants.TaskRunState lastTaskRunState = Constants.TaskRunState.PENDING;

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

        public Constants.TaskRunState getLastTaskRunState() {
            return lastTaskRunState;
        }

        public void setLastTaskRunState(Constants.TaskRunState lastTaskRunState) {
            this.lastTaskRunState = lastTaskRunState;
        }
    }

    @Deprecated
    public ShowMaterializedViewStatus(long id, String dbName, String name) {
        this.id = id;
        this.dbName = dbName;
        this.name = name;
    }

    public static ShowMaterializedViewStatus of(String dbName, MaterializedView mv, List<TaskRunStatus> taskTaskStatusJob) {
        ShowMaterializedViewStatus status = new ShowMaterializedViewStatus(mv.getId(), dbName, mv.getName());
        // refresh_type
        final MaterializedView.MvRefreshScheme refreshScheme = mv.getRefreshScheme();
        if (refreshScheme == null) {
            status.setRefreshType("UNKNOWN");
        } else {
            status.setRefreshType(String.valueOf(mv.getRefreshScheme().getType()));
        }
        // is_active
        status.setActive(mv.isActive());
        status.setInactiveReason(Optional.ofNullable(mv.getInactiveReason()).map(String::valueOf).orElse(null));
        // partition info
        if (mv.getPartitionInfo() != null && mv.getPartitionInfo().getType() != null) {
            status.setPartitionType(mv.getPartitionInfo().getType().toString());
        }
        // row count
        status.setRows(mv.getRowCount());
        // materialized view ddl
        status.setText(mv.getMaterializedViewDdlStmt(true));
        // rewrite status
        status.setQueryRewriteStatus(mv.getQueryRewriteStatus());
        // task_name
        final TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        if (task != null) {
            status.setTaskId(task.getId());
            status.setTaskName(task.getName());
        }
        status.setLastJobTaskRunStatus(taskTaskStatusJob);
        return status;
    }

    public static ShowMaterializedViewStatus of(String dbName, OlapTable olapTable, MaterializedIndexMeta indexMeta) {
        ShowMaterializedViewStatus status = new ShowMaterializedViewStatus(indexMeta.getIndexId(), dbName,
                olapTable.getIndexNameById(indexMeta.getIndexId()));
        // refresh_type
        status.setRefreshType("ROLLUP");
        // is_active
        status.setActive(true);
        // partition type
        if (olapTable.getPartitionInfo() != null && olapTable.getPartitionInfo().getType() != null) {
            status.setPartitionType(olapTable.getPartitionInfo().getType().toString());
        }
        // text
        if (indexMeta.getOriginStmt() == null) {
            final String mvName = olapTable.getIndexNameById(indexMeta.getIndexId());
            status.setText(buildCreateMVSql(olapTable, mvName, indexMeta));
        } else {
            status.setText(indexMeta.getOriginStmt().replace("\n", "").replace("\t", "")
                    .replaceAll("[ ]+", " "));
        }
        // rows
        if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
            final Partition partition = olapTable.getPartitions().iterator().next();
            final MaterializedIndex index = partition.getDefaultPhysicalPartition().getIndex(indexMeta.getIndexId());
            status.setRows(index.getRowCount());
        } else {
            status.setRows(0L);
        }
        return status;
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

        // Task creator
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(firstTaskRunStatus.getTaskName());
        if (task != null) {
            if (task.getUserIdentity() != null) {
                status.setTaskOwner(task.getUserIdentity().toString());
            } else {
                status.setTaskOwner(task.getCreateUser());
            }
        }

        // extra message
        ExtraMessage extraMessage = new ExtraMessage();
        List<String> queryIds = applyTaskRunStatusWith(x -> x.getQueryId());
        // queryIds
        extraMessage.setQueryIds(queryIds);
        extraMessage.setLastTaskRunState(lastTaskRunStatus.getState());
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
        long mvRefreshCreateTime = firstTaskRunStatus.getCreateTime();
        status.setMvRefreshStartTime(mvRefreshCreateTime);

        // process time
        long mvRefreshStartTime = firstTaskRunStatus.getProcessStartTime();
        status.setMvRefreshProcessTime(mvRefreshStartTime);

        // last refresh job id
        status.setJobId(lastTaskRunStatus.getStartTaskRunId());

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
                    .map(TaskRunStatus::calculateRefreshProcessDuration)
                    .collect(Collectors.summingLong(Long::longValue));
            status.setTotalProcessDuration(totalProcessDuration);
            status.setErrorCode(String.valueOf(lastTaskRunStatus.getErrorCode()));
            status.setErrorMsg(Strings.nullToEmpty(lastTaskRunStatus.getErrorMessage()));
        }
        return status;
    }

    public String getQueryRewriteStatus() {
        return queryRewriteStatus;
    }

    public void setQueryRewriteStatus(String queryRewriteStatus) {
        this.queryRewriteStatus = queryRewriteStatus;
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
        status.setTask_id(String.valueOf(this.taskId));
        status.setTask_name(this.taskName);
        // start time
        status.setLast_refresh_start_time(TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshStartTime()));
        // process time
        status.setLast_refresh_process_time(TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshProcessTime()));
        // last_refresh_job_id
        status.setLast_refresh_job_id(refreshJobStatus.getJobId());
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

        // query_rewrite_status
        status.setQuery_rewrite_status(queryRewriteStatus);
        // creator
        status.setCreator(refreshJobStatus.getTaskOwner());

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
        addField(resultRow, this.getTaskId());
        // task name
        addField(resultRow, Strings.nullToEmpty(this.getTaskName()));
        // start time
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
        // query_rewrite_status
        addField(resultRow, queryRewriteStatus);
        // owner
        addField(resultRow, refreshJobStatus.getTaskOwner());
        // process start time
        addField(resultRow, TimeUtils.longToTimeString(refreshJobStatus.getMvRefreshProcessTime()));
        // last refresh job id
        addField(resultRow, refreshJobStatus.getJobId());

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

    @Override
    public String toString() {
        return "ShowMaterializedViewStatus{" +
                "id=" + id +
                ", dbName='" + dbName + '\'' +
                ", name='" + name + '\'' +
                ", refreshType='" + refreshType + '\'' +
                ", isActive=" + isActive +
                ", text='" + text + '\'' +
                ", rows=" + rows +
                ", partitionType='" + partitionType + '\'' +
                ", lastCheckTime=" + lastCheckTime +
                ", inactiveReason='" + inactiveReason + '\'' +
                ", queryRewriteStatus='" + queryRewriteStatus + '\'' +
                ", lastJobTaskRunStatus=" + lastJobTaskRunStatus +
                '}';
    }

    public static String buildCreateMVSql(OlapTable olapTable, String mv, MaterializedIndexMeta mvMeta) {
        StringBuilder originStmtBuilder = new StringBuilder(
                "create materialized view " + mv +
                        " as select ");
        String groupByString = "";
        for (Column column : mvMeta.getSchema()) {
            if (column.isKey()) {
                groupByString += column.getName() + ",";
            }
        }
        originStmtBuilder.append(groupByString);
        for (Column column : mvMeta.getSchema()) {
            if (!column.isKey()) {
                originStmtBuilder.append(column.getAggregationType().toString()).append("(")
                        .append(column.getName()).append(")").append(",");
            }
        }
        originStmtBuilder.delete(originStmtBuilder.length() - 1, originStmtBuilder.length());
        originStmtBuilder.append(" from ").append(olapTable.getName()).append(" group by ")
                .append(groupByString);
        originStmtBuilder.delete(originStmtBuilder.length() - 1, originStmtBuilder.length());
        return originStmtBuilder.toString();
    }

    private static ShowMaterializedViewStatus getASyncMVStatus(String dbName,
                                                               MaterializedView mvTable,
                                                               List<TaskRunStatus> taskTaskStatusJob) {
        try {
            return ShowMaterializedViewStatus.of(dbName, mvTable, taskTaskStatusJob);
        } catch (Exception e) {
            long mvId = mvTable.getId();
            LOG.warn("get async mv status failed, mvId: {}, dbName: {}, mvName: {}, error: {}",
                    mvId, dbName, mvTable.getName(), e.getMessage());
            return new ShowMaterializedViewStatus(mvId, dbName, mvTable.getName());
        }
    }

    private static ShowMaterializedViewStatus getSyncMVStatus(String dbName,
                                                              OlapTable olapTable,
                                                              MaterializedIndexMeta mvMeta) {
        try {
            return ShowMaterializedViewStatus.of(dbName, olapTable, mvMeta);
        } catch (Exception e) {
            final long mvId = mvMeta.getIndexId();
            LOG.warn("get sync mv status failed, mvId: {}, dbName: {}, mvName: {}, error: {}",
                    mvId, dbName, olapTable.getIndexNameById(mvId), e.getMessage());
            return new ShowMaterializedViewStatus(mvId, dbName, olapTable.getIndexNameById(mvId));
        }
    }

    public static List<ShowMaterializedViewStatus> listMaterializedViewStatus(
            String dbName,
            List<MaterializedView> mvs,
            List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs) {
        final List<ShowMaterializedViewStatus> rowSets = Lists.newArrayList();

        // Now there are two MV cases:
        //  1. Table's type is MATERIALIZED_VIEW, this is the new MV type which the MV table is separated from
        //     the base table and supports multi table in MV definition.
        //  2. Table's type is OLAP, this is the old MV type which the MV table is associated with the base
        //     table and only supports single table in MV definition.
        final Map<String, List<TaskRunStatus>> taskNameToStatusMap = Maps.newHashMap();
        if (!mvs.isEmpty()) {
            try {
                taskNameToStatusMap.putAll(GlobalStateMgr.getCurrentState().getTaskManager()
                        .listMVRefreshedTaskRunStatus(dbName,
                                mvs.stream()
                                        .map(mv -> TaskBuilder.getMvTaskName(mv.getId()))
                                        .collect(Collectors.toSet())
                        ));
            } catch (Exception e) {
                LOG.warn("Failed to list MV refreshed task run status, fallback to unknown status. db: {}",
                        dbName, e);
            }
        }
        // async materialized views
        mvs.forEach(mvTable ->
                rowSets.add(getASyncMVStatus(dbName, mvTable, taskNameToStatusMap.getOrDefault(
                        TaskBuilder.getMvTaskName(mvTable.getId()), Lists.newArrayList())))
        );
        // sync materialized views
        singleTableMVs.forEach(singleTableMV ->
                rowSets.add(getSyncMVStatus(dbName, singleTableMV.first, singleTableMV.second))
        );
        return rowSets;
    }
}
