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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TaskRunStatus implements Writable {
    private static final Logger LOG = LogManager.getLogger(TaskRun.class);

    // A refresh may contain a batch of task runs, startTaskRunId is to mark the unique id of the batch task run status.
    // You can use the startTaskRunId to find the batch of task runs.
    @SerializedName("startTaskRunId")
    private String startTaskRunId;

    @SerializedName("queryId")
    private String queryId;

    @SerializedName("taskId")
    private long taskId;

    @SerializedName("taskName")
    private String taskName;

    // task run submit/created time
    @SerializedName("createTime")
    private long createTime;

    @SerializedName("catalogName")
    private String catalogName;

    @SerializedName("dbName")
    private String dbName;

    @Deprecated
    @SerializedName("definition")
    private String definition;

    @SerializedName("postRun")
    private String postRun;

    @SerializedName("user")
    @Deprecated
    private String user;

    @SerializedName("userIdentity")
    private UserIdentity userIdentity;

    @SerializedName("expireTime")
    private long expireTime;

    // the larger the value, the higher the priority, the default value is 0
    @SerializedName("priority")
    private int priority = Constants.TaskRunPriority.LOWEST.value();

    @SerializedName("mergeRedundant")
    private boolean mergeRedundant = false;

    @SerializedName("source")
    private Constants.TaskSource source = Constants.TaskSource.CTAS;

    //////////// Variables should be volatile which can be visited by multi threads ///////////

    @SerializedName("errorCode")
    private volatile int errorCode;

    @SerializedName("errorMessage")
    private volatile String errorMessage;

    // task run success/fail time which this task run is finished
    // NOTE: finishTime - createTime =
    //          pending time in task queue  + process task time + other time
    @SerializedName("finishTime")
    private volatile long finishTime;

    // task run starts to process time
    // NOTE: finishTime - processStartTime = process task run time(exclude pending time)
    @SerializedName("processStartTime")
    private volatile long processStartTime = 0;

    @SerializedName("state")
    private volatile Constants.TaskRunState state = Constants.TaskRunState.PENDING;

    @SerializedName("progress")
    private volatile int progress;

    @SerializedName("mvExtraMessage")
    private volatile MVTaskRunExtraMessage mvTaskRunExtraMessage = new MVTaskRunExtraMessage();

    @SerializedName("dataCacheSelectExtraMessage")
    private volatile String dataCacheSelectExtraMessage;

    @SerializedName("properties")
    private volatile Map<String, String> properties;

    public TaskRunStatus() {
    }

    public String getStartTaskRunId() {
        // NOTE: startTaskRunId may not be set since it's initialized in TaskRun#executeTaskRun
        // But properties must contain START_TASK_RUN_ID first.
        if (properties != null && properties.containsKey(TaskRun.START_TASK_RUN_ID)) {
            return properties.get(TaskRun.START_TASK_RUN_ID);
        }
        return startTaskRunId;
    }

    public void setStartTaskRunId(String startTaskRunId) {
        this.startTaskRunId = startTaskRunId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
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

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public Constants.TaskRunState getState() {
        return state;
    }

    public void setState(Constants.TaskRunState state) {
        this.state = state;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getDbName() {
        return ClusterNamespace.getNameFromFullName(dbName);
    }

    public void setDbName(String dbName) {
        // // compatible with old version
        this.dbName = ClusterNamespace.getFullName(dbName);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public String getPostRun() {
        return postRun;
    }

    public void setPostRun(String postRun) {
        this.postRun = postRun;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public boolean isMergeRedundant() {
        return mergeRedundant;
    }

    public void setMergeRedundant(boolean mergeRedundant) {
        this.mergeRedundant = mergeRedundant;
    }

    public Constants.TaskSource getSource() {
        return source;
    }

    public void setSource(Constants.TaskSource source) {
        this.source = source;
    }

    public MVTaskRunExtraMessage getMvTaskRunExtraMessage() {
        return mvTaskRunExtraMessage;
    }

    public void setMvTaskRunExtraMessage(MVTaskRunExtraMessage mvTaskRunExtraMessage) {
        this.mvTaskRunExtraMessage = mvTaskRunExtraMessage;
    }

    public String getExtraMessage() {
        if (source == Constants.TaskSource.MV) {
            return GsonUtils.GSON.toJson(mvTaskRunExtraMessage);
        } else if (source == Constants.TaskSource.DATACACHE_SELECT) {
            return dataCacheSelectExtraMessage;
        } else {
            return "";
        }
    }
    public void setExtraMessage(String extraMessage) {
        if (extraMessage == null) {
            return;
        }

        if (source == Constants.TaskSource.MV) {
            this.mvTaskRunExtraMessage =
                    GsonUtils.GSON.fromJson(extraMessage, MVTaskRunExtraMessage.class);
        } else if (source == Constants.TaskSource.DATACACHE_SELECT) {
            this.dataCacheSelectExtraMessage = extraMessage;
        } else {
            // do nothing
        }
    }

    public long getProcessStartTime() {
        return processStartTime;
    }

    public void setProcessStartTime(long processStartTime) {
        this.processStartTime = processStartTime;
        // update process start time in mvTaskRunExtraMessage to display in the web page
        if (mvTaskRunExtraMessage != null) {
            mvTaskRunExtraMessage.setProcessStartTime(processStartTime);
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getPropertiesJson() {
        if (MapUtils.isEmpty(properties)) {
            return null;
        }
        return GsonUtils.GSON.toJson(properties);
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Constants.TaskRunState getLastRefreshState() {
        if (isRefreshFinished()) {
            Preconditions.checkArgument(state.isFinishState(), String.format("state %s must be finish state", state));
            return state;
        } else {
            // {@code processStartTime == 0} means taskRun have not been scheduled, its state should be pending.
            // TODO: how to distinguish TaskRunStatus per partition.
            return processStartTime == 0 ? state : Constants.TaskRunState.RUNNING;
        }
    }

    public boolean isRefreshFinished() {
        if (state.equals(Constants.TaskRunState.FAILED)) {
            return true;
        }
        if (!state.isFinishState()) {
            return false;
        }
        if (!Strings.isNullOrEmpty(mvTaskRunExtraMessage.getNextPartitionEnd()) ||
                !Strings.isNullOrEmpty(mvTaskRunExtraMessage.getNextPartitionStart())) {
            return false;
        }
        return true;
    }

    public long calculateRefreshProcessDuration() {
        if (finishTime > processStartTime) {
            // NOTE:
            // It's mostly because of tech debt, before this pr, the processStartTime can be persisted as 0 .
            // In this case to avoid return a weird duration we choose the createTime as startTime
            if (processStartTime > 0) {
                return finishTime - processStartTime;
            } else {
                return finishTime - createTime;
            }
        } else {
            return 0L;
        }
    }

    public boolean matchByTaskName(String dbName, Set<String> taskNames) {
        if (dbName != null && !dbName.equals(getDbName())) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(taskNames) && !taskNames.contains(getTaskName())) {
            return false;
        }
        return true;
    }

    public boolean match(TGetTasksParams params) {
        if (params == null) {
            return true;
        }
        String dbName = params.db;
        if (dbName != null && !dbName.equals(getDbName())) {
            return false;
        }
        String taskName = params.task_name;
        if (taskName != null && !taskName.equalsIgnoreCase(getTaskName())) {
            return false;
        }
        String queryId = params.query_id;
        if (queryId != null && !queryId.equalsIgnoreCase(getQueryId())) {
            return false;
        }
        String state = params.state;
        if (state != null && !state.equalsIgnoreCase(getState().name())) {
            return false;
        }
        return true;
    }


    public static TaskRunStatus read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TaskRunStatus.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        return "TaskRunStatus{" +
                "queryId='" + queryId + '\'' +
                ", taskName='" + taskName + '\'' +
                ", startTaskRunId='" + startTaskRunId + '\'' +
                ", createTime=" + createTime +
                ", finishTime=" + finishTime +
                ", processStartTime=" + processStartTime +
                ", state=" + state +
                ", progress=" + progress + "%" +
                ", dbName='" + getDbName() + '\'' +
                ", definition='" + definition + '\'' +
                ", postRun='" + postRun + '\'' +
                ", user='" + user + '\'' +
                ", errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                ", expireTime=" + expireTime +
                ", priority=" + priority +
                ", mergeRedundant=" + mergeRedundant +
                ", extraMessage=" + getExtraMessage() +
                '}';
    }

    public String toJSON() {
        return GsonUtils.GSON.toJson(this);
    }

    public static TaskRunStatus fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, TaskRunStatus.class);
    }

    /**
     * Only used for deserialization of ResultBatch
     */
    public static class TaskRunStatusJSONRecord {
        /**
         * Only one item in the array, like:
         * { data: [ {TaskRunStatus} ] }
         */
        @SerializedName("data")
        public List<TaskRunStatus> data;

        public static TaskRunStatusJSONRecord fromJson(String json) {
            return GsonUtils.GSON.fromJson(json, TaskRunStatusJSONRecord.class);
        }
    }

    public static List<TaskRunStatus> fromResultBatch(List<TResultBatch> batches) {
        List<TaskRunStatus> res = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                String jsonString = "";
                try {
                    ByteBuf copied = Unpooled.copiedBuffer(buffer);
                    jsonString = copied.toString(Charset.defaultCharset());
                    res.addAll(ListUtils.emptyIfNull(TaskRunStatusJSONRecord.fromJson(jsonString).data));
                } catch (Exception e) {
                    // If the task run history is corrupted, we can use `ignore_task_run_history_replay_error` config to ignore
                    // it and continue to process the next one.
                    if (!Config.ignore_task_run_history_replay_error) {
                        LOG.warn("Failed to deserialize TaskRunStatus from json， please delete it from " +
                                "_statistics_.task_run_history table: {}", jsonString, e);
                        throw new RuntimeException("Failed to deserialize TaskRunStatus from json， please delete it from " +
                                "_statistics_.task_run_history table: " + jsonString, e);
                    }
                }
            }
        }
        return res;
    }
}
