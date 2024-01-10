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

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import org.apache.commons.collections.MapUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class TaskRunStatus implements Writable {

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

    // task run success/fail time which this task run is finished
    // NOTE: finishTime - createTime =
    //          pending time in task queue  + process task time + other time
    @SerializedName("finishTime")
    private long finishTime;

    // task run starts to process time
    // NOTE: finishTime - processStartTime = process task run time(exclude pending time)
    @SerializedName("processStartTime")
    private long processStartTime;

    @SerializedName("state")
    private Constants.TaskRunState state = Constants.TaskRunState.PENDING;

    @SerializedName("progress")
    private int progress;

    @SerializedName("dbName")
    private String dbName;

    @Deprecated
    @SerializedName("definition")
    private String definition;

    @SerializedName("postRun")
    private String postRun;

    @SerializedName("user")
    private String user;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMessage")
    private String errorMessage;

    @SerializedName("expireTime")
    private long expireTime;

    // the larger the value, the higher the priority, the default value is 0
    @SerializedName("priority")
    private int priority = Constants.TaskRunPriority.LOWEST.value();

    @SerializedName("mergeRedundant")
    private boolean mergeRedundant = false;

    @SerializedName("source")
    private Constants.TaskSource source = Constants.TaskSource.CTAS;

    @SerializedName("mvExtraMessage")
    private MVTaskRunExtraMessage mvTaskRunExtraMessage = new MVTaskRunExtraMessage();

    @SerializedName("properties")
    private Map<String, String> properties;

    public TaskRunStatus() {
    }

    public String getStartTaskRunId() {
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
        } else {
            // do nothing
        }
    }

    public long getProcessStartTime() {
        return processStartTime;
    }

    public void setProcessStartTime(long processStartTime) {
        this.processStartTime = processStartTime;
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
            return Constants.TaskRunState.SUCCESS;
        }

        if (!state.equals(Constants.TaskRunState.FAILED)) {
            return Constants.TaskRunState.RUNNING;
        } else {
            return state;
        }
    }

    public boolean isRefreshFinished() {
        if (state.equals(Constants.TaskRunState.FAILED)) {
            return true;
        }
        if (!state.equals(Constants.TaskRunState.SUCCESS)) {
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
            return finishTime - processStartTime;
        } else {
            return 0L;
        }
    }

    public long calculateRefreshDuration() {
        if (finishTime > createTime) {
            return finishTime - createTime;
        } else {
            return 0L;
        }
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
}
