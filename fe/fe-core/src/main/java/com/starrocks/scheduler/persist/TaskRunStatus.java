// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaskRunStatus implements Writable {

    @SerializedName("queryId")
    private String queryId;

    @SerializedName("taskName")
    private String taskName;

    @SerializedName("createTime")
    private long createTime;

    @SerializedName("finishTime")
    private long finishTime;

    @SerializedName("state")
    private Constants.TaskRunState state = Constants.TaskRunState.PENDING;

    @SerializedName("progress")
    private int progress;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("definition")
    private String definition;

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

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
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

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
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
                ", createTime=" + createTime +
                ", finishTime=" + finishTime +
                ", state=" + state +
                ", progress=" + progress + "%" +
                ", dbName='" + getDbName() + '\'' +
                ", definition='" + definition + '\'' +
                ", user='" + user + '\'' +
                ", errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                ", expireTime=" + expireTime +
                ", priority=" + priority +
                ", mergeRedundant=" + mergeRedundant +
                '}';
    }
}
