// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.statistic.Constants;

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

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("definition")
    private String definition;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMessage")
    private String errorMessage;

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

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public static TaskRunStatus read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TaskRunStatus.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
