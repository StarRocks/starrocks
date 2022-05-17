// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

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

    @SerializedName("completeTime")
    private long completeTime;

    @SerializedName("state")
    private Constants.TaskRunState state = Constants.TaskRunState.PENDING;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("definition")
    private String definition;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMsg")
    private String errorMsg;

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

    public long getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(long completeTime) {
        this.completeTime = completeTime;
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

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
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
