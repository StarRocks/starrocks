// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.job.task;


import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.statistic.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

public class TaskRun implements Writable {

    private static final Logger LOG = LogManager.getLogger(TaskRun.class);

    @SerializedName("queryId")
    private String queryId = null;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("taskId")
    private long taskId;

    @SerializedName("definition")
    private String definition;

    @SerializedName("status")
    private Constants.TaskRunStatus status = Constants.TaskRunStatus.PENDING;

    @SerializedName("createTime")
    private LocalDateTime createTime;

    @SerializedName("startTime")
    private LocalDateTime startTime;

    @SerializedName("completeTime")
    private LocalDateTime completeTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMsg")
    private String errorMsg;

    private Future<?> future;

    private Task task;

    private ConnectContext ctx;

    private TaskRunProcessor processor;

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public Constants.TaskRunStatus getStatus() {
        return status;
    }

    public void setStatus(Constants.TaskRunStatus status) {
        this.status = status;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(LocalDateTime completeTime) {
        this.completeTime = completeTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Future<?> getFuture() {
        return future;
    }

    public void setFuture(Future<?> future) {
        this.future = future;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public TaskRunProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(TaskRunProcessor processor) {
        this.processor = processor;
    }

    public boolean executeTaskRun() throws Exception {
        TaskRunContext taskRunContext = new TaskRunContext();
        taskRunContext.setDefinition(definition);
        // copy a ConnectContext to avoid concurrency leading to abnormal results.
        ConnectContext newCtx = new ConnectContext();
        newCtx.setCluster(ctx.getClusterName());
        newCtx.setCatalog(ctx.getGlobalStateMgr());
        newCtx.setDatabase(ctx.getDatabase());
        newCtx.setQualifiedUser(ctx.getQualifiedUser());
        newCtx.setCurrentUserIdentity(ctx.getCurrentUserIdentity());
        newCtx.getState().reset();
        newCtx.setQueryId(UUID.fromString(queryId));
        SessionVariable sessionVariable = (SessionVariable) ctx.getSessionVariable().clone();
        if (properties != null) {
            for (String key : properties.keySet()) {
                VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(properties.get(key))),
                        true);
            }
        }
        newCtx.setSessionVariable(sessionVariable);
        taskRunContext.setCtx(newCtx);
        processor.processTaskRun(taskRunContext);
        QueryState queryState = newCtx.getState();
        if (newCtx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            errorMsg = queryState.getErrorMessage();
            if (queryState.getErrorCode() != null) {
                errorCode = queryState.getErrorCode().getCode();
            }
            return false;
        }
        return true;
    }

    public ConnectContext getCtx() {
        return ctx;
    }

    public void setCtx(ConnectContext ctx) {
        this.ctx = ctx;
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

    @Override
    public String toString() {
        return "TaskRun{" +
                "queryId='" + queryId + '\'' +
                ", dbName='" + dbName + '\'' +
                ", taskId=" + taskId +
                ", status=" + status +
                ", createTime=" + createTime +
                ", startTime=" + startTime +
                ", endTime=" + completeTime +
                ", properties=" + properties +
                '}';
    }

    public static TaskRun read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TaskRun.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

}
