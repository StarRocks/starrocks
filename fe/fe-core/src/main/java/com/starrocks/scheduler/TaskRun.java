// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.scheduler;


import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.persist.TaskRunStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

public class TaskRun {

    private static final Logger LOG = LogManager.getLogger(TaskRun.class);

    private long taskId;

    private Map<String, String> properties;

    private Future<?> future;

    private Task task;

    private ConnectContext ctx;

    private TaskRunProcessor processor;

    private TaskRunStatus status;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
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
        taskRunContext.setDefinition(status.getDefinition());
        // copy a ConnectContext to avoid concurrency leading to abnormal results.
        ConnectContext newCtx = new ConnectContext();
        newCtx.setCluster(ctx.getClusterName());
        newCtx.setGlobalStateMgr(ctx.getGlobalStateMgr());
        newCtx.setDatabase(task.getDbName());
        newCtx.setQualifiedUser(ctx.getQualifiedUser());
        newCtx.setCurrentUserIdentity(ctx.getCurrentUserIdentity());
        newCtx.getState().reset();
        newCtx.setQueryId(UUID.fromString(status.getQueryId()));
        SessionVariable sessionVariable = (SessionVariable) ctx.getSessionVariable().clone();
        if (properties != null) {
            for (String key : properties.keySet()) {
                VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(properties.get(key))),
                        true);
            }
        }
        newCtx.setSessionVariable(sessionVariable);
        taskRunContext.setCtx(newCtx);
        taskRunContext.setRemoteIp(ctx.getMysqlChannel().getRemoteHostPortString());
        processor.processTaskRun(taskRunContext);
        QueryState queryState = newCtx.getState();
        if (newCtx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            status.setErrorMessage(queryState.getErrorMessage());
            int errorCode = -1;
            if (queryState.getErrorCode() != null) {
                errorCode = queryState.getErrorCode().getCode();
            }
            status.setErrorCode(errorCode);
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

    public TaskRunStatus getStatus() {
        return status;
    }

    public TaskRunStatus initStatus(String queryId, Long createTime) {
        TaskRunStatus status = new TaskRunStatus();
        status.setQueryId(queryId);
        status.setTaskName(task.getName());
        if (createTime == null) {
            status.setCreateTime(System.currentTimeMillis());
        } else {
            status.setCreateTime(createTime);
        }
        status.setDbName(task.getDbName());
        status.setDefinition(task.getDefinition());
        status.setExpireTime(System.currentTimeMillis() + Config.task_runs_ttl_second * 1000L);
        this.status = status;
        return status;
    }

}
