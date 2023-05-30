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

package com.starrocks.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TaskRun implements Comparable<TaskRun> {

    private static final Logger LOG = LogManager.getLogger(TaskRun.class);

    public static final String PARTITION_START = "PARTITION_START";
    public static final String PARTITION_END = "PARTITION_END";
    public static final String FORCE = "FORCE";

    private long taskId;

    private Map<String, String> properties;

    private final CompletableFuture<Constants.TaskRunState> future;

    private Task task;

    private ConnectContext runCtx;

    private ConnectContext parentRunCtx;

    private TaskRunProcessor processor;

    private TaskRunStatus status;

    private Constants.TaskType type;

    TaskRun() {
        future = new CompletableFuture<>();
    }

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

    public CompletableFuture<Constants.TaskRunState> getFuture() {
        return future;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public void setConnectContext(ConnectContext context) {
        this.parentRunCtx = context;
    }

    public TaskRunProcessor getProcessor() {
        return processor;
    }
    public void setProcessor(TaskRunProcessor processor) {
        this.processor = processor;
    }

    public void setType(Constants.TaskType type) {
        this.type = type;
    }

    public Constants.TaskType getType() {
        return this.type;
    }

    public Map<String, String>  refreshTaskProperties(ConnectContext ctx) {
        Map<String, String> newProperties = Maps.newHashMap();
        if (task.getSource() != Constants.TaskSource.MV) {
            return newProperties;
        }

        try {
            // NOTE: mvId is set in Task's properties when creating
            long mvId = Long.parseLong(properties.get(PartitionBasedMvRefreshProcessor.MV_ID));
            Database database = GlobalStateMgr.getCurrentState().getDb(ctx.getDatabase());
            if (database == null) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}", ctx.getDatabase(), mvId);
                return newProperties;
            }

            Table table = database.getTable(mvId);
            if (table == null) {
                LOG.warn("materialized view:{} in database:{} do not exist when refreshing", mvId,
                        ctx.getDatabase());
                return newProperties;
            }
            MaterializedView materializedView = (MaterializedView) table;
            Preconditions.checkState(materializedView != null);
            newProperties = materializedView.getProperties();
        } catch (Exception e) {
            LOG.warn("refresh task properties failed:", e);
        }
        return newProperties;
    }

    public boolean executeTaskRun() throws Exception {
        TaskRunContext taskRunContext = new TaskRunContext();
        Preconditions.checkNotNull(status.getDefinition(), "The definition of task run should not null");
        taskRunContext.setDefinition(status.getDefinition());
        taskRunContext.setPostRun(status.getPostRun());
        runCtx = new ConnectContext(null);
        if (parentRunCtx != null) {
            runCtx.setParentConnectContext(parentRunCtx);
        }
        runCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        runCtx.setDatabase(task.getDbName());
        runCtx.setQualifiedUser(status.getUser());
        runCtx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(status.getUser(), "%"));
        runCtx.setCurrentRoleIds(runCtx.getCurrentUserIdentity());
        runCtx.getState().reset();
        runCtx.setQueryId(UUID.fromString(status.getQueryId()));

        Map<String, String> newProperties = refreshTaskProperties(runCtx);
        properties.putAll(newProperties);

        Map<String, String> taskRunContextProperties = Maps.newHashMap();
        runCtx.resetSessionVariable();
        if (properties != null) {
            for (String key : properties.keySet()) {
                try {
                    runCtx.modifySystemVariable(new SystemVariable(key, new StringLiteral(properties.get(key))), true);
                } catch (DdlException e) {
                    // not session variable
                    taskRunContextProperties.put(key, properties.get(key));
                }
            }
        }
        taskRunContext.setCtx(runCtx);
        taskRunContext.setRemoteIp(runCtx.getMysqlChannel().getRemoteHostPortString());
        taskRunContext.setProperties(taskRunContextProperties);
        taskRunContext.setPriority(status.getPriority());
        taskRunContext.setTaskType(type);
        taskRunContext.setStatus(status);

        processor.processTaskRun(taskRunContext);
        QueryState queryState = runCtx.getState();
        if (runCtx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            status.setErrorMessage(queryState.getErrorMessage());
            int errorCode = -1;
            if (queryState.getErrorCode() != null) {
                errorCode = queryState.getErrorCode().getCode();
            }
            status.setErrorCode(errorCode);
            return false;
        }

        // Execute post task action, but ignore any exception
        if (StringUtils.isNotEmpty(taskRunContext.getPostRun())) {
            try {
                processor.postTaskRun(taskRunContext);
            } catch (Exception ignored) {
                LOG.warn("Execute post taskRun failed {} ", status, ignored);
            }
        }
        return true;
    }

    public ConnectContext getRunCtx() {
        return runCtx;
    }

    public TaskRunStatus getStatus() {
        if (status == null) {
            return null;
        }
        switch (status.getState()) {
            case RUNNING:
                if (runCtx != null) {
                    StmtExecutor executor = runCtx.getExecutor();
                    if (executor != null && executor.getCoordinator() != null) {
                        long jobId = executor.getCoordinator().getJobId();
                        if (jobId != -1) {
                            InsertLoadJob job = (InsertLoadJob) GlobalStateMgr.getCurrentState()
                                    .getLoadMgr().getLoadJob(jobId);
                            int progress = job.getProgress();
                            if (progress == 100) {
                                progress = 99;
                            }
                            status.setProgress(progress);
                        }
                    }
                }
                break;
            case SUCCESS:
                status.setProgress(100);
                break;
        }
        return status;
    }

    public TaskRunStatus initStatus(String queryId, Long createTime) {
        TaskRunStatus status = new TaskRunStatus();
        status.setQueryId(queryId);
        status.setTaskId(task.getId());
        status.setTaskName(task.getName());
        status.setSource(task.getSource());
        if (createTime == null) {
            status.setCreateTime(System.currentTimeMillis());
        } else {
            status.setCreateTime(createTime);
        }
        status.setUser(task.getCreateUser());
        status.setDbName(task.getDbName());
        status.setDefinition(task.getDefinition());
        status.setPostRun(task.getPostRun());
        status.setExpireTime(System.currentTimeMillis() + Config.task_runs_ttl_second * 1000L);
        this.status = status;
        return status;
    }

    @Override
    public int compareTo(@NotNull TaskRun taskRun) {
        if (this.getStatus().getPriority() != taskRun.getStatus().getPriority()) {
            return taskRun.getStatus().getPriority() - this.getStatus().getPriority();
        } else {
            return this.getStatus().getCreateTime() > taskRun.getStatus().getCreateTime() ? 1 : -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskRun taskRun = (TaskRun) o;
        return status.getDefinition().equals(taskRun.getStatus().getDefinition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }

    @Override
    public String toString() {
        return "TaskRun{" +
                "taskId=" + taskId +
                ", properties=" + properties +
                ", future=" + future +
                ", task=" + task +
                ", runCtx=" + runCtx +
                ", processor=" + processor +
                ", status=" + status +
                ", type=" + type +
                '}';
    }
}
