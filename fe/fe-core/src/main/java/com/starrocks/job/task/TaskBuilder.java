// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.job.task;

import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.statistic.Constants;

import java.time.LocalDateTime;

// TaskBuilder is responsible for converting Stmt to Task Class
// and also responsible for generating taskId and taskName
public class TaskBuilder {

    public static Task buildTask(SubmitTaskStmt submitTaskStmt, ConnectContext context) {
        Task task = new Task();
        long taskId = GlobalStateMgr.getCurrentState().getNextId();
        task.setId(taskId);
        String taskName = submitTaskStmt.getTaskName();
        if (taskName == null) {
            taskName = "ctas-" + DebugUtil.printId(context.getExecutionId());
        }
        task.setName(taskName);
        task.setCreateTime(LocalDateTime.now());
        task.setDbName(submitTaskStmt.getDbName());
        task.setDefinition(submitTaskStmt.getSqlText());
        task.setTaskType(Constants.TaskType.MANUAL);
        task.setProcessor(new SqlTaskRunProcessor());
        task.initTaskBuilder();
        return task;
    }

}
