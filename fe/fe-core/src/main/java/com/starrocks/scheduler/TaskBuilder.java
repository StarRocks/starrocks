// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.scheduler;

import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SubmitTaskStmt;

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
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName(submitTaskStmt.getDbName());
        task.setDefinition(submitTaskStmt.getSqlText());
        task.setProperties(submitTaskStmt.getProperties());
        task.setExpireTime(System.currentTimeMillis() + Config.task_ttl_second * 1000L);
        return task;
    }

}
