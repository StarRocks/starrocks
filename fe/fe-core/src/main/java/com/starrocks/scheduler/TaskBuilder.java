// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.SubmitTaskStmt;

import java.util.Map;

// TaskBuilder is responsible for converting Stmt to Task Class
// and also responsible for generating taskId and taskName
public class TaskBuilder {

    public static Task buildTask(SubmitTaskStmt submitTaskStmt, ConnectContext context) {
        String taskName = submitTaskStmt.getTaskName();
        if (taskName == null) {
            taskName = "ctas-" + DebugUtil.printId(context.getExecutionId());
        }
        Task task = new Task(taskName);
        task.setSource(Constants.TaskSource.CTAS);
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName(submitTaskStmt.getDbName());
        task.setDefinition(submitTaskStmt.getSqlText());
        task.setProperties(submitTaskStmt.getProperties());
        task.setExpireTime(System.currentTimeMillis() + Config.task_ttl_second * 1000L);
        return task;
    }

    public static Task buildMvTask(MaterializedView materializedView, String dbName) {
        Task task = new Task(getMvTaskName(materializedView.getId()));
        task.setSource(Constants.TaskSource.MV);
        task.setDbName(dbName);
        Map<String, String> taskProperties = Maps.newHashMap();
        taskProperties.put(PartitionBasedMaterializedViewRefreshProcessor.MV_ID, String.valueOf(materializedView.getId()));
        taskProperties.put(SessionVariable.ENABLE_INSERT_STRICT, "false");
        task.setProperties(taskProperties);
        task.setDefinition(
                "insert overwrite " + materializedView.getName() + " " + materializedView.getViewDefineSql());
        task.setExpireTime(0L);
        return task;
    }

    public static String getMvTaskName(long mvId) {
        return "mv-" + mvId;
    }
}
