// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.optimizer.Utils;

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

    public static Task rebuildMvTask(MaterializedView materializedView, String dbName,
                                     Map<String, String> previousTaskProperties) {
        Task task = new Task(getMvTaskName(materializedView.getId()));
        task.setSource(Constants.TaskSource.MV);
        task.setDbName(dbName);
        previousTaskProperties.put(PartitionBasedMaterializedViewRefreshProcessor.MV_ID,
                String.valueOf(materializedView.getId()));
        task.setProperties(previousTaskProperties);
        task.setDefinition(
                "insert overwrite " + materializedView.getName() + " " + materializedView.getViewDefineSql());
        task.setExpireTime(0L);
        return task;
    }

    public static void updateTaskInfo(Task task, RefreshSchemeDesc refreshSchemeDesc)
            throws DdlException {
        MaterializedView.RefreshType refreshType = refreshSchemeDesc.getType();
        if (refreshType == MaterializedView.RefreshType.MANUAL) {
            task.setType(Constants.TaskType.MANUAL);
        } else if (refreshType == MaterializedView.RefreshType.ASYNC) {
            if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
                AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
                IntervalLiteral intervalLiteral = asyncRefreshSchemeDesc.getIntervalLiteral();
                if (intervalLiteral == null) {
                    task.setType(Constants.TaskType.EVENT_TRIGGERED);
                } else {
                    final IntLiteral step = (IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue();
                    long startTime = Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime());
                    TaskSchedule taskSchedule = new TaskSchedule(startTime, step.getLongValue(),
                            TimeUtils.convertUnitIdentifierToTimeUnit(intervalLiteral.getUnitIdentifier().getDescription()));
                    task.setSchedule(taskSchedule);
                    task.setType(Constants.TaskType.PERIODICAL);
                }
            }
        }
    }

    public static void updateTaskInfo(Task task, MaterializedView materializedView)
            throws DdlException {

        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                materializedView.getRefreshScheme().getAsyncRefreshContext();
        MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
        // mapping refresh type to task type
        if (refreshType == MaterializedView.RefreshType.MANUAL) {
            task.setType(Constants.TaskType.MANUAL);
        } else if (refreshType == MaterializedView.RefreshType.ASYNC) {
            if (asyncRefreshContext.getTimeUnit() == null) {
                task.setType(Constants.TaskType.EVENT_TRIGGERED);
            } else {
                long startTime = asyncRefreshContext.getStartTime();
                TaskSchedule taskSchedule = new TaskSchedule(startTime,
                        asyncRefreshContext.getStep(),
                        TimeUtils.convertUnitIdentifierToTimeUnit(asyncRefreshContext.getTimeUnit()));
                task.setSchedule(taskSchedule);
                task.setType(Constants.TaskType.PERIODICAL);
            }
        }
    }

    public static String getMvTaskName(long mvId) {
        return "mv-" + mvId;
    }
}
