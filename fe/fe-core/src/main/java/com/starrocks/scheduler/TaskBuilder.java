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

import com.google.common.collect.Maps;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.optimizer.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

// TaskBuilder is responsible for converting Stmt to Task Class
// and also responsible for generating taskId and taskName
public class TaskBuilder {

    public static Task buildTask(SubmitTaskStmt submitTaskStmt, ConnectContext context) {
        String taskName = submitTaskStmt.getTaskName();
        String taskNamePrefix;
        Constants.TaskSource taskSource;
        if (submitTaskStmt.getInsertStmt() != null) {
            taskNamePrefix = "insert-";
            taskSource = Constants.TaskSource.INSERT;
        } else if (submitTaskStmt.getCreateTableAsSelectStmt() != null) {
            taskNamePrefix = "ctas-";
            taskSource = Constants.TaskSource.CTAS;
        } else {
            throw new SemanticException("Submit task statement is not supported");
        }
        if (taskName == null) {
            taskName = taskNamePrefix + DebugUtil.printId(context.getExecutionId());
        }
        Task task = new Task(taskName);
        task.setSource(taskSource);
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName(submitTaskStmt.getDbName());
        task.setDefinition(submitTaskStmt.getSqlText());
        task.setProperties(submitTaskStmt.getProperties());
        task.setExpireTime(System.currentTimeMillis() + Config.task_ttl_second * 1000L);
        return task;
    }

    public static String getAnalyzeMVStmt(String tableName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return "";
        }
        String analyze = ctx.getSessionVariable().getAnalyzeForMV();
        String stmt;
        String async = Config.mv_auto_analyze_async ? " WITH ASYNC MODE" : "";
        if ("sample".equalsIgnoreCase(analyze)) {
            stmt = "ANALYZE SAMPLE TABLE " + tableName + async;
        } else if ("full".equalsIgnoreCase(analyze)) {
            stmt = "ANALYZE TABLE " + tableName + async;
        } else {
            stmt = "";
        }
        return stmt;
    }

    public static Task buildMvTask(MaterializedView materializedView, String dbName) {
        Task task = new Task(getMvTaskName(materializedView.getId()));
        task.setSource(Constants.TaskSource.MV);
        task.setDbName(dbName);
        Map<String, String> taskProperties = Maps.newHashMap();
        taskProperties.put(PartitionBasedMvRefreshProcessor.MV_ID,
                String.valueOf(materializedView.getId()));
        taskProperties.putAll(materializedView.getProperties());

        task.setProperties(taskProperties);
        task.setDefinition(
                "insert overwrite " + materializedView.getName() + " " + materializedView.getViewDefineSql());
        task.setPostRun(getAnalyzeMVStmt(materializedView.getName()));
        task.setExpireTime(0L);
        return task;
    }

    public static Task rebuildMvTask(MaterializedView materializedView, String dbName,
                                     Map<String, String> previousTaskProperties) {
        Task task = new Task(getMvTaskName(materializedView.getId()));
        task.setSource(Constants.TaskSource.MV);
        task.setDbName(dbName);
        previousTaskProperties.put(PartitionBasedMvRefreshProcessor.MV_ID,
                String.valueOf(materializedView.getId()));
        task.setProperties(previousTaskProperties);
        task.setDefinition(
                "insert overwrite " + materializedView.getName() + " " + materializedView.getViewDefineSql());
        task.setPostRun(getAnalyzeMVStmt(materializedView.getName()));
        task.setExpireTime(0L);
        return task;
    }

    public static void updateTaskInfo(Task task, RefreshSchemeDesc refreshSchemeDesc, MaterializedView materializedView)
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
                    long period = ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getLongValue();
                    TimeUnit timeUnit = TimeUtils.convertUnitIdentifierToTimeUnit(
                            intervalLiteral.getUnitIdentifier().getDescription());
                    long startTime;
                    if (asyncRefreshSchemeDesc.isDefineStartTime()) {
                        startTime = Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime());
                    } else {
                        MaterializedView.AsyncRefreshContext asyncRefreshContext = materializedView.getRefreshScheme()
                                .getAsyncRefreshContext();
                        long currentTimeSecond = System.currentTimeMillis() / 1000;
                        startTime = TimeUtils.getNextValidTimeSecond(asyncRefreshContext.getStartTime(),
                                currentTimeSecond, period, timeUnit);
                    }
                    TaskSchedule taskSchedule = new TaskSchedule(startTime, period, timeUnit);
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
