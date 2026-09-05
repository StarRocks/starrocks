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
import com.starrocks.alter.OptimizeTask;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.pipe.PipeTaskDesc;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.starrocks.scheduler.TaskRun.MV_ID;

// TaskBuilder is responsible for converting Stmt to Task Class
// and also responsible for generating taskId and taskName
public class TaskBuilder {
    private static final Logger LOG = LogManager.getLogger(TaskBuilder.class);

    public static Task buildPipeTask(PipeTaskDesc desc) {
        Task task = new Task(desc.getUniqueTaskName());
        task.setSource(Constants.TaskSource.PIPE);
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName(desc.getDbName());
        task.setDefinition(desc.getSqlTask());
        task.setProperties(desc.getVariables());

        handleSpecialTaskProperties(task);
        return task;
    }

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
        } else if (submitTaskStmt.getDataCacheSelectStmt() != null) {
            taskNamePrefix = "DataCacheSelect-";
            taskSource = Constants.TaskSource.DATACACHE_SELECT;
        } else {
            throw new SemanticException("Submit task statement is not supported");
        }
        if (taskName == null) {
            taskName = taskNamePrefix + DebugUtil.printId(context.getExecutionId());
        }
        Task task = new Task(taskName);
        task.setSource(taskSource);
        task.setCreateTime(System.currentTimeMillis());
        task.setCatalogName(submitTaskStmt.getCatalogName());
        task.setDbName(submitTaskStmt.getDbName());
        task.setDefinition(submitTaskStmt.getSqlText());

        Map<String, String> taskProperties = Maps.newHashMap();
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouse(context.getCurrentWarehouseId());
        taskProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, warehouse.getName());
        // the property of submit task has higher priority
        taskProperties.putAll(submitTaskStmt.getProperties());
        task.setProperties(taskProperties);

        task.setCreateUser(ConnectContext.get().getCurrentUserIdentity().getUser());
        task.setUserIdentity(ConnectContext.get().getCurrentUserIdentity());
        task.setSchedule(submitTaskStmt.getSchedule());
        task.setType(submitTaskStmt.getSchedule() != null ? Constants.TaskType.PERIODICAL : Constants.TaskType.MANUAL);
        if (submitTaskStmt.getSchedule() == null) {
            task.setExpireTime(System.currentTimeMillis() + Config.task_ttl_second * 1000L);
        }

        handleSpecialTaskProperties(task);
        return task;
    }

    /**
     * Handle some special task properties like warehouse, session variables...
     */
    private static void handleSpecialTaskProperties(Task task) {
        Map<String, String> properties = task.getProperties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(SessionVariable.WAREHOUSE_NAME)) {
                Warehouse wa = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(entry.getValue());
                Preconditions.checkArgument(wa != null, "warehouse not exists: " + entry.getValue());
            }
        }
    }

    public static OptimizeTask buildOptimizeTask(String name, Map<String, String> properties, String sql, String dbName,
                                                 long warehouseId) {
        OptimizeTask task = new OptimizeTask(name);
        task.setSource(Constants.TaskSource.INSERT);
        task.setDbName(dbName);
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouse(warehouseId);
        properties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, warehouse.getName());
        task.setProperties(properties);
        task.setDefinition(sql);
        task.setExpireTime(0L);
        handleSpecialTaskProperties(task);
        return task;
    }

    public static Task buildMvTask(MaterializedView materializedView, String dbName) {
        Task task = new Task(getMvTaskName(materializedView.getId()));
        task.setSource(Constants.TaskSource.MV);
        task.setDbName(dbName);

        Map<String, String> taskProperties = Maps.newHashMap();
        taskProperties.put(MV_ID, String.valueOf(materializedView.getId()));
        // Don't put mv table properties into task properties since mv refresh doesn't need them, and the properties
        // will cause task run's meta-data too large.
        // NOTE: Don't persist warehouse property in task since it may be changed at runtime via
        // "ALTER MATERIALIZED VIEW SET WAREHOUSE". The warehouse will be fetched from MV in refreshTaskProperties.
        task.setProperties(taskProperties);

        task.setDefinition(materializedView.getTaskDefinition());
        task.setExpireTime(0L);
        if (ConnectContext.get() != null) {
            task.setCreateUser(ConnectContext.get().getCurrentUserIdentity().getUser());
            task.setUserIdentity(ConnectContext.get().getCurrentUserIdentity());
        }
        handleSpecialTaskProperties(task);
        return task;
    }

    public static Task rebuildMvTask(MaterializedView materializedView, String dbName,
                                     Map<String, String> previousTaskProperties, Task previousTask) {
        Task task = new Task(getMvTaskName(materializedView.getId()));
        task.setSource(Constants.TaskSource.MV);
        task.setDbName(dbName);
        String mvId = String.valueOf(materializedView.getId());
        previousTaskProperties.put(MV_ID, mvId);
        task.setProperties(previousTaskProperties);
        task.setDefinition(materializedView.getTaskDefinition());
        task.setExpireTime(0L);
        if (previousTask != null) {
            task.setCreateUser(previousTask.getCreateUser());
            task.setUserIdentity(previousTask.getUserIdentity());
        }
        handleSpecialTaskProperties(task);
        return task;
    }

    public static void updateTaskInfo(Task task, RefreshSchemeClause refreshSchemeDesc, MaterializedView materializedView)
            throws DdlException {
        MaterializedViewRefreshType refreshType = MaterializedViewRefreshType.getType(refreshSchemeDesc);

        if (refreshType == MaterializedViewRefreshType.MANUAL) {
            task.setType(Constants.TaskType.MANUAL);
        } else if (refreshType == MaterializedViewRefreshType.ASYNC) {
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
        MaterializedViewRefreshType refreshType = materializedView.getRefreshScheme().getType();
        // mapping refresh type to task type
        if (refreshType == MaterializedViewRefreshType.MANUAL) {
            task.setType(Constants.TaskType.MANUAL);
        } else if (refreshType == MaterializedViewRefreshType.ASYNC) {
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

    public static void rebuildMVTask(String dbName,
                                     MaterializedView materializedView) throws DdlException {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task currentTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        Task task;
        if (currentTask == null) {
            task = TaskBuilder.buildMvTask(materializedView, dbName);
            TaskBuilder.updateTaskInfo(task, materializedView);
            taskManager.createTask(task);
        } else {
            Map<String, String> previousTaskProperties = currentTask.getProperties() == null ?
                    Maps.newHashMap() : Maps.newHashMap(currentTask.getProperties());
            Task changedTask = TaskBuilder.rebuildMvTask(materializedView, dbName, previousTaskProperties, currentTask);
            TaskBuilder.updateTaskInfo(changedTask, materializedView);
            taskManager.alterTask(currentTask, changedTask);
            task = currentTask;
        }

        // for event triggered type, run task
        if (task.getType() == Constants.TaskType.EVENT_TRIGGERED) {
            taskManager.executeTask(task.getName(), ExecuteOption.makeMergeRedundantOption());
        }
    }

    public static String getMvTaskName(long mvId) {
        return "mv-" + mvId;
    }

    /**
     * Get MaterializedView object from task if the task is mv task, otherwise return null.
     */
    public static MaterializedView getMvFromTask(Task task) {
        if (task == null || task.getSource() != Constants.TaskSource.MV) {
            return null;
        }

        Map<String, String> properties = task.getProperties();
        if (properties == null || !properties.containsKey(MV_ID)) {
            LOG.warn("mv id is missing in task properties, task: {}", task.getName());
            return null;
        }
        String dbName = task.getDbName();
        if (dbName == null) {
            LOG.warn("db name is missing in task, task: {}", task.getName());
            return null;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.warn("db not found: {}, task: {}", dbName, task.getName());
            return null;
        }
        long mvId = Long.parseLong(properties.get(MV_ID));
        Table table = db.getTable(mvId);
        if (table == null || !(table instanceof MaterializedView)) {
            LOG.warn("mv not found: {}, task: {}", mvId, task.getName());
            return null;
        }
        return (MaterializedView) table;
    }
}
