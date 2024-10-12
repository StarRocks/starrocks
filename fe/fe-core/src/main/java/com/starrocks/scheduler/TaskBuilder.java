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
import com.google.common.collect.Range;
import com.starrocks.alter.OptimizeTask;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
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
import com.starrocks.sql.ast.CreateExternalCooldownStmt;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.warehouse.Warehouse;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.starrocks.scheduler.PartitionBasedCooldownProcessor.PARTITION_END;
import static com.starrocks.scheduler.PartitionBasedCooldownProcessor.PARTITION_ID;
import static com.starrocks.scheduler.PartitionBasedCooldownProcessor.PARTITION_START;
import static com.starrocks.scheduler.PartitionBasedCooldownProcessor.TABLE_ID;
import static com.starrocks.scheduler.TaskRun.MV_ID;

// TaskBuilder is responsible for converting Stmt to Task Class
// and also responsible for generating taskId and taskName
public class TaskBuilder {

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
        if (FeConstants.runningUnitTest) {
            stmt = "";
        }
        return stmt;
    }

    public static OptimizeTask buildOptimizeTask(String name, Map<String, String> properties, String sql, String dbName) {
        OptimizeTask task = new OptimizeTask(name);
        task.setSource(Constants.TaskSource.INSERT);
        task.setDbName(dbName);
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
        // In PropertyAnalyzer.analyzeMVProperties, it removed the warehouse property, because
        // it only keeps session started properties
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouse(materializedView.getWarehouseId());
        taskProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, warehouse.getName());
        task.setProperties(taskProperties);

        task.setDefinition(materializedView.getTaskDefinition());
        task.setPostRun(getAnalyzeMVStmt(materializedView.getName()));
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
        task.setPostRun(getAnalyzeMVStmt(materializedView.getName()));
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

    public static void rebuildMVTask(String dbName,
                                     MaterializedView materializedView) throws DdlException {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task currentTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        Task task;
        if (currentTask == null) {
            task = TaskBuilder.buildMvTask(materializedView, dbName);
            TaskBuilder.updateTaskInfo(task, materializedView);
            taskManager.createTask(task, false);
        } else {
            Map<String, String> previousTaskProperties = currentTask.getProperties() == null ?
                     Maps.newHashMap() : Maps.newHashMap(currentTask.getProperties());
            Task changedTask = TaskBuilder.rebuildMvTask(materializedView, dbName, previousTaskProperties, currentTask);
            TaskBuilder.updateTaskInfo(changedTask, materializedView);
            taskManager.alterTask(currentTask, changedTask, false);
            task = currentTask;
        }

        // for event triggered type, run task
        if (task.getType() == Constants.TaskType.EVENT_TRIGGERED) {
            taskManager.executeTask(task.getName(), ExecuteOption.makeMergeRedundantOption());
        }
    }

    public static Task buildExternalCooldownTask(CreateExternalCooldownStmt externalCooldownStmt) {
        TableName tableName = externalCooldownStmt.getTableName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(tableName.getDb(), tableName.getTbl());
        if (!(table instanceof OlapTable)) {
            throw new SemanticException("only support cooldown for olap table, got " + tableName);
        }
        OlapTable olapTable = (OlapTable) table;

        Task task = new Task(getExternalCooldownTaskName(table.getId()));
        task.setSource(Constants.TaskSource.EXTERNAL_COOLDOWN);
        task.setDbName(db.getOriginName());
        Map<String, String> taskProperties = getExternalCooldownTaskProperties(externalCooldownStmt, table);

        task.setDefinition(String.format("INSERT OVERWRITE %s SELECT * FROM %s",
                TableName.fromString(olapTable.getExternalCoolDownTarget()).toSql(),
                externalCooldownStmt.getTableName().toSql()));
        task.setProperties(taskProperties);
        task.setExpireTime(0L);
        handleSpecialTaskProperties(task);
        return task;
    }

    public static Task buildExternalCooldownTask(Database db, OlapTable olapTable, Partition partition) {
        Task task = new Task(getExternalCooldownTaskName(olapTable.getId(), partition.getId())
                + "-" + System.currentTimeMillis());
        task.setSource(Constants.TaskSource.EXTERNAL_COOLDOWN);
        task.setDbName(db.getOriginName());
        Map<String, String> taskProperties = getExternalCooldownTaskProperties(olapTable, partition);
        task.setDefinition(String.format("INSERT OVERWRITE %s SELECT * FROM %s",
                olapTable.getExternalCoolDownTargetTableName().toSql(),
                (new TableName(db.getOriginName(), olapTable.getName())).toSql()
                ));
        task.setProperties(taskProperties);
        task.setExpireTime(0L);
        handleSpecialTaskProperties(task);
        return task;
    }

    @NotNull
    private static Map<String, String> getExternalCooldownTaskProperties(OlapTable table, Partition partition) {
        Map<String, String> taskProperties = Maps.newHashMap();
        taskProperties.put(TABLE_ID, String.valueOf(table.getId()));
        PartitionInfo partitionInfo = table.getPartitionInfo();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Range<PartitionKey> range =  rangePartitionInfo.getRange(partition.getId());
        taskProperties.put(PARTITION_ID, String.valueOf(partition.getId()));
        taskProperties.put(PARTITION_START,
                String.valueOf(range.lowerEndpoint().getKeys().get(0).getStringValue()));
        taskProperties.put(PARTITION_END,
                String.valueOf(range.upperEndpoint().getKeys().get(0).getStringValue()));
        taskProperties.put(TABLE_ID, String.valueOf(table.getId()));
        return taskProperties;
    }

    public static ExecuteOption getCooldownExecuteOption(OlapTable table, Partition partition) {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Range<PartitionKey> range =  rangePartitionInfo.getRange(partition.getId());
        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(PARTITION_ID, String.valueOf(partition.getId()));
        taskRunProperties.put(TaskRun.PARTITION_START, range.lowerEndpoint().getKeys().get(0).getStringValue());
        taskRunProperties.put(TaskRun.PARTITION_END, range.upperEndpoint().getKeys().get(0).getStringValue());
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
        ExecuteOption executeOption = new ExecuteOption(
                Constants.TaskRunPriority.HIGH.value(), false, taskRunProperties);
        executeOption.setManual(true);
        executeOption.setSync(false);
        return executeOption;
    }

    @NotNull
    private static Map<String, String> getExternalCooldownTaskProperties(CreateExternalCooldownStmt stmt, Table table) {
        Map<String, String> taskProperties = Maps.newHashMap();
        taskProperties.put(TABLE_ID, String.valueOf(table.getId()));
        if (stmt.getPartitionRangeDesc() != null) {
            taskProperties.put(PARTITION_START,
                    String.valueOf(stmt.getPartitionRangeDesc().getPartitionStart()));
            taskProperties.put(PARTITION_END,
                    String.valueOf(stmt.getPartitionRangeDesc().getPartitionEnd()));
        }
        taskProperties.put(TABLE_ID, String.valueOf(table.getId()));
        return taskProperties;
    }

    public static String getMvTaskName(long mvId) {
        return "mv-" + mvId;
    }

    public static String getExternalCooldownTaskName(Long tableId) {
        return "external-cooldown-" + tableId;
    }

    public static String getExternalCooldownTaskName(Long tableId, long partitionId) {
        return "external-cooldown-" + tableId + "-" + partitionId;
    }
}
