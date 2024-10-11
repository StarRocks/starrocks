// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.scheduler;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.externalcooldown.ExternalCooldownPartitionSelector;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;


public class PartitionBasedCooldownProcessor extends BaseTaskRunProcessor {
    private static final Logger LOG = LogManager.getLogger(PartitionBasedCooldownProcessor.class);
    public static final String TABLE_ID = "TABLE_ID";
    public static final String PARTITION_ID = "PARTITION_ID";
    public static final String PARTITION_START = "PARTITION_START";
    public static final String PARTITION_END = "PARTITION_END";
    public static final String FORCE = "FORCE";
    private static final String TABLE_PREFIX = "Table ";
    private Database db;
    private OlapTable olapTable;
    private Partition partition;
    private String externalTableName;
    private ExternalCooldownPartitionSelector partitionSelector;

    private void prepare(TaskRunContext context) {
        db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context.ctx.getCurrentCatalog(), context.ctx.getDatabase());
        long tableId = Long.parseLong(context.getProperties().get(TABLE_ID));
        String partitionStart = context.getProperties().get(PARTITION_START);
        String partitionEnd = context.getProperties().get(PARTITION_END);
        boolean isForce = Boolean.parseBoolean(context.getProperties().get(FORCE));
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new DmlException("Table with id[" + tableId + "] not found");
        }
        if (!(table instanceof OlapTable)) {
            throw new DmlException(TABLE_PREFIX + table.getName() + " not found or not olap table");
        }
        olapTable = (OlapTable) table;
        externalTableName = olapTable.getExternalCoolDownTarget();
        if (Strings.isNullOrEmpty(externalTableName)) {
            throw new DmlException(TABLE_PREFIX + table.getName() + " not found external cool down target table");
        }
        Table iTable = olapTable.getExternalCoolDownTable();
        if (iTable == null) {
            throw new DmlException(TABLE_PREFIX + table.getName() + " get external cool down target table failed");
        }
        if (!(iTable instanceof IcebergTable)) {
            throw new DmlException(TABLE_PREFIX + table.getName() + "'s external table is not iceberg table");
        }

        partitionSelector = new ExternalCooldownPartitionSelector(
                db, olapTable, partitionStart, partitionEnd, isForce);
        if (!partitionSelector.isTableSatisfied()) {
            throw new DmlException(TABLE_PREFIX + table.getName() + " don't satisfy external cool down condition");
        }
        if (!partitionSelector.hasPartitionSatisfied()) {
            throw new DmlException(TABLE_PREFIX + table.getName() + " has no partition satisfy external cool down condition");
        }
        if (context.getProperties().containsKey(PARTITION_ID)) {
            long partitionId = Long.parseLong(context.getProperties().get(PARTITION_ID));
            partition = olapTable.getPartition(partitionId);
        } else {
            partition = partitionSelector.getOneSatisfiedPartition();
        }
    }

    private boolean generateNextTaskRun(TaskRunContext context) {
        this.partition = partitionSelector.getNextSatisfiedPartition(this.partition);
        if (this.partition == null) {
            return false;
        }

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> newProperties = context.getProperties();
        newProperties.put(TABLE_ID, String.valueOf(olapTable.getId()));
        newProperties.put(PARTITION_ID, String.valueOf(partition.getId()));
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.NORMAL.value(), true, newProperties);
        String taskName = TaskBuilder.getExternalCooldownTaskName(olapTable.getId());
        taskManager.executeTask(taskName, option);
        return true;
    }

    private String generateInsertSql(ConnectContext ctx) {
        ctx.setThreadLocalInfo();
        TableName externalTable = TableName.fromString(externalTableName);
        TableName olapTableName = new TableName(db.getFullName(), olapTable.getName());
        return String.format("INSERT OVERWRITE %s SELECT * FROM %s PARTITION(%s)",
                externalTable.toSql(), olapTableName.toSql(), partition.getName());
    }

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        prepare(context);
        StmtExecutor executor = null;
        try {
            ConnectContext ctx = context.getCtx();
            String definition = generateInsertSql(ctx);
            context.setDefinition(definition);
            ctx.getState().setOk();
            ctx.getAuditEventBuilder().reset();
            ctx.getAuditEventBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp(context.getRemoteIp())
                    .setUser(ctx.getQualifiedUser())
                    .setDb(ctx.getDatabase())
                    .setCatalog(ctx.getCurrentCatalog())
                    .setStmt(definition);

            long visibleVersionTime = partition.getVisibleVersionTime();
            StatementBase sqlStmt = SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
            sqlStmt.setOrigStmt(new OriginStatement(definition, 0));
            executor = new StmtExecutor(ctx, sqlStmt);
            ctx.setExecutor(executor);
            ctx.setThreadLocalInfo();

            LOG.info("[QueryId:{}] start to external cooldown table {} partition {} with sql {}",
                    ctx.getQueryId(), olapTable.getName(), partition.getName(), definition);
            try {
                executor.execute();
            } catch (Exception e) {
                LOG.warn("[QueryId:{}] execute external cooldown table {} partition {} failed, err: {}", ctx.getQueryId(),
                        olapTable.getName(), partition.getName(), e.getMessage());
                throw new DdlException(e.getMessage());
            } finally {
                LOG.info("[QueryId:{}] finished to external cooldown table {} partition {}", ctx.getQueryId(),
                        olapTable.getName(), partition.getName());
                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            }

            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("[QueryId:{}] external cooldown task table {} partition {} failed, err: {}",
                        DebugUtil.printId(ctx.getQueryId()), olapTable.getName(),
                        partition.getName(), ctx.getState().getErrorMessage());
                throw new DdlException(ctx.getState().getErrorMessage());
            }

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            long partitionId = partition.getId();

            partitionInfo.setExternalCoolDownSyncedTimeMs(partitionId, visibleVersionTime);
            // update olap table partition external cool down time
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            // persist editlog
            editLog.logModifyPartition(new ModifyPartitionInfo(db.getId(), olapTable.getId(), partitionId,
                    partitionInfo.getDataProperty(partitionId),
                    partitionInfo.getReplicationNum(partitionId),
                    partitionInfo.getIsInMemory(partitionId),
                    visibleVersionTime,
                    -1L));
            LOG.info("[QueryId:{}] finished modify partition cooldown flag as " +
                            "external cooldown table {} partition {} finished",
                    ctx.getQueryId(), olapTable.getName(), partition.getName());

            if (!generateNextTaskRun(context)) {
                LOG.info("[QueryId:{}] finished cooldown table {} " +
                                "as no partition satisfied cooldown condition",
                        ctx.getQueryId(), olapTable.getName());
            }
        } finally {
            if (executor != null) {
                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            } else {
                // executor can be null if we encounter analysis error.
                auditAfterExec(context, null, null);
            }
        }
    }
}
