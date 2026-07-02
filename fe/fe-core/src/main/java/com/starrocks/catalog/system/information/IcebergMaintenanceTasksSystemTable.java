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
package com.starrocks.catalog.system.information;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.connector.iceberg.IcebergMaintenanceTaskRecord;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetIcebergMaintenanceTasksParams;
import com.starrocks.thrift.TGetIcebergMaintenanceTasksResult;
import com.starrocks.thrift.TIcebergMaintenanceTaskInfo;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.JsonType;
import com.starrocks.type.TypeFactory;

import java.util.List;

import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.IntegerType.BIGINT;

/**
 * information_schema.iceberg_maintenance_tasks: history of iceberg metadata
 * maintenance tasks (expire_snapshots / remove_orphan_files / rewrite_manifests),
 * both auto-triggered (trigger_reason = schedule) and manual ALTER TABLE EXECUTE
 * (trigger_reason = manual). Backed by the leader-resident in-memory history in
 * {@link com.starrocks.connector.iceberg.IcebergMaintenanceTaskHistory}.
 */
public class IcebergMaintenanceTasksSystemTable {
    public static final String NAME = "iceberg_maintenance_tasks";

    public static SystemTable create() {
        return new SystemTable(SystemId.ICEBERG_MAINTENANCE_TASKS_ID,
                NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        // identity columns are always populated; declare them NOT NULL to match
                        // the BE scanner's _s_tbls_columns (is_null = false)
                        .column("TASK_ID", TypeFactory.createVarcharType(64), false)
                        .column("CATALOG_NAME", TypeFactory.createVarcharType(SystemTable.NAME_CHAR_LEN), false)
                        .column("DATABASE_NAME", TypeFactory.createVarcharType(SystemTable.NAME_CHAR_LEN), false)
                        .column("TABLE_NAME", TypeFactory.createVarcharType(SystemTable.NAME_CHAR_LEN), false)
                        .column("ACTION", TypeFactory.createVarcharType(32))
                        .column("TRIGGER_REASON", TypeFactory.createVarcharType(16))
                        .column("STMT", TypeFactory.createVarcharType(SystemTable.MAX_FIELD_VARCHAR_LENGTH))
                        .column("START_TIME", DATETIME)
                        .column("END_TIME", DATETIME)
                        .column("DURATION_MS", BIGINT)
                        .column("STATUS", TypeFactory.createVarcharType(16))
                        .column("FAILURE_REASON",
                                TypeFactory.createVarcharType(IcebergMaintenanceTaskRecord.MAX_FAILURE_REASON_LENGTH))
                        .column("DETAILS", JsonType.JSON)
                        .build(), TSchemaTableType.SCH_ICEBERG_MAINTENANCE_TASKS);
    }

    public static TGetIcebergMaintenanceTasksResult query(TGetIcebergMaintenanceTasksParams params) {
        TGetIcebergMaintenanceTasksResult result = new TGetIcebergMaintenanceTasksResult();
        List<TIcebergMaintenanceTaskInfo> tasks = Lists.newArrayList();
        result.setTasks(tasks);

        ConnectContext context = new ConnectContext();
        if (params.isSetCurrent_user_ident()) {
            UserIdentityUtils.setAuthInfoFromThrift(context, params.current_user_ident);
        }

        long limit = params.isSetPagination() && params.pagination.isSetLimit()
                ? params.pagination.limit : Long.MAX_VALUE;
        List<IcebergMaintenanceTaskRecord> records =
                GlobalStateMgr.getCurrentState().getIcebergMaintenanceTaskHistory().getRecords();
        for (IcebergMaintenanceTaskRecord record : records) {
            if (tasks.size() >= limit) {
                break;
            }
            if (!matches(params.isSetCatalog_name() ? params.catalog_name : null, record.getCatalogName())
                    || !matches(params.isSetDatabase_name() ? params.database_name : null, record.getDatabaseName())
                    || !matches(params.isSetTable_name() ? params.table_name : null, record.getTableName())) {
                continue;
            }
            try {
                Authorizer.checkAnyActionOnTable(context, new TableName(
                        record.getCatalogName(), record.getDatabaseName(), record.getTableName()));
            } catch (Exception e) {
                // no privilege on this table
                continue;
            }
            tasks.add(toThrift(record));
        }
        return result;
    }

    private static boolean matches(String filter, String value) {
        return Strings.isNullOrEmpty(filter) || filter.equalsIgnoreCase(value);
    }

    private static TIcebergMaintenanceTaskInfo toThrift(IcebergMaintenanceTaskRecord record) {
        TIcebergMaintenanceTaskInfo info = new TIcebergMaintenanceTaskInfo();
        info.setTask_id(record.getTaskId());
        info.setCatalog_name(record.getCatalogName());
        info.setDatabase_name(record.getDatabaseName());
        info.setTable_name(record.getTableName());
        info.setAction(record.getAction());
        info.setTrigger_reason(record.getTriggerReason());
        if (record.getStmt() != null) {
            info.setStmt(record.getStmt());
        }
        info.setStart_time(record.getStartTimeMs() / 1000);
        info.setEnd_time(record.getEndTimeMs() / 1000);
        info.setDuration_ms(record.getDurationMs());
        info.setStatus(record.getStatus());
        if (record.getFailureReason() != null) {
            info.setFailure_reason(record.getFailureReason());
        }
        info.setDetails(record.getDetailsJson());
        return info;
    }
}
