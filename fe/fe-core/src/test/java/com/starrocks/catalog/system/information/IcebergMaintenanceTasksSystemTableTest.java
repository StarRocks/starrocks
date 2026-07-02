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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.connector.iceberg.IcebergMaintenanceTaskHistory;
import com.starrocks.connector.iceberg.IcebergMaintenanceTaskRecord;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetIcebergMaintenanceTasksParams;
import com.starrocks.thrift.TGetIcebergMaintenanceTasksResult;
import com.starrocks.thrift.TRequestPagination;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class IcebergMaintenanceTasksSystemTableTest {

    @Test
    public void testSchema() {
        SystemTable table = IcebergMaintenanceTasksSystemTable.create();
        Assertions.assertEquals("iceberg_maintenance_tasks", table.getName());
        List<String> columns = table.getBaseSchema().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(List.of("TASK_ID", "CATALOG_NAME", "DATABASE_NAME", "TABLE_NAME",
                "ACTION", "TRIGGER_REASON", "STMT", "START_TIME", "END_TIME", "DURATION_MS",
                "STATUS", "FAILURE_REASON", "DETAILS"), columns);
    }

    private static IcebergMaintenanceTaskHistory buildHistory() {
        IcebergMaintenanceTaskHistory history = new IcebergMaintenanceTaskHistory();
        history.addRecord(finishedRecord("cat1", "db1", "t1"));
        history.addRecord(finishedRecord("cat1", "db2", "t2"));
        history.addRecord(finishedRecord("cat2", "db1", "t3"));
        return history;
    }

    private static IcebergMaintenanceTaskRecord finishedRecord(String catalog, String db, String tbl) {
        IcebergMaintenanceTaskRecord record = IcebergMaintenanceTaskRecord.start(
                catalog, db, tbl, IcebergMaintenanceTaskRecord.TRIGGER_REASON_SCHEDULE, null);
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(5);
        record.setStatus(IcebergMaintenanceTaskRecord.STATUS_SUCCESS);
        record.finish(stats);
        return record;
    }

    private static void allowAll() {
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
                    throws AccessDeniedException {
            }
        };
    }

    @Test
    public void testQueryReturnsAllRecords(@Mocked GlobalStateMgr globalStateMgr) {
        IcebergMaintenanceTaskHistory history = buildHistory();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getIcebergMaintenanceTaskHistory();
                minTimes = 0;
                result = history;
            }
        };
        allowAll();

        TGetIcebergMaintenanceTasksResult result =
                IcebergMaintenanceTasksSystemTable.query(new TGetIcebergMaintenanceTasksParams());
        Assertions.assertEquals(3, result.getTasks().size());
        Assertions.assertEquals("expire_snapshots", result.getTasks().get(0).getAction());
        Assertions.assertEquals("schedule", result.getTasks().get(0).getTrigger_reason());
        Assertions.assertEquals("success", result.getTasks().get(0).getStatus());
        Assertions.assertTrue(result.getTasks().get(0).getDetails().contains("snapshot_count_input"));
    }

    @Test
    public void testQueryWithFilters(@Mocked GlobalStateMgr globalStateMgr) {
        IcebergMaintenanceTaskHistory history = buildHistory();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getIcebergMaintenanceTaskHistory();
                minTimes = 0;
                result = history;
            }
        };
        allowAll();

        TGetIcebergMaintenanceTasksParams params = new TGetIcebergMaintenanceTasksParams();
        params.setCatalog_name("cat1");
        TGetIcebergMaintenanceTasksResult result = IcebergMaintenanceTasksSystemTable.query(params);
        Assertions.assertEquals(2, result.getTasks().size());

        params.setDatabase_name("db2");
        result = IcebergMaintenanceTasksSystemTable.query(params);
        Assertions.assertEquals(1, result.getTasks().size());
        Assertions.assertEquals("t2", result.getTasks().get(0).getTable_name());

        params.setTable_name("not_exists");
        result = IcebergMaintenanceTasksSystemTable.query(params);
        Assertions.assertEquals(0, result.getTasks().size());
    }

    @Test
    public void testQueryWithLimit(@Mocked GlobalStateMgr globalStateMgr) {
        IcebergMaintenanceTaskHistory history = buildHistory();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getIcebergMaintenanceTaskHistory();
                minTimes = 0;
                result = history;
            }
        };
        allowAll();

        TGetIcebergMaintenanceTasksParams params = new TGetIcebergMaintenanceTasksParams();
        TRequestPagination pagination = new TRequestPagination();
        pagination.setLimit(2);
        params.setPagination(pagination);
        TGetIcebergMaintenanceTasksResult result = IcebergMaintenanceTasksSystemTable.query(params);
        Assertions.assertEquals(2, result.getTasks().size());
    }

    @Test
    public void testQueryAccessDenied(@Mocked GlobalStateMgr globalStateMgr) {
        IcebergMaintenanceTaskHistory history = buildHistory();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getIcebergMaintenanceTaskHistory();
                minTimes = 0;
                result = history;
            }
        };
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
                    throws AccessDeniedException {
                throw new AccessDeniedException();
            }
        };

        TGetIcebergMaintenanceTasksResult result =
                IcebergMaintenanceTasksSystemTable.query(new TGetIcebergMaintenanceTasksParams());
        Assertions.assertEquals(0, result.getTasks().size());
    }

    @Test
    public void testQueryAuthorizesPerTable(@Mocked GlobalStateMgr globalStateMgr) {
        IcebergMaintenanceTaskHistory history = buildHistory();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getIcebergMaintenanceTaskHistory();
                minTimes = 0;
                result = history;
            }
        };
        // The check is per table by name: only t1 is authorized, so a user with privilege on t1 must not see
        // the sibling tables' rows in the same db.
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
                    throws AccessDeniedException {
                if (!"t1".equals(tableName.getTbl())) {
                    throw new AccessDeniedException();
                }
            }
        };

        TGetIcebergMaintenanceTasksResult result =
                IcebergMaintenanceTasksSystemTable.query(new TGetIcebergMaintenanceTasksParams());
        Assertions.assertEquals(1, result.getTasks().size());
        Assertions.assertEquals("t1", result.getTasks().get(0).getTable_name());
    }
}
