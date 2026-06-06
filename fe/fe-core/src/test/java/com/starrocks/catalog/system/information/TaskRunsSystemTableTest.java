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

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetTaskRunInfoResult;
import com.starrocks.thrift.TGetTasksParams;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TaskRunsSystemTableTest {

    @Test
    public void testSchemaContainsWarehouseColumn() {
        SystemTable table = TaskRunsSystemTable.getInstance();
        boolean hasWarehouse = table.getBaseSchema().stream()
                .map(Column::getName)
                .anyMatch("WAREHOUSE"::equalsIgnoreCase);
        Assertions.assertTrue(hasWarehouse, "task_runs system table should have a WAREHOUSE column");
    }

    @Test
    public void testQuerySetsWarehouseFromStatus(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) {
        TaskRunStatus status = new TaskRunStatus();
        status.setQueryId("q1");
        status.setTaskName("task1");
        status.setDbName("db1");
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, "my_warehouse");
        status.setProperties(props);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getTaskManager();
                minTimes = 0;
                result = taskManager;

                taskManager.getMatchedTaskRunStatus((TGetTasksParams) any);
                result = Lists.newArrayList(status);
            }
        };

        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext context, String catalogName, String db)
                    throws AccessDeniedException {
            }
        };

        TGetTaskRunInfoResult result = TaskRunsSystemTable.query(new TGetTasksParams());

        Assertions.assertEquals(1, result.getTask_runs().size());
        Assertions.assertEquals("my_warehouse", result.getTask_runs().get(0).getWarehouse());
    }

    @Test
    public void testQuerySetsDefaultWarehouseWhenPropertiesAbsent(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked TaskManager taskManager) {
        TaskRunStatus status = new TaskRunStatus();
        status.setQueryId("q2");
        status.setTaskName("task2");
        status.setDbName("db1");
        // no properties → getWarehouseName() returns DEFAULT_WAREHOUSE_NAME

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getTaskManager();
                minTimes = 0;
                result = taskManager;

                taskManager.getMatchedTaskRunStatus((TGetTasksParams) any);
                result = Lists.newArrayList(status);
            }
        };

        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext context, String catalogName, String db)
                    throws AccessDeniedException {
            }
        };

        TGetTaskRunInfoResult result = TaskRunsSystemTable.query(new TGetTasksParams());

        Assertions.assertEquals(1, result.getTask_runs().size());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                result.getTask_runs().get(0).getWarehouse());
    }
}
