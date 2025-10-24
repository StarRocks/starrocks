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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodName.class)
public class PCTRefreshRangePartitionOlapTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }


    @Test
    public void testMVForceRefreshPropagateForce() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");
        TaskRun taskRun = buildMVTaskRun(mv, "test");

        // refresh with force
        ExecPlan execPlan = getMVRefreshExecPlan(taskRun, true);
        Assertions.assertNotNull(execPlan);

        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        TaskRun nextTaskRun = processor.getNextTaskRun();
        String v = nextTaskRun.getProperties().get(TaskRun.FORCE);
        Assertions.assertEquals("true", v);

    }
}