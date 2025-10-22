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
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;

@TestMethodOrder(MethodName.class)
public class PCTRefreshNonPartitionOlapTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.query_detail_explain_level = "NORMAL";
        MVTestBase.beforeClass();
    }

    @Test
    public void testMVForceRefresh() throws Exception {
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
                "REFRESH DEFERRED MANUAL\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        ExecPlan execPlan;
        // explain without force
        {
            execPlan = getMVRefreshExecPlan(taskRun);
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);
            execPlan = getMVRefreshExecPlan(taskRun);
            Assertions.assertNull(execPlan);

            String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized " +
                    "view test_mv1;");
            Assertions.assertTrue(plan.contains("PLAN NOT AVAILABLE"));
        }

        // refresh with force
        Map<String, String> props = taskRun.getProperties();
        props.put(TaskRun.FORCE, "true");
        String result = "";
        // explain with refresh
        {
            ExecuteOption executeOption = new ExecuteOption(taskRun.getTask());
            Map<String, String> explainProps = executeOption.getTaskRunProperties();
            explainProps.put(TaskRun.FORCE, "true");

            execPlan =
                    getMVRefreshExecPlan(mv, "explain refresh materialized view test_mv1 " +
                            "force;");
            Assertions.assertNotNull(execPlan);

            String plan = explainMVRefreshExecPlan(mv, executeOption, "explain refresh materialized view test_mv1 " +
                    "force;");
            Assertions.assertTrue(plan.contains("MVToRefreshedPartitions: [test_mv1]"));

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            result = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2");
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            result = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2");
            Assertions.assertNotNull(execPlan);
        }
    }
}