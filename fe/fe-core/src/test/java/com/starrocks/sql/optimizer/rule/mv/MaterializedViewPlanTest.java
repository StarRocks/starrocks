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


package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.common.Pair;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaterializedViewPlanTest extends PlanTestBase {

    @Before
    public void before() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(true);
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(false);
    }

    @Test
    public void testCreateIncrementalMV() throws Exception {
        String sql = "create materialized view rtmv \n" +
                "distributed by hash(v1) " +
                "refresh incremental as " +
                "select v1, count(*) as cnt from t0 join t1 on t0.v1 = t1.v4 group by v1";

        Pair<CreateMaterializedViewStatement, ExecPlan> pair = UtFrameUtils.planMVMaintenance(connectContext, sql);
        String plan = UtFrameUtils.printPlan(pair.second);
        Assert.assertEquals(plan, "- Output => [1:v1, 7:count]\n" +
                "    - StreamAgg[1:v1]\n" +
                "            Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 0.0}\n" +
                "            7:count := count()\n" +
                "        - StreamJoin/INNER JOIN [1:v1 = 4:v4] => [1:v1]\n" +
                "                Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 0.0}\n" +
                "            - StreamScan [t0] => [1:v1]\n" +
                "                    Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 0.0}\n" +
                "                    predicate: 1:v1 IS NOT NULL\n" +
                "            - StreamScan [t1] => [4:v4]\n" +
                "                    Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 0.0}\n" +
                "                    predicate: 4:v4 IS NOT NULL\n");
    }

    @Test
    public void testSelectFromBinlog() throws Exception {
        String createTableStmtStr = "CREATE TABLE test.binlog_test(k1 int, v1 int, v2 varchar(20)) " +
                "duplicate key(k1) distributed by hash(k1) buckets 2 properties('replication_num' = '1', " +
                "'binlog_enable' = 'false', 'binlog_ttl_second' = '100', 'binlog_max_size' = '100');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.
                parseStmtWithNewParser(createTableStmtStr, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);

        connectContext.getSessionVariable().setMVPlanner(true);
        String sql = "select * from binlog_test [_BINLOG_]";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        String explainString = pair.second.getExplainString(StatementBase.ExplainLevel.NORMAL);
        assertContains(explainString, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: k1 | 2: v1 | 3: v2 | 4: _binlog_op | 5: _binlog_version |" +
                " 6: _binlog_seq_id | 7: _binlog_timestamp\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:BinlogScanNode\n"
        );
    }

    @Test
    public void testTableSink() throws Exception {
        String sql = "create materialized view rtmv \n" +
                "distributed by hash(v1) " +
                "refresh incremental as " +
                "select v1, count(*) as cnt from t0 join t1 on t0.v1 = t1.v4 group by v1";
        Pair<CreateMaterializedViewStatement, ExecPlan> pair = UtFrameUtils.planMVMaintenance(connectContext, sql);
        String verbosePlan = pair.second.getExplainString(StatementBase.ExplainLevel.VERBOSE);
        assertContains(verbosePlan, "  OLAP TABLE SINK\n" +
                "    TABLE: rtmv\n" +
                "    TUPLE ID: 4\n" +
                "    RANDOM");
    }
}
