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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PushDownAggregationWithMVTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(500);
        cluster = PseudoCluster.getInstance();
        MVTestBase.beforeClass();
        // Create test tables needed for the tests
        createTestTables();
    }

    private static void createTestTables() throws Exception {
        // Create base table
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);

        // Create base table
        String createTableStmt = "CREATE TABLE test_agg_pushdown.base_table (\n" +
                "  `id` int NOT NULL COMMENT \"\",\n" +
                "  `name` varchar(50) NOT NULL COMMENT \"\",\n" +
                "  `city` varchar(50) NOT NULL COMMENT \"\",\n" +
                "  `amount` int NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        String db = "test_agg_pushdown";
        starRocksAssert.withDatabase(db).useDatabase(db);
        starRocksAssert.withTable(createTableStmt);

        // Create materialized view
        String createMvStmt = "CREATE MATERIALIZED VIEW test_agg_pushdown.mv1 \n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT id, name, city, amount FROM test_agg_pushdown.base_table;";
        starRocksAssert.withMaterializedView(createMvStmt);

    }

    @Test
    public void testAggregationPushDown() throws Exception {
        ConnectContext testContext = starRocksAssert.getCtx();
        testContext.getSessionVariable().setCboPushDownAggregateMode(1);

        // SQL using MV with aggregation that should be pushed down
        String sql = "SELECT t1.city, SUM(t1.amount) as total_amount " +
                "FROM base_table t1 JOIN base_table t2 ON t1.id = t2.id " +
                "GROUP BY t1.city";

        // Get the execution plan with push down enabled
        String planWithPushDown = getFragmentPlan(sql);
        Assert.assertTrue(planWithPushDown, planWithPushDown.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(17: amount)\n" +
                "  |  group by: 14: id, 16: city\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv1"));
        // Now test with push down disabled
        testContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        String planWithoutPushDown = getFragmentPlan(sql);

        // Check that the aggregation is not pushed down
        Assert.assertTrue(planWithoutPushDown, planWithoutPushDown.contains("  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: id = 5: id\n" +
                "  |  \n" +
                "  |----3:Project\n" +
                "  |    |  <slot 5> : 10: id\n" +
                "  |    |  \n" +
                "  |    2:OlapScanNode\n" +
                "  |       TABLE: mv1\n" +
                "  |       PREAGGREGATION: ON\n" +
                "  |       partitions=1/1\n" +
                "  |       rollup: mv1\n" +
                "  |       tabletRatio=3/3\n" +
                "  |       tabletList=10017,10019,10021\n" +
                "  |       cardinality=1\n" +
                "  |       avgRowSize=2.0\n" +
                "  |       MaterializedView: true\n" +
                "  |    \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 14: id\n" +
                "  |  <slot 3> : 16: city\n" +
                "  |  <slot 4> : 17: amount\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv1\n" +
                "     tabletRatio=3/3\n" +
                "     tabletList=10017,10019,10021\n" +
                "     cardinality=1\n" +
                "     avgRowSize=6.0\n" +
                "     MaterializedView: true"));
    }
}
