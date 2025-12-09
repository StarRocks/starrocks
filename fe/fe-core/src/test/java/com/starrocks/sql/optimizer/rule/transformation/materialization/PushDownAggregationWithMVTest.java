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
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PushDownAggregationWithMVTest extends MVTestBase {

    @BeforeAll
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
    public void testAggregationPushDown1() throws Exception {
        ConnectContext testContext = starRocksAssert.getCtx();
        testContext.getSessionVariable().setCboPushDownAggregateMode(1);

        // SQL using MV with aggregation that should be pushed down
        String sql = "SELECT t1.city, SUM(t1.amount) as total_amount " +
                "FROM base_table t1 JOIN base_table t2 ON t1.id = t2.id " +
                "GROUP BY t1.city";

        // Get the execution plan with push down enabled
        String planWithPushDown = getFragmentPlan(sql);
        PlanTestBase.assertContains(planWithPushDown, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(19: amount)\n" +
                "  |  group by: 18: city, 16: id\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv1");
        // Now test with push down disabled
        testContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        String planWithoutPushDown = getFragmentPlan(sql);

        // Check that the aggregation is not pushed down
        PlanTestBase.assertContains(planWithoutPushDown, "  4:HASH JOIN\n" +
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
                "  |  <slot 1> : 16: id\n" +
                "  |  <slot 3> : 18: city\n" +
                "  |  <slot 4> : 19: amount\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv1");
    }

    @Test
    public void testAggregationPushDown2() throws Exception {
        ConnectContext testContext = starRocksAssert.getCtx();
        // SQL using MV with aggregation that should be pushed down
        String sql = "SELECT t1.city, SUM(t1.amount) as total_amount " +
                "FROM base_table t1 JOIN base_table t2 ON t1.id = t2.id " +
                "GROUP BY t1.city";
        testContext.getSessionVariable().setEnableMaterializedViewPushDownRewrite(false);
        String planWithoutPushDown = getFragmentPlan(sql);

        // Check that the aggregation is not pushed down
        PlanTestBase.assertContains(planWithoutPushDown, "  4:HASH JOIN\n" +
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
                "  |  <slot 1> : 16: id\n" +
                "  |  <slot 3> : 18: city\n" +
                "  |  <slot 4> : 19: amount\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv1");
        testContext.getSessionVariable().setEnableMaterializedViewPushDownRewrite(true);
    }

    @Test
    public void testAggPushDownFailWithGroupByFunc0() throws Exception {
        starRocksAssert.withTable("CREATE TABLE fact_event_requests\n" +
                "(\n" +
                "    event_date                DATE                        NOT NULL,\n" +
                "    request_id                VARCHAR(64)                NOT NULL,\n" +
                "    event_time                DATETIME                  NOT NULL, \n" +
                "    event_status              VARCHAR(32)                       ,\n" +
                "    region_id                 INT                               ,\n" +
                "    site_id                   INT                               ,\n" +
                "    event_type                VARCHAR(16)                       ,\n" +
                "    location_path             ARRAY<VARCHAR(12)>               ,\n" +
                "    channel                   VARCHAR(8)                        \n" +
                ")\n" +
                "ENGINE = olap\n" +
                "PARTITION BY RANGE (event_date)\n" +
                "(\n" +
                "    START (\"20250101\") END (\"20260101\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(channel, request_id) BUCKETS 16\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");\n");
        starRocksAssert.withTable("CREATE TABLE dim_location_area\n" +
                "(\n" +
                "    geohash              VARCHAR(12) NOT NULL,\n" +
                "    area_name            VARCHAR(64)        \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(geohash) BUCKETS 8\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_hourly_events\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "Partition by (event_date)\n" +
                "AS\n" +
                "SELECT\n" +
                "    event_date,\n" +
                "    DATE_TRUNC('hour', event_time) as metric_time_1h,\n" +
                "    region_id,\n" +
                "    site_id,\n" +
                "    channel,\n" +
                "    location_path[cardinality(location_path)] as last_geohash,\n" +
                "    COUNT(request_id) as total_requests\n" +
                "FROM fact_event_requests  \n" +
                "WHERE event_type = 'TYPE_A'\n" +
                "GROUP BY event_date, metric_time_1h, region_id, site_id, channel, last_geohash;\n");
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
        {
            String query = "SELECT " +
                    "    DATE_TRUNC('hour', event_time) as metric_time_1h,\n" +
                    "    location_path[cardinality(location_path)] as last_geohash,\n" +
                    "    COUNT(request_id) as total_requests\n" +
                    "FROM fact_event_requests  \n" +
                    "WHERE event_type = 'TYPE_A'\n" +
                    "GROUP BY metric_time_1h, last_geohash;";
            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "mv_hourly_events");
        }
    }

    @Test
    public void testAggPushDownFailWithGroupByFunc2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE fact_event_requests\n" +
                "(\n" +
                "    event_date                DATE                        NOT NULL,\n" +
                "    request_id                VARCHAR(64)                NOT NULL,\n" +
                "    event_time                DATETIME                  NOT NULL, \n" +
                "    event_status              VARCHAR(32)                       ,\n" +
                "    region_id                 INT                               ,\n" +
                "    site_id                   INT                               ,\n" +
                "    event_type                VARCHAR(16)                       ,\n" +
                "    location_path             ARRAY<VARCHAR(12)>               ,\n" +
                "    channel                   VARCHAR(8)                        \n" +
                ")\n" +
                "ENGINE = olap\n" +
                "PARTITION BY RANGE (event_date)\n" +
                "(\n" +
                "    START (\"20250101\") END (\"20260101\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(channel, request_id) BUCKETS 16\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");\n");
        starRocksAssert.withTable("CREATE TABLE dim_location_area\n" +
                "(\n" +
                "    geohash              VARCHAR(12) NOT NULL,\n" +
                "    area_name            VARCHAR(64)        \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(geohash) BUCKETS 8\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_hourly_events\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "Partition by (event_date)\n" +
                "AS\n" +
                "SELECT\n" +
                "    event_date,\n" +
                "    DATE_TRUNC('hour', event_time) as metric_time_1h,\n" +
                "    region_id,\n" +
                "    site_id,\n" +
                "    channel,\n" +
                "    location_path[cardinality(location_path)] as last_geohash,\n" +
                "    COUNT(request_id) as total_requests\n" +
                "FROM fact_event_requests  \n" +
                "WHERE event_type = 'TYPE_A'\n" +
                "GROUP BY event_date, metric_time_1h, region_id, site_id, channel, last_geohash;\n");
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
        String sql = "SELECT\n" +
                " \n" +
                "  DATE_TRUNC('hour', event_time) as metric_time_1h,\n" +
                "  CAST(COUNT(request_id) AS DOUBLE)\n" +
                "FROM\n" +
                "  fact_event_requests AS fact_data_source\n" +
                "LEFT JOIN (\n" +
                "  SELECT\n" +
                "    geohash,\n" +
                "    max(area_name) AS area_name\n" +
                "  FROM\n" +
                "    dim_location_area\n" +
                "  GROUP BY\n" +
                "    1) AS dim_location_area \n" +
                "    ON dim_location_area.geohash = location_path[cardinality(location_path)]\n" +
                "WHERE event_date = 20251001\n" +
                "  AND (region_id = 2\n" +
                "    AND site_id IN (4)\n" +
                "      AND area_name IN ('Zone-1')\n" +
                "        AND event_type = 'TYPE_A'\n" +
                "        AND channel IN ('mobile'))\n" +
                "GROUP BY 1";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "mv_hourly_events");
    }
}
