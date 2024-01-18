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

package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.structure.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;

public class CreateSyncMaterializedViewTest {
    private static final Logger LOG = LogManager.getLogger(CreateSyncMaterializedViewTest.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestName name = new TestName();

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database testDb;
    private static GlobalStateMgr currentState;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(temp.newFolder().toURI().toString());

        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.TBL1 \n" +
                        "(\n" +
                        "    K1 date,\n" +
                        "    K2 int,\n" +
                        "    V1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(K1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(K2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `aggregate_table_with_null` (\n" +
                        "`k1` date,\n" +
                        "`v2` datetime MAX,\n" +
                        "`v3` char(20) MIN,\n" +
                        "`v4` bigint SUM,\n" +
                        "`v8` bigint SUM,\n" +
                        "`v5` HLL HLL_UNION,\n" +
                        "`v6` BITMAP BITMAP_UNION,\n" +
                        "`v7` PERCENTILE PERCENTILE_UNION\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withView("CREATE VIEW v1 AS SELECT * FROM aggregate_table_with_null;")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('10'),\n" +
                        "    PARTITION p2 values less than('20')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `duplicate_tbl` (\n" +
                        "    `k1` date NULL COMMENT \"\",   \n" +
                        "    `k2` datetime NULL COMMENT \"\",   \n" +
                        "    `k3` char(20) NULL COMMENT \"\",   \n" +
                        "    `k4` varchar(20) NULL COMMENT \"\",   \n" +
                        "    `k5` boolean NULL COMMENT \"\",   \n" +
                        "    `k6` tinyint(4) NULL COMMENT \"\",   \n" +
                        "    `k7` smallint(6) NULL COMMENT \"\",   \n" +
                        "    `k8` int(11) NULL COMMENT \"\",   \n" +
                        "    `k9` bigint(20) NULL COMMENT \"\",   \n" +
                        "    `k10` largeint(40) NULL COMMENT \"\",   \n" +
                        "    `k11` float NULL COMMENT \"\",   \n" +
                        "    `k12` double NULL COMMENT \"\",   \n" +
                        "    `k13` decimal128(27, 9) NULL COMMENT \"\",   \n" +
                        "    INDEX idx1 (`k6`) USING BITMAP \n" +
                        ") \n" +
                        "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) \n" +
                        "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 \n" +
                        "PROPERTIES ( \n" +
                        "    \"replication_num\" = \"1\" \n" +
                        ")")
                .withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE test2.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.mocked_cloud_table\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .useDatabase("test");
        starRocksAssert.withView("create view test.view_to_tbl1 as select * from test.tbl1;");
        currentState = GlobalStateMgr.getCurrentState();
        testDb = currentState.getDb("test");
    }

    private Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        return table;
    }

    private MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    @Test
    public void testSelectFromSyncMV() throws Exception {
        // `tbl1`'s distribution keys is k2, sync_mv1 no `k2` in its outputs.
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from tbl1 group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);

        waitingRollupJobV2Finish();
        sql = "select * from sync_mv1 [_SYNC_MV_];";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        String explainString = pair.second.getExplainString(StatementBase.ExplainLevel.NORMAL);
        Assert.assertTrue(explainString.contains("partitions=2/2\n" +
                "     rollup: sync_mv1\n" +
                "     tabletRatio=6/6"));
        starRocksAssert.dropMaterializedView("sync_mv1");
    }

    // create sync mv that mv's name already existed in the db
    @Test
    public void testCreateSyncMV1() throws Exception {
        String sql = "create materialized view aggregate_table_with_null as select k1, sum(v1) from tbl1 group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        try {
            // aggregate_table_with_null already existed in the db
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Table [aggregate_table_with_null] already exists in the db test"));
        }
    }

    // create sync mv that mv's name already existed in the same table
    @Test
    public void testCreateSyncMV2() throws Exception {
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from tbl1 group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);

        waitingRollupJobV2Finish();
        OlapTable tbl1 = (OlapTable) (getTable("test", "tbl1"));
        Assert.assertTrue(tbl1 != null);
        Assert.assertTrue(tbl1.hasMaterializedIndex("sync_mv1"));

        // sync_mv1 already existed in the tbl1
        sql = "create materialized view sync_mv1 as select k1, sum(v1) from tbl1 group by k1;";
        createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains("Materialized view[sync_mv1] already exists in " +
                    "the table tbl1"));
        }
        starRocksAssert.dropMaterializedView("sync_mv1");
    }

    // create sync mv that mv's name already existed in other table
    @Test
    public void testCreateSyncMV3() throws Exception {
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from tbl1 group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);

        waitingRollupJobV2Finish();
        OlapTable tbl1 = (OlapTable) (getTable("test", "tbl1"));
        Assert.assertTrue(tbl1 != null);
        Assert.assertTrue(tbl1.hasMaterializedIndex("sync_mv1"));
        // sync_mv1 already existed in tbl1
        sql = "create materialized view sync_mv1 as select k1, sum(v1) from tbl3 group by k1;";
        createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains("Materialized view[sync_mv1] already exists " +
                    "in table tbl1"));
        }
        starRocksAssert.dropMaterializedView("sync_mv1");
    }

    @Test
    public void testCreateSyncMV_WithUpperColumn() throws Exception {
        // `tbl1`'s distribution keys is k2, sync_mv1 no `k2` in its outputs.
        String sql = "create materialized view UPPER_MV1 as select K1, sum(V1) from TBL1 group by K1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);

        waitingRollupJobV2Finish();
        {
            sql = "select * from UPPER_MV1 [_SYNC_MV_];";
            Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            String explainString = pair.second.getExplainString(StatementBase.ExplainLevel.NORMAL);
            // output columns should be same with the base table.
            Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                    " OUTPUT EXPRS:1: K1 | 2: mv_sum_V1\n" +
                    "  PARTITION: UNPARTITIONED"));
        }
        {
            sql = "select K1, sum(V1) from TBL1 group by K1";
            Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            String explainString = pair.second.getExplainString(StatementBase.ExplainLevel.NORMAL);
            Assert.assertTrue(explainString.contains("1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: sum(4: mv_sum_V1)\n" +
                    "  |  group by: 1: K1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: TBL1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2\n" +
                    "     rollup: UPPER_MV1"));
        }
        starRocksAssert.dropMaterializedView("UPPER_MV1");
    }

    @Test
    public void testCreateSyncMV_WithLowerColumn() throws Exception {
        // `tbl1`'s distribution keys is k2, sync_mv1 no `k2` in its outputs.
        String sql = "create materialized view lower_mv1 as select k1, sum(v1) from tbl1 group by K1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);

        waitingRollupJobV2Finish();
        {
            sql = "select * from lower_mv1 [_SYNC_MV_];";
            Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            String explainString = pair.second.getExplainString(StatementBase.ExplainLevel.NORMAL);
            // output columns should be same with the base table.
            Assert.assertTrue(explainString.contains("PLAN FRAGMENT 0\n" +
                    " OUTPUT EXPRS:1: k1 | 2: mv_sum_v1\n" +
                    "  PARTITION: UNPARTITIONED"));
        }
        {
            sql = "select K1, sum(v1) from tbl1 group by K1";
            Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            String explainString = pair.second.getExplainString(StatementBase.ExplainLevel.NORMAL);
            Assert.assertTrue(explainString.contains("1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: sum(4: mv_sum_v1)\n" +
                    "  |  group by: 1: k1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: tbl1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2\n" +
                    "     rollup: lower_mv1"));
        }
        starRocksAssert.dropMaterializedView("lower_mv1");
    }

    @Test
    public void testCreateSynchronousMVOnLakeTable() throws Exception {
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from mocked_cloud_table group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        Table table = getTable("test", "mocked_cloud_table");
        // Change table type to cloud native table
        Deencapsulation.setField(table, "type", Table.TableType.CLOUD_NATIVE);
        DdlException e = Assert.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
        });
        Assert.assertTrue(e.getMessage().contains("Creating synchronous materialized view(rollup) is not supported in " +
                "shared data clusters.\nPlease use asynchronous materialized view instead.\n" +
                "Refer to https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements" +
                "/data-definition/CREATE%20MATERIALIZED%20VIEW#asynchronous-materialized-view for details."));
    }

    @Test
    public void testCreateSynchronousMVOnAnotherMV() throws Exception {
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from mocked_cloud_table group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        Table table = getTable("test", "mocked_cloud_table");
        // Change table type to materialized view
        Deencapsulation.setField(table, "type", Table.TableType.MATERIALIZED_VIEW);
        DdlException e = Assert.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
        });
        Assert.assertTrue(e.getMessage().contains("Do not support create synchronous materialized view(rollup) on"));
    }

    @Test
    public void testCreateSyncMaterializedViewWithWhereMultiSlots1() throws Exception {
        String mv1 = "CREATE MATERIALIZED VIEW test_mv_with_multi_slots1 \n" +
                "as\n" +
                "select k1, sum(k6+k7) as sum1, max(k7*k10) as max1 from duplicate_tbl group by k1;";
        starRocksAssert.withMaterializedView(mv1);

        {
            String query = "select k1, sum(k6+k7) as sum1, max(k7*k10) as max1 from duplicate_tbl group by k1;";
            starRocksAssert.query(query).explainContains("test_mv_with_multi_slots1");
        }
        {
            String query = "select k1, sum(k6+k7) as sum1 from duplicate_tbl group by k1;";
            starRocksAssert.query(query).explainContains("test_mv_with_multi_slots1");
        }
        {
            String query = "select k1, sum(k6+k7+1) as sum1 from duplicate_tbl group by k1;";
            starRocksAssert.query(query).explainWithout("test_mv_with_multi_slots1");
        }
        starRocksAssert.dropMaterializedView("test_mv_with_multi_slots1");
    }

    @Test
    public void testCreateSyncMaterializedViewWithWhereMultiSlots2() throws Exception {
        String mv1 = "CREATE MATERIALIZED VIEW test_mv_with_multi_slots1 \n" +
                "as\n" +
                "select k1, (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 from duplicate_tbl;";
        starRocksAssert.withMaterializedView(mv1);

        {
            String query = "select k1, (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl;";
            starRocksAssert.query(query).explainContains("test_mv_with_multi_slots1");
        }
        {
            String query = "select (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl;";
            starRocksAssert.query(query).explainContains("test_mv_with_multi_slots1");
        }
        {
            String query = "select (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl where k1>'2023-01-01';";
            starRocksAssert.query(query).explainContains("test_mv_with_multi_slots1");
        }
        {
            String query = "select (case when k6 + k7 + 1> 0 then 1 when k6 + k7 + 1 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl where k1>'2023-01-01';";
            starRocksAssert.query(query).explainWithout("test_mv_with_multi_slots1");
        }
        starRocksAssert.dropMaterializedView("test_mv_with_multi_slots1");
    }

    @Test
    public void testCreateSyncMaterializedViewWithWhereExpr1() throws Exception {
        String mv1 = "CREATE MATERIALIZED VIEW test_mv_with_where1\n" +
                "as\n" +
                "select k1, sum(k6) as sum1, sum(k6+k7) as sum2 from duplicate_tbl where k1 > '2023-01-01' group by k1;";
        starRocksAssert.withMaterializedView(mv1);
        {
            String query = "select k1, sum(k6) as sum1, sum(k6+k7) as sum2 from duplicate_tbl " +
                    "where k1 > '2023-01-01' group by k1;";
            starRocksAssert.query(query).explainContains("test_mv_with_where1");
        }
        {
            String query = "select k1, sum(k6) as sum1, sum(k6+k7) as sum2 from duplicate_tbl " +
                    "where k1 > '2023-02-01' group by k1;";
            starRocksAssert.query(query).explainContains("test_mv_with_where1");
        }
        {
            String query = "select k1, sum(k6) as sum1, sum(k6+k7*10) as sum2 from duplicate_tbl " +
                    "where k1 > '2023-02-01' group by k1;";
            starRocksAssert.query(query).explainWithout("test_mv_with_where1");
        }
        starRocksAssert.dropMaterializedView("test_mv_with_where1");
    }

    @Test
    public void testCreateSyncMaterializedViewWithWhereExpr2() throws Exception {
        String mv1 = "CREATE MATERIALIZED VIEW test_mv_with_where1\n" +
                "as\n" +
                "select k1, (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                "from duplicate_tbl where k1>'2023-01-01';";
        starRocksAssert.withMaterializedView(mv1);

        {
            String query = "select k1, (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl where k1>'2023-01-01';";
            starRocksAssert.query(query).explainContains("test_mv_with_where1");
        }
        {
            String query = "select (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl where k1>'2023-01-01';";
            starRocksAssert.query(query).explainContains("test_mv_with_where1");
        }
        {
            String query = "select (case when k6 + k7> 0 then 1 when k6 + k7 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl where k1>'2023-02-01';";
            starRocksAssert.query(query).explainContains("test_mv_with_where1");
        }
        {
            String query = "select (case when k6 + k7 + 1> 0 then 1 when k6 + k7 + 1 < 0 then -1 else 0 end) as case1 " +
                    "from duplicate_tbl where k1>'2023-01-01';";
            starRocksAssert.query(query).explainWithout("test_mv_with_where1");
        }
        starRocksAssert.dropMaterializedView("test_mv_with_where1");
    }
}