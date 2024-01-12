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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.optimizer.MVTestUtils.waitForSchemaChangeAlterJobFinish;
import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;

public class MVRewriteWithSchemaChangeTest extends MvRewriteTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        // For mv rewrite with schema change or rollup, need to set it false, otherwise unit tests will be failed.
        FeConstants.runningUnitTest = false;

        starRocksAssert.withTable(cluster, "t0");
        starRocksAssert.withTable(cluster, "test_all_type");
    }

    @Test
    public void testSyncMVRewrite_PartitionPrune() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `sync_tbl_t1` (\n" +
                "                  `dt` date NOT NULL COMMENT \"\",\n" +
                "                  `a` bigint(20) NOT NULL COMMENT \"\",\n" +
                "                  `b` bigint(20) NOT NULL COMMENT \"\",\n" +
                "                  `c` bigint(20) NOT NULL COMMENT \"\"\n" +
                "                ) \n" +
                "                DUPLICATE KEY(`dt`)\n" +
                "                COMMENT \"OLAP\"\n" +
                "                PARTITION BY RANGE(`dt`)\n" +
                "                (PARTITION p20220501 VALUES [('2022-05-01'), ('2022-05-02')),\n" +
                "                 PARTITION p20220502 VALUES [('2022-05-02'), ('2022-05-03')),\n" +
                "                PARTITION p20220503 VALUES [('2022-05-03'), ('2022-05-04')))\n" +
                "                DISTRIBUTED BY HASH(`a`) BUCKETS 32\n" +
                "                PROPERTIES (\n" +
                "                \"replication_num\" = \"1\"" +
                "                );");
        String sql = "CREATE MATERIALIZED VIEW sync_mv1 AS select a, b*10 as col2, c+1 as col3 from sync_tbl_t1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createMaterializedView((CreateMaterializedViewStmt) statementBase);
        waitingRollupJobV2Finish();
        String query = "select a, b*10 as col2, c+1 as col3 from sync_tbl_t1 order by a;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "sync_mv1");
    }


    @Test
    public void testMVCacheInvalidAndReValid() throws Exception {
        starRocksAssert.withTable("\n" +
                "CREATE TABLE test_base_tbl(\n" +
                "  `dt` datetime DEFAULT NULL,\n" +
                "  `col1` bigint(20) DEFAULT NULL,\n" +
                "  `col2` bigint(20) DEFAULT NULL,\n" +
                "  `col3` bigint(20) DEFAULT NULL,\n" +
                "  `error_code` varchar(1048576) DEFAULT NULL\n" +
                ")\n" +
                "DUPLICATE KEY (dt)\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW  test_cache_mv1 \n" +
                "DISTRIBUTED BY HASH(col1, dt) BUCKETS 32\n" +
                "--DISTRIBUTED BY RANDOM BUCKETS 32\n" +
                "partition by date_trunc('day', dt)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS select\n" +
                "      col1,\n" +
                "        dt,\n" +
                "        sum(col2) AS sum_col2,\n" +
                "        sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3\n" +
                "    FROM\n" +
                "        test_base_tbl AS f\n" +
                "    GROUP BY\n" +
                "        col1,\n" +
                "        dt;");
        refreshMaterializedView("test", "test_cache_mv1");

        String sql = "select\n" +
                "      col1,\n" +
                "        sum(col2) AS sum_col2,\n" +
                "        sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3\n" +
                "    FROM\n" +
                "        test_base_tbl AS f\n" +
                "    WHERE (dt >= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s'))\n" +
                "        AND (dt <= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s'))\n" +
                "    GROUP BY col1;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_cache_mv1");

        {
            // invalid base table
            String alterSql = "alter table test_base_tbl modify column col1  varchar(30);";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql,
                    connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
            waitForSchemaChangeAlterJobFinish();

            // check mv invalid
            Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
            MaterializedView mv1 = ((MaterializedView) testDb.getTable("test_cache_mv1"));
            Assert.assertFalse(mv1.isActive());
            try {
                cluster.runSql("test", "alter materialized view test_cache_mv1 active;");
                Assert.fail("could not active the mv");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("mv schema changed"));
            }

            plan = getFragmentPlan(sql);
            PlanTestBase.assertNotContains(plan, "test_cache_mv1");
        }

        {
            // alter the column to original one
            String alterSql = "alter table test_base_tbl modify column col1 bigint;";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSql,
                    connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
            waitForSchemaChangeAlterJobFinish();

            cluster.runSql("test", "alter materialized view test_cache_mv1 active;");
            plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "test_cache_mv1");
        }
    }

    @Test
    public void testMVAggregateTable() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t1_agg` (\n" +
                "  `c_1_0` datetime NULL COMMENT \"\",\n" +
                "  `c_1_1` decimal128(24, 8) NOT NULL COMMENT \"\",\n" +
                "  `c_1_2` double SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`c_1_0`, `c_1_1`)\n" +
                "DISTRIBUTED BY HASH(`c_1_1`) BUCKETS 3");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_t1_v0 " +
                "AS " +
                "SELECT t1_17.c_1_0, t1_17.c_1_1, SUM(t1_17.c_1_2) " +
                "FROM t1_agg AS t1_17 " +
                "GROUP BY t1_17.c_1_0, t1_17.c_1_1 ORDER BY t1_17.c_1_0 DESC, t1_17.c_1_1 ASC");

        // NOTE: change `selectBestRowCountIndex` to prefer non-baseIndexId so can choose the mv for
        // mv's rowCount and columnSize is the same with the base table.
        {
            String query = "select * from t1_agg";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, "table: t1_agg, rollup: mv_t1_v0\n");
        }
        {

            String query = "select c_1_0, c_1_1, sum(c_1_2) from t1_agg group by c_1_0, c_1_1";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, "table: t1_agg, rollup: mv_t1_v0\n");
        }

        starRocksAssert.dropMaterializedView("mv_t1_v0");
        starRocksAssert.dropTable("t1_agg");
    }

    @Test
    public void testMVWithViewAndSubQuery1() throws Exception {
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1, t1d, t1c from (select t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d) t" +
                    " where v1 < 100");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT * from view1");
            {
                String query = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            {
                String query = "SELECT (t1d + 1) * 2, t1c from view1 where v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }
            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }

        // nested views
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1 from t0");

            starRocksAssert.withView("create view view2 as " +
                    " SELECT t1d, t1c from test_all_type");

            starRocksAssert.withView("create view view3 as " +
                    " SELECT v1, t1d, t1c from (select view1.v1, view2.t1d, view2.t1c" +
                    " from view1 join view2" +
                    " on view1.v1 = view2.t1d) t" +
                    " where v1 < 100");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v1)" +
                    " as " +
                    " SELECT * from view3");
            {
                String query = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            {
                String query = "SELECT (t1d + 1) * 2, t1c" +
                        " from view3 where v1 < 100";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            starRocksAssert.dropView("view2");
            starRocksAssert.dropView("view3");
            dropMv("test", "join_mv_1");
        }

        // duplicate views
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1 from t0");

            createAndRefreshMv("create materialized view join_mv_1" +
                    " distributed by hash(v11)" +
                    " as " +
                    " SELECT v11, v12 from (select vv1.v1 v11, vv2.v1 v12 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1 ) t");
            {
                String query = "SELECT vv1.v1, vv2.v1 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }


        {
            starRocksAssert.withTables(cluster, List.of("depts", "emps_par"),
                    () -> {
                        starRocksAssert.withView("create view view1 as " +
                                " select deptno1, deptno2, empid, name " +
                                "from " +
                                "(" +
                                "   SELECT emps_par.deptno as deptno1, depts.deptno as deptno2, " +
                                "   emps_par.empid, emps_par.name from emps_par " +
                                "join depts" +
                                " on emps_par.deptno = depts.deptno) t");

                        createAndRefreshMv("create materialized view join_mv_2" +
                                " distributed by hash(deptno2)" +
                                " partition by deptno1" +
                                " as " +
                                " SELECT deptno1, deptno2, empid, name from view1 " +
                                " union " +
                                " SELECT deptno1, deptno2, empid, name from view1");

                        createAndRefreshMv("create materialized view join_mv_1" +
                                " distributed by hash(deptno2)" +
                                " partition by deptno1" +
                                " as " +
                                " SELECT deptno1, deptno2, empid, name from view1");

                        {
                            String query = "SELECT deptno1, deptno2, empid, name from view1";
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "join_mv_1");
                        }
                    });

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
            dropMv("test", "join_mv_2");
        }
    }
}
