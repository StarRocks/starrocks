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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.MVTestUtils.waitForSchemaChangeAlterJobFinish;
import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;

public class MvRewriteTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        MvRewriteTestBase.prepareDefaultDatas();
        prepareDatas();
    }

    public static void prepareDatas() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_partition_tbl1 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl2 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        cluster.runSql("test", "insert into test_partition_tbl1 values (\"2019-01-01\",1,1),(\"2019-01-01\",1,2)," +
                "(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                "(\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                "(\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");
    }

    @Test
    public void testViewBasedMv() throws Exception {
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
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
                    " SELECT view1.v1, view2.t1d, view2.t1c" +
                    " from view1 join view2" +
                    " on view1.v1 = view2.t1d" +
                    " where view1.v1 < 100");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
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

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(v11)" +
                    " as " +
                    " SELECT vv1.v1 v11, vv2.v1 v12 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1");
            {
                String query = "SELECT vv1.v1, vv2.v1 from view1 vv1 join view1 vv2 on vv1.v1 = vv2.v1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
        }

        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT emps_par.deptno as deptno1, depts.deptno as deptno2, emps_par.empid, emps_par.name" +
                    " from emps_par join depts" +
                    " on emps_par.deptno = depts.deptno");

            createAndRefreshMv("test", "join_mv_2", "create materialized view join_mv_2" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1 union SELECT deptno1, deptno2, empid, name from view1");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1");

            {
                String query = "SELECT deptno1, deptno2, empid, name from view1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
            dropMv("test", "join_mv_2");
        }
    }

    @Test
    public void testMVWithViewAndSubQuery1() throws Exception {
        {
            starRocksAssert.withView("create view view1 as " +
                    " SELECT v1, t1d, t1c from (select t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                    " from t0 join test_all_type" +
                    " on t0.v1 = test_all_type.t1d) t" +
                    " where v1 < 100");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
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

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
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

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
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
            starRocksAssert.withView("create view view1 as " +
                    " select deptno1, deptno2, empid, name " +
                    "from " +
                    "(SELECT emps_par.deptno as deptno1, depts.deptno as deptno2, emps_par.empid, emps_par.name from emps_par " +
                    "join depts" +
                    " on emps_par.deptno = depts.deptno) t");

            createAndRefreshMv("test", "join_mv_2", "create materialized view join_mv_2" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1 union SELECT deptno1, deptno2, empid, name from view1");

            createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                    " distributed by hash(deptno2)" +
                    " partition by deptno1" +
                    " as " +
                    " SELECT deptno1, deptno2, empid, name from view1");

            {
                String query = "SELECT deptno1, deptno2, empid, name from view1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "join_mv_1");
            }

            starRocksAssert.dropView("view1");
            dropMv("test", "join_mv_1");
            dropMv("test", "join_mv_2");
        }
    }

    @Test
    public void testJoinMvRewrite() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000000);
        createAndRefreshMv("test", "join_mv_1", "create materialized view join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100");

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "join_mv_1");
        OptExpression optExpression1 = getOptimizedPlan(query1, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optExpression1, "join_mv_1");
        Assert.assertEquals(1, scanOperators.size());
        // column prune
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("t1d"));

        // t1e is not the output of mv
        String query2 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c, test_all_type.t1e" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "join_mv_1");

        String query3 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 99";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "1:Project\n" +
                "  |  <slot 6> : 17: t1c\n" +
                "  |  <slot 14> : 15: v1 + 1 * 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: v1 = 99");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        String query4 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 101";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "join_mv_1");

        String query5 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100 and t0.v1 > 10";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "join_mv_1");

        String query6 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "join_mv_1");

        dropMv("test", "join_mv_1");

        createAndRefreshMv("test", "join_mv_2", "create materialized view join_mv_2" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 <= 100");

        // test on equivalence classes for output and predicates
        String query7 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "join_mv_2");

        String query8 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 10";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "1:Project\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 14> : 15: v1 + 1 * 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 15: v1 < 10");
        String query9 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d = 100";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "join_mv_2");

        String query10 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 between 1 and 10";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "join_mv_2");

        dropMv("test", "join_mv_2");

        createAndRefreshMv("test", "join_mv_3", "create materialized view join_mv_3" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, depts.deptno, depts.name from emps join depts using (deptno)");
        String query11 = "select empid, depts.deptno from emps join depts using (deptno) where empid = 1";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "join_mv_3");
        String costPlan2 = getFragmentPlan(query11);
        PlanTestBase.assertContains(costPlan2, "join_mv_3");
        PlanTestBase.assertNotContains(costPlan2, "name-->");
        String newQuery11 = "select depts.deptno from emps join depts using (deptno) where empid = 1";
        String newPlan11 = getFragmentPlan(newQuery11);
        PlanTestBase.assertContains(newPlan11, "join_mv_3");
        OptExpression optExpression11 = getOptimizedPlan(newQuery11, connectContext);
        List<PhysicalScanOperator> scanOperators11 = getScanOperators(optExpression11, "join_mv_3");
        Assert.assertEquals(1, scanOperators11.size());
        // column prune
        Assert.assertFalse(scanOperators11.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));

        String newQuery12 = "select depts.name from emps join depts using (deptno)";
        OptExpression optExpression12 = getOptimizedPlan(newQuery12, connectContext);
        List<PhysicalScanOperator> scanOperators12 = getScanOperators(optExpression12, "join_mv_3");
        Assert.assertEquals(1, scanOperators12.size());
        // deptno is not projected
        Assert.assertFalse(scanOperators12.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // join to scan with projection
        String newQuery13 = "select upper(depts.name) from emps join depts using (deptno)";
        OptExpression optExpression13 = getOptimizedPlan(newQuery13, connectContext);
        List<PhysicalScanOperator> scanOperators13 = getScanOperators(optExpression13, "join_mv_3");
        Assert.assertEquals(1, scanOperators13.size());
        // deptno is not projected
        Assert.assertFalse(scanOperators13.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // output on equivalence classes
        String query12 = "select empid, emps.deptno from emps join depts using (deptno) where empid = 1";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "join_mv_3");

        String query13 = "select empid, emps.deptno from emps join depts using (deptno) where empid > 1";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "join_mv_3");

        String query14 = "select empid, emps.deptno from emps join depts using (deptno) where empid < 1";
        String plan14 = getFragmentPlan(query14);
        PlanTestBase.assertContains(plan14, "join_mv_3");

        // query delta(query has three tables and view has two tabels) is supported
        // depts.name should be in the output of mv
        String query15 = "select emps.empid from emps join depts using (deptno)" +
                " join dependents on (depts.name = dependents.name)";
        String plan15 = getFragmentPlan(query15);
        PlanTestBase.assertContains(plan15, "join_mv_3");
        OptExpression optExpression15 = getOptimizedPlan(query15, connectContext);
        List<PhysicalScanOperator> scanOperators15 = getScanOperators(optExpression15, "join_mv_3");
        Assert.assertEquals(1, scanOperators15.size());
        // column prune
        Assert.assertFalse(scanOperators15.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        // query delta depends on join reorder
        String query16 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno)";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertContains(plan16, "join_mv_3");
        OptExpression optExpression16 = getOptimizedPlan(query16, connectContext);
        List<PhysicalScanOperator> scanOperators16 = getScanOperators(optExpression16, "join_mv_3");
        Assert.assertEquals(1, scanOperators16.size());
        // column prune
        Assert.assertFalse(scanOperators16.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));

        String query23 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join emps on (emps.deptno = depts.deptno) where emps.deptno = 1";
        String plan23 = getFragmentPlan(query23);
        PlanTestBase.assertContains(plan23, "join_mv_3");

        // more tables
        String query17 = "select dependents.empid from depts join dependents on (depts.name = dependents.name)" +
                " join locations on (locations.name = dependents.name) join emps on (emps.deptno = depts.deptno)";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "join_mv_3");

        dropMv("test", "join_mv_3");

        createAndRefreshMv("test", "join_mv_4", "create materialized view join_mv_4" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.name as name1, emps.deptno, depts.name as name2 from emps join depts using (deptno)" +
                " where (depts.name is not null and emps.name ='a')" +
                " or (depts.name is not null and emps.name = 'b')" +
                " or (depts.name is not null and emps.name = 'c')");

        String query18 = "select depts.deptno, depts.name from emps join depts using (deptno)" +
                " where (depts.name is not null and emps.name = 'a')" +
                " or (depts.name is not null and emps.name = 'b')";
        String plan18 = getFragmentPlan(query18);
        PlanTestBase.assertContains(plan18, "join_mv_4");
        dropMv("test", "join_mv_4");

        createAndRefreshMv("test", "join_mv_5", "create materialized view join_mv_5" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid, emps.name as name1, emps.deptno, depts.name as name2 from emps join depts using (deptno)" +
                " where emps.name = 'a'");

        createAndRefreshMv("test", "join_mv_6", "create materialized view join_mv_6" +
                " distributed by hash(empid)" +
                " as " +
                " select empid, deptno, name2 from join_mv_5 where name2 like \"%abc%\"");

        String query19 = "select emps.deptno, depts.name from emps join depts using (deptno)" +
                " where emps.name = 'a' and depts.name like \"%abc%\"";
        String plan19 = getFragmentPlan(query19);
        // the nested rewrite succeed, but the result depends on cost
        PlanTestBase.assertContains(plan19, "join_mv_");

        dropMv("test", "join_mv_5");
        dropMv("test", "join_mv_6");

        createAndRefreshMv("test", "join_mv_7", "create materialized view join_mv_7" +
                " distributed by hash(empid)" +
                " as" +
                " select emps.empid from emps join depts using (deptno)");

        // TODO: rewrite on subquery
        String query20 = "select emps.empid from emps where deptno in (select deptno from depts)";
        String plan20 = getFragmentPlan(query20);
        PlanTestBase.assertNotContains(plan20, "join_mv_7");
        dropMv("test", "join_mv_7");

        // multi relations test
        createAndRefreshMv("test", "join_mv_8", "create materialized view join_mv_8" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)");
        String query21 =
                "select emps1.name, emps2.empid from emps emps1 join emps emps2 on (emps1.empid = emps2.empid)";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertContains(plan21, "join_mv_8");
        dropMv("test", "join_mv_8");

        createAndRefreshMv("test", "join_mv_9", "create materialized view join_mv_9" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name as name1, depts.name as name2 from emps emps1 join depts using (deptno)" +
                " join emps emps2 on (emps1.empid = emps2.empid)");
        String query22 = "select emps2.empid, emps1.name as name1, depts.name as name2" +
                " from emps emps2 join depts using (deptno)" +
                " join emps emps1 on (emps1.empid = emps2.empid)";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertContains(plan22, "join_mv_9");
        dropMv("test", "join_mv_9");

    }

    @Test
    public void testCrossJoin() throws Exception {
        createAndRefreshMv("test", "cross_join_mv1", "create materialized view cross_join_mv1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " where t0.v1 < 100");

        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "cross_join_mv1");
        dropMv("test", "cross_join_mv1");

        createAndRefreshMv("test", "cross_join_mv2", "create materialized view cross_join_mv2" +
                " distributed by hash(empid)" +
                " as" +
                " select emps1.empid, emps2.name as name1, depts.name as name2 from emps emps1 join depts" +
                " join emps emps2 on (emps1.empid = emps2.empid)");
        String query22 = "select depts.name as name2" +
                " from emps emps2 join depts" +
                " join emps emps1 on (emps1.empid = emps2.empid)";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertContains(plan22, "cross_join_mv2");
        dropMv("test", "cross_join_mv2");
    }

    @Test
    public void testAggregateMvRewrite() throws Exception {
        createAndRefreshMv("test", "agg_join_mv_1", "create materialized view agg_join_mv_1" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d");

        String query1 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 = 1" +
                " group by v1, test_all_type.t1d";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : clone(16: v1)\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 = 1");

        String query2 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d";
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewPlanCache(false);
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : clone(16: v1)\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1");

        String query3 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1, test_all_type.t1d";
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewPlanCache(true);
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : clone(16: v1)\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 < 99");

        String query4 = "SELECT t0.v1 as v1, " +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertContains(plan4, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 14> : 18: total_sum\n" +
                "  |  <slot 15> : 19: total_num\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 < 99");

        // test group key not equal
        String query5 = "SELECT t0.v1 + 1 as alias, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by alias, test_all_type.t1d";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "agg_join_mv_1");
        PlanTestBase.assertContains(plan5, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(19: total_sum), sum(20: total_num)\n" +
                "  |  group by: 23: add, 17: v1");
        PlanTestBase.assertContains(plan5, "  1:Project\n" +
                "  |  <slot 17> : 17: v1\n" +
                "  |  <slot 18> : 18: t1d\n" +
                "  |  <slot 19> : 19: total_sum\n" +
                "  |  <slot 20> : 20: total_num\n" +
                "  |  <slot 23> : 17: v1 + 1");

        dropMv("test", "agg_join_mv_1");

        createAndRefreshMv("test", "agg_join_mv_2", "create materialized view agg_join_mv_2" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1b, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1b");
        String query6 = "SELECT t0.v1 as v1, " +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        // rollup test
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(18: total_sum), sum(19: total_num)\n" +
                "  |  group by: 16: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: agg_join_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 16: v1 < 99");
        dropMv("test", "agg_join_mv_2");

        createAndRefreshMv("test", "agg_join_mv_3", "create materialized view agg_join_mv_3" +
                " distributed by hash(v1) as SELECT t0.v1 as v1," +
                " test_all_type.t1b, sum(test_all_type.t1c) * 2 as total_sum," +
                " count(distinct test_all_type.t1c) + 1 as total_num" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1b");

        // rollup with distinct
        String query7 = "SELECT t0.v1 as v1, " +
                " (sum(test_all_type.t1c)  * 2) + (count(distinct test_all_type.t1c) + 1) as total_sum," +
                " (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertNotContains(plan7, "agg_join_mv_3");

        // distinct rewrite without rollup
        String query8 = "SELECT t0.v1, test_all_type.t1b," +
                " (sum(test_all_type.t1c) * 2) + 1 as total_sum, (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by v1, test_all_type.t1b";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "agg_join_mv_3");

        // test group by keys order change
        String query9 = "SELECT t0.v1, test_all_type.t1b," +
                " (sum(test_all_type.t1c) * 2) + 1 as total_sum, (count(distinct test_all_type.t1c) + 1) * 2 as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 99" +
                " group by test_all_type.t1b, v1";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "agg_join_mv_3");

        dropMv("test", "agg_join_mv_3");

        createAndRefreshMv("test", "agg_join_mv_4", "create materialized view agg_join_mv_4" +
                " distributed by hash(`deptno`) as SELECT deptno, count(*) as num from emps group by deptno");
        String query10 = "select deptno, count(*) from emps group by deptno";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "agg_join_mv_4");

        String query11 = "select count(*) from emps";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "agg_join_mv_4");
        OptExpression optExpression11 = getOptimizedPlan(query11, connectContext);
        List<PhysicalScanOperator> scanOperators11 = getScanOperators(optExpression11, "agg_join_mv_4");
        Assert.assertEquals(1, scanOperators11.size());
        // column prune
        Assert.assertFalse(scanOperators11.get(0).getColRefToColumnMetaMap().keySet().toString().contains("deptno"));
        dropMv("test", "agg_join_mv_4");

        createAndRefreshMv("test", "agg_join_mv_5", "create materialized view agg_join_mv_5" +
                " distributed by hash(`deptno`) as SELECT deptno, count(1) as num from emps group by deptno");
        String query12 = "select deptno, count(1) from emps group by deptno";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertContains(plan12, "agg_join_mv_5");

        dropMv("test", "agg_join_mv_5");

        // test aggregate with projection
        createAndRefreshMv("test", "agg_mv_6", "create materialized view agg_mv_6" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid, avg(salary) as total" +
                " from emps group by empid");

        MaterializedView mv = getMv("test", "agg_mv_6");
        Column idColumn = mv.getColumn("empid");
        Assert.assertFalse(idColumn.isAllowNull());
        Column totalColumn = mv.getColumn("total");
        Assert.assertTrue(totalColumn.isAllowNull());

        String query13 = "select empid, abs(empid), avg(salary) from emps group by empid";
        String plan13 = getFragmentPlan(query13);
        PlanTestBase.assertContains(plan13, "agg_mv_6");

        String query14 = "select empid, avg(salary) from emps group by empid";
        OptExpression optExpression14 = getOptimizedPlan(query14, connectContext);
        List<PhysicalScanOperator> scanOperators14 = getScanOperators(optExpression14, "agg_mv_6");
        Assert.assertEquals(1, scanOperators14.size());
        // column prune
        Assert.assertFalse(scanOperators14.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        String query15 = "select abs(empid), avg(salary) from emps group by empid";
        String plan15 = getFragmentPlan(query15);
        PlanTestBase.assertContains(plan15, "agg_mv_6");

        // avg can not be rolled up
        String query16 = "select avg(salary) from emps";
        String plan16 = getFragmentPlan(query16);
        PlanTestBase.assertNotContains(plan16, "agg_mv_6");
        dropMv("test", "agg_mv_6");

        createAndRefreshMv("test", "agg_mv_7", "create materialized view agg_mv_7" +
                " distributed by hash(`empid`) as select empid, abs(empid) as abs_empid," +
                " sum(salary) as total, count(salary) as cnt" +
                " from emps group by empid");

        String query17 = "select empid, abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan17 = getFragmentPlan(query17);
        PlanTestBase.assertContains(plan17, "agg_mv_7");

        String query19 = "select abs(empid), sum(salary), count(salary) from emps group by empid";
        String plan19 = getFragmentPlan(query19);
        PlanTestBase.assertContains(plan19, "agg_mv_7");

        String query20 = "select sum(salary), count(salary) from emps";
        OptExpression optExpression20 = getOptimizedPlan(query20, connectContext);
        List<PhysicalScanOperator> scanOperators20 = getScanOperators(optExpression20, "agg_mv_7");
        Assert.assertEquals(1, scanOperators20.size());
        // column prune
        Assert.assertFalse(scanOperators20.get(0).getColRefToColumnMetaMap().keySet().toString().contains("empid"));
        Assert.assertFalse(scanOperators20.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        String query27 = "select sum(salary), count(salary) from emps";
        OptExpression optExpression27 = getOptimizedPlan(query27, connectContext);
        List<PhysicalScanOperator> scanOperators27 = getScanOperators(optExpression27, "agg_mv_7");
        Assert.assertEquals(1, scanOperators27.size());
        // column prune
        Assert.assertFalse(scanOperators27.get(0).getColRefToColumnMetaMap().keySet().toString().contains("empid"));
        Assert.assertFalse(scanOperators27.get(0).getColRefToColumnMetaMap().keySet().toString().contains("abs_empid"));

        dropMv("test", "agg_mv_7");

        createAndRefreshMv("test", "agg_mv_8", "create materialized view agg_mv_8" +
                " distributed by hash(`empid`) as select empid, deptno," +
                " sum(salary) as total, count(salary) + 1 as cnt" +
                " from emps group by empid, deptno");

        // abs(empid) can not be rewritten
        String query21 = "select abs(empid), sum(salary) from emps group by empid";
        String plan21 = getFragmentPlan(query21);
        PlanTestBase.assertContains(plan21, "agg_mv_8");

        // count(salary) + 1 cannot be rewritten
        String query22 = "select sum(salary), count(salary) + 1 from emps";
        String plan22 = getFragmentPlan(query22);
        PlanTestBase.assertNotContains(plan22, "agg_mv_8");

        String query23 = "select sum(salary) from emps";
        String plan23 = getFragmentPlan(query23);
        PlanTestBase.assertContains(plan23, "agg_mv_8");

        String query24 = "select empid, sum(salary) from emps group by empid";
        String plan24 = getFragmentPlan(query24);
        PlanTestBase.assertContains(plan24, "agg_mv_8");

        dropMv("test", "agg_mv_8");

        createAndRefreshMv("test", "agg_mv_9", "create materialized view agg_mv_9" +
                " distributed by hash(`deptno`) as select deptno," +
                " count(distinct empid) as num" +
                " from emps group by deptno");

        String query25 = "select deptno, count(distinct empid) from emps group by deptno";
        String plan25 = getFragmentPlan(query25);
        PlanTestBase.assertContains(plan25, "agg_mv_9");
        dropMv("test", "agg_mv_9");

        starRocksAssert.withTable("CREATE TABLE `test_table_1` (\n" +
                "  `dt` date NULL COMMENT \"\",\n" +
                "  `experiment_id` bigint(20) NULL COMMENT \"\",\n" +
                "  `hour` varchar(65533) NULL COMMENT \"\",\n" +
                "  `player_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `metric_value` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`dt`, `experiment_id`, `hour`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p202207 VALUES [(\"2022-07-01\"), (\"2022-08-01\")),\n" +
                "PARTITION p202208 VALUES [(\"2022-08-01\"), (\"2022-09-01\")),\n" +
                "PARTITION p202209 VALUES [(\"2022-09-01\"), (\"2022-10-01\")),\n" +
                "PARTITION p202210 VALUES [(\"2022-10-01\"), (\"2022-11-01\")))\n" +
                "DISTRIBUTED BY HASH(`player_id`) BUCKETS 160 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        cluster.runSql("test", "insert into test_table_1 values('2022-07-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-08-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-09-01', 1, '08:00:00', 'player_id_1', 20.0)");
        cluster.runSql("test", "insert into test_table_1 values('2022-10-01', 1, '08:00:00', 'player_id_1', 20.0)");

        createAndRefreshMv("test", "agg_mv_10", "create materialized view agg_mv_10" +
                " distributed by hash(experiment_id)\n" +
                " refresh manual\n" +
                " as\n" +
                " SELECT `dt`, `experiment_id`," +
                "       count(DISTINCT `player_id`) AS `count_distinct_player_id`\n" +
                " FROM `test_table_1`\n" +
                " GROUP BY `dt`, `experiment_id`;");

        String query26 = "SELECT `dt`, `experiment_id`," +
                " count(DISTINCT `player_id`) AS `count_distinct_player_id`\n" +
                "FROM `test_table_1`\n" +
                "GROUP BY `dt`, `experiment_id`";
        String plan26 = getFragmentPlan(query26);
        PlanTestBase.assertContains(plan26, "agg_mv_10");
        dropMv("test", "agg_mv_10");
        starRocksAssert.dropTable("test_table_1");
    }

    @Test
    public void testAggExprRewrite() throws Exception {
        {
            // Group by Cast Expr
            String mvName = "mv_q15";
            createAndRefreshMv("test", mvName, "CREATE MATERIALIZED VIEW `mv_q15`\n" +
                    "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                    "REFRESH ASYNC\n" +
                    "AS \n" +
                    "SELECT \n" +
                    "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                    "    count(d_user) AS `cnt`\n" +
                    "FROM `json_tbl`\n" +
                    "GROUP BY `gender`");
            String query =
                    "SELECT \n" +
                            "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                            "    count(d_user) AS `cnt`\n" +
                            "FROM `json_tbl`\n" +
                            "GROUP BY `gender`;";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        }

        {
            // Agg with Cast Expr
            String mvName = "mv_q16";
            createAndRefreshMv("test", mvName, "CREATE MATERIALIZED VIEW `mv_q16`\n" +
                    "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                    "REFRESH ASYNC\n" +
                    "AS \n" +
                    "SELECT \n" +
                    "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                    "    sum(cast(d_user->'age' as int)) AS `sum`\n" +
                    "FROM `json_tbl`\n" +
                    "GROUP BY `gender`");
            String query =
                    "SELECT \n" +
                            "    CAST((`d_user`->'gender') AS string) AS `gender`, \n" +
                            "    sum(cast(d_user->'age' as int)) AS `sum`\n" +
                            "FROM `json_tbl`\n" +
                            "GROUP BY `gender`;";
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        }
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
    public void testCardinality() throws Exception {
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            createAndRefreshMv("test", "emp_lowcard_sum", "CREATE MATERIALIZED VIEW emp_lowcard_sum" +
                    " DISTRIBUTED BY HASH(empid) AS SELECT empid, name, sum(salary) as sum_sal from emps group by " +
                    "empid, name;");
            String sql = "select name from emp_lowcard_sum group by name";
            String plan = getFragmentPlan(sql);
            Assert.assertTrue(plan.contains("Decode"));
        } finally {
            dropMv("test", "emp_lowcard_sum");
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }

    @Test
    public void testPkFk() throws SQLException {
        cluster.runSql("test", "CREATE TABLE test.parent_table1(\n" +
                "k1 INT,\n" +
                "k2 VARCHAR(20),\n" +
                "k3 INT,\n" +
                "k4 VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(k1)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n," +
                "\"unique_constraints\" = \"k1,k2\"\n" +
                ");");

        cluster.runSql("test", "CREATE TABLE test.parent_table2(\n" +
                "k1 INT,\n" +
                "k2 VARCHAR(20),\n" +
                "k3 INT,\n" +
                "k4 VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(k1)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n," +
                "\"unique_constraints\" = \"k1,k2\"\n" +
                ");");

        OlapTable olapTable = (OlapTable) getTable("test", "parent_table1");
        Assert.assertNotNull(olapTable.getUniqueConstraints());
        Assert.assertEquals(1, olapTable.getUniqueConstraints().size());
        UniqueConstraint uniqueConstraint = olapTable.getUniqueConstraints().get(0);
        Assert.assertEquals(2, uniqueConstraint.getUniqueColumns().size());
        Assert.assertEquals("k1", uniqueConstraint.getUniqueColumns().get(0));
        Assert.assertEquals("k2", uniqueConstraint.getUniqueColumns().get(1));

        cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"k1, k2; k3; k4\")");
        Assert.assertNotNull(olapTable.getUniqueConstraints());
        Assert.assertEquals(3, olapTable.getUniqueConstraints().size());
        UniqueConstraint uniqueConstraint2 = olapTable.getUniqueConstraints().get(0);
        Assert.assertEquals(2, uniqueConstraint2.getUniqueColumns().size());
        Assert.assertEquals("k1", uniqueConstraint2.getUniqueColumns().get(0));
        Assert.assertEquals("k2", uniqueConstraint2.getUniqueColumns().get(1));

        UniqueConstraint uniqueConstraint3 = olapTable.getUniqueConstraints().get(1);
        Assert.assertEquals(1, uniqueConstraint3.getUniqueColumns().size());
        Assert.assertEquals("k3", uniqueConstraint3.getUniqueColumns().get(0));

        UniqueConstraint uniqueConstraint4 = olapTable.getUniqueConstraints().get(2);
        Assert.assertEquals(1, uniqueConstraint4.getUniqueColumns().size());
        Assert.assertEquals("k4", uniqueConstraint4.getUniqueColumns().get(0));

        cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"\")");
        Assert.assertTrue(olapTable.getUniqueConstraints().isEmpty());

        cluster.runSql("test", "alter table parent_table1 set(\"unique_constraints\"=\"k1, k2\")");

        cluster.runSql("test", "CREATE TABLE test.base_table1(\n" +
                "k1 INT,\n" +
                "k2 VARCHAR(20),\n" +
                "k3 INT,\n" +
                "k4 VARCHAR(20),\n" +
                "k5 INT,\n" +
                "k6 VARCHAR(20),\n" +
                "k7 INT,\n" +
                "k8 VARCHAR(20),\n" +
                "k9 INT,\n" +
                "k10 VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(k1)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"foreign_key_constraints\" = \"(k3,k4) REFERENCES parent_table1(k1, k2)\"\n" +
                ");");
        OlapTable baseTable = (OlapTable) getTable("test", "base_table1");
        Assert.assertNotNull(baseTable.getForeignKeyConstraints());
        List<ForeignKeyConstraint> foreignKeyConstraints = baseTable.getForeignKeyConstraints();
        Assert.assertEquals(1, foreignKeyConstraints.size());
        BaseTableInfo parentTable = foreignKeyConstraints.get(0).getParentTableInfo();
        Assert.assertEquals(olapTable.getId(), parentTable.getTableId());
        Assert.assertEquals(2, foreignKeyConstraints.get(0).getColumnRefPairs().size());
        Assert.assertEquals("k3", foreignKeyConstraints.get(0).getColumnRefPairs().get(0).first);
        Assert.assertEquals("k1", foreignKeyConstraints.get(0).getColumnRefPairs().get(0).second);
        Assert.assertEquals("k4", foreignKeyConstraints.get(0).getColumnRefPairs().get(1).first);
        Assert.assertEquals("k2", foreignKeyConstraints.get(0).getColumnRefPairs().get(1).second);

        cluster.runSql("test", "alter table base_table1 set(" +
                "\"foreign_key_constraints\"=\"(k3,k4) references parent_table1(k1, k2);" +
                "(k5,k6) REFERENCES parent_table2(k1, k2)\")");

        List<ForeignKeyConstraint> foreignKeyConstraints2 = baseTable.getForeignKeyConstraints();
        Assert.assertEquals(2, foreignKeyConstraints2.size());
        BaseTableInfo parentTableInfo2 = foreignKeyConstraints2.get(1).getParentTableInfo();
        OlapTable parentTable2 = (OlapTable) getTable("test", "parent_table2");
        Assert.assertEquals(parentTable2.getId(), parentTableInfo2.getTableId());
        Assert.assertEquals(2, foreignKeyConstraints2.get(1).getColumnRefPairs().size());
        Assert.assertEquals("k5", foreignKeyConstraints2.get(1).getColumnRefPairs().get(0).first);
        Assert.assertEquals("k1", foreignKeyConstraints2.get(1).getColumnRefPairs().get(0).second);
        Assert.assertEquals("k6", foreignKeyConstraints2.get(1).getColumnRefPairs().get(1).first);
        Assert.assertEquals("k2", foreignKeyConstraints2.get(1).getColumnRefPairs().get(1).second);

        cluster.runSql("test", "show create table base_table1");
        cluster.runSql("test", "alter table base_table1 set(" +
                "\"foreign_key_constraints\"=\"\")");
        List<ForeignKeyConstraint> foreignKeyConstraints3 = baseTable.getForeignKeyConstraints();
        Assert.assertNull(foreignKeyConstraints3);
    }

    @Test
    public void testTabletHintForbidMvRewrite() throws Exception {
        createAndRefreshMv("test", "forbid_mv_1", "create materialized view forbid_mv_1" +
                " distributed by hash(t1d) as SELECT " +
                " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type" +
                " group by test_all_type.t1d");

        String query1 = "SELECT test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type" +
                " group by test_all_type.t1d";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "  0:OlapScanNode\n" +
                "     TABLE: forbid_mv_1\n" +
                "     PREAGGREGATION: ON");
        ShowResultSet tablets = starRocksAssert.showTablet("test", "test_all_type");
        List<String> tabletIds = tablets.getResultRows().stream().map(r -> r.get(0)).collect(Collectors.toList());
        String tabletHint = String.format("tablet(%s)", tabletIds.get(0));
        String query2 = "SELECT test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from test_all_type " + tabletHint +
                " group by test_all_type.t1d";

        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON");
        dropMv("test", "forbid_mv_1");
    }

    @Test
    public void testJoinPredicatePushdown() throws Exception {
        cluster.runSql("test", "CREATE TABLE pushdown_t1 (\n" +
                "    `c0` string,\n" +
                "    `c1` string,\n" +
                "    `c2` string,\n" +
                "    `c3` string,\n" +
                "    `c4` string,\n" +
                "    `c5` string ,\n" +
                "    `c6` string,\n" +
                "    `c7`  date\n" +
                ") \n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "properties('replication_num' = '1');");
        cluster.runSql("test", "CREATE TABLE `pushdown_t2` (\n" +
                "  `c0` varchar(65533) NULL ,\n" +
                "  `c1` varchar(65533) NULL ,\n" +
                "  `c2` varchar(65533) NULL ,\n" +
                "  `c3` varchar(65533) NULL ,\n" +
                "  `c4` varchar(65533) NULL ,\n" +
                "  `c5` varchar(65533) NULL ,\n" +
                "  `c6` varchar(65533) NULL ,\n" +
                "  `c7` varchar(65533) NULL ,\n" +
                "  `c8` varchar(65533) NULL ,\n" +
                "  `c9` varchar(65533) NULL ,\n" +
                "  `c10` varchar(65533) NULL ,\n" +
                "  `c11` date NULL ,\n" +
                "  `c12` datetime NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        // With null-rejecting predicate
        createAndRefreshMv("test", "_pushdown_predicate_join_mv1",
                "CREATE MATERIALIZED VIEW `_pushdown_predicate_join_mv1`  \n" +
                        "DISTRIBUTED BY HASH(c12) BUCKETS 18 \n" +
                        "REFRESH MANUAL \n" +
                        "PROPERTIES ( \"replication_num\" = \"1\", \"storage_medium\" = \"HDD\") \n" +
                        "AS\n" +
                        "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                        "FROM\n" +
                        "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                        "    LEFT OUTER JOIN \n" +
                        "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                        "    ON `t2`.`c0` = `t1`.`c0`\n" +
                        "    AND t2.c0 IS NOT NULL " +
                        "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                        "   ;");

        String query = "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND t2.c0 IS NOT NULL " +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "_pushdown_predicate_join_mv1");
    }

    @Ignore("outer join and pushdown predicate does not work")
    @Test
    public void testJoinPredicatePushdown1() throws Exception {
        cluster.runSql("test", "CREATE TABLE pushdown_t1 (\n" +
                "    `c0` string,\n" +
                "    `c1` string,\n" +
                "    `c2` string,\n" +
                "    `c3` string,\n" +
                "    `c4` string,\n" +
                "    `c5` string ,\n" +
                "    `c6` string,\n" +
                "    `c7`  date\n" +
                ") \n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "properties('replication_num' = '1');");
        cluster.runSql("test", "CREATE TABLE `pushdown_t2` (\n" +
                "  `c0` varchar(65533) NULL ,\n" +
                "  `c1` varchar(65533) NULL ,\n" +
                "  `c2` varchar(65533) NULL ,\n" +
                "  `c3` varchar(65533) NULL ,\n" +
                "  `c4` varchar(65533) NULL ,\n" +
                "  `c5` varchar(65533) NULL ,\n" +
                "  `c6` varchar(65533) NULL ,\n" +
                "  `c7` varchar(65533) NULL ,\n" +
                "  `c8` varchar(65533) NULL ,\n" +
                "  `c9` varchar(65533) NULL ,\n" +
                "  `c10` varchar(65533) NULL ,\n" +
                "  `c11` date NULL ,\n" +
                "  `c12` datetime NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY (c0)\n" +
                "DISTRIBUTED BY HASH(c0)\n" +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        // Without null-rejecting predicate
        createAndRefreshMv("test", "_pushdown_predicate_join_mv2",
                "CREATE MATERIALIZED VIEW `_pushdown_predicate_join_mv2`  \n" +
                        "DISTRIBUTED BY HASH(c12) BUCKETS 18 \n" +
                        "REFRESH MANUAL \n" +
                        "PROPERTIES ( \"replication_num\" = \"1\", \"storage_medium\" = \"HDD\") \n" +
                        "AS\n" +
                        "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                        "FROM\n" +
                        "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                        "    LEFT OUTER JOIN \n" +
                        "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                        "    ON `t2`.`c0` = `t1`.`c0`\n" +
                        "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                        "   ;");

        String query = "SELECT t1.c0, t1.c1, t2.c7, t2.c12\n" +
                "FROM\n" +
                "    ( SELECT `c0`, `c7`, `c12` FROM `pushdown_t2`) t2\n" +
                "    LEFT OUTER JOIN \n" +
                "    ( SELECT c0, c1, c7 FROM pushdown_t1 ) t1\n" +
                "    ON `t2`.`c0` = `t1`.`c0`\n" +
                "    AND date(t2.`c12`) = `t1`.`c7`\n" +
                "   ;";

        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "_pushdown_predicate_join_mv2");
    }

    @Test
    public void testNonpartitionedMvWithPartitionPredicate() throws Exception {
        createAndRefreshMv("test", "mv_with_partition_predicate_1",
                "create materialized view mv_with_partition_predicate_1 distributed by hash(`k1`)" +
                        " as select k1, v1 from t1 where k1 = 3;");
        String query = "select k1, v1 from t1 where k1 = 3;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_with_partition_predicate_1");
        starRocksAssert.dropMaterializedView("mv_with_partition_predicate_1");
    }

    @Test
    public void testJoinWithTypeCast() throws Exception {
        String sql1 = "create table test.dim_tbl1 (\n" +
                "    col1 string,\n" +
                "    col1_name string\n" +
                ")DISTRIBUTED BY HASH(col1)" +
                ";\n";
        String sql2 = "CREATE TABLE test.fact_tbl1( \n" +
                "          fdate  int,\n" +
                "          fqqid STRING ,\n" +
                "          col1 BIGINT  ,\n" +
                "          flcnt BIGINT\n" +
                " )PARTITION BY range(fdate) (\n" +
                "    PARTITION p1 VALUES [ (\"20230702\"),(\"20230703\")),\n" +
                "    PARTITION p2 VALUES [ (\"20230703\"),(\"20230704\")),\n" +
                "    PARTITION p3 VALUES [ (\"20230705\"),(\"20230706\"))\n" +
                " )\n" +
                " DISTRIBUTED BY HASH(fqqid);";
        String mv = "create MATERIALIZED VIEW test.test_mv1\n" +
                "DISTRIBUTED BY HASH(fdate,col1_name)\n" +
                "REFRESH MANUAL\n" +
                "AS \n" +
                "    select t1.fdate, t2.col1_name,  count(DISTINCT t1.fqqid) AS index_0_8228, sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` >= 20230701 and t1.fdate <= 20230705\n" +
                "    GROUP BY  fdate, `col1_name`;";
        starRocksAssert.withTable(sql1);
        starRocksAssert.withTable(sql2);
        starRocksAssert.withMaterializedView(mv);
        String sql = "select t1.fdate, t2.col1_name,  count(DISTINCT t1.fqqid) AS index_0_8228, sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` >= 20230702 and t1.fdate <= 20230705\n" +
                "    GROUP BY  fdate, `col1_name`;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");

        sql = "select t2.col1_name,  count(DISTINCT t1.fqqid) AS index_0_8228, sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` = 20230705\n" +
                "    GROUP BY   `col1_name`;";
        plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");

        sql = "select t2.col1_name,  sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` = 20230705\n" +
                "    GROUP BY   `col1_name`;";
        plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");

        sql = "select t2.col1_name,  sum(t1.flcnt)as index_xxx\n" +
                "    FROM test.fact_tbl1 t1 \n" +
                "    LEFT JOIN test.dim_tbl1 t2\n" +
                "    ON t1.`col1` = t2.`col1`\n" +
                "    WHERE t1.`fdate` >= 20230702 and t1.fdate <= 20230705\n" +
                "    GROUP BY   `col1_name`;";
        plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_mv1");
    }

    @Test
    public void testPartitionPrune1() throws Exception {
        createAndRefreshMv("test", "test_partition_tbl_mv1",
                "CREATE MATERIALIZED VIEW test_partition_tbl_mv1\n" +
                        "               PARTITION BY k1\n" +
                        "               DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "               REFRESH ASYNC\n" +
                        "               PROPERTIES(\n" +
                        "               \"partition_ttl_number\"=\"4\",\n" +
                        "               \"auto_refresh_partitions_limit\"=\"4\"\n" +
                        "               )\n" +
                        "               AS SELECT k1, sum(v1) as sum_v1 FROM test_partition_tbl1 group by k1;");
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-11' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "PREDICATES: 5: k1 >= '2020-02-11'");
            PlanTestBase.assertContains(plan, "partitions=4/4");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-01' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "partitions=4/4\n" +
                    "     rollup: test_partition_tbl_mv1");
        }
    }

    @Test
    public void testPartitionPrune_SyncMV1() throws Exception {
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
                "                \"in_memory\" = \"false\",\n" +
                "                \"storage_format\" = \"DEFAULT\",\n" +
                "                \"enable_persistent_index\" = \"false\"\n" +
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
    public void testMapArrayRewrite() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_map_array (\n" +
                "                            k1 date,\n" +
                "                            k2 varchar(20),\n" +
                "                            v1 map<string, string>,\n" +
                "                            v2 array<int>, \n" +
                "                            v3 int)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        DISTRIBUTED BY HASH(k1);");

        // MV1: MAP MV
        {
            String mvName = "mv_test_map_element";
            createAndRefreshMv("test", mvName,
                    "CREATE MATERIALIZED VIEW \n" + mvName +
                            "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                            "REFRESH ASYNC \n" +
                            "AS SELECT k1, element_at(v1, 'k1') as col1, sum(v3) as sum_v3 \n" +
                            "FROM test_map_array\n" +
                            "GROUP BY k1, element_at(v1, 'k1') ");

            // query1: exactly-same aggregation
            {
                String query = "SELECT k1, element_at(v1, 'k1'), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by k1, element_at(v1, 'k1')";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            // query2: rollup aggregation
            {
                String query = "SELECT element_at(v1, 'k1'), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by element_at(v1, 'k1')";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            dropMv("test", mvName);
        }

        // MV2: Array Element MV
        {
            String mvName = "mv_test_array_element";
            createAndRefreshMv("test", mvName,
                    "CREATE MATERIALIZED VIEW \n" + mvName +
                            "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                            "REFRESH ASYNC \n" +
                            "AS SELECT k1, element_at(v2, 1) as col1, sum(v3) as sum_v3 \n" +
                            "FROM test_map_array\n" +
                            "GROUP BY k1, element_at(v2, 1) ");

            // query1: exactly-same aggregation
            {
                String query = "SELECT k1, element_at(v2, 1), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by k1, element_at(v2, 1)";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            // query2: rollup aggregation
            {
                String query = "SELECT element_at(v2, 1), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by element_at(v2, 1)";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
            dropMv("test", mvName);
        }
        // MV3: Array Slice MV
        {
            String mvName = "mv_test_array_slice";
            createAndRefreshMv("test", mvName,
                    "CREATE MATERIALIZED VIEW \n" + mvName +
                            "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                            "REFRESH ASYNC \n" +
                            "AS SELECT k1, array_slice(v2, 1, 1) as col1, sum(v3) as sum_v3 \n" +
                            "FROM test_map_array\n" +
                            "GROUP BY k1, array_slice(v2, 1, 1) ");

            // query1: exactly-same aggregation
            {
                String query = "SELECT k1, array_slice(v2, 1, 1), sum(v3) as sum_v1 " +
                        " FROM test_map_array group by k1, array_slice(v2, 1, 1)";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
    }

    @Test
    public void testProjectConstant() throws Exception {
        String tableName = "test_tbl_project_constant";
        starRocksAssert.withTable(String.format("CREATE TABLE %s(\n" +
                "                            k1 date,\n" +
                "                            k2 varchar(20),\n" +
                "                            v3 int)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        DISTRIBUTED BY HASH(k1);", tableName));

        // MV1: Projection MV
        {
            String mvName = "mv_projection_const";
            createAndRefreshMv("test", mvName,
                    "CREATE MATERIALIZED VIEW \n" + mvName +
                            "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                            "REFRESH ASYNC \n" +
                            "AS SELECT k1, k2, v3 " +
                            "FROM " + tableName);
            {
                String query = "SELECT 'hehe', k1, k2" +
                        " FROM " + tableName;
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
        // MV2: Aggregation MV
        {
            String mvName = "mv_aggregation_projection_const";
            createAndRefreshMv("test", mvName,
                    "CREATE MATERIALIZED VIEW \n" + mvName +
                            "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                            "REFRESH ASYNC \n" +
                            "AS SELECT k1, sum(v3) as sum_v3 \n" +
                            "FROM " + tableName + "\n" +
                            "GROUP BY k1");

            {
                String query = String.format("SELECT 'hehe', k1, sum(v3) as sum_v1 " +
                        " FROM %s group by 'hehe', k1", tableName);
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
    }

    @Test
    public void testMVWithAutomaticPartitionBaseTable() throws Exception {
        starRocksAssert.withTable("\n" +
                "CREATE TABLE test_empty_partition_tbl(\n" +
                "  `dt` datetime DEFAULT NULL,\n" +
                "  `col1` bigint(20) DEFAULT NULL,\n" +
                "  `col2` bigint(20) DEFAULT NULL,\n" +
                "  `col3` bigint(20) DEFAULT NULL,\n" +
                "  `error_code` varchar(1048576) DEFAULT NULL\n" +
                ")\n" +
                "DUPLICATE KEY (dt, col1)\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW  test_empty_partition_mv1 \n" +
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
                "        test_empty_partition_tbl AS f\n" +
                "    GROUP BY\n" +
                "        col1,\n" +
                "        dt;");
        String insertSql = "insert into test_empty_partition_tbl values('2022-08-16', 1, 1, 1, 'a')";
        cluster.runSql("test", insertSql);
        refreshMaterializedView("test", "test_empty_partition_mv1");

        String sql = "select\n" +
                "      col1,\n" +
                "        sum(col2) AS sum_col2,\n" +
                "        sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3\n" +
                "    FROM\n" +
                "        test_empty_partition_tbl AS f\n" +
                "    WHERE (dt >= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s'))\n" +
                "        AND (dt <= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s'))\n" +
                "    GROUP BY col1;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "test_empty_partition_mv1");
    }

    @Test
    public void testJoinWithConstExprs1() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("test", "test_partition_tbl_mv3",
                "CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(v1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1, a.v1,sum(a.v1) as sum_v1 \n" +
                        "FROM test_partition_tbl1 as a \n" +
                        "join test_partition_tbl2 as b " +
                        "on a.k1=b.k1 and a.v1=b.v1\n" +
                        "group by a.k1, a.v1;");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.k1=b.k1 " +
                    "where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should rollup
        {
            String query = "select a.k1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1='2020-01-01' and a.v1=1 " +
                    "group by a.k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs2() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("test", "test_partition_tbl_mv3",
                "CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                        "PARTITION BY a_k1\n" +
                        "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1 as a_k1, b.k1 as b_k1, " +
                        "a.v1 as a_v1, b.v1 as b_v1,sum(a.v1) as sum_v1 \n" +
                        "FROM test_partition_tbl1 as a \n" +
                        "left join test_partition_tbl2 as b " +
                        "on a.k1=b.k1 and a.v1=b.v1\n" +
                        "group by a.k1, b.k1, a.v1, b.v1;");
        {
            String query = "SELECT a.k1 as a_k1, b.k1 as b_k1, " +
                    "a.v1 as a_v1, b.v1 as b_v1,sum(a.v1) as sum_v1 \n" +
                    "FROM test_partition_tbl1 as a \n" +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 and a.v1=b.v1 and a.k1=b.k1 and b.v1=a.v1 \n" +
                    "group by a.k1, b.k1, a.v1, b.v1";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partitions=6/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should not be rollup
        {
            String query = "select a.k1, b.k1, a.v1, b.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, b.k1, a.v1,b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', 9: b_k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should be rollup
        {
            String query = "select a.k1,a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', 9: b_k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should be rollup
        {
            String query = "select a.k1,a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.k1=b.k1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', " +
                    "9: b_k1 IS NOT NULL\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        // should be rollup
        {
            String query = "select a.k1,a.v1, sum(a.v1) " +
                    "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.v1=b.v1 and a.v1=b.v1 " +
                    "where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_k1 = '2020-01-01', 9: b_k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs3() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("test", "test_partition_tbl_mv3",
                "CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                        "PARTITION BY a_k1\n" +
                        "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1 as a_k1, a.v1 as a_v1, a.v2 as a_v2, " +
                        "b.k1 as b_k1, b.v1 as b_v1, b.v2 as b_v2 \n" +
                        "FROM test_partition_tbl1 as a \n" +
                        "join test_partition_tbl2 as b " +
                        "on a.v1=b.v1 and a.v2=b.v2 \n");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv3");
        }

        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 and a.v2=b.v2 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: a_v1 = 1, 9: a_v2 = 1\n" +
                    "     partitions=6/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs4() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("test", "test_partition_tbl_mv3",
                "CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                        "PARTITION BY a_k1\n" +
                        "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1 as a_k1, a.v1 as a_v1, a.v2 as a_v2, " +
                        "b.k1 as b_k1, b.v1 as b_v1, b.v2 as b_v2 \n" +
                        "FROM test_partition_tbl1 as a \n" +
                        "join test_partition_tbl2 as b " +
                        "on a.v1=b.v1 \n");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, " TABLE: test_partition_tbl_mv3\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 8: a_v1 = 1, 12: b_v2 = 1\n" +
                    "     partitions=6/6");
        }

        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testJoinWithConstExprs5() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("test", "test_partition_tbl_mv3",
                "CREATE MATERIALIZED VIEW test_partition_tbl_mv3\n" +
                        "PARTITION BY a_k1\n" +
                        "DISTRIBUTED BY HASH(a_v1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1 as a_k1, a.v1 as a_v1, a.v2 as a_v2, " +
                        "b.k1 as b_k1, b.v1 as b_v1, b.v2 as b_v2 \n" +
                        "FROM test_partition_tbl1 as a \n" +
                        "join test_partition_tbl2 as b " +
                        "on a.v1=b.v1 and a.v1=b.v2 \n");
        // should not be rollup
        {
            // if a.k1=b.k1
            String query = "select a.k1, a.v1 " +
                    "FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.v1=b.v1 " +
                    "where a.v1=1 and b.v2=1 ;";
            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertNotContains(plan, "PREDICATES: 8: a_v1 = 1, 11: b_v1 = 1, 12: b_v2 = 1\n" +
                    "     partitions=6/6\n" +
                    "     rollup: test_partition_tbl_mv3");
        }

        starRocksAssert.dropMaterializedView("test_partition_tbl_mv3");
    }

    @Test
    public void testPlanCache() throws Exception {
        {
            String mvSql = "create materialized view agg_join_mv_1" +
                    " distributed by hash(v1) as SELECT t0.v1 as v1," +
                    " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                    " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100" +
                    " group by v1, test_all_type.t1d";
            starRocksAssert.withMaterializedView(mvSql);

            MaterializedView mv = getMv("test", "agg_join_mv_1");
            MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, true);
            Assert.assertNotNull(planContext);
            Assert.assertTrue(CachingMvPlanContextBuilder.getInstance().contains(mv));
            planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, false);
            Assert.assertNotNull(planContext);
            starRocksAssert.dropMaterializedView("agg_join_mv_1");
        }

        {
            String mvSql = "create materialized view mv_with_window" +
                    " distributed by hash(t1d) as" +
                    " SELECT test_all_type.t1d, row_number() over (partition by t1c)" +
                    " from test_all_type";
            starRocksAssert.withMaterializedView(mvSql);

            MaterializedView mv = getMv("test", "mv_with_window");
            MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, true);
            Assert.assertNotNull(planContext);
            Assert.assertFalse(CachingMvPlanContextBuilder.getInstance().contains(mv));
            starRocksAssert.dropMaterializedView("mv_with_window");
        }

        {
            for (int i = 0; i < 1010; i++) {
                String mvName = "plan_cache_mv_" + i;
                String mvSql = String.format("create materialized view %s" +
                        " distributed by hash(v1) as SELECT t0.v1 as v1," +
                        " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                        " where t0.v1 < 100" +
                        " group by v1, test_all_type.t1d", mvName);
                starRocksAssert.withMaterializedView(mvSql);

                MaterializedView mv = getMv("test", mvName);
                MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, true);
                Assert.assertNotNull(planContext);
            }
            for (int i = 0; i < 1010; i++) {
                String mvName = "plan_cache_mv_" + i;
                starRocksAssert.dropMaterializedView(mvName);
            }
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
    public void testExclusivePredicate_StructType() throws Exception {
        String createTable = "CREATE TABLE `t1_event_struct` (\n" +
                "  `c1_event_date` date NOT NULL COMMENT \"\",\n" +
                "  `c2_event` struct<name varchar(65533)> NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c1_event_date`)\n" +
                "PARTITION BY RANGE(`c1_event_date`)\n" +
                "(PARTITION p1 VALUES [(\"2023-07-02\"), (\"2023-07-03\")),\n" +
                "PARTITION p2 VALUES [(\"2023-07-03\"), (\"2023-07-04\")),\n" +
                "PARTITION p3 VALUES [(\"2023-07-05\"), (\"2023-07-06\")))\n" +
                "DISTRIBUTED BY HASH(`c1_event_date`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        String createMv = "CREATE MATERIALIZED VIEW `mv_filter_1` (`c1_event_date`, `name`)\n" +
                "PARTITION BY (`c1_event_date`)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `t1_event_struct`.`c1_event_date`, `t1_event_struct`.`c2_event`.`name` AS `name`\n" +
                "FROM `t1_event_struct`\n" +
                "WHERE `t1_event_struct`.`c2_event`.`name` != 'haha';";

        String tableName = "t1_event_struct";
        String mvName = "mv_filter_1";
        starRocksAssert.withTable(createTable);
        createAndRefreshMv("test", mvName, createMv);

        // equal query 1
        {
            String query = "SELECT c1_event_date FROM t1_event_struct " +
                    "WHERE c2_event.name = 'hehe' and c1_event_date = '2023-07-02'";
            starRocksAssert.query(query).explainContains(mvName,
                    "PREDICATES: 3: c1_event_date = '2023-07-02', 4: name = 'hehe'\n");
        }
        // equal query 2
        {
            String query = "SELECT c1_event_date FROM t1_event_struct " +
                    "WHERE c2_event.name = 'hehe' and c2_event.name != 'haha' and c1_event_date = '2023-07-02'";
            starRocksAssert.query(query).explainContains(mvName,
                    "PREDICATES: 3: c1_event_date = '2023-07-02', 4: name = 'hehe'\n");
        }

        // not-equal query 1
        {
            String query = "SELECT c1_event_date FROM t1_event_struct " +
                    "WHERE c2_event.name <> 'haha' and c1_event_date = '2023-07-02'";
            starRocksAssert.query(query).explainContains(mvName,
                    "PREDICATES: 3: c1_event_date = '2023-07-02', (4: name < 'haha') OR (4: name > 'haha')\n");
        }
        // not-equal query 2
        {
            String query = "SELECT c1_event_date FROM t1_event_struct " +
                    "WHERE c2_event.name <> 'e1' and c1_event_date = '2023-07-02'";
            starRocksAssert.query(query).explainWithout(mvName);
        }

        // in query
        {
            String query = "SELECT c1_event_date FROM t1_event_struct " +
                    "WHERE c2_event.name in ('e1', 'e2', 'e3') and c1_event_date = '2023-07-02'";
            starRocksAssert.query(query).explainContains(mvName,
                    "PREDICATES: 3: c1_event_date = '2023-07-02', 4: name IN ('e1', 'e2', 'e3')\n");
        }

        starRocksAssert.dropTable(tableName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testExclusivePredicate_SimpleType() throws Exception {
        String createTable = "CREATE TABLE `t1_event_struct` (\n" +
                "  `c1_event_date` date NOT NULL COMMENT \"\",\n" +
                "  `c2_event_name` string NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c1_event_date`)\n" +
                "PARTITION BY RANGE(`c1_event_date`)\n" +
                "(PARTITION p1 VALUES [(\"2023-07-02\"), (\"2023-07-03\")),\n" +
                "PARTITION p2 VALUES [(\"2023-07-03\"), (\"2023-07-04\")),\n" +
                "PARTITION p3 VALUES [(\"2023-07-05\"), (\"2023-07-06\")))\n" +
                "DISTRIBUTED BY HASH(`c1_event_date`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        String createMv = "CREATE MATERIALIZED VIEW `mv_filter_1` \n" +
                "PARTITION BY (`c1_event_date`)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `t1_event_struct`.`c1_event_date`, `t1_event_struct`.`c2_event_name` AS `name`\n" +
                "FROM `t1_event_struct`\n" +
                "WHERE `t1_event_struct`.`c2_event_name` != 'haha';";
        String query = "SELECT c1_event_date FROM t1_event_struct " +
                "WHERE c2_event_name = 'hehe' and c1_event_date = '2023-07-02'";

        String tableName = "t1_event_struct";
        String mvName = "mv_filter_1";
        starRocksAssert.withTable(createTable);
        createAndRefreshMv("test", mvName, createMv);
        starRocksAssert.query(query).explainContains(mvName);

        starRocksAssert.dropTable(tableName);
        starRocksAssert.dropMaterializedView(mvName);
    }
}
