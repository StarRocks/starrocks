// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class MvRewriteOptimizationTest extends MvRewriteTestBase {

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

    public void testSingleTableEqualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid = 5");
        String query = "select empid, deptno, name, salary from emps where empid = 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1");
        PlanTestBase.assertContains(plan, "tabletRatio=1/6");

        String query2 = "select empid, deptno, name, salary from emps where empid = 6";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "mv_1");

        String query3 = "select empid, deptno, name, salary from emps where empid > 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "mv_1");

        String query4 = "select empid, deptno, name, salary from emps where empid < 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "mv_1");

        String query5 = "select empid, length(name), (salary + 1) * 2 from emps where empid = 5";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1\n" +
                "     tabletRatio=1/6");

        String query7 = "select empid, deptno from emps where empid = 5";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "mv_1");
        OptExpression optimizedPlan7 = getOptimizedPlan(query7, connectContext);
        List<PhysicalScanOperator> scanOperators = getScanOperators(optimizedPlan7, "mv_1");
        Assert.assertEquals(1, scanOperators.size());
        // column prune
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("name"));
        Assert.assertFalse(scanOperators.get(0).getColRefToColumnMetaMap().keySet().toString().contains("salary"));

        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        String query6 = "select empid, deptno, name, salary from emps where empid = 5";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "mv_1");

        dropMv("test", "mv_1");
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);
    }

    @Test
    public void testHiveSingleTableEqualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "hive_mv_1",
                "create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 5");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");

        String query2 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 6";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "hive_mv_1");

        String query3 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey > 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "hive_mv_1");

        String query4 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_mv_1");

        String query5 =
                "select s_suppkey, s_name, s_address, (s_acctbal + 1) * 2 from hive0.tpch.supplier where s_suppkey = 5";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_mv_1");

        dropMv("test", "hive_mv_1");
    }

    public void testSingleTableRangePredicateRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        String query = "select empid, deptno, name, salary from emps where empid < 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: mv_1\n" +
                "     tabletRatio=6/6");

        String query2 = "select empid, deptno, name, salary from emps where empid < 4";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 5: empid\n" +
                "  |  <slot 2> : 6: deptno\n" +
                "  |  <slot 3> : 7: name\n" +
                "  |  <slot 4> : 8: salary\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: empid < 4");

        String query3 = "select empid, deptno, name, salary from emps where empid <= 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "mv_1");

        String query4 = "select empid, deptno, name, salary from emps where empid > 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "mv_1");

        String query5 = "select empid, length(name), (salary + 1) * 2 from emps where empid = 4";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 7: empid = 4");

        String query6 = "select empid, length(name), (salary + 1) * 2 from emps where empid between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n");

        String query7 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "1:Project\n" +
                "  |  <slot 1> : 7: empid\n" +
                "  |  <slot 5> : length(9: name)\n" +
                "  |  <slot 6> : 10: salary + 1.0 * 2.0\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 10: salary > 100.0");
        dropMv("test", "mv_1");

        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 and salary > 100");
        String query8 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertNotContains(plan8, "mv_2");

        String query9 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 90";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertNotContains(plan9, "mv_2");
        dropMv("test", "mv_2");

        createAndRefreshMv("test", "mv_3",
                "create materialized view mv_3 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 or salary > 100");
        String query10 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "mv_3");

        String query11 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 or salary > 100";
        String plan11 = getFragmentPlan(query11);
        PlanTestBase.assertContains(plan11, "mv_3");

        String query12 = "select empid, length(name), (salary + 1) * 2 from emps" +
                " where empid < 5 or salary > 100 or salary < 10";
        String plan12 = getFragmentPlan(query12);
        PlanTestBase.assertNotContains(plan12, "mv_3");
        dropMv("test", "mv_3");
    }

    @Test
    public void testHiveSingleTableRangePredicateRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        createAndRefreshMv("test", "hive_mv_1",
                "create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");

        String query2 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 4";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "hive_mv_1");

        String query3 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey <= 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "hive_mv_1");

        String query4 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey > 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_mv_1");

        String query5 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 4";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_mv_1");

        String query6 =
                "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "hive_mv_1");

        String query7 =
                "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5 and " +
                        "s_acctbal > 100.0";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "hive_mv_1");

        dropMv("test", "hive_mv_1");
    }

    public void testSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where name like \"%abc%\" and salary * deptno > 100");
        String query =
                "select empid, deptno, name, salary from emps where salary * deptno > 100 and name like \"%abc%\"";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_1");
        dropMv("test", "mv_1");
    }

    @Test
    public void testHiveSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("test", "hive_mv_1",
                "create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where " +
                        "s_suppkey * s_acctbal > 100 and s_name like \"%abc%\"");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where " +
                "s_suppkey * s_acctbal > 100 and s_name like \"%abc%\"";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");
        dropMv("test", "hive_mv_1");
    }

    public void testMultiMvsForSingleTable() throws Exception {
        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 6 and salary > 100");
        String query = "select empid, length(name), (salary + 1) * 2 from emps where empid < 3 and salary > 110";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");

        createAndRefreshMv("test", "mv_1",
                "create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("test", "mv_2",
                "create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 and salary > 100");
        String query1 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 110";
        String plan2 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan2, "mv_");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");

        createAndRefreshMv("test", "agg_mv_1",
                "create materialized view agg_mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, sum(salary) as total_salary from emps" +
                        " where empid < 5 group by empid, deptno");
        createAndRefreshMv("test", "agg_mv_2",
                "create materialized view agg_mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, sum(salary) as total_salary from emps" +
                        " where empid < 10 group by empid, deptno");
        String query2 = "select empid, sum(salary) from emps where empid < 5 group by empid";
        String plan3 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan3, "agg_mv_");
        dropMv("test", "agg_mv_1");
        dropMv("test", "agg_mv_2");
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
                    " SELECT v1, t1d, t1c from view1");
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
                    " SELECT v1, t1d, t1c from view3");
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
        System.out.println(plan1);
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
    public void testHiveJoinMvRewrite() throws Exception {
        createAndRefreshMv("test", "hive_join_mv_1", "create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_suppkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_suppkey < 100");

        String query1 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "hive_join_mv_1");

        String query2 = "SELECT (s_suppkey + 1) * 2, n_name, n_comment" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 100";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "hive_join_mv_1");

        String query3 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey = 99";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "hive_join_mv_1");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        String query4 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 101";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_join_mv_1");

        String query5 = "SELECT (s_suppkey + 1) * 2, n_name from hive0.tpch.supplier join hive0.tpch.nation on " +
                "s_nationkey = n_nationkey where s_suppkey < 100 and s_suppkey > 10";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_join_mv_1");

        String query6 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "hive_join_mv_1");

        dropMv("test", "hive_join_mv_1");

        createAndRefreshMv("test", "hive_join_mv_2", "create materialized view hive_join_mv_2" +
                " distributed by hash(s_nationkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_nationkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey <= 100");

        // test on equivalence classes for output and predicates
        String query7 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey < 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "hive_join_mv_2");

        String query8 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey < 10";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "hive_join_mv_2");

        String query9 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey = 100";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "hive_join_mv_2");

        String query10 = "SELECT (n_nationkey + 1) * 2, n_name from hive0.tpch.supplier join hive0.tpch.nation on " +
                "s_nationkey = n_nationkey where n_nationkey between 10 and 20";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "hive_join_mv_2");

        dropMv("test", "hive_join_mv_2");
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
                "  |  <slot 7> : 16: v1\n" +
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
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : 16: v1\n" +
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
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "1:Project\n" +
                "  |  <slot 1> : 16: v1\n" +
                "  |  <slot 7> : 16: v1\n" +
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
                "\"storage_format\" = \"DEFAULT\",\n" +
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
    public void testHiveAggregateMvRewrite() throws Exception {
        createAndRefreshMv("test", "hive_agg_join_mv_1", "create materialized view hive_agg_join_mv_1" +
                " distributed by hash(s_nationkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 100 " +
                "group by s_nationkey , n_name");

        String query1 = " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey = 1 " +
                "group by s_nationkey , n_name";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "hive_agg_join_mv_1");

        String query2 = " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 100 " +
                "group by s_nationkey , n_name";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "hive_agg_join_mv_1");

        String query3 = " SELECT s_nationkey , sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 99 " +
                "group by s_nationkey";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "hive_agg_join_mv_1");
    }

    @Test
    public void testHiveUnionRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_union_mv_1",
                "create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "0:UNION");
        PlanTestBase.assertContains(plan1, "hive_union_mv_1");
        PlanTestBase.assertContains(plan1, "1:HdfsScanNode\n" +
                "     TABLE: supplier\n" +
                "     NON-PARTITION PREDICATES: 13: s_suppkey < 10, 13: s_suppkey >= 5");

        dropMv("test", "hive_union_mv_1");
    }

    @Test
    public void testHiveQueryWithMvs() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        // enforce choose the hive scan operator, not mv plan
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        createAndRefreshMv("test", "hive_union_mv_1",
                "create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        createAndRefreshMv("test", "hive_join_mv_1", "create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_suppkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_suppkey < 100");

        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "TABLE: supplier", "NON-PARTITION PREDICATES: 19: s_suppkey < 10, 19: s_suppkey >= 5");

        connectContext.getSessionVariable().setUseNthExecPlan(0);
        dropMv("test", "hive_union_mv_1");
        dropMv("test", "hive_join_mv_1");
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
        Assert.assertNull(olapTable.getUniqueConstraints());

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


    public List<PhysicalScanOperator> getScanOperators(OptExpression root, String name) {
        List<PhysicalScanOperator> results = Lists.newArrayList();
        getScanOperators(root, name, results);
        return results;
    }

    private void getScanOperators(OptExpression root, String name, List<PhysicalScanOperator> results) {
        if (root.getOp() instanceof PhysicalScanOperator
                && ((PhysicalScanOperator) root.getOp()).getTable().getName().equals(name)) {
            results.add((PhysicalScanOperator) root.getOp());
        }
        for (OptExpression child : root.getInputs()) {
            getScanOperators(child, name, results);
        }
    }

    @Test
    public void testNestedMVs1() throws Exception {
        createAndRefreshMv("test", "nested_mv1", "create materialized view nested_mv1 " +
                " distributed by hash(empid) as" +
                " select empid, deptno, name, salary from emps;");
        createAndRefreshMv("test", "nested_mv2", "create materialized view nested_mv2 " +
                " distributed by hash(empid) as" +
                " select empid, sum(deptno) as sum1 from emps group by empid;");
        createAndRefreshMv("test", "nested_mv3", "create materialized view nested_mv3 " +
                " distributed by hash(empid) as" +
                " select empid, sum1 from nested_mv2 where empid > 1;");
        String plan = getFragmentPlan("select empid, sum(deptno) from emps where empid > 1 group by empid");
        System.out.println(plan);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: nested_mv3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: nested_mv3"));
        dropMv("test", "nested_mv1");
        dropMv("test", "nested_mv2");
        dropMv("test", "nested_mv3");
    }

    @Test
    public void testExternalNestedMVs1() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_nested_mv_1",
                "create materialized view hive_nested_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier");
        createAndRefreshMv("test", "hive_nested_mv_2",
                "create materialized view hive_nested_mv_2 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, sum(s_acctbal) as sum1 from hive0.tpch.supplier group by s_suppkey");
        createAndRefreshMv("test", "hive_nested_mv_3",
                "create materialized view hive_nested_mv_3 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, sum1 from hive_nested_mv_2 where s_suppkey > 1");
        String query1 = "select s_suppkey, sum(s_acctbal) from hive0.tpch.supplier where s_suppkey > 1 group by s_suppkey ";
        String plan1 = getFragmentPlan(query1);
        System.out.println(plan1);
        Assert.assertTrue(plan1.contains("0:OlapScanNode\n" +
                "     TABLE: hive_nested_mv_3\n" +
                "     PREAGGREGATION: ON\n"));
        dropMv("test", "hive_nested_mv_1");
        dropMv("test", "hive_nested_mv_2");
        dropMv("test", "hive_nested_mv_3");
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
    public void testNullPartitionRewriteWithLoad() throws Exception {
        {
            cluster.runSql("test", "insert into test_base_part values(1, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(100, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(200, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(1000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2500, 1, 2, 3)");

            createAndRefreshMv("test", "partial_mv_12", "CREATE MATERIALIZED VIEW `partial_mv_12`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"storage_medium\" = \"HDD\"\n" +
                    ")\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    "FROM `test_base_part`\n" +
                    "WHERE `c3` is null\n" +
                    "GROUP BY `c3`, `c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_12", "PREDICATES: 10: c3 IS NOT NULL");
            starRocksAssert.dropMaterializedView("partial_mv_12");
        }

        {
            cluster.runSql("test", "insert into test_base_part values(1, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(100, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(200, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(1000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2000, 1, 2, 3)");
            cluster.runSql("test", "insert into test_base_part values(2500, 1, 2, 3)");

            createAndRefreshMv("test", "partial_mv_13", "CREATE MATERIALIZED VIEW `partial_mv_13`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"storage_medium\" = \"HDD\"\n" +
                    ")\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    "FROM `test_base_part`\n" +
                    "WHERE `c3` is null\n" +
                    "GROUP BY `c3`, `c1`;");

            // test update for null partition
            cluster.runSql("test", "insert into test_base_part partition(p1) values(null, 1, null, 3)");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "partial_mv_13");
            starRocksAssert.dropMaterializedView("partial_mv_13");
        }
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
    public void testProjectConstant() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_map_array (\n" +
                "                            k1 date,\n" +
                "                            k2 varchar(20),\n" +
                "                            v3 int)\n" +
                "                        DUPLICATE KEY(k1)\n" +
                "                        DISTRIBUTED BY HASH(k1);");

        // MV1: Projection MV
        {
            String mvName = "mv_projection_const";
            createAndRefreshMv("test", mvName,
                    "CREATE MATERIALIZED VIEW \n" + mvName +
                            "\nDISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                            "REFRESH ASYNC \n" +
                            "AS SELECT k1, k2, v3 " +
                            "FROM test_map_array\n");
            {
                String query = "SELECT 'hehe', k1, k2" +
                        " FROM test_map_array ";
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
                            "FROM test_map_array\n" +
                            "GROUP BY k1");

            {
                String query = "SELECT 'hehe', k1, sum(v3) as sum_v1 " +
                        " FROM test_map_array group by 'hehe', k1";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
            }
        }
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
        cluster.runSql("test", "set enable_mv_optimizer_trace_log=true;");
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
}
