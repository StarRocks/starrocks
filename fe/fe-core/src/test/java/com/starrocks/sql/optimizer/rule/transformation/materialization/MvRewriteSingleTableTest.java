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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MvRewriteSingleTableTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "emps");
    }

    @Test
    public void testSingleTableEqualPredicateRewrite() throws Exception {
        createAndRefreshMv("create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid = 5");
        String query = "select empid, deptno, name, salary from emps where empid = 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "TABLE: mv_1");

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
        PlanTestBase.assertContains(plan5, "mv_1");

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
        createAndRefreshMv("create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
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

    @Test
    public void testSingleTableRangePredicateRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        createAndRefreshMv("create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        String query = "select empid, deptno, name, salary from emps where empid < 5";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_1");

        String query2 = "select empid, deptno, name, salary from emps where empid < 4";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "mv_1");

        String query3 = "select empid, deptno, name, salary from emps where empid <= 5";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "mv_1");

        String query4 = "select empid, deptno, name, salary from emps where empid > 5";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "mv_1");

        String query5 = "select empid, length(name), (salary + 1) * 2 from emps where empid = 4";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "mv_1");

        String query6 = "select empid, length(name), (salary + 1) * 2 from emps where empid between 3 and 4";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "mv_1");

        String query7 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "mv_1", "salary > 100");
        dropMv("test", "mv_1");

        createAndRefreshMv("create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 and salary > 100");
        String query8 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertNotContains(plan8, "mv_2");

        String query9 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 90";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertNotContains(plan9, "mv_2");
        dropMv("test", "mv_2");

        createAndRefreshMv("create materialized view mv_3 distributed by hash(empid)" +
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
        createAndRefreshMv("create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
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

    @Test
    public void testSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where name like \"%abc%\" and salary * deptno > 100");
        String query =
                "select empid, deptno, name, salary from emps where salary * deptno > 100 and name like \"%abc%\"";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_1");
        dropMv("test", "mv_1");
    }

    @Test
    public void testHiveSingleTableResidualPredicateRewrite() throws Exception {
        createAndRefreshMv("create materialized view hive_mv_1 distributed by hash(s_suppkey) " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where " +
                        "s_suppkey * s_acctbal > 100 and s_name like \"%abc%\"");
        String query = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where " +
                "s_suppkey * s_acctbal > 100 and s_name like \"%abc%\"";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "hive_mv_1");
        dropMv("test", "hive_mv_1");
    }

    @Test
    public void testMultiMvsForSingleTable() throws Exception {
        createAndRefreshMv("create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 6 and salary > 100");
        String query = "select empid, length(name), (salary + 1) * 2 from emps where empid < 3 and salary > 110";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");

        createAndRefreshMv("create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5 and salary > 100");
        String query1 = "select empid, length(name), (salary + 1) * 2 from emps where empid < 5 and salary > 110";
        String plan2 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan2, "mv_");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");

        createAndRefreshMv("create materialized view agg_mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, sum(salary) as total_salary from emps" +
                        " where empid < 5 group by empid, deptno");
        createAndRefreshMv("create materialized view agg_mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, sum(salary) as total_salary from emps" +
                        " where empid < 10 group by empid, deptno");
        String query2 = "select empid, sum(salary) from emps where empid < 5 group by empid";
        String plan3 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan3, "agg_mv_");
        dropMv("test", "agg_mv_1");
        dropMv("test", "agg_mv_2");
    }

    @Test
    public void testNestedMvOnSingleTable() throws Exception {
        createAndRefreshMv("create materialized view mv_1 distributed by hash(empid)" +
                        " as select empid, deptno, name, salary from emps where empid < 5");
        createAndRefreshMv("create materialized view mv_2 distributed by hash(empid)" +
                        " as select empid, deptno, salary from mv_1 where salary > 100");
        String query = "select empid, deptno, (salary + 1) * 2 from emps where empid < 5 and salary > 110";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "mv_2");
        dropMv("test", "mv_1");
        dropMv("test", "mv_2");
    }
}
