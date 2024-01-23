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

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteJDBCTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
    }

    @Test
    public void testJDBCSingleTableEqualPredicateRewrite() throws Exception {
        createAndRefreshMv("create materialized view jdbc_mv_1 distributed by hash(a) " +
                        "PROPERTIES (\n" +
                        "\"query_rewrite_consistency\" = \"loose\"\n" +
                        ") " +
                        " as select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d = 20230803");
        String query = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d = 20230803";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "jdbc_mv_1");

        String query2 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d = 20230804";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "jdbc_mv_1");
        String query3 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d > 20230803";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "jdbc_mv_1");

        String query4 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d < 20230803";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "jdbc_mv_1");

        String query5 = "select a, b, c, d + 1 from jdbc0.partitioned_db0.tbl0 where d = 20230803";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "jdbc_mv_1");

        dropMv("test", "jdbc_mv_1");
    }

    @Test
    public void testJDBCSingleTableEqualPredicateRewriteUseStr2DateWithVARCHAR() throws Exception {
        createAndRefreshMv("create materialized view jdbc_mv_varchar " +
                        "partition by str2date(d, '%Y%m%d') " +
                        "distributed by hash(a) " +
                        "PROPERTIES (\n" +
                        "\"query_rewrite_consistency\" = \"loose\"\n" +
                        ") " +
                        " as select a, b, c, d from jdbc0.partitioned_db0.tbl1 where d = '20230803'");
        String query = "select a, b, c, d from jdbc0.partitioned_db0.tbl1 where d = '20230803'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "jdbc_mv_varchar");

        dropMv("test", "jdbc_mv_varchar");
    }

    @Test
    public void testJDBCSingleTableEqualPredicateRewriteUseStr2DateWithVARCHAR2() throws Exception {
        createAndRefreshMv("create materialized view jdbc_mv_varchar2 " +
                        "partition by ss " +
                        "distributed by hash(a) " +
                        "PROPERTIES (\n" +
                        "\"query_rewrite_consistency\" = \"loose\"\n" +
                        ") " +
                        " as select a, b, c, str2date(d, '%Y%m%d') as ss from jdbc0.partitioned_db0.tbl1 where d = '20230803'");
        String query = "select a, b, c, d from jdbc0.partitioned_db0.tbl1 where d = '20230803'";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertNotContains(plan, "jdbc_mv_varchar2");

        dropMv("test", "jdbc_mv_varchar2");
    }

    @Test
    public void testJDBCSingleTableRangePredicateRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        createAndRefreshMv("create materialized view jdbc_mv_2 distributed by hash(a) " +
                        "PROPERTIES (\n" +
                        "\"query_rewrite_consistency\" = \"loose\"\n" +
                        ") " +
                        " as select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d < 20230804");
        String query = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d < 20230803";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "jdbc_mv_2");

        String query2 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d < 20230802";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "jdbc_mv_2");

        String query3 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d <= 20230804";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertNotContains(plan3, "jdbc_mv_2");

        String query4 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d > 20230804";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "jdbc_mv_2");

        String query5 = "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d = 20230803";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "jdbc_mv_2");

        String query6 =
                "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d between 20230802 and 20230803";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertContains(plan6, "jdbc_mv_2");

        String query7 =
                "select a, b, c, d from jdbc0.partitioned_db0.tbl0 where d < 20230803 and " +
                        "a is not null";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "jdbc_mv_2");

        dropMv("test", "jdbc_mv_2");
    }
}
