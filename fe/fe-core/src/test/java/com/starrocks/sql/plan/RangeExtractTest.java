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


package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class RangeExtractTest extends PlanTestBase {

    @Test
    public void testRangePredicate1() throws Exception {
        String sql = "select * from t0 where (v1 = 1 and v2 > 2 and v2 < 5) or (v1 = 3 and v2 > 7 and v2 < 10)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1: v1 IN (1, 3), 2: v2 > 2, 2: v2 < 10"));
    }

    @Test
    public void testRangePredicate2() throws Exception {
        String sql =
                "select * from t0 where ((v1 = 1 and v2 > 2 and v2 < 5) or (v3 > 2 and v3 < 5)) or (v1 = 3 and v2 > 7"
                        + " and v2 < 10)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("     PREDICATES: ((((1: v1 = 1) AND (2: v2 > 2)) AND (2: v2 < 5)) OR ((3: v3"
                + " > 2) AND (3: v3 < 5))) OR (((1: v1 = 3) AND (2: v2 > 7)) AND (2: v2 < 10))\n"));
    }

    @Test
    public void testRangePredicate3() throws Exception {
        String sql =
                "select * from t0 where ((v1 = 1 and v2 > 2 and v2 < 5) or (v1 > 2 and v1 < 5)) or ((v1 = 3 and v2 > "
                        + "7 and v2 < 10) or (v1 > 7 and v1 < 10))";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: ((((1: v1 = 1) AND (2: v2 > 2)) AND (2: v2 < 5)) OR ((1: v1 > 2) AND (1: v1 < 5))) OR (("
                        + "((1: v1 = 3) AND (2: v2 > 7)) AND (2: v2 < 10)) OR ((1: v1 > 7) AND (1: v1 < 10))), 1: v1 "
                        + ">= 1, 1: v1 < 10\n"));
    }

    @Test
    public void testRangePredicate4() throws Exception {
        String sql = "select * from t0 where (v1 = 1 and v2 > 2 and v2 < 1) or (v1 = 3 and v2 > 7 and v2 < 10)";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains(
                "PREDICATES: (((1: v1 = 1) AND (2: v2 > 2)) AND (2: v2 < 1)) OR (((1: v1 = 3) AND (2: v2 > 7)) AND "
                        + "(2: v2 < 10)), 1: v1 IN (1, 3), 2: v2 > 7, 2: v2 < 10\n"));
    }

    @Test
    public void testRangePredicate5() throws Exception {
        String sql = "select * from t0 where (v1 = 1 and v2 > 2 and v2 < 1) or (v1 = 3 and v2 = 8)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: (((1: v1 = 1) AND (2: v2 > 2)) AND (2: v2 < 1)) OR ((1: v1 = 3) AND (2: v2 = 8)), "
                        + "1: v1 IN (1, 3), 2: v2 = 8\n"));
    }

    @Test
    public void testRangePredicate6() throws Exception {
        String sql = "select * from t0 where (v1 = 1 and abs(v2) > 2) or (v1 = 3 and abs(v2) = 8)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: ((1: v1 = 1) AND (abs(2: v2) > 2)) OR ((1: v1 = 3) AND (abs(2: v2) = 8)), 1: v1 IN (1, "
                        + "3)"));
    }

    @Test
    public void testRangePredicate7() throws Exception {
        String sql =
                "select t0.* from t0 inner join t1 on v1 = v4 and (v2 = 1 and abs(v5) > 2) or (v2 = 3 and abs(v5) = 8)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 2: v2 IN (1, 3)\n"));
        Assert.assertTrue(plan.contains("PREDICATES: abs(5: v5) > 2\n"));
    }

    @Test
    public void testRangePredicate8() throws Exception {
        String sql = "select * from t0 where v1 = 1 and v1 = 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testRangePredicate9() throws Exception {
        String sql = "select * from t0 where v1 > 1 and v1 <= 1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testRangePredicate10() throws Exception {
        String sql = "select * from t0 where v1 > 1 and v1 <= 0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testRangePredicate11() throws Exception {
        String sql =
                "select t0.* from t0 inner join t1 on v1 = v4 and ((v1 = 1 and abs(v4) > 2) or (v1 = 3 and abs(v4) = 8))";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 1: v1 IN (1, 3), abs(1: v1) > 2\n"));
        Assert.assertTrue(plan.contains("PREDICATES: abs(4: v4) > 2, 4: v4 IN (1, 3)\n"));
    }

    @Test
    public void testRangePredicate12() throws Exception {
        String sql = "select * from test_all_type where t1a = '12345' and t1a = 12345";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 1: t1a = '12345'\n"));
    }

}
