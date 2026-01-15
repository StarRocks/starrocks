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

package com.starrocks.qe.scheduler.plan;

import com.starrocks.qe.scheduler.SchedulerTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Attention: cases need execute in a fix order or the result may change.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class SchedulerColocateSetTest extends SchedulerTestBase {
    @BeforeAll
    public static void beforeAll() throws Exception {
        SchedulerTestBase.beforeClass();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        SchedulerTestBase.afterClass();
    }

    void runFileUnitTestHelper(String sql, String resultFile) {
        String enableColocateSetResult = resultFile + "_enable_colocate_set";
        String disableColocateSetResult = resultFile + "_disable_colocate_set";
        runFileUnitTest(sql, enableColocateSetResult);
        String sql2 = sql.replace("select", "select /*+ SET_VAR(disable_colocate_set=true)*/");
        runFileUnitTest(sql2, disableColocateSetResult);
    }

    @Test
    public void testColocateIntersect() {
        String sql = "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "intersect\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "intersect\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "intersect\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/intersect_q1");
    }

    @Test
    public void testColocateIntersectWithAggBelow1() {
        String sql = "with cte as(\n" +
                "select L_ORDERKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem0 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' " +
                "group by L_ORDERKEY\n" +
                "intersect\n" +
                "select L_ORDERKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem1 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' " +
                "group by L_ORDERKEY\n" +
                "intersect\n" +
                "select L_ORDERKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem2 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' " +
                "group by L_ORDERKEY\n" +
                "intersect\n" +
                "select L_ORDERKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem3 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' " +
                "group by L_ORDERKEY\n" +
                ")\n" +
                "select L_ORDERKEY, sum(L_QUANTITY_SUM)\n" +
                "from cte\n" +
                "group by L_ORDERKEY;";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/intersect_q2");
    }

    @Test
    public void testColocateIntersectWithAggBelow2() {
        String sql = "with cte as(\n" +
                "select L_ORDERKEY, L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem0 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                "intersect\n" +
                "select L_ORDERKEY, L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem1 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                "intersect\n" +
                "select L_ORDERKEY, L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem2 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                "intersect\n" +
                "select L_ORDERKEY, L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem3 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                ")\n" +
                "select L_ORDERKEY, L_PARTKEY, sum(L_QUANTITY_SUM)\n" +
                "from cte\n" +
                "group by L_ORDERKEY,L_PARTKEY;";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/intersect_q3");
    }

    @Test
    public void testColocateIntersectWithAggBelow3() {
        String sql = "with cte as(\n" +
                "select L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem0 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                "intersect\n" +
                "select L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem1 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                "intersect\n" +
                "select L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem2 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                "intersect\n" +
                "select L_PARTKEY, sum(L_QUANTITY) L_QUANTITY_SUM from lineitem3 " +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "group by L_ORDERKEY,L_PARTKEY\n" +
                ")\n" +
                "select L_PARTKEY, sum(L_QUANTITY_SUM)\n" +
                "from cte\n" +
                "group by L_PARTKEY;";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/intersect_q4");
    }

    @Test
    public void testColocateIntersectWithJoinAbove() {
        String sql = "with cte as(\n" +
                "select L_ORDERKEY, L_PARTKEY\n" +
                "from lineitem0 \n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "INTERSECT\n" +
                "select L_ORDERKEY, L_PARTKEY\n" +
                "from lineitem1\n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                ")\n" +
                "select cte.* \n" +
                "from cte \n" +
                "     inner join [bucket] orders on L_ORDERKEY = O_ORDERKEY\n" +
                "     inner join [broadcast] part on L_PARTKEY = P_PARTKEY;";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/intersect_q5");
    }

    @Test
    public void testColocateExcept() {
        String sql = "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/except_q1");
    }

    @Test
    public void testColocateExceptWithAggAbove1() {
        String sql = "with cte as(\n" +
                "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                ")\n" +
                "select L_ORDERKEY, sum(L_QUANTITY)\n" +
                "from cte\n" +
                "group by L_ORDERKEY;\n";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/except_q2");
    }

    @Test
    public void testColocateExceptWithAggAbove2() {
        String sql = "with cte as(\n" +
                "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                ")\n" +
                "select L_ORDERKEY, L_PARTKEY, sum(L_QUANTITY)\n" +
                "from cte\n" +
                "group by L_ORDERKEY, L_PARTKEY;\n";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/except_q3");
    }

    @Test
    public void testColocateExceptWithAggAbove3() {
        String sql = "with cte as(\n" +
                "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "except\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                ")\n" +
                "select L_PARTKEY, sum(L_QUANTITY)\n" +
                "from cte\n" +
                "group by L_PARTKEY;\n";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/except_q4");
    }

    @Test
    public void testRandomUnionDistinct() {
        String sql = "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/union_q1");
    }

    @Test
    public void testColocateUnionDistinct() {
        String sql = "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/union_q2");
    }

    @Test
    public void testRandomUnionAll() {
        String sql = "select * from lineitem0 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union all\n" +
                "select * from lineitem1 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union all\n" +
                "select * from lineitem2 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "union all\n" +
                "select * from lineitem3 where L_SHIPDATE between '1993-01-01' AND '1993-12-31'";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/union_q3");
    }

    @Test
    public void testColocateUnionDistinctWithJoinBelow1() {
        String sql = "with cte as(\n" +
                "select L_ORDERKEY, O_ORDERDATE, P_NAME\n" +
                "from lineitem0 \n" +
                "     inner join [bucket] orders on L_ORDERKEY = O_ORDERKEY\n" +
                "     inner join [broadcast] part on L_PARTKEY = P_PARTKEY\n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "UNION\n" +
                "select L_ORDERKEY, O_ORDERDATE, P_NAME\n" +
                "from lineitem1\n" +
                "     inner join [bucket] orders on L_ORDERKEY = O_ORDERKEY\n" +
                "     inner join [broadcast] part on L_PARTKEY = P_PARTKEY \n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                ")\n" +
                "select * from cte;";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/union_q4");
    }

    @Test
    public void testColocateUnionDistinctWithJoinBelow2() {
        String sql = "with cte as(\n" +
                "select O_ORDERDATE, P_NAME\n" +
                "from lineitem0 \n" +
                "     inner join [bucket] orders on L_ORDERKEY = O_ORDERKEY\n" +
                "     inner join [broadcast] part on L_PARTKEY = P_PARTKEY\n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                "UNION\n" +
                "select O_ORDERDATE, P_NAME\n" +
                "from lineitem1\n" +
                "     inner join [bucket] orders on L_ORDERKEY = O_ORDERKEY\n" +
                "     inner join [broadcast] part on L_PARTKEY = P_PARTKEY \n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                ")\n" +
                "select * from cte;";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/union_q5");
    }

    @Test
    public void testColocateUnionDistinctWithJoinAbove() {
        String sql = "with cte as(\n" +
                "select L_ORDERKEY, L_PARTKEY\n" +
                "from lineitem0 \n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31'\n" +
                "UNION\n" +
                "select L_ORDERKEY, L_PARTKEY\n" +
                "from lineitem1\n" +
                "where L_SHIPDATE between '1993-01-01' AND '1993-12-31' \n" +
                ")\n" +
                "select cte.* \n" +
                "from cte \n" +
                "     inner join [bucket] orders on L_ORDERKEY = O_ORDERKEY\n" +
                "     inner join [broadcast] part on L_PARTKEY = P_PARTKEY; ";
        runFileUnitTestHelper(sql, "scheduler/colocate_set/union_q6");
    }
}