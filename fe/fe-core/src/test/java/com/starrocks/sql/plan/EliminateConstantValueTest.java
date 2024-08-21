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

import com.starrocks.common.FeConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EliminateConstantValueTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @AfterClass
    public static void afterClass() {
        PlanTestBase.afterClass();
    }

    @Test
    public void testWithCrossJoin() throws Exception {
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from lineitem_partition t1 join (select '2000-01-01' as col) t2 on true ";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : '2000-01-01'\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition");
        }
        {
            String sql = "select t1.*, t2.col from lineitem_partition t1 join (select '2000-01-01' as col) t2 on true ";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 3> : 3: L_SUPPKEY\n" +
                    "  |  <slot 4> : 4: L_LINENUMBER\n" +
                    "  |  <slot 5> : 5: L_QUANTITY\n" +
                    "  |  <slot 6> : 6: L_EXTENDEDPRICE\n" +
                    "  |  <slot 7> : 7: L_DISCOUNT\n" +
                    "  |  <slot 8> : 8: L_TAX\n" +
                    "  |  <slot 9> : 9: L_RETURNFLAG\n" +
                    "  |  <slot 10> : 10: L_LINESTATUS\n" +
                    "  |  <slot 11> : 11: L_SHIPDATE\n" +
                    "  |  <slot 12> : 12: L_COMMITDATE\n" +
                    "  |  <slot 13> : 13: L_RECEIPTDATE\n" +
                    "  |  <slot 14> : 14: L_SHIPINSTRUCT\n" +
                    "  |  <slot 15> : 15: L_SHIPMODE\n" +
                    "  |  <slot 16> : 16: L_COMMENT\n" +
                    "  |  <slot 17> : 17: PAD\n" +
                    "  |  <slot 19> : '2000-01-01'\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partitio");
        }
    }

    @Test
    public void testWithInnerJoin() throws Exception {
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from lineitem_partition t1 join (select '2000-01-01' as col) t2 on true ";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : '2000-01-01'\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition");
        }

        // join on single column
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from lineitem_partition t1 join (select '2000-01-01' as col) t2 on t1.L_SHIPDATE = t2.col";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : '2000-01-01'\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 11: L_SHIPDATE = '2000-01-01', CAST(11: L_SHIPDATE AS DATETIME) IS NOT NULL");
        }

        // join on multi columns
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col1, t2.col2 " +
                    "from lineitem_partition t1 join (select '2000-01-01' as col1, 'key1' as col2) t2 " +
                    "on t1.L_SHIPDATE = t2.col1 and t1.L_PARTKEY = t2.col2;";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : '2000-01-01'\n" +
                    "  |  <slot 20> : 'key1'\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 11: L_SHIPDATE = '2000-01-01', CAST(2: L_PARTKEY AS VARCHAR(1048576)) = 'key1', " +
                    "CAST(11: L_SHIPDATE AS DATETIME) IS NOT NULL, CAST(2: L_PARTKEY AS VARCHAR(1048576)) IS NOT NULL");
        }
    }

    @Test
    public void testWithLeftOuterJoin() throws Exception {
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from lineitem_partition t1 left outer join (select '2000-01-01' as col) t2 on true ";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : '2000-01-01'\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition");
        }

        // join on single column
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from lineitem_partition t1 left outer join (select '2000-01-01' as col) t2 on t1.L_SHIPDATE = t2.col";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : CASE WHEN CAST(11: L_SHIPDATE AS DATETIME) = CAST('2000-01-01' AS DATETIME) " +
                    "THEN '2000-01-01' ELSE NULL END\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition");
        }

        // join on multi columns
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col1, t2.col2 " +
                    "from lineitem_partition t1 left outer join (select '2000-01-01' as col1, 'key1' as col2) t2 " +
                    "on t1.L_SHIPDATE = t2.col1 and t1.L_PARTKEY = t2.col2;";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 1> : 1: L_ORDERKEY\n" +
                    "  |  <slot 2> : 2: L_PARTKEY\n" +
                    "  |  <slot 19> : CASE WHEN 25: expr THEN '2000-01-01' ELSE NULL END\n" +
                    "  |  <slot 20> : CASE WHEN 25: expr THEN 'key1' ELSE NULL END\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 21> : CAST(11: L_SHIPDATE AS DATETIME)\n" +
                    "  |  <slot 22> : CAST(2: L_PARTKEY AS VARCHAR(1048576))\n" +
                    "  |  <slot 23> : 21: cast = CAST('2000-01-01' AS DATETIME)\n" +
                    "  |  <slot 24> : 22: cast = 'key1'\n" +
                    "  |  <slot 25> : (23: expr) AND (24: expr)\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition");
        }
    }

    @Test
    public void testWithRightOuterJoin1() throws Exception {
        // right outer join which constant operators are in right side cannot be eliminated.
        String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                "from lineitem_partition t1 right outer join (select '2000-01-01' as col) t2 on true ";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "  4:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----3:Project\n" +
                "  |    |  <slot 19> : '2000-01-01'\n" +
                "  |    |  \n" +
                "  |    2:UNION\n" +
                "  |       constant exprs: \n" +
                "  |           NULL");
    }

    @Test
    public void testWithRightOuterJoin2() throws Exception {
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from (select '2000-01-01' as col) t2 right outer join lineitem_partition t1 on true ";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 2> : '2000-01-01'\n" +
                    "  |  <slot 3> : 3: L_ORDERKEY\n" +
                    "  |  <slot 4> : 4: L_PARTKEY\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition");
        }

        // join on single column
        {
            String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                    "from (select '2000-01-01' as col) t2 right outer join lineitem_partition t1 on t1.L_SHIPDATE = t2.col";
            String plan = getFragmentPlan(sql);
            PlanTestBase.assertContains(plan, "  1:Project\n" +
                    "  |  <slot 2> : CASE WHEN CAST(13: L_SHIPDATE AS DATETIME) = CAST('2000-01-01' AS DATETIME) " +
                    "THEN '2000-01-01' ELSE NULL END\n" +
                    "  |  <slot 3> : 3: L_ORDERKEY\n" +
                    "  |  <slot 4> : 4: L_PARTKEY\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: lineitem_partition\n" +
                    "     PREAGGREGATION: ON");
        }
    }

    @Test
    public void testWithLeftSemiJoin() throws Exception {
        String sql = "select * " +
                "from (select '2000-01-01' as col) t2 left semi join lineitem_partition t1 on t1.L_SHIPDATE = t2.col";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "  5:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 21: cast = 20: cast");
    }

    @Test
    public void testWithRightSemiJoin() throws Exception {
        String sql = "select * " +
                "from (select '2000-01-01' as col) t2 right semi join lineitem_partition t1 on t1.L_SHIPDATE = t2.col";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "  5:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 20: cast = 21: cast");
    }

    @Test
    public void testJoinWithMultiRows() throws Exception {
        String sql = "select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col " +
                "from lineitem_partition t1 join (select '2000-01-01' as col union all select '2000-01-01' as col) t2 on true ";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "  3:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: ");
    }
}
