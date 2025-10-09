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

import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.common.LargeInPredicateException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LargeInPredicateToJoinTest extends PlanTestBase {

    @BeforeEach
    public void before() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(100000);
    }

    private void assertLargeInTransformation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "RAW_VALUES");
    }

    private void assertLargeNotInTransformation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LEFT ANTI JOIN");
        assertContains(plan, "RAW_VALUES");
    }

    private void assertNoLargeInTransformation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "LEFT SEMI JOIN");
        assertNotContains(plan, "LEFT ANTI JOIN");
        assertNotContains(plan, "RAW_VALUES");
    }

    private void assertLargeInException(String sql, String expectedMessage) {
        ExceptionChecker.expectThrowsWithMsg(LargeInPredicateException.class,
                expectedMessage, () -> getFragmentPlan(sql));
    }


    // ========== Basic Transformation Tests ==========

    @Test
    public void testBasicTransformations() throws Exception {
        // Test basic IN transformation
        assertLargeInTransformation("select * from t0 where v1 in (1, 2, 3, 4) and v2 = 3 and v3 < 6");
        
        // Test basic NOT IN transformation
        assertLargeNotInTransformation("select * from t0 where v1 not in (1, 2, 3, 4)");
    }

    @Test
    public void testSupportedDataTypes() throws Exception {
        // Test integer types (smallint, int, bigint) with BIGINT constants
        assertLargeInTransformation("select * from tall where tb in (1, 2, 3, 4)"); // smallint
        assertLargeInTransformation("select * from tall where tc in (100, 200, 300, 400)"); // int
        assertLargeInTransformation("select * from tall where td in (1000, 2000, 3000, 4000)"); // bigint
        
        // Test string types (varchar, char) with string constants
        assertLargeInTransformation("select * from tall where ta in ('a', 'b', 'c', 'd')"); // varchar
        assertLargeInTransformation("select * from tall where tt in ('str1', 'str2', 'str3', 'str4')"); // char
    }

    @Test
    public void testComplexExpressions() throws Exception {
        // Test arithmetic expressions
        String sql1 = "select * from t0 where (v1 + 1) in (2, 3, 4, 5)";
        String plan1 = getFragmentPlan(sql1);
        assertContains(plan1, "LEFT SEMI JOIN");
        assertContains(plan1, "RAW_VALUES");
        assertContains(plan1, "equal join conjunct: 5: add = 4: const_value");

        // Test function calls with casting
        String sql2 = "select * from t0 where cast(abs(v1) as bigint) in (1, 2, 3, 4)";
        String plan2 = getFragmentPlan(sql2);
        assertContains(plan2, "LEFT SEMI JOIN");
        assertContains(plan2, "RAW_VALUES");
        assertContains(plan2, "equal join conjunct: 5: cast = 4: const_value");

        // Test other complex expressions
        assertLargeInTransformation("select * from tall where upper(ta) in ('A', 'B', 'C', 'D')");
        assertLargeInTransformation("select * from t0 where cast(v1 as string) in ('1', '2', '3', '4')");
        assertLargeInTransformation("select * from tall where cast(tb as bigint) in (10, 20, 30, 40)");
        assertLargeInTransformation("select * from t0 where coalesce(v1, v2) in (1, 2, 3, 4)");
        
        // Test CASE expression
        String sql3 = "select * from t0 where case when v2 > 100 then v1 else v2 end in (1, 2, 3, 4)";
        String plan3 = getFragmentPlan(sql3);
        assertContains(plan3, "LEFT SEMI JOIN");
        assertContains(plan3, "RAW_VALUES");
        assertContains(plan3, "equal join conjunct: 5: if = 4: const_value");
    }

    @Test
    public void testSQLContexts() throws Exception {
        // Test in different SQL contexts
        assertLargeInTransformation("select v1, count(*) from t0 group by v1 having v1 in (1, 2, 3, 4)"); // HAVING
        assertLargeInTransformation("select * from t1 where v4 in (select v1 from t0 where v1 in (1, 2, 3, 4))"); // Subquery
        assertLargeInTransformation("select * from t0 where v1 in (1, 2, 3, 4) order by v2 limit 10"); // ORDER BY + LIMIT
        assertLargeInTransformation("with cte as (select * from t0 where v1 in (1, 2, 3, 4)) select * from cte"); // CTE
        
        // Test with JOINs
        String joinSql = "select * from t0 join t1 on t0.v1 = t1.v4 where t0.v1 in (1, 2, 3, 4)";
        String joinPlan = getFragmentPlan(joinSql);
        assertContains(joinPlan, "LEFT SEMI JOIN");
        assertContains(joinPlan, "RAW_VALUES");
        assertContains(joinPlan, "INNER JOIN"); // Original join should still exist
    }

    @Test
    public void testPredicateCombination() throws Exception {
        // Test LargeInPredicate combined with other predicates using AND
        String sql = "select * from t0 where v1 in (1, 2, 3, 4) and v2 > 100";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: const_value\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: v2 > 100");
    }

    @Test
    public void testSpecialValues() throws Exception {
        // Test with duplicate values
        assertLargeInTransformation("select * from t0 where v1 in (1, 1, 2, 2, 3, 3)");
        
        // Test with special characters in strings
        assertLargeInTransformation("select * from tall where ta in ('a\\'b', 'c\"d', 'e\\\\f', 'g\\nh')");
        
        // Test NULL handling - should NOT use LargeInPredicate
        assertNoLargeInTransformation("select * from t0 where v1 in (1, 2, 3, NULL)");
    }

    @Test
    public void testAdvancedSQLFeatures() throws Exception {
        // Test with window functions
        String windowSql = "select v1, row_number() over (order by v2) from t0 where v1 in (1, 2, 3, 4)";
        String windowPlan = getFragmentPlan(windowSql);
        assertContains(windowPlan, "LEFT SEMI JOIN");
        assertContains(windowPlan, "RAW_VALUES");
        assertContains(windowPlan, "ANALYTIC");
        
        // Test with UNION
        String unionSql = "select * from t0 where v1 in (1, 2, 3, 4) union all select * from t0 where v1 in (5, 6, 7, 8)";
        String unionPlan = getFragmentPlan(unionSql);
        assertContains(unionPlan, "LEFT SEMI JOIN");
        assertContains(unionPlan, "RAW_VALUES");
        assertContains(unionPlan, "UNION");
        
        // Test with set operations
        assertLargeInTransformation("select * from t0 where v1 in (1, 2, 3, 4) intersect select * from t0 where v2 > 100");
        
        // Test with complex aggregation
        assertLargeInTransformation("select v1, count(*), sum(v2), avg(v3) from t0 where v1 in (1, 2, 3, 4)" +
                " group by v1 having count(*) > 1");
        
        // Test with arithmetic expressions
        assertLargeInTransformation("select * from t0 where (v1 * v2 + v3) in (100, 200, 300, 400)");
        
        // Test broadcast hint
        String broadcastSql = "select * from t0 where v1 in (1, 2, 3, 4)";
        String broadcastPlan = getFragmentPlan(broadcastSql);
        assertContains(broadcastPlan, "LEFT SEMI JOIN");
        assertContains(broadcastPlan, "RAW_VALUES");
        assertContains(broadcastPlan, "BROADCAST");
    }


    // ========== Error Cases and Limitations ==========

    @Test
    public void testLimitationsAndExceptions() {
        // Test multiple LargeInPredicates in one query
        assertLargeInException(
                "select * from t0 where v1 in (1, 2, 3, 4) and v2 in (10, 20, 30, 40)",
                "LargeInPredicate does not support multiple LargeInPredicate in one query");
        
        // Test OR compound predicates
        assertLargeInException(
                "select * from t0 where v1 in (1, 2, 3, 4) or v2 > 100",
                "LargeInPredicate does not support OR compound predicates");
        
        // Test unsupported type combinations
        assertLargeInException(
                "select * from tall where ta in (1, 2, 3, 4)", // varchar with integers
                "LargeInPredicate only supports");
        
        assertLargeInException(
                "select * from tall where th in ('2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04')", // datetime
                "LargeInPredicate only supports");
    }

    @Test
    public void testFallbackScenarios() throws Exception {
        // Test float types - should fallback to regular InPredicate
        assertNoLargeInTransformation("select * from tall where te in (1.1, 2.2, 3.3, 4.4)");
        
        // Test small IN list (below threshold)
        connectContext.getSessionVariable().setLargeInPredicateThreshold(10);
        try {
            assertNoLargeInTransformation("select * from t0 where v1 in (1, 2)");
        } finally {
            connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
        }
        
        // Test disabled feature
        connectContext.getSessionVariable().setEnableLargeInPredicate(false);
        try {
            String sql = "select * from t0 where v1 in (1, 2, 3, 4)";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "LEFT SEMI JOIN");
            assertNotContains(plan, "RAW_VALUES");
            assertContains(plan, "PREDICATES");
        } finally {
            connectContext.getSessionVariable().setEnableLargeInPredicate(true);
        }
    }

    @Test
    public void testComplexScenarios() throws Exception {
        // Test nested subqueries
        assertLargeInTransformation("select * from t0 where v1 in (select v4 from t1 where v4 in (1, 2, 3, 4))");
        
        // Test multi-level JOINs
        String multiJoinSql = "select * from t0 join t1 on t0.v1 = t1.v4 join tall on t1.v5 = tall.tb" +
                " where t0.v1 in (1, 2, 3, 4)";
        String multiJoinPlan = getFragmentPlan(multiJoinSql);
        assertContains(multiJoinPlan, "LEFT SEMI JOIN");
        assertContains(multiJoinPlan, "RAW_VALUES");
        assertContains(multiJoinPlan, "INNER JOIN");
        
        // Test complex join conditions
        String complexJoinSql = "select * from t0 join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 where t0.v1 in (1, 2, 3, 4)";
        String complexJoinPlan = getFragmentPlan(complexJoinSql);
        assertContains(complexJoinPlan, "LEFT SEMI JOIN");
        assertContains(complexJoinPlan, "RAW_VALUES");
        assertContains(complexJoinPlan, "INNER JOIN");
        
        // Test window function with partitioning
        String windowPartitionSql = "select v1, v2, row_number() over (partition by v1 order by v2) from t0 " +
                "where v1 in (1, 2, 3, 4)";
        String windowPartitionPlan = getFragmentPlan(windowPartitionSql);
        assertContains(windowPartitionPlan, "LEFT SEMI JOIN");
        assertContains(windowPartitionPlan, "RAW_VALUES");
        assertContains(windowPartitionPlan, "ANALYTIC");
    }

    @Test
    public void testCardinality() throws Exception {
        String sql = "select * from t0 where v1 in (1, 2, 3, 4, 5, 6, 7, 8, 9)";
        String plan = getCostExplain(sql);
        assertContains(plan, "1:RAW_VALUES\n" +
                "     RAW VALUES\n" +
                "     constant count: 9\n" +
                "     constant type: BIGINT\n" +
                "     sample values: 1, 2, 3, 4, 5, 6, 7, 8, 9\n" +
                "     cardinality: 9\n" +
                "     column statistics: \n" +
                "     * const_value-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");
    }

    @Test
    public void testRuntimeFilter() throws Exception {
        String sql = "select * from t0 where v1 in (1, 2, 3, 4)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [4: const_value, BIGINT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (4: const_value), remote = false\n" +
                "  |  output columns: 1, 2, 3\n" +
                "  |  cardinality: 4");
        assertContains(plan, "0:OlapScanNode\n" +
                "     table: t0, rollup: t0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=3.0\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v1)");
    }
}
