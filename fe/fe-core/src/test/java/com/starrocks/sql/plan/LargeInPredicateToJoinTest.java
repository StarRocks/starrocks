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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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

    @Test
    public void testPrint() throws Exception {
        String sql = "select * from t0 where v1 in (1, 2, 3, 4)";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(plan, "RAW_VALUES(constantType=BIGINT, count=4, sample=1, 2, 3, 4)");
    }

    @Test
    public void testRawValuesOperatorMethods() {
        List<ColumnRefOperator> columnRefs1 = Lists.newArrayList(
                new ColumnRefOperator(1, Type.BIGINT, "const_value", true)
        );
        List<ColumnRefOperator> columnRefs2 = Lists.newArrayList(
                new ColumnRefOperator(2, Type.BIGINT, "const_value", true)
        );
        
        Type intType = Type.BIGINT;
        Type stringType = Type.VARCHAR;
        String rawText1 = "1, 2, 3, 4";
        String rawText2 = "5, 6, 7, 8";
        String longRawText = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25";
        
        List<Object> rawConstants1 = Lists.newArrayList(1L, 2L, 3L, 4L);
        List<Object> rawConstants2 = Lists.newArrayList(5L, 6L, 7L, 8L);
        List<Object> stringConstants = Lists.newArrayList("a", "b", "c", "d");
        
        // Test LogicalRawValuesOperator
        LogicalRawValuesOperator logical1 = new LogicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        LogicalRawValuesOperator logical2 = new LogicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        LogicalRawValuesOperator logical3 = new LogicalRawValuesOperator(columnRefs2, intType, rawText2, rawConstants2, 4);
        LogicalRawValuesOperator logical4 = new LogicalRawValuesOperator(columnRefs1, stringType, rawText1, stringConstants, 4);
        LogicalRawValuesOperator logicalLong = new LogicalRawValuesOperator(columnRefs1, intType, longRawText, rawConstants1, 4);
        
        // Test equals method
        assertEquals(logical1, logical2);
        assertNotEquals(logical1, logical3);
        assertNotEquals(logical1, logical4);
        assertNotEquals(null, logical1);
        assertNotEquals("not an operator", logical1);
        assertEquals(logical1, logical1); // self equality
        
        // Test hashCode method
        assertEquals(logical1.hashCode(), logical2.hashCode());
        assertNotEquals(logical1.hashCode(), logical3.hashCode());
        
        // Test toString method
        String toString1 = logical1.toString();
        assertContains(toString1, "LogicalRawValues");
        assertContains(toString1, "constantType=BIGINT");
        assertContains(toString1, "count=4");
        
        // Test long text truncation in toString
        String toStringLong = logicalLong.toString();
        assertContains(toStringLong, "...");
        
        // Test PhysicalRawValuesOperator
        PhysicalRawValuesOperator physical1 = new PhysicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        PhysicalRawValuesOperator physical2 = new PhysicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        PhysicalRawValuesOperator physical3 = new PhysicalRawValuesOperator(columnRefs2, intType, rawText2, rawConstants2, 4);
        PhysicalRawValuesOperator physical4 = new PhysicalRawValuesOperator(
                columnRefs1, stringType, rawText1, stringConstants, 4);
        PhysicalRawValuesOperator physicalLong = new PhysicalRawValuesOperator(
                columnRefs1, intType, longRawText, rawConstants1, 4);
        
        // Test equals method for PhysicalRawValuesOperator
        assertEquals(physical1, physical2);
        assertNotEquals(physical1, physical3);
        assertNotEquals(physical1, physical4);
        assertNotEquals(null, physical1);
        assertNotEquals("not an operator", physical1);
        assertEquals(physical1, physical1); // self equality
        
        // Test hashCode method for PhysicalRawValuesOperator
        assertEquals(physical1.hashCode(), physical2.hashCode());
        assertNotEquals(physical1.hashCode(), physical3.hashCode());
        
        // Test toString method for PhysicalRawValuesOperator
        String physicalToString1 = physical1.toString();
        assertContains(physicalToString1, "PhysicalRawValues");
        assertContains(physicalToString1, "constantType=BIGINT");
        assertContains(physicalToString1, "count=4");
        
        // Test long text truncation in toString for PhysicalRawValuesOperator
        String physicalToStringLong = physicalLong.toString();
        assertContains(physicalToStringLong, "...");
        
        // Test getter methods coverage
        assertEquals(columnRefs1, logical1.getColumnRefSet());
        assertEquals(intType, logical1.getConstantType());
        assertEquals(rawText1, logical1.getRawText());
        assertEquals(rawConstants1, logical1.getRawConstantList());
        assertEquals(4, logical1.getConstantCount());
        
        assertEquals(intType, physical1.getConstantType());
        assertEquals(rawText1, physical1.getRawText());
        assertEquals(rawConstants1, physical1.getRawConstantList());
        assertEquals(4, physical1.getConstantCount());
        assertEquals(columnRefs1, physical1.getColumnRefSet());
    }

    @Test
    public void testLargeInPredicateMethods() {

        Type intType = Type.BIGINT;
        Type stringType = Type.VARCHAR;
        String rawText1 = "1, 2, 3, 4";
        String rawText2 = "5, 6, 7, 8";
        List<Object> rawConstants1 = Lists.newArrayList(1L, 2L, 3L, 4L);
        List<Object> rawConstants2 = Lists.newArrayList(5L, 6L, 7L, 8L);
        List<Object> stringConstants = Lists.newArrayList("a", "b", "c", "d");
        
        SlotRef slotRef1 = new SlotRef(null, "v1");
        SlotRef slotRef2 = new SlotRef(null, "v2");
        List<Expr> inList1 = Lists.newArrayList(new IntLiteral(1L, Type.BIGINT));
        List<Expr> inList2 = Lists.newArrayList(new IntLiteral(2L, Type.BIGINT));
        List<Expr> stringInList = Lists.newArrayList(new StringLiteral("a"));

        LargeInPredicate largeIn1 = new LargeInPredicate(slotRef1, rawText1, rawConstants1, 4, false, inList1, null);
        LargeInPredicate largeIn2 = new LargeInPredicate(slotRef1, rawText1, rawConstants1, 4, false, inList1, null);
        LargeInPredicate largeIn3 = new LargeInPredicate(slotRef2, rawText2, rawConstants2, 4, false, inList2, null);
        LargeInPredicate largeIn4 = new LargeInPredicate(slotRef1, rawText1, stringConstants, 4, true, stringInList, null);

        // Test LargeInPredicate equals method
        assertEquals(largeIn1, largeIn2);
        assertNotEquals(largeIn1, largeIn3);
        assertNotEquals(largeIn1, largeIn4);
        assertNotEquals(null, largeIn1);
        assertNotEquals("not a predicate", largeIn1);
        assertEquals(largeIn1, largeIn1);

        // Test LargeInPredicate hashCode method
        assertEquals(largeIn1.hashCode(), largeIn2.hashCode());
        assertNotEquals(largeIn1.hashCode(), largeIn3.hashCode());

        // Test LargeInPredicate toString method
        String largeInToString1 = largeIn1.toString();
        assertContains(largeInToString1, "LargeInPredicate");

        // Test LargeInPredicate getter methods
        assertEquals(rawText1, largeIn1.getRawText());
        assertEquals(rawConstants1, largeIn1.getRawConstantList());
        assertEquals(4, largeIn1.getConstantCount());
        assertEquals(4, largeIn1.getInElementNum());

        // Test LargeInPredicateOperator by directly creating instances
        ColumnRefOperator columnRef1 = new ColumnRefOperator(1, Type.BIGINT, "v1", true);
        ColumnRefOperator columnRef2 = new ColumnRefOperator(2, Type.BIGINT, "v2", true);
        
        List<ScalarOperator> children1 = Lists.newArrayList(columnRef1);
        List<ScalarOperator> children2 = Lists.newArrayList(columnRef2);
        
        LargeInPredicateOperator largeInOp1 = new LargeInPredicateOperator(rawText1, rawConstants1, 4, false, intType, children1);
        LargeInPredicateOperator largeInOp2 = new LargeInPredicateOperator(rawText1, rawConstants1, 4, false, intType, children1);
        LargeInPredicateOperator largeInOp3 = new LargeInPredicateOperator(rawText2, rawConstants2, 4, false, intType, children2);
        LargeInPredicateOperator largeInOp4 = new LargeInPredicateOperator(
                rawText1, stringConstants, 4, true, stringType, children1);

        // Test LargeInPredicateOperator equals method
        assertEquals(largeInOp1, largeInOp2);
        assertNotEquals(largeInOp1, largeInOp3);
        assertNotEquals(largeInOp1, largeInOp4);
        assertNotEquals(null, largeInOp1);
        assertEquals(largeInOp1, largeInOp1);

        // Test LargeInPredicateOperator hashCode method
        assertEquals(largeInOp1.hashCode(), largeInOp2.hashCode());
        assertNotEquals(largeInOp1.hashCode(), largeInOp3.hashCode());

        // Test LargeInPredicateOperator toString method
        String largeInOpToString1 = largeInOp1.toString();
        assertContains(largeInOpToString1, "v1 IN");

        // Test LargeInPredicateOperator getter methods
        assertEquals(rawText1, largeInOp1.getRawText());
        assertEquals(rawConstants1, largeInOp1.getRawConstantList());
        assertEquals(4, largeInOp1.getConstantCount());
        assertEquals(intType, largeInOp1.getConstantType());
        assertFalse(largeInOp1.isNotIn());
    }
}
