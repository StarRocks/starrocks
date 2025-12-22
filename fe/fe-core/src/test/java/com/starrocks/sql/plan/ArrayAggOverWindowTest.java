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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ArrayAggOverWindowTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.unitTestView = false;
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `s1` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int(11) NULL COMMENT \"\",    \n" +
                "  `s1` struct<count bigint, sum double, avg double> NULL COMMENT \"\",    \n" +
                "  `a1` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `a2` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `map_v1` map<int,varchar(65533)>,\n" +
                "  `map_v2` map<int, map<int,double>>,\n" +
                "   nested_array array<array<int>>,\n" +
                "   nested_struct struct<a int, b array<varchar(65533)>>,\n" +
                "   complex_map map<int, struct<x int, y varchar(65533)>>\n" +
                ") ENGINE=OLAP    \n" +
                "DUPLICATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",       \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"true\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"light_schema_change\" = \"true\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");
    }

    @Test
    public void testArrayAgg() throws Exception {
        String sql = "select v1,v2,v3,\n" +
                "      array_agg(v3) \n" +
                "      over(partition by v1, v2)\n" +
                "from t0;";
        assertPlanContains(sql, "array_agg", "PARTITION", "ANALYTIC");
    }

    // Test basic array_agg with different window definitions
    @Test
    public void testArrayAggBasicWindows() throws Exception {
        // Test 1: over() - no partition, no order by
        String sql1 = "select v1, array_agg(v3) over() from t0;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test 2: over(partition by...) - partition only
        String sql2 = "select v1, array_agg(v3) over(partition by v1) from t0;";
        assertPlanContains(sql2, "array_agg", "PARTITION", "ANALYTIC");

        // Test 3: over(partition by... order by...) - partition and order by
        String sql3 = "select v1, array_agg(v3) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql3, "array_agg", "PARTITION", "ORDER BY", "ANALYTIC");

        // Test 4: over(order by...) - order by only
        String sql4 = "select v1, array_agg(v3) over(order by v1) from t0;";
        assertPlanContains(sql4, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with different column types
    @Test
    public void testArrayAggDifferentTypes() throws Exception {
        // Test with bigint columns
        String sql1 = "select v1, array_agg(v2) over(partition by v1 order by v3) from t0;";
        assertPlanContains(sql1, "array_agg", "PARTITION", "ANALYTIC");

        // Test with string columns
        String sql2 = "select k1, array_agg(k2) over(partition by k1 order by k3) from t7;";
        assertPlanContains(sql2, "array_agg", "PARTITION", "ANALYTIC");

        // Test with decimal columns
        String sql3 =
                "select id_decimal, array_agg(id_decimal) over(partition by id_decimal order by id_decimal) " +
                        "from test_all_type;";
        assertPlanContains(sql3, "array_agg", "PARTITION", "ANALYTIC");

        // Test with float columns
        String sql4 = "select t1e, array_agg(t1e) over(partition by t1e order by t1f) from test_all_type;";
        assertPlanContains(sql4, "array_agg", "PARTITION", "ANALYTIC");

        // Test with double columns
        String sql5 = "select t1f, array_agg(t1f) over(partition by t1f order by t1e) from test_all_type;";
        assertPlanContains(sql5, "array_agg", "PARTITION", "ANALYTIC");
    }

    // Test array_agg with ORDER BY clause in function
    @Test
    public void testArrayAggWithOrderBy() throws Exception {
        // Test array_agg(column order by column)
        String sql1 = "select v1, array_agg(v3 order by v2) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "ORDER BY", "ANALYTIC");

        // Test array_agg(column order by different_column)
        String sql2 = "select v1, array_agg(v3 order by v2) over(partition by v1 order by v3) from t0;";
        assertPlanContains(sql2, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with string order by
        String sql3 = "select k1, array_agg(k2 order by k3 desc) over(partition by k1) from t7;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with DISTINCT
    @Test
    public void testArrayAggDistinct() throws Exception {
        // Test array_agg(distinct column)
        String sql1 = "select v1, array_agg(distinct v3) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "DISTINCT", "ANALYTIC");

        // Test array_agg(distinct column order by column)
        String sql2 = "select v1, array_agg(distinct v3 order by v2) over(partition by v1) from t0;";
        assertPlanContains(sql2, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");

        // Test with string distinct
        String sql3 = "select k1, array_agg(distinct k2) over(partition by k1 order by k3) from t7;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ANALYTIC");
    }

    // Test array_agg with complex types (struct, array)
    @Test
    public void testArrayAggComplexTypes() throws Exception {
        // Test with struct column
        String sql1 = "select v1, array_agg(s1) over(partition by v1) from s1;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with array column
        String sql2 = "select v1, array_agg(a1) over(partition by v1) from s1;";
        assertPlanContains(sql2, "array_agg", "ANALYTIC");

        // Test with multiple complex columns
        String sql3 = "select v1, array_agg(a2) over(partition by v1 order by v2) from s1;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with complex window scenarios
    @Test
    public void testArrayAggComplexWindows() throws Exception {
        // Test with multiple partition columns
        String sql1 = "select v1, v2, array_agg(v3) over(partition by v1, v2 order by v3) from t0;";
        assertPlanContains(sql1, "array_agg", "PARTITION", "ANALYTIC");

        // Test with no partition, with order by
        String sql2 = "select v1, array_agg(v3) over(order by v1, v2) from t0;";
        assertPlanContains(sql2, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with partition and multiple order by columns
        String sql3 = "select v1, array_agg(v3) over(partition by v1 order by v2, v3 desc) from t0;";
        assertPlanContains(sql3, "array_agg", "PARTITION", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with decimal precision
    @Test
    public void testArrayAggDecimalTypes() throws Exception {
        // Test with cast decimal
        String sql1 = "select v1, array_agg(cast(v3 as decimal(19,2))) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");
        
        // Test with decimal distinct
        String sql2 =
                "select v1, array_agg(distinct cast(v3 as decimal(10,5))) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql2, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");

        // Test with existing decimal column
        String sql3 =
                "select id_decimal, array_agg(distinct id_decimal) over(partition by id_decimal) from test_all_type;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ANALYTIC");
    }

    // Test array_agg with float/double precision
    @Test
    public void testArrayAggFloatTypes() throws Exception {
        // Test with float
        String sql1 = "select t1e, array_agg(t1e) over(partition by t1e order by t1f) from test_all_type;";
        assertPlanContains(sql1, "array_agg", "PARTITION", "ANALYTIC");

        // Test with double
        String sql2 = "select t1f, array_agg(distinct t1f) over(partition by t1f) from test_all_type;";
        assertPlanContains(sql2, "array_agg", "DISTINCT", "ANALYTIC");

        // Test with cast float
        String sql3 = "select v1, array_agg(cast(v3 as float)) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with mixed data types in same query
    @Test
    public void testArrayAggMixedTypes() throws Exception {
        String sql = "select v1,\n" +
                "       array_agg(v2) over(partition by v1),\n" +
                "       array_agg(distinct v3) over(partition by v1 order by v2),\n" +
                "       array_agg(v3 order by v2 desc) over(partition by v1)\n" +
                "from t0;";
        assertPlanContains(sql, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with complex expressions
    @Test
    public void testArrayAggComplexExpressions() throws Exception {
        // Test with arithmetic expression
        String sql1 = "select v1, array_agg(v2 + v3) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with string concatenation
        String sql2 = "select k1, array_agg(k2 || k3) over(partition by k1) from t7;";
        assertPlanContains(sql2, "array_agg", "ANALYTIC");

        // Test with case expression
        String sql3 = "select v1, array_agg(case when v2 > 5 then v3 else null end) over(partition by v1) from t0;";
        assertPlanContains(sql3, "array_agg", "ANALYTIC");
    }

    // Test array_agg with limit in window (if supported)
    @Test
    public void testArrayAggWithWindowFrame() throws Exception {
        // Test range frame
        String sql1 =
                "select v1, array_agg(v3) " +
                        "over(partition by v1 order by v2 rows between unbounded preceding and current row) from t0;";
        assertPlanContains(sql1, "array_agg", "PARTITION", "ROWS", "ANALYTIC");

        // Test range frame with values
        String sql2 =
                "select v1, array_agg(v3) " +
                        "over(partition by v1 order by v2 range between 2 preceding and 2 following) from t0;";
        Assertions.assertThrows(SemanticException.class, () -> getCostExplain(sql2));
    }

    // Test array_agg with NULL value handling
    @Test
    public void testArrayAggWithNullValues() throws Exception {
        // Test array_agg with NULL values
        String sql1 = "select v1, array_agg(v3) over(partition by v1) from t0 where v3 is null;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test array_agg(distinct) with NULL values
        String sql2 = "select v1, array_agg(distinct v3) over(partition by v1) from t0;";
        assertPlanContains(sql2, "array_agg", "DISTINCT", "ANALYTIC");

        // Test array_agg with ORDER BY and NULL values
        String sql3 = "select v1, array_agg(v3 order by v2 nulls first) over(partition by v1) from t0;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with map types and complex scenarios
    @Test
    public void testArrayAggWithMapTypes() throws Exception {
        // Test with simple map
        String sql1 = "select v1, array_agg(map_v1) over(partition by v1) from s1;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with nested map
        String sql2 = "select v1, array_agg(map_v1) over(partition by v1 order by v2) from s1;";
        assertPlanContains(sql2, "array_agg", "ORDER BY", "ANALYTIC");

        // Test array_agg(distinct) with map
        String sql3 = "select v1, array_agg(distinct map_v2) over(partition by v1) from s1;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ANALYTIC");
    }

    // Test array_agg with various decimal precision and scale combinations
    @Test
    public void testArrayAggWithVariousDecimalPrecisions() throws Exception {
        // Test with different decimal precisions
        String sql1 = "select v1, array_agg(cast(v3 as decimal(10,2))) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql1, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with decimal(38,10)
        String sql2 = "select v1, array_agg(cast(v3 as decimal(38,10))) over(partition by v1) from t0;";
        assertPlanContains(sql2, "array_agg", "ANALYTIC");

        // Test with decimal(5,0) - integer-like decimal
        String sql3 = "select v1, array_agg(cast(v3 as decimal(5,0))) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with high precision decimal
        String sql4 = "select v1, array_agg(distinct cast(v3 as decimal(27,9))) over(partition by v1) from t0;";
        assertPlanContains(sql4, "array_agg", "DISTINCT", "ANALYTIC");
    }

    // Test array_agg with nested complex types
    @Test
    public void testArrayAggWithNestedComplexTypes() throws Exception {
        // Test with nested array
        String sql1 = "select v1, array_agg(nested_array) over(partition by v1) from s1;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with nested struct
        String sql2 = "select v1, array_agg(nested_struct) over(partition by v1 order by v2) from s1;";
        assertPlanContains(sql2, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with complex map
        String sql3 = "select v1, array_agg(distinct complex_map order by v1) over(partition by v1) from s1;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ANALYTIC");
    }

    // Test array_agg with window frame boundaries
    @Test
    public void testArrayAggWithWindowFrameBoundaries() throws Exception {
        // Test with rows between unbounded preceding and current row
        String sql1 =
                "select v1, array_agg(v3) " +
                        "over(partition by v1 order by v2 rows between unbounded preceding and current row) from t0;";
        assertPlanContains(sql1, "array_agg", "PARTITION", "ROWS", "ANALYTIC");

        // Test with range between unbounded preceding and unbounded following
        String sql2 =
                "select v1, array_agg(v3) " +
                        "over(partition by v1 order by v2 range between unbounded preceding and unbounded following)" +
                        " from t0;";
        assertPlanContains(sql2, "array_agg", "PARTITION", "RANGE", "ANALYTIC");

        // Test with rows between current row and unbounded following
        String sql3 =
                "select v1, array_agg(distinct v3) " +
                        "over(partition by v1 order by v2 rows between current row and unbounded following) from t0;";
        Assertions.assertThrows(SemanticException.class, () -> getCostExplain(sql3));

        // Test with range between value preceding and value following
        String sql4 =
                "select v1, array_agg(v3 order by v2) " +
                        "over(partition by v1 order by v2 range between 2 preceding and 2 following) from t0;";
        Assertions.assertThrows(SemanticException.class, () -> getCostExplain(sql4));
    }

    // Test array_agg with different numeric type conversions
    @Test
    public void testArrayAggWithNumericConversions() throws Exception {
        // Test with bigint to int conversion
        String sql1 = "select v1, array_agg(cast(v2 as int)) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with double to decimal conversion
        String sql2 =
                "select t1f, array_agg(cast(t1f as decimal(10,2))) " +
                        "over(partition by t1f order by t1e) from test_all_type;";
        assertPlanContains(sql2, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with float to decimal conversion
        String sql3 =
                "select t1e, array_agg(distinct cast(t1e as decimal(15,5))) over(partition by t1e) from test_all_type;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ANALYTIC");
    }

    // Test array_agg with string operations and functions
    @Test
    public void testArrayAggWithStringOperations() throws Exception {
        // Test with string concatenation
        String sql1 = "select k1, array_agg(concat(k2, k3)) over(partition by k1 order by k2) from t7;";
        assertPlanContains(sql1, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with substring function
        String sql2 = "select k1, array_agg(substring(k2, 1, 5)) over(partition by k1) from t7;";
        assertPlanContains(sql2, "array_agg", "ANALYTIC");

        // Test with upper function
        String sql3 = "select k1, array_agg(distinct upper(k2)) over(partition by k1 order by k3) from t7;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");

        // Test with length function
        String sql4 = "select k1, array_agg(length(k2)) over(partition by k1 order by k2, k3) from t7;";
        assertPlanContains(sql4, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with conditional logic and case statements
    @Test
    public void testArrayAggWithConditionalLogic() throws Exception {
        // Test with case statement
        String sql1 = "select v1, array_agg(case when v2 > 5 then v3 else 0 end) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with if function
        String sql2 = "select v1, array_agg(if(v2 > 5, v3, null)) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql2, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with coalesce function
        String sql3 = "select v1, array_agg(distinct coalesce(v3, -1)) over(partition by v1) from t0;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ANALYTIC");

        // Test with nullif function
        String sql4 = "select v1, array_agg(nullif(v3, 0)) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql4, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with date/time functions and types
    @Test
    public void testArrayAggWithDateTimeTypes() throws Exception {
        // Test with date type
        String sql1 =
                "select id_date, array_agg(id_date) over(partition by id_date order by id_decimal) from test_all_type;";
        assertPlanContains(sql1, "array_agg", "PARTITION", "ANALYTIC");

        // Test with datetime type
        String sql2 =
                "select id_datetime, array_agg(distinct id_datetime) over(partition by id_datetime) from test_all_type;";
        assertPlanContains(sql2, "array_agg", "DISTINCT", "ANALYTIC");

        // Test with date functions
        String sql3 =
                "select id_date, array_agg(year(id_date)) " +
                        "over(partition by id_date order by id_decimal) from test_all_type;";
        assertPlanContains(sql3, "array_agg", "PARTITION", "ANALYTIC");

        // Test with datetime functions
        String sql4 =
                "select id_datetime, array_agg(month(id_datetime)) " +
                        "over(partition by id_datetime order by id_date) from test_all_type;";
        assertPlanContains(sql4, "array_agg", "PARTITION", "ANALYTIC");
    }

    // Test array_agg with mathematical functions and operations
    @Test
    public void testArrayAggWithMathFunctions() throws Exception {
        // Test with absolute value
        String sql1 = "select v1, array_agg(abs(v3)) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql1, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with square root
        String sql2 = "select v1, array_agg(sqrt(abs(v3))) over(partition by v1) from t0;";
        assertPlanContains(sql2, "array_agg", "ANALYTIC");

        // Test with power function
        String sql3 = "select v1, array_agg(distinct pow(v2, 2)) over(partition by v1 order by v3) from t0;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");

        // Test with rounding
        String sql4 = "select t1e, array_agg(round(t1f, 2)) over(partition by t1e order by t1f) from test_all_type;";
        assertPlanContains(sql4, "array_agg", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg with very large datasets and performance scenarios
    @Test
    public void testArrayAggWithLargeDatasets() throws Exception {
        // Test with multiple array_agg calls in same query
        String sql1 = "select v1,\n" +
                "       array_agg(v2) over(partition by v1 order by v3),\n" +
                "       array_agg(distinct v3) over(partition by v1),\n" +
                "       array_agg(v2 order by v3 desc) over(partition by v1)\n" +
                "from t0;";
        assertPlanContains(sql1, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");

        // Test with many partition columns
        String sql2 = "select v1, v2, v3,\n" +
                "       array_agg(v1 + v2 + v3) over(partition by v1, v2, v3 order by v1)\n" +
                "from t0;";
        assertPlanContains(sql2, "array_agg", "PARTITION", "ORDER BY", "ANALYTIC");

        // Test with complex expressions and functions
        String sql3 = "select k1,\n" +
                "       array_agg(length(k2) + length(k3)) over(partition by k1 order by k2),\n" +
                "       array_agg(distinct concat(k2, k3)) over(partition by k1)\n" +
                "from t7;";
        assertPlanContains(sql3, "array_agg", "DISTINCT", "ORDER BY", "ANALYTIC");
    }

    // Test array_agg edge cases and error conditions
    @Test
    public void testArrayAggEdgeCases() throws Exception {
        // Test with constant values
        String sql1 = "select v1, array_agg(1) over(partition by v1) from t0;";
        assertPlanContains(sql1, "array_agg", "ANALYTIC");

        // Test with expression resulting in same value
        String sql2 = "select v1, array_agg(v2 - v2) over(partition by v1) from t0;";
        assertPlanContains(sql2, "array_agg", "ANALYTIC");

        // Test with very small decimal values
        String sql3 = "select v1, array_agg(cast(0.00001 as decimal(10,8))) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "ANALYTIC");

        // Test with very large decimal values
        String sql4 = "select v1, array_agg(cast(999999.99 as decimal(10,2))) over(partition by v1) from t0;";
        assertPlanContains(sql4, "array_agg", "ANALYTIC");
    }

    // Test array_agg compatibility with other window functions
    @Test
    public void testArrayAggWithOtherWindowFunctions() throws Exception {
        // Test array_agg with row_number
        String sql1 =
                "select v1, " +
                        "array_agg(v2) over(partition by v1), row_number() over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql1, "array_agg", "ROW_NUMBER", "ANALYTIC");

        // Test array_agg with rank
        String sql2 =
                "select v1, array_agg(distinct v2) " +
                        "over(partition by v1 order by v3), rank() over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql2, "array_agg", "DISTINCT", "RANK", "ANALYTIC");

        // Test array_agg with dense_rank
        String sql3 =
                "select v1, array_agg(v2 order by v3) " +
                        "over(partition by v1), dense_rank() over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql3, "array_agg", "ORDER BY", "DENSE_RANK", "ANALYTIC");

        // Test array_agg with lag
        String sql4 =
                "select v1, array_agg(v2) " +
                        "over(partition by v1 order by v2), lag(v2, 1) over(partition by v1 order by v2) from t0;";
        assertPlanContains(sql4, "array_agg", "LAG", "ANALYTIC");
    }
}
