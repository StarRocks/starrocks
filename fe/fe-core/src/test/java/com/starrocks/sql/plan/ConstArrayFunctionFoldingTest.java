// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.plan;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConstArrayFunctionFoldingTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testConstantArrayFunctions() throws Exception {
        String[][] cases = new String[][] {
                {"array_contains(['A', 'B'], 'C')", "<slot 2> : FALSE"},
                {"array_contains(['A', 'B'], 'B')", "<slot 2> : TRUE"},
                {"array_contains(['A', 'B', NULL], NULL)", "<slot 2> : TRUE"},
                {"array_contains(['A', 'B', 'C'], NULL)", "<slot 2> : FALSE"},
                {"array_contains(NULL, NULL)", "<slot 2> : NULL"}, {"array_remove(['A', 'B'], 'B')", "['A']"},
                {"array_remove(['A', 'B', 'B'], 'B')", "['A']"}, {"array_remove(['A', 'B'], 'C')", "['A','B']"},
                {"array_remove(['A', 'B'], NULL)", "['A','B']"}, {"array_remove(['A', 'B', NULL], NULL)", "['A','B']"},
                {"array_remove(['A', 'B', NULL, NULL], NULL)", "['A','B']"},
                {"array_remove(NULL, NULL)", "<slot 2> : NULL"}, {"array_remove(NULL, 'A')", "<slot 2> : NULL"},
                {"array_append(['A', 'B'], 'C')", "['A','B','C']"},
                {"array_append(['A', 'B'], NULL)", "['A','B',NULL]"}, {"array_append(NULL, NULL)", "<slot 2> : NULL"},
                {"array_append(NULL, 'A')", "<slot 2> : NULL"}, {"array_position(['A', 'B'], 'A')", "<slot 2> : 1"},
                {"array_position(['A', 'B'], 'C')", "<slot 2> : 0"},
                {"array_position(['A', 'B'], NULL)", "<slot 2> : 0"},
                {"array_position([NULL,'A', 'B'], NULL)", "<slot 2> : 1"},
                {"array_position(NULL, NULL)", "<slot 2> : NULL"}, {"array_position(NULL, 'A')", "<slot 2> : NULL"},
                {"array_length(['A', 'B'])", "<slot 2> : 2"}, {"array_length([])", "<slot 2> : 0"},
                {"array_length(NULL)", "<slot 2> : NULL"}, {"array_sum([1, 2, 3, 4])", "<slot 2> : 10"},
                {"array_sum([NULL,1, 2, 3, 4])", "<slot 2> : 10"}, {"array_sum([])", "<slot 2> : NULL"},
                {"array_sum(NULL)", "<slot 2> : NULL"}, {"array_sum([NULL, NULL])", "<slot 2> : NULL"},
                {"array_min([1, 2, 3, 4])", "<slot 2> : 1"}, {"array_min([])", "<slot 2> : NULL"},
                {"array_min([NULL, 4, 2, 3])", "<slot 2> : 2"}, {"array_min([NULL, 4, 2, 1, NULL])", "<slot 2> : 1"},
                {"array_min(NULL)", "<slot 2> : NULL"}, {"array_max([1, 2, 3, 4])", "<slot 2> : 4"},
                {"array_max([])", "<slot 2> : NULL"}, {"array_max([NULL, 4, 10, 7])", "<slot 2> : 10"},
                {"array_max([NULL, 4, 7, 11, NULL])", "<slot 2> : 11"}, {"array_max(NULL)", "<slot 2> : NULL"},
                {"array_avg([1, 2, 3, 4])", "<slot 2> : 2.5"}, {"array_avg([NULL,1, 2, 3, 4])", "<slot 2> : 2"},
                {"array_avg([])", "<slot 2> : NULL"}, {"array_avg(NULL)", "<slot 2> : NULL"},
                {"array_avg([NULL, NULL])", "<slot 2> : NULL"}
        };
        String sqlFmt = "select {FUNC}";
        for (String[] tc : cases) {
            String sql = sqlFmt.replace("{FUNC}", tc[0]);
            String expect = tc[1];
            System.out.println(sql);
            String plan = getFragmentPlan(sql);
            assertContains(plan, expect);
        }
    }

    @Test
    public void testConstantArrayDecimal() throws Exception {
        String[][] cases = new String[][] {
                {"array_remove([1.0,2.1,3.2,4.3], 1)", "<slot 2> : [2.1,3.2,4.3]"},
                {"array_position([1.0,2.1,3.2,4.3], 1)", "<slot 2> : 1"},
                {"array_position([1.0,2.1,3.2,4.3], NULL)", "<slot 2> : 0"},
                {"array_avg([1.0, 2.0, 3.0, 4.0])", "<slot 2> : 2.5"},
                {"array_contains([1.0,2.1,3.2,4.3], 1)", "<slot 2> : TRUE"},
                {"array_avg([1.1, 2.22, 3.333, 4.4444, NULL])", "<slot 2> : 2.21948"},
                {"array_sum([1.0, 2.0, 3.0, 4.0])", "<slot 2> : 10.0"},
                {"array_sum([1.1, 2.22, 3.333, 4.4444, NULL])", "<slot 2> : 11.0974"},
        };
        String sqlFmt = "select {FUNC}";
        for (String[] tc : cases) {
            String sql = sqlFmt.replace("{FUNC}", tc[0]);
            String expect = tc[1];
            System.out.println(sql);
            String plan = getFragmentPlan(sql);
            assertContains(plan, expect);
        }
    }
    @Test
    public void testConstArrayFunctionsReturnNullExistsNullArgument() throws Exception {
        String[] queryList = new String[] {"SELECT 'split' as func_name, split(NULL, ',') as result;",
                "SELECT 'split' as func_name, split('a,b', NULL) as result;",
                "SELECT 'str_to_map' as func_name, str_to_map(NULL, ',', ':') as result;",
                "SELECT 'str_to_map' as func_name, str_to_map('a:1,b:2', NULL, ':') as result;",
                "SELECT 'str_to_map' as func_name, str_to_map('a:1,b:2', ',', NULL) as result;",
                "SELECT 'regexp_extract_all' as func_name, regexp_extract_all(NULL, '(\\d+)', 1) as result;",
                "SELECT 'regexp_extract_all' as func_name, regexp_extract_all('abc123', NULL, 1) as result;",
                "SELECT 'regexp_extract_all' as func_name, regexp_extract_all('abc123', '(\\d+)', NULL) as result;",
                "SELECT 'regexp_split' as func_name, regexp_split(NULL, ',', 1) as result;",
                "SELECT 'regexp_split' as func_name, regexp_split('a,b', NULL, 1) as result;",
                "SELECT 'regexp_split' as func_name, regexp_split('a,b', ',', NULL) as result;",
                "SELECT 'array_length' as func_name, array_length(NULL) as result;",
                "SELECT 'array_sum' as func_name, array_sum(NULL) as result;",
                "SELECT 'array_avg' as func_name, array_avg(NULL) as result;",
                "SELECT 'array_min' as func_name, array_min(NULL) as result;",
                "SELECT 'array_max' as func_name, array_max(NULL) as result;",
                "SELECT 'array_distinct' as func_name, array_distinct(NULL) as result;",
                "SELECT 'array_sort' as func_name, array_sort(NULL) as result;",
                "SELECT 'reverse' as func_name, reverse(NULL) as result;",
                "SELECT 'array_join' as func_name, array_join(NULL, ',') as result;",
                "SELECT 'array_join' as func_name, array_join(['a', 'b'], NULL) as result;",
                "SELECT 'array_difference' as func_name, array_difference(NULL) as result;",
                "SELECT 'array_slice' as func_name, array_slice(NULL, 1, 2) as result;",
                "SELECT 'array_slice' as func_name, array_slice([1, 2, 3], NULL, 2) as result;",
                "SELECT 'array_slice' as func_name, array_slice([1, 2, 3], 1, NULL) as result;",
                "SELECT 'array_concat' as func_name, array_concat(NULL, [3, 4]) as result;",
                "SELECT 'array_concat' as func_name, array_concat([1, 2], NULL) as result;",
                "SELECT 'arrays_overlap' as func_name, arrays_overlap(NULL, [1, 2]) as result;",
                "SELECT 'arrays_overlap' as func_name, arrays_overlap([1, 2], NULL) as result;",
                "SELECT 'array_intersect' as func_name, array_intersect(NULL, [3, 4]) as result;",
                "SELECT 'array_intersect' as func_name, array_intersect([1, 2], NULL) as result;",
                "SELECT 'array_cum_sum' as func_name, array_cum_sum(NULL) as result;",
                "SELECT 'array_contains_all' as func_name, array_contains_all(NULL, [1, 2]) as result;",
                "SELECT 'array_contains_all' as func_name, array_contains_all([1, 2], NULL) as result;",
                "SELECT 'array_contains_seq' as func_name, array_contains_seq(NULL, [1, 2]) as result;",
                "SELECT 'array_contains_seq' as func_name, array_contains_seq([1, 2], NULL) as result;",
                "SELECT 'all_match' as func_name, all_match(NULL) as result;",
                "SELECT 'any_match' as func_name, any_match(NULL) as result;",
                "SELECT 'array_generate' as func_name, array_generate(NULL, 1, 1) as result;",
                "SELECT 'array_generate' as func_name, array_generate(1, NULL, 1) as result;",
                "SELECT 'array_generate' as func_name, array_generate(1, 1, NULL) as result;",
                "SELECT 'array_repeat' as func_name, array_repeat(NULL, 3) as result;",
                "SELECT 'array_repeat' as func_name, array_repeat(1, NULL) as result;",
                "SELECT 'array_flatten' as func_name, array_flatten(NULL) as result;",
                "SELECT 'map_size' as func_name, map_size(NULL) as result;",
                "SELECT 'map_keys' as func_name, map_keys(NULL) as result;",
                "SELECT 'map_values' as func_name, map_values(NULL) as result;",
                "SELECT 'map_from_arrays' as func_name, map_from_arrays(NULL, [1, 2]) as result;",
                "SELECT 'map_from_arrays' as func_name, map_from_arrays(['a', 'b'], NULL) as result;",
                "SELECT 'distinct_map_keys' as func_name, distinct_map_keys(NULL) as result;",
                "SELECT 'cardinality' as func_name, cardinality(NULL) as result;",
                "SELECT 'tokenize' as func_name, tokenize(NULL, ' ') as result;",
                "SELECT 'tokenize' as func_name, tokenize('hello world', NULL) as result;"};
        for (String q : queryList) {
            System.out.println(q);
            String plan = getFragmentPlan(q);
            assertContains(plan, "<slot 3> : NULL");
        }
    }

    @Test
    public void testLeftJoin() throws Exception {
        String sql = "select ta.v1, tb.v3 " + "from t0 ta left join t0 tb on ta.v1 = tb.v2 " +
                "where array_contains([1,2,3], tb.v3)";
        String plan = getFragmentPlan(sql);
        assertCContains(plan,
                "  3:HASH JOIN\n" + "  |  join op: INNER JOIN (BROADCAST)\n" + "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 5: v2 = 1: v1");
        assertCContains(plan, "  0:OlapScanNode\n" + "     TABLE: t0\n" + "     PREAGGREGATION: ON\n" +
                "     PREDICATES: array_contains([1,2,3], 6: v3)");
    }

    @Test
    public void testNestedArraySum() throws Exception {
        String sql = "select array_sum([1, array_sum([1, 2, 3]), 3]),array_sum([1, array_sum([1, 2, 3]), 3]);";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "<slot 3> : 10");
    }

    @Test
    public void test() throws Exception {
        String sql = "select\n" +
                "array_sum([]),\n" +
                "array_sum([null]),\n" +
                "array_sum([-170141183460469231731687303715884105728]),\n" +
                "array_sum([170141183460469231731687303715884105727]),\n" +
                "array_sum([-170141183460469231731687303715884105728,170141183460469231731687303715884105727]),\n" +
                "array_sum([-398391541.5063392]),\n" +
                "array_sum([398391541.5063392]),\n" +
                "array_sum([-398391541.5063392,398391541.5063392]),\n" +
                "array_sum([398391541.5063392,398391541.5063392]),\n" +
                "array_sum([-398391541.5063392,-398391541.5063392]),\n" +
                "array_sum([true,false]),\n" +
                "array_sum([1,false]);";
        String s = UtFrameUtils.getPlanThriftString(starRocksAssert.getCtx(), sql);
        Pattern decimalTypePattern = Pattern.compile("type:DECIMAL\\w+, precision:(-)?\\d+, scale:(-)?\\d+");
        Matcher m = decimalTypePattern.matcher(s);
        Assertions.assertTrue(m.find());
        List<String> types = m.results().map(MatchResult::group).collect(Collectors.toList());
        Assertions.assertFalse(types.isEmpty());
        Assertions.assertTrue(types.stream().noneMatch(t -> t.contains("-1")));
    }
}
