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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class MvRewriteEnumerateTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() throws Exception {
        MVTestBase.tearDown();
    }

    @ParameterizedTest(name = "{index}-{0}-{1}-{2}")
    @MethodSource("generateArguments_ArrayAgg")
    public void testArrayAgg(String select, String predicate, String groupBy) throws Exception {
        String mvName = "mv_array";
        starRocksAssert.useTable("json_tbl");
        createAndRefreshMv("CREATE MATERIALIZED VIEW IF NOT EXISTS `mv_array`\n" +
                "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                "REFRESH ASYNC\n" +
                "AS \n" +
                "SELECT \n" +
                "    get_json_string(`d_user`, 'region') AS `region`, \n" +
                "    get_json_string(`d_user`, 'gender') AS `gender`, \n" +
                "    array_agg_distinct(get_json_string(d_user, 'gender') ) AS `distinct_gender`, \n" +
                "    array_agg_distinct(get_json_int(d_user, 'age') ) AS `distinct_age`, \n" +
                "    array_agg_distinct(cast(get_json_string(d_user, 'age') as int) ) AS `distinct_string_age`\n" +
                "FROM `json_tbl`\n" +
                "GROUP BY region, `gender`");

        String query = String.format("select %s from json_tbl  %s  %s", select, predicate, groupBy);
        starRocksAssert.query(query).explainContains(mvName);
    }

    @Test
    public void testArrayAgg_NotSupport() throws Exception {
        String mvName = "mv_array";
        starRocksAssert.useTable("json_tbl");
        createAndRefreshMv("CREATE MATERIALIZED VIEW IF NOT EXISTS `mv_array`\n" +
                "DISTRIBUTED BY HASH(`gender`) BUCKETS 2\n" +
                "REFRESH ASYNC\n" +
                "AS \n" +
                "SELECT \n" +
                "    get_json_string(`d_user`, 'region') AS `region`, \n" +
                "    get_json_string(`d_user`, 'gender') AS `gender`, \n" +
                "    array_agg_distinct(get_json_string(d_user, 'gender') ) AS `distinct_gender`\n" +
                "FROM `json_tbl`\n" +
                "GROUP BY region, `gender`");

        // different group by key
        starRocksAssert.query("SELECT " +
                " get_json_string(d_user, 'sss'), " +
                " count(distinct get_json_string(d_user, 'gender')) " +
                "FROM json_tbl " +
                "GROUP BY 1 ").explainWithout(mvName);

        // different aggregation
        starRocksAssert.query("SELECT " +
                " get_json_string(d_user, 'region'), " +
                " count(distinct get_json_string(d_user, 'age')) " +
                "FROM json_tbl " +
                "GROUP BY 1 ").explainWithout(mvName);
    }

    private static Stream<Arguments> generateArguments_ArrayAgg() {
        List<String> selectList = List.of(
                "array_agg_distinct(get_json_string(d_user, 'gender'))",
                "array_sort(array_agg_distinct(get_json_string(d_user, 'gender') ))",
                "array_length(array_agg_distinct(get_json_string(d_user, 'gender') ))",
                "count(distinct get_json_string(d_user, 'gender') )",
                "sum(distinct get_json_int(d_user, 'age') )",
                "sum(distinct cast(get_json_string(d_user, 'age') as int) )"
        );
        List<String> predicatelist = List.of("", "where get_json_string(d_user, 'gender') = 'male'");
        List<String> groupList = List.of(
                "",
                "group by get_json_string(d_user, 'region')",
                "group by get_json_string(d_user, 'gender')",
                "group by get_json_string(d_user, 'region'), get_json_string(d_user, 'gender') "
        );

        var enumerator = new TestCaseEnumerator(List.of(selectList.size(), predicatelist.size(), groupList.size()));
        return enumerator.enumerate().map(argList ->
                TestCaseEnumerator.ofArguments(argList, selectList, predicatelist, groupList));
    }

    /**
     * Enumerate all test cases from combinations
     */
    static class TestCaseEnumerator {

        private List<List<Integer>> testCases;

        public TestCaseEnumerator(List<Integer> space) {
            this.testCases = Lists.newArrayList();
            List<Integer> testCase = Lists.newArrayList();
            search(space, testCase, 0);
        }

        private void search(List<Integer> space, List<Integer> testCase, int k) {
            if (k >= space.size()) {
                testCases.add(Lists.newArrayList(testCase));
                return;
            }

            int n = space.get(k);
            for (int i = 0; i < n; i++) {
                testCase.add(i);
                search(space, testCase, k + 1);
                testCase.remove(testCase.size() - 1);
            }
        }

        public Stream<List<Integer>> enumerate() {
            return testCases.stream();
        }

        @SafeVarargs
        public static <T> Arguments ofArguments(List<Integer> permutation, List<T>... args) {
            Object[] objects = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                int index = permutation.get(i);
                objects[i] = args[i].get(index);
            }
            return Arguments.of(objects);
        }
    }

}
