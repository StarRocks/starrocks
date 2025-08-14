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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static java.util.stream.Stream.of;

public class JsonPathRewriteTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        starRocksAssert.withTable("create table extend_predicate( c1 int, c2 json ) properties('replication_num'='1')");
        starRocksAssert.withTable("create table extend_predicate2( c1 int, c2 json ) properties" +
                "('replication_num'='1')");
    }

    @ParameterizedTest
    @MethodSource("jsonPathRewriteTestCases")
    public void testExtendPredicateParameterized(String sql, String expectedPlanFragment, String expectedColumnPath)
            throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlanFragment);

        String verbosePlan = getVerboseExplain(sql);
        assertContains(verbosePlan, expectedColumnPath);
    }

    private static Stream<Arguments> jsonPathRewriteTestCases() {
        return of(
                // Projection: JSON expression in SELECT list
                Arguments.of(
                        "select get_json_string(c2, 'f2') as f2_str from extend_predicate",
                        "  1:Project\n  |  <slot 3> : 4: c2.f2\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f2(varchar)]"
                ),
                Arguments.of(
                        "select get_json_string(c2, '$.f2.f3') as f2_str from extend_predicate",
                        "  1:Project\n  |  <slot 3> : 4: c2.f2.f3\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f2(varchar)/f3(varchar)]"
                ),
                Arguments.of(
                        "select get_json_string(c2, 'f1.f2.f3') as f2_str from extend_predicate",
                        "  1:Project\n  |  <slot 3> : 4: c2.f1.f2.f3\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f1(varchar)/f2(varchar)/f3(varchar)]"
                ),
                Arguments.of(
                        "select get_json_string(c2, 'f1[1]') as f2_str from extend_predicate",
                        "  |  <slot 3> : get_json_string(2: c2, 'f1[1]')\n",
                        ""
                ),
                // TODO: support this case
                Arguments.of(
                        "select get_json_string(c2, 'f1\".a\"') as f2_str from extend_predicate",
                        "  |  <slot 3> : get_json_string(2: c2, 'f1\".a\"')\n",
                        ""
                ),
                // Filter: JSON expression in WHERE clause with different function
                Arguments.of(
                        "select * from extend_predicate where get_json_double(c2, 'f3') > 1.5",
                        "PREDICATES: 3: c2.f3 > 1.5",
                        ""
                ),
                // Order By: JSON expression in ORDER BY
                Arguments.of(
                        "select * from extend_predicate order by get_json_string(c2, 'f4')",
                        "order by: <slot 3> 3: get_json_string ASC",
                        ""
                ),
                // Aggregation: JSON expression in GROUP BY
                Arguments.of(
                        "select get_json_int(c2, 'f5'), count(*) from extend_predicate group by get_json_int(c2, 'f5')",
                        "  2:AGGREGATE (update finalize)\n" +
                                "  |  output: count(*)\n" +
                                "  |  group by: 3: get_json_int\n" +
                                "  |  \n" +
                                "  1:Project\n" +
                                "  |  <slot 3> : 5: c2.f5\n",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f5(bigint(20))]"
                ),
                // Aggregation: JSON expression in aggregation function
                Arguments.of(
                        "select sum(get_json_int(c2, 'f6')) from extend_predicate",
                        "  2:AGGREGATE (update finalize)\n" +
                                "  |  output: sum(3: get_json_int)\n" +
                                "  |  group by: \n" +
                                "  |  \n" +
                                "  1:Project\n" +
                                "  |  <slot 3> : 5: c2.f6\n",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f6(bigint(20))]"
                ),
                // Join: JSON expression in join condition
                Arguments.of(
                        "select * from extend_predicate t1 join extend_predicate2 t2 on get_json_int(t1.c2, 'f7') = " +
                                "get_json_int(t2.c2, 'f7')",
                        "  |  <slot 5> : 9: c2.f7\n",
                        ""
                ),
                // Join: JSON expression in projection after join
                Arguments.of(
                        "select get_json_string(t1.c2, 'f8'), get_json_string(t2.c2, 'f8') from extend_predicate t1 " +
                                "join extend_predicate2 t2 on t1.c1 = t2.c1",
                        "  3:Project\n" +
                                "  |  <slot 3> : 3: c1\n" +
                                "  |  <slot 8> : 10: c2.f8\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f8(varchar)]"
                ),
                // Join: self-join with JSON predicate pushdown
                Arguments.of(
                        """
                                select t1.c2 
                                from extend_predicate t1 
                                join extend_predicate t2 on 
                                    get_json_int(t1.c1, 'f1') = get_json_int(t2.c1, 'f1') 
                                where get_json_int(t1.c2, 'f2') > 10
                                """,
                        "c2.f2",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f2(bigint(20))]"
                ),
                // Join: self-join with JSON projection pushdown
                Arguments.of(
                        """
                                select get_json_int(t1.c2 , 'f2'), get_json_int(t2.c2, 'f2')
                                from extend_predicate t1 
                                join extend_predicate t2 on 
                                    get_json_int(t1.c1, 'f1') = get_json_int(t2.c1, 'f1') 
                                """,
                        "c2.f2",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f2(bigint(20))]"
                ),
                // Join: self-join without JSON expression pushdown
                Arguments.of(
                        """
                                select t1.c2 
                                from extend_predicate t1 
                                join extend_predicate t2 on 
                                    get_json_int(t1.c1, 'f1') = get_json_int(t2.c1, 'f1') 
                                """,
                        "get_json_int",
                        "equal join conjunct: [5: get_json_int, BIGINT, true] = [6: get_json_int, BIGINT, true]"
                ),
                // JSON expression in HAVING clause
                Arguments.of(
                        "select get_json_int(c2, 'f9'), count(*) from extend_predicate group by get_json_int(c2, " +
                                "'f9') having get_json_int(c2, 'f9') > 10",
                        "  1:Project\n" +
                                "  |  <slot 3> : 5: c2.f9\n",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f9(bigint(20))]"
                ),
                // JSON expression in complex filter (AND/OR)
                Arguments.of(
                        "select * from extend_predicate where get_json_int(c2, 'f10') = 1 or get_json_string(c2, " +
                                "'f11') = 'abc'",
                        "PREDICATES: (3: c2.f10 = 1) OR (4: c2.f11 = 'abc')",
                        " ExtendedColumnAccessPath: [/c2(bigint(20))/f10(bigint(20)), /c2(varchar)/f11(varchar)]"
                ),
                // JSON expression in nested function
                Arguments.of(
                        "select abs(get_json_int(c2, 'f12')) from extend_predicate",
                        " <slot 3> : abs(4: c2.f12)",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f12(bigint(20))]"
                ),
                // JSON path exceeding depth limit should not be rewritten
                Arguments.of(
                        "select get_json_string(c2, 'f1.f2.f3.f4.f5.f6.f7.f8.f9.f10.f11.f12.f13.f14.f15.f16.f17.f18" +
                                ".f19.f20.f21') from extend_predicate",
                        "  |  <slot 3> : get_json_string(2: c2, 'f1.f2.f3.f4.f5.f6.f7.f8.f9.f10.f11.f12.f13.f14.f1.." +
                                ".')",
                        ""
                ),
                // JSON path with mixed data types should not be rewritten
                Arguments.of(
                        "select get_json_string(c2, 'f2'), get_json_int(c2, 'f2') from extend_predicate",
                        "get_json_string(2: c2, 'f2')",
                        ""
                )

        );
    }
}
