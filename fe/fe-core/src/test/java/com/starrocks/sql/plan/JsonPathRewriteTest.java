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

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
    }

    @ParameterizedTest
    @MethodSource("jsonPathRewriteTestCases")
    public void testExtendPredicateParameterized(String sql, String expectedPlanFragment, String expectedColumnPath)
            throws Exception {
        connectContext.getSessionVariable().setEnableJSONV2DictOpt(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setQueryTimeoutS(10000);
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlanFragment);

        String verbosePlan = getVerboseExplain(sql);
        assertContains(verbosePlan, expectedColumnPath);
    }

    private static Stream<Arguments> jsonPathRewriteTestCases() {
        return of(
                // [1] Projection: JSON expression in SELECT list
                Arguments.of(
                        "select get_json_string(c2, 'f2') as f2_str from extend_predicate",
                        "  1:Project\n  |  <slot 3> : 4: c2.f2\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f2(varchar)]"
                ),
                // [2]
                Arguments.of(
                        "select get_json_string(c2, '$.f2.f3') as f2_str from extend_predicate",
                        "  1:Project\n  |  <slot 3> : 4: c2.f2.f3\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f2(varchar)/f3(varchar)]"
                ),
                // [3]
                Arguments.of(
                        "select get_json_string(c2, 'f1.f2.f3') as f2_str from extend_predicate",
                        "  1:Project\n  |  <slot 3> : 4: c2.f1.f2.f3\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f1(varchar)/f2(varchar)/f3(varchar)]"
                ),
                // [4]
                Arguments.of(
                        "select get_json_string(c2, 'f1[1]') as f2_str from extend_predicate",
                        "  |  <slot 3> : get_json_string(2: c2, 'f1[1]')\n",
                        ""
                ),
                // TODO: support this case
                // [5]
                Arguments.of(
                        "select get_json_string(c2, 'f1\".a\"') as f2_str from extend_predicate",
                        "  |  <slot 3> : get_json_string(2: c2, 'f1\".a\"')\n",
                        ""
                ),
                // [6] Filter: JSON expression in WHERE clause with different function
                Arguments.of(
                        "select * from extend_predicate where get_json_double(c2, 'f3') > 1.5",
                        "PREDICATES: 3: c2.f3 > 1.5",
                        ""
                ),
                // [7] Order By: JSON expression in ORDER BY
                Arguments.of(
                        "select * from extend_predicate order by get_json_string(c2, 'f4')",
                        "order by: <slot 3> 3: get_json_string ASC",
                        ""
                ),
                // [8] Aggregation: JSON expression in GROUP BY
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
                // [9] Aggregation: JSON expression in aggregation function
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
                // [10] Join: JSON expression in join condition
                Arguments.of(
                        "select * from extend_predicate t1 join extend_predicate2 t2 on get_json_int(t1.c2, 'f7') = " +
                                "get_json_int(t2.c2, 'f7')",
                        "  |  <slot 5> : 10: c2.f7\n",
                        ""
                ),
                // [11] Join: JSON expression in projection after join
                Arguments.of(
                        "select get_json_string(t1.c2, 'f8'), get_json_string(t2.c2, 'f8') from extend_predicate t1 " +
                                "join extend_predicate2 t2 on t1.c1 = t2.c1",
                        "  3:Project\n" +
                                "  |  <slot 3> : 3: c1\n" +
                                "  |  <slot 8> : 9: c2.f8\n",
                        "ExtendedColumnAccessPath: [/c2(varchar)/f8(varchar)]"
                ),
                // [12] Join: self-join with JSON predicate pushdown
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
                // [13] Join: self-join with JSON projection pushdown
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
                // [14] Join: self-join without JSON expression pushdown
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
                // [15] JSON expression in HAVING clause
                Arguments.of(
                        """
                                select get_json_int(c2, 'f9'), count(*) from extend_predicate 
                                group by get_json_int(c2, 'f9') 
                                having get_json_int(c2, 'f9') > 10
                                """,
                        "  1:Project\n  |  <slot 3> : 5: c2.f9\n",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f9(bigint(20))]"
                ),
                // [16] JSON expression in complex filter (AND/OR)
                Arguments.of(
                        "select * from extend_predicate where get_json_int(c2, 'f10') = 1 or get_json_string(c2, " +
                                "'f11') = 'abc'",
                        "PREDICATES: (3: c2.f10 = 1) OR (4: c2.f11 = 'abc')",
                        " ExtendedColumnAccessPath: [/c2(bigint(20))/f10(bigint(20)), /c2(varchar)/f11(varchar)]"
                ),
                // [17] JSON expression in nested function
                Arguments.of(
                        "select abs(get_json_int(c2, 'f12')) from extend_predicate",
                        " <slot 3> : abs(4: c2.f12)",
                        "ExtendedColumnAccessPath: [/c2(bigint(20))/f12(bigint(20))]"
                ),
                // [18] JSON path exceeding depth limit should not be rewritten
                Arguments.of(
                        "select get_json_string(c2, 'f1.f2.f3.f4.f5.f6.f7.f8.f9.f10.f11.f12.f13.f14.f15.f16.f17.f18" +
                                ".f19.f20.f21') from extend_predicate",
                        "<slot 3> : 4: c2.f1.f2.f3.f4.f5.f6.f7.f8.f9.f10.f11.f12.f13.f14.f15.f16.f17.f18.f19.f20.f21\n",
                        ""
                ),
                // [19] JSON path with mixed data types should not be rewritten
                Arguments.of(
                        "select get_json_string(c2, 'f2'), get_json_int(c2, 'f2') from extend_predicate",
                        "get_json_string(2: c2, 'f2')",
                        ""
                ),
                // [20] MetaScan
                Arguments.of(
                        "select dict_merge(get_json_string(c2, 'f1'), 255) from extend_predicate [_META_]",
                        "0:MetaScan\n" +
                                "     Table: extend_predicate\n" +
                                "     <id 6> : dict_merge_c2.f1\n",
                        "     ExtendedColumnAccessPath: [/c2(varchar)/f1(varchar)]\n"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("globalDictRewriteCases")
    public void testGlobalDictRewrite(String sql, String expectedPlanFragment) throws Exception {
        connectContext.getSessionVariable().setEnableJSONV2DictOpt(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        String verbosePlan = getFragmentPlan(sql);
        assertContains(verbosePlan, expectedPlanFragment);
    }

    private static Stream<Arguments> globalDictRewriteCases() {
        return of(
                // 1. Predicate
                Arguments.of(
                        "select count(*) from extend_predicate2 where get_json_string(c2, 'f1') = 'value'",
                        "PREDICATES: DictDecode(7: c2.f1, [<place-holder> = 'value'])"
                ),
                // 2. Predicate with count on get_json_string
                Arguments.of(
                        "select count(get_json_string(c2, 'f1')) from extend_predicate2 where get_json_string(c2, " +
                                "'f1') = 'value'",
                        "PREDICATES: DictDecode(6: c2.f1, [<place-holder> = 'value'])"
                ),

                // 3. Aggregation with group by JSON path
                Arguments.of(
                        "select get_json_string(c2, 'f1') k1, count(*) from extend_predicate2 group by k1",
                        "<slot 7> : DictDefine(6: c2.f1, [<place-holder>])"
                ),
                // 4. Aggregation with min on JSON path
                Arguments.of(
                        "select min(get_json_string(c2, 'f1')) from extend_predicate2",
                        "<slot 7> : DictDefine(6: c2.f1, [<place-holder>])"
                ),

                // 5. Multiple JSON path accesses in select and where clause
                Arguments.of(
                        "select get_json_string(c2, 'f1'), get_json_string(c2, 'f2') from extend_predicate2 where " +
                                "get_json_string(c2, 'f1') = 'value' and get_json_string(c2, 'f2') = 'other'",
                        "PREDICATES: DictDecode(7: c2.f1, [<place-holder> = 'value']), DictDecode(8: c2.f2, " +
                                "[<place-holder> = 'other'])"
                ),
                // 6. Aggregation on JSON path
                Arguments.of(
                        "select get_json_string(c2, 'f3'), sum(c1) from extend_predicate2 group by get_json_string" +
                                "(c2, 'f3')",
                        "<slot 7> : DictDefine(6: c2.f3, [<place-holder>])"
                ),
                // 7. Nested function using JSON path
                Arguments.of(
                        "select upper(get_json_string(c2, 'f4')) from extend_predicate2",
                        "<slot 3> : DictDecode(5: c2.f4, [upper(<place-holder>)])"
                ),
                // 8. JSON path in having clause
                Arguments.of(
                        "select get_json_string(c2, 'f5'), count(*) from extend_predicate2 group by get_json_string" +
                                "(c2, 'f5') having get_json_string(c2, 'f5') = 'foo'",
                        "<slot 7> : DictDefine(6: c2.f5, [<place-holder>])"
                ),
                // 9. JSON path in order by
                Arguments.of(
                        "select get_json_string(c2, 'f6') from extend_predicate2 order by get_json_string(c2, 'f6')",
                        "<slot 6> : DictDefine(5: c2.f6, [<place-holder>])"
                ),
                // 10. JSON path in both select and where, different fields
                Arguments.of(
                        "select get_json_string(c2, 'f7') from extend_predicate2 where get_json_string(c2, 'f8') = " +
                                "'bar'",
                        "<slot 3> : DictDecode(6: c2.f7, [<place-holder>])"
                ),
                // 11. JSON path in a function argument
                Arguments.of(
                        "select length(get_json_string(c2, 'f9')) from extend_predicate2",
                        "<slot 3> : DictDecode(5: c2.f9, [length(<place-holder>)])"
                ),
                // 12. JSON path in a filter with OR
                Arguments.of(
                        "select count(*) from extend_predicate2 where get_json_string(c2, 'f10') = 'a' or " +
                                "get_json_string(c2, 'f11') = 'b'",
                        " PREDICATES: (DictDecode(8: c2.f10, [<place-holder> = 'a'])) OR (DictDecode(9: c2.f11, " +
                                "[<place-holder> = 'b']))"
                ),
                // 13. JSON path in a subquery
                Arguments.of(
                        "select * from extend_predicate2 where c1 in (select c1 from extend_predicate2 where " +
                                "get_json_string(c2, 'f12') = 'baz')",
                        "PREDICATES: 3: c1 IS NOT NULL, DictDecode(7: c2.f12, [<place-holder> = 'baz'])"
                ),
                // 14. JSON path in a case expression
                Arguments.of(
                        "select case when get_json_string(c2, 'f13') = 'x' then 1 else 0 end from extend_predicate2",
                        "<slot 3> : DictDecode(5: c2.f13, [if(<place-holder> = 'x', 1, 0)])"
                ),
                // 15. JSON with MinMaxStats optimization
                Arguments.of(
                        "select get_json_string(c2, 'f1') k1, count(*) from extend_predicate2 group by k1",
                        "<slot 7> : DictDefine(6: c2.f1, [<place-holder>])"
                )

        );
    }
}
