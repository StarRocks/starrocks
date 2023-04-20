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
import com.starrocks.common.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ImplicitCastTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @ParameterizedTest
    @MethodSource("predicateImplicitCastSql")
    @Order(1)
    public void testPredicateImplicitCast(Pair<String, String> pair) throws Exception {
        connectContext.getSessionVariable().setEnableStrictType(false);
        String plan = getFragmentPlan(pair.first);
        assertContains(plan, pair.second);
        connectContext.getSessionVariable().setEnableStrictType(true);
        try {
            getFragmentPlan(pair.first);
        } catch (Exception e) {
            assertContains(e.getMessage(), "can not be converted to boolean type");
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedImplicitCastSql")
    @Order(2)
    public void testUnsupportedImplicitCast(String sql) throws Exception {
        connectContext.getSessionVariable().setEnableStrictType(false);
        try {
            getFragmentPlan(sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assertContains(e.getMessage(), "can not be converted to boolean type");
        }
    }

    private static Stream<Arguments> predicateImplicitCastSql() {
        List<Pair<String, String>> sqlList = Lists.newArrayList();
        sqlList.add(Pair.create("select 1 from t1 where 1", "PREDICATES: TRUE"));
        sqlList.add(Pair.create("select 1 from t1 where 100", "PREDICATES: CAST(100 AS BOOLEAN)"));
        sqlList.add(Pair.create("select 1 from t1 where 2.3456", "PREDICATES: CAST(2.3456 AS BOOLEAN)"));
        sqlList.add(Pair.create("select 1 from t1 where '2020-02-02'", "PREDICATES: CAST('2020-02-02' AS BOOLEAN)"));
        sqlList.add(Pair.create("select 1 from t1 where '2.3456'", "PREDICATES: CAST('2.3456' AS BOOLEAN)"));
        sqlList.add(Pair.create("select 1 from t1 where '0'", "0:EMPTYSET"));
        sqlList.add(Pair.create("select 1 and 'a' is null from t1 where 1 and 'a' is null", "0:EMPTYSET"));
        sqlList.add(Pair.create("select 1 or 'a' is null from t1 where 1 or 'a' is null", "PREDICATES: TRUE"));
        sqlList.add(Pair.create("select case v1 when 'a' then 1 else 0 end from t0 where case v1 when 'a' then 1 else 0 end",
                "CAST(if(CAST(1: v1 AS VARCHAR) = 'a', 1, 0) AS BOOLEAN)"));
        sqlList.add(Pair.create("select * from t0 where case v1 when 'a' then 1 else 0 end " +
                "and case v2 when 'a' then 2 else 0 end",
                "PREDICATES: CAST(if(CAST(1: v1 AS VARCHAR) = 'a', 1, 0) AS BOOLEAN), " +
                        "CAST(if(CAST(2: v2 AS VARCHAR) = 'a', 2, 0) AS BOOLEAN)"));
        sqlList.add(Pair.create("select * from tarray where v3[1] and case v1 when 1 then 1 else 0 end and v2",
                "PREDICATES: CAST(3: v3[1] AS BOOLEAN), CAST(if(1: v1 = 1, 1, 0) AS BOOLEAN), CAST(2: v2 AS BOOLEAN)"));
        sqlList.add(Pair.create("select * from bitmap_table where BITMAP_COUNT(id2)",
                "CAST(bitmap_count(2: id2) AS BOOLEAN)"));
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> unsupportedImplicitCastSql() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from tarray where v3");
        sqlList.add("select * from bitmap_table where id2");
        sqlList.add("select * from tarray where v3 and 1");
        sqlList.add("select * from bitmap_table where id2 or abs(1) < abs(2)");
        sqlList.add("select * from test_agg where h1");
        return sqlList.stream().map(e -> Arguments.of(e));
    }
}
