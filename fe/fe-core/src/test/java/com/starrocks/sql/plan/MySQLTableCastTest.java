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

import com.google.api.client.util.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class MySQLTableCastTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }


    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("explicitCastSqls")
    void testExplicitCast(Pair<String, String> pair) throws Exception {
        String sql = pair.first;
        String plan = getFragmentPlan(sql);
        assertContains(plan, pair.second);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("implicitCastSqls")
    void testImplicitCast(Pair<String, String> pair) throws Exception {
        String sql = pair.first;
        String plan = getFragmentPlan(sql);
        assertContains(plan, pair.second);
    }

    private static Stream<Arguments> explicitCastSqls() {
        List<Pair<String, String>> sqls = Lists.newArrayList();
        sqls.add(Pair.create("select * from ods_order where cast(cast(org_order_no as float) as date)",
                "WHERE (CAST(CAST(org_order_no AS FLOAT) AS DATE))"));

        sqls.add(Pair.create("select * from ods_order where cast(org_order_no as varchar)", "WHERE (org_order_no)"));

        sqls.add(Pair.create("select * from ods_order where cast(org_order_no as date)", "WHERE (CAST(org_order_no AS DATE))"));

        sqls.add(Pair.create("select * from ods_order join mysql_table where cast(org_order_no as date) " +
                        "and k1 = cast(k2 as date)", "WHERE (k1 = CAST(k2 AS DATE))"));

        sqls.add(Pair.create("select * from ods_order where cast(org_order_no as int)",
                "predicates: CAST(CAST(org_order_no AS INT) AS BOOLEAN)"));

        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> implicitCastSqls() {
        List<Pair<String, String>> sqls = Lists.newArrayList();
        sqls.add(Pair.create("select * from ods_order where org_order_no", "WHERE (org_order_no)"));
        sqls.add(Pair.create("select * from ods_order where org_order_no or up_trade_no",
                "WHERE ((org_order_no) OR (up_trade_no))"));
        sqls.add(Pair.create("select * from ods_order where org_order_no and cast(up_trade_no as date)",
                "WHERE (org_order_no) AND (CAST(up_trade_no AS DATE))"));
        sqls.add(Pair.create("select * from ods_order where org_order_no or (up_trade_no = order_dt) or (mchnt_no = pay_st)",
                "WHERE (((org_order_no) OR (up_trade_no = order_dt)) OR (mchnt_no = pay_st))"));
        sqls.add(Pair.create("select * from ods_order join mysql_table where k1  = 'a' and order_dt = 'c'",
                "WHERE (order_dt = 'c')"));
        sqls.add(Pair.create("select * from (select * from ods_order join mysql_table where k1  = 'a' and order_dt = 'c')" +
                " t1 where t1.k2 = 'c'", "WHERE (k2 = 'c') AND (k1 = 'a')"));

        return sqls.stream().map(e -> Arguments.of(e));
    }
}
