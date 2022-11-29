// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SqlModeHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

class ValidatePlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }


    @ParameterizedTest
    @MethodSource("invalidDateSqlList")
    void invalidDateSqlTest(String sql) {
        FeConstants.runningUnitTest = false;
        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_FORBID_INVALID_DATE);
        String plan = "";
        try {
            plan = getFragmentPlan(sql);
            fail("sql cannot execute with validation, but plan is: " + plan);
        } catch (Exception e) {
            assertContains(e.getMessage(), "Incorrect");
        }

        connectContext.getSessionVariable().setSqlMode(0);
        try {
            getFragmentPlan(sql);
        } catch (Exception e) {
            fail("sql can execute without validation, but error happens: " + plan);
        }
    }


    private static Stream<Arguments> invalidDateSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select cast('a' as date)");
        sqlList.add("select cast('a' as datetime)");
        sqlList.add("select cast(20221131 as date)");
        sqlList.add("select cast('2021-12-32' as date) from t1");
        sqlList.add("select * from test_all_type where id_date > '2021-11-31'");
        sqlList.add("select * from test_all_type where id_date > '2021-11-29' or id_date < '2021-09-00'");
        sqlList.add("select * from test_all_type t1 join test_all_type t2 on t1.id_date > " +
                "t2.id_date + str2date('2021-09-00', 'yyyy-MM-dd')");
        sqlList.add("select * from test_all_type where id_date > '2021-11-30' or id_date < " +
                "cast(str_to_date('2021-11-31', 'yyyy-MM-dd') as int)");
        sqlList.add("select *, cast(20211131 as date) from t1 union all select *, cast(20211130 as date) from t2");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

}
