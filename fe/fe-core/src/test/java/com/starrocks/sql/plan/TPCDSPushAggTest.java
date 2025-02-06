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
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

public class TPCDSPushAggTest extends TPCDS1TTestBase {
    @BeforeAll
    public static void beforeAll() {
        FeConstants.unitTestView = false;
    }

    @AfterAll
    public static void afterAll() {
        FeConstants.unitTestView = true;
    }

    private String check(int mode, String sql, int aggNum) throws Exception {
        String plan = getPlan(mode, sql);
        int actual = StringUtils.countMatches(plan, ":AGGREGATE ");
        String msg = "\nmode: " + mode + ", except: " + aggNum + ", actual: " + actual + "\n" + plan;
        Assertions.assertEquals(aggNum, actual, msg);
        return plan;
    }

    private void check(int mode, String sql, int aggNum, boolean planChanged, String origPlan) throws Exception {
        String plan = check(mode, sql, aggNum);
        if (planChanged) {
            Assertions.assertNotEquals(origPlan, plan);
        } else {
            Assertions.assertEquals(origPlan, plan);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testPushDownProvider")
    public void testTPCDSPushDownAgg(String sql, int orig, int auto, boolean autoChange, int force, boolean forceChange, int mid,
                                     boolean midChange, int high, boolean highChange) throws Exception {
        String origPlan = check(-1, sql, orig);
        check(0, sql, auto, autoChange, origPlan);
        check(1, sql, force, forceChange, origPlan);
        check(2, sql, mid, midChange, origPlan);
        check(3, sql, high, highChange, origPlan);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testUnPushDownProvider")
    public void testTPCDSUnPushDownAgg(String sql, int orig, int auto, int force, int mid, int high) throws Exception {
        check(-1, sql, orig);
        check(0, sql, auto);
        check(1, sql, force);
        check(2, sql, mid);
        check(3, sql, high);
    }

    @Test
    public void testQuery58() throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        String sql = getTPCDS("Q58");
        String plan = getCostExplain(sql);
        assertContains(plan, "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 73049\n" +
                "  |       probe runtime filters:\n" +
                "  |       - filter_id = 3, probe_expr = (191: d_date)");
    }

    //    @ParameterizedTest(name = "{0}")
    //    @MethodSource("testPushDownProvider")
    public void debugTPCDSPushDownAgg(String sql, int orig, int auto, boolean autoChange, int force, boolean forceChange, int mid,
                                      boolean midChange, int high, boolean highChange) throws Exception {
        orig = getAggNum(-1, sql);
        auto = getAggNum(0, sql);
        force = getAggNum(1, sql);
        mid = getAggNum(2, sql);
        high = getAggNum(3, sql);

        String origPlan = getPlan(-1, sql);
        String autoPlan = getPlan(0, sql);
        String forcePlan = getPlan(1, sql);
        String midPlan = getPlan(2, sql);
        String highPlan = getPlan(3, sql);

        System.out.printf("Arguments.of(\"%s\", %d, %d, %s, %d, %s, %d, %s, %d, %s),\n", sql, orig,
                auto, !origPlan.equals(autoPlan),
                force, !origPlan.equals(forcePlan),
                mid, !origPlan.equals(midPlan),
                high, !origPlan.equals(highPlan));
    }

    //    @ParameterizedTest(name = "{0}")
    //    @MethodSource("testUnPushDownProvider")
    public void debugTPCDSUnPushDownAgg(String sql, int orig, int auto, int force, int mid, int high) throws Exception {
        orig = getAggNum(-1, sql);
        auto = getAggNum(0, sql);
        force = getAggNum(1, sql);
        mid = getAggNum(2, sql);
        high = getAggNum(3, sql);
        System.out.printf("Arguments.of(\"%s\", %d, %d, %d, %d, %d),\n", sql, orig, auto, force, mid, high);
    }

    // @Test
    public void debugPlan() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000000L);
        String sql = "Q75";
        String plan = getPlan(1, sql);
        System.out.println(plan);
    }

    private int getAggNum(int cboPushDownAggregateMode, String sql) throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(cboPushDownAggregateMode);
        sql = getTPCDS(sql);
        String plan = getFragmentPlan(sql);
        return StringUtils.countMatches(plan, ":AGGREGATE ");
    }

    private String getPlan(int cboPushDownAggregateMode, String sql) throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(cboPushDownAggregateMode);
        sql = getTPCDS(sql);
        return getFragmentPlan(sql).replaceAll("\\d+", "1");
    }

    private static Stream<Arguments> testPushDownProvider() {
        // orig(-1), auto(0), force(1), mid(2), high(3)
        Arguments[] cases = new Arguments[] {
                Arguments.of("Q01", 4, 4, false, 6, true, 4, false, 6, true),
                Arguments.of("Q02", 2, 6, true, 6, true, 6, true, 6, true),
                Arguments.of("Q03", 2, 2, false, 4, true, 4, true, 4, true),
                // Although the number of aggregators is the same, the aggregator was pushed down.
                // This is caused by the CTE. orig: CTE inline, auto~high: CTE
                Arguments.of("Q04", 12, 12, true, 12, true, 12, true, 12, true),
                Arguments.of("Q05", 8, 16, true, 16, true, 16, true, 16, true),
                Arguments.of("Q08", 4, 6, true, 6, true, 6, true, 6, true),
                Arguments.of("Q11", 8, 8, true, 8, true, 8, true, 8, true),
                Arguments.of("Q12", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q15", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q19", 2, 2, false, 4, true, 2, false, 2, false),
                Arguments.of("Q20", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q23_1", 10, 13, true, 13, true, 13, true, 13, true),
                Arguments.of("Q24_1", 6, 6, false, 7, true, 6, false, 6, false),
                Arguments.of("Q24_2", 6, 6, false, 7, true, 6, false, 6, false),
                Arguments.of("Q30", 4, 4, false, 6, true, 4, false, 4, false),
                Arguments.of("Q31", 4, 8, true, 8, true, 8, true, 8, true),
                Arguments.of("Q33", 8, 8, false, 14, true, 14, true, 14, true),
                Arguments.of("Q37", 2, 2, false, 8, true, 6, true, 7, true),
                Arguments.of("Q38", 8, 14, true, 20, true, 14, true, 17, true),
                Arguments.of("Q41", 4, 4, false, 6, true, 4, false, 4, false),
                Arguments.of("Q42", 2, 4, true, 4, true, 4, true, 4, true),
                Arguments.of("Q43", 2, 4, true, 4, true, 4, true, 4, true),
                Arguments.of("Q45", 6, 6, false, 8, true, 6, false, 8, true),
                Arguments.of("Q46", 2, 2, false, 4, true, 2, false, 2, false),
                Arguments.of("Q47", 2, 2, true, 4, true, 4, true, 4, true),
                Arguments.of("Q51", 4, 4, false, 8, true, 8, true, 8, true),
                Arguments.of("Q52", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q53", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q54", 9, 9, false, 18, true, 11, true, 17, true),
                Arguments.of("Q55", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q56", 8, 8, false, 14, true, 14, true, 14, true),
                Arguments.of("Q57", 2, 2, true, 4, true, 4, true, 4, true),
                Arguments.of("Q58", 6, 12, true, 12, true, 12, true, 12, true),
                Arguments.of("Q59", 2, 4, true, 4, true, 4, true, 4, true),
                Arguments.of("Q60", 8, 8, false, 14, true, 14, true, 14, true),
                Arguments.of("Q63", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q65", 6, 6, false, 10, true, 10, true, 10, true),
                Arguments.of("Q68", 1, 1, false, 3, true, 1, false, 1, false),
                Arguments.of("Q70", 4, 6, true, 6, true, 6, true, 6, true),
                Arguments.of("Q71", 2, 2, false, 8, true, 8, true, 8, true),
                Arguments.of("Q74", 8, 8, true, 8, true, 8, true, 8, true),
                Arguments.of("Q75", 4, 4, false, 16, true, 4, false, 4, false),
                Arguments.of("Q77", 14, 26, true, 26, true, 26, true, 26, true),
                Arguments.of("Q78", 6, 6, false, 9, true, 6, false, 6, false),
                Arguments.of("Q79", 2, 2, false, 4, true, 2, false, 2, false),
                Arguments.of("Q81", 4, 4, false, 6, true, 4, false, 4, false),
                Arguments.of("Q82", 2, 2, false, 8, true, 6, true, 7, true),
                Arguments.of("Q83", 6, 12, true, 12, true, 12, true, 12, true),
                Arguments.of("Q87", 8, 14, true, 20, true, 14, true, 17, true),
                Arguments.of("Q89", 2, 2, false, 4, true, 4, true, 4, true),
                Arguments.of("Q91", 2, 4, true, 4, true, 4, true, 4, true),
                Arguments.of("Q97", 6, 6, false, 12, true, 10, true, 12, true),
                Arguments.of("Q98", 2, 2, false, 4, true, 4, true, 4, true),
        };

        return Arrays.stream(cases);
    }

    private static Stream<Arguments> testUnPushDownProvider() {
        // orig(-1), auto(0), force(1), mid(2), high(3)
        Arguments[] cases = new Arguments[] {
                Arguments.of("Q06", 6, 6, 6, 6, 6),
                Arguments.of("Q07", 2, 2, 2, 2, 2),
                Arguments.of("Q09", 30, 30, 30, 30, 30),
                Arguments.of("Q10", 2, 2, 2, 2, 2),
                Arguments.of("Q13", 2, 2, 2, 2, 2),
                Arguments.of("Q14_1", 16, 16, 16, 16, 16),
                Arguments.of("Q14_2", 12, 12, 12, 12, 12),
                Arguments.of("Q16", 4, 4, 4, 4, 4),
                Arguments.of("Q17", 2, 2, 2, 2, 2),
                Arguments.of("Q18", 2, 2, 2, 2, 2),
                Arguments.of("Q21", 2, 2, 2, 2, 2),
                Arguments.of("Q22", 2, 2, 2, 2, 2),
                Arguments.of("Q25", 2, 2, 2, 2, 2),
                Arguments.of("Q26", 2, 2, 2, 2, 2),
                Arguments.of("Q27", 2, 2, 2, 2, 2),
                Arguments.of("Q28", 24, 24, 24, 24, 24),
                Arguments.of("Q29", 2, 2, 2, 2, 2),
                Arguments.of("Q32", 4, 4, 4, 4, 4),
                Arguments.of("Q34", 2, 2, 2, 2, 2),
                Arguments.of("Q35", 2, 2, 2, 2, 2),
                Arguments.of("Q36", 2, 2, 2, 2, 2),
                Arguments.of("Q39_1", 2, 2, 2, 2, 2),
                Arguments.of("Q39_2", 2, 2, 2, 2, 2),
                Arguments.of("Q40", 2, 2, 2, 2, 2),
                Arguments.of("Q44", 8, 8, 8, 8, 8),
                Arguments.of("Q48", 2, 2, 2, 2, 2),
                Arguments.of("Q49", 8, 8, 8, 8, 8),
                Arguments.of("Q50", 2, 2, 2, 2, 2),
                Arguments.of("Q61", 4, 4, 4, 4, 4),
                Arguments.of("Q62", 2, 2, 2, 2, 2),
                Arguments.of("Q64", 4, 4, 4, 4, 4),
                Arguments.of("Q66", 6, 6, 6, 6, 6),
                Arguments.of("Q67", 2, 2, 2, 2, 2),
                Arguments.of("Q69", 2, 2, 2, 2, 2),
                Arguments.of("Q72", 2, 2, 2, 2, 2),
                Arguments.of("Q73", 2, 2, 2, 2, 2),
                Arguments.of("Q76", 2, 2, 2, 2, 2),
                Arguments.of("Q80", 8, 8, 8, 8, 8),
                Arguments.of("Q84", 0, 0, 0, 0, 0),
                Arguments.of("Q85", 2, 2, 2, 2, 2),
                Arguments.of("Q86", 2, 2, 2, 2, 2),
                Arguments.of("Q88", 16, 16, 16, 16, 16),
                Arguments.of("Q90", 4, 4, 4, 4, 4),
                Arguments.of("Q92", 4, 4, 4, 4, 4),
                Arguments.of("Q93", 2, 2, 2, 2, 2),
                Arguments.of("Q94", 4, 4, 4, 4, 4),
                Arguments.of("Q95", 4, 4, 4, 4, 4),
                Arguments.of("Q96", 2, 2, 2, 2, 2),
                Arguments.of("Q99", 2, 2, 2, 2, 2),
        };

        return Arrays.stream(cases);
    }

}
