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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

public class TPCDSPushAggTest extends TPCDS1TTestBase {
    private void check(int mode, String sql, int aggNum) throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(mode);
        sql = getTPCDS(sql);
        String plan = getFragmentPlan(sql);
        int actual = StringUtils.countMatches(plan, ":AGGREGATE ");
        String msg = "\nmode: " + mode + ", except: " + aggNum + ", actual: " + actual + "\n" + plan;
        Assertions.assertEquals(aggNum, actual, msg);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCastProvider")
    public void testTPCDSPushDownAgg(String sql, int orig, int auto, int force, int mid, int high) throws Exception {
        check(-1, sql, orig);
        check(0, sql, auto);
        check(1, sql, force);
        check(2, sql, mid);
        check(3, sql, high);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCastProvider")
    public void debugTPCDSPushDownAgg(String sql, int orig, int auto, int force, int mid, int high) throws Exception {
        orig = getAggNum(-1, sql);
        auto = getAggNum(0, sql);
        force = getAggNum(1, sql);
        mid = getAggNum(2, sql);
        high = getAggNum(3, sql);
        System.out.printf("Arguments.of(\"%s\", %d, %d, %d, %d, %d),\n", sql, orig, auto, force, mid, high);
    }

    private int getAggNum(int cboPushDownAggregateMode, String sql) throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(cboPushDownAggregateMode);
        sql = getTPCDS(sql);
        String plan = getFragmentPlan(sql);
        return StringUtils.countMatches(plan, ":AGGREGATE ");
    }

    private static Stream<Arguments> testCastProvider() {
        // orig(-1), auto(0), force(1), mid(2), high(3)
        Arguments[] cases = new Arguments[] {
                Arguments.of("Q01", 4, 4, 6, 4, 6),
                Arguments.of("Q02", 2, 5, 5, 5, 5),
                Arguments.of("Q03", 2, 4, 4, 4, 4),
                Arguments.of("Q04", 12, 10, 12, 10, 12),
                Arguments.of("Q05", 8, 16, 16, 16, 16),
                Arguments.of("Q06", 5, 5, 5, 5, 5),
                Arguments.of("Q07", 2, 2, 2, 2, 2),
                Arguments.of("Q08", 4, 6, 6, 6, 6),
                Arguments.of("Q09", 30, 30, 30, 30, 30),
                Arguments.of("Q10", 2, 2, 2, 2, 2),
                Arguments.of("Q11", 8, 6, 8, 6, 8),
                Arguments.of("Q12", 2, 4, 4, 4, 4),
                Arguments.of("Q13", 2, 2, 2, 2, 2),
                Arguments.of("Q14_1", 16, 16, 16, 16, 16),
                Arguments.of("Q14_2", 12, 12, 12, 12, 12),
                Arguments.of("Q15", 2, 4, 4, 4, 4),
                Arguments.of("Q16", 4, 4, 4, 4, 4),
                Arguments.of("Q17", 2, 2, 2, 2, 2),
                Arguments.of("Q18", 2, 2, 2, 2, 2),
                Arguments.of("Q19", 2, 2, 4, 2, 2),
                Arguments.of("Q20", 2, 4, 4, 4, 4),
                Arguments.of("Q21", 2, 2, 2, 2, 2),
                Arguments.of("Q22", 2, 2, 2, 2, 2),
                Arguments.of("Q23_1", 9, 12, 12, 12, 12),
                // Arguments.of("Q23_2", 11, 16, 18, 16, 18),
                Arguments.of("Q24_1", 6, 6, 7, 6, 6),
                Arguments.of("Q24_2", 6, 6, 7, 6, 6),
                Arguments.of("Q25", 2, 2, 2, 2, 2),
                Arguments.of("Q26", 2, 2, 2, 2, 2),
                Arguments.of("Q27", 2, 2, 2, 2, 2),
                Arguments.of("Q28", 24, 24, 24, 24, 24),
                Arguments.of("Q29", 2, 2, 2, 2, 2),
                Arguments.of("Q30", 4, 4, 6, 4, 4),
                Arguments.of("Q31", 4, 8, 8, 8, 8),
                Arguments.of("Q32", 4, 4, 4, 4, 4),
                Arguments.of("Q33", 8, 14, 14, 14, 14),
                Arguments.of("Q34", 2, 2, 2, 2, 2),
                Arguments.of("Q35", 2, 2, 2, 2, 2),
                Arguments.of("Q36", 2, 2, 2, 2, 2),
                Arguments.of("Q37", 2, 6, 8, 6, 7),
                Arguments.of("Q38", 8, 12, 20, 12, 17),
                Arguments.of("Q39_1", 2, 2, 2, 2, 2),
                Arguments.of("Q39_2", 2, 2, 2, 2, 2),
                Arguments.of("Q40", 2, 2, 2, 2, 2),
                Arguments.of("Q41", 2, 2, 4, 2, 2),
                Arguments.of("Q42", 2, 4, 4, 4, 4),
                Arguments.of("Q43", 2, 4, 4, 4, 4),
                Arguments.of("Q44", 8, 8, 8, 8, 8),
                Arguments.of("Q45", 5, 5, 7, 5, 7),
                Arguments.of("Q46", 1, 1, 3, 1, 1),
                Arguments.of("Q47", 2, 2, 4, 4, 4),
                Arguments.of("Q48", 2, 2, 2, 2, 2),
                Arguments.of("Q49", 8, 8, 8, 8, 8),
                Arguments.of("Q50", 2, 2, 2, 2, 2),
                Arguments.of("Q51", 4, 8, 8, 8, 8),
                Arguments.of("Q52", 2, 4, 4, 4, 4),
                Arguments.of("Q53", 2, 2, 4, 4, 4),
                Arguments.of("Q54", 7, 9, 16, 9, 15),
                Arguments.of("Q55", 2, 4, 4, 4, 4),
                Arguments.of("Q56", 8, 14, 14, 14, 14),
                Arguments.of("Q57", 2, 2, 4, 4, 4),
                Arguments.of("Q58", 3, 9, 9, 9, 9),
                Arguments.of("Q59", 2, 4, 4, 4, 4),
                Arguments.of("Q60", 8, 14, 14, 14, 14),
                Arguments.of("Q61", 4, 4, 4, 4, 4),
                Arguments.of("Q62", 2, 2, 2, 2, 2),
                Arguments.of("Q63", 2, 2, 4, 4, 4),
                Arguments.of("Q64", 4, 4, 4, 4, 4),
                Arguments.of("Q65", 6, 6, 10, 10, 10),
                Arguments.of("Q66", 6, 6, 6, 6, 6),
                Arguments.of("Q67", 2, 2, 2, 2, 2),
                Arguments.of("Q68", 1, 1, 3, 1, 1),
                Arguments.of("Q69", 2, 2, 2, 2, 2),
                Arguments.of("Q70", 4, 6, 6, 6, 6),
                Arguments.of("Q71", 2, 2, 8, 8, 8),
                Arguments.of("Q72", 2, 2, 2, 2, 2),
                Arguments.of("Q73", 2, 2, 2, 2, 2),
                Arguments.of("Q74", 8, 6, 8, 6, 8),
                Arguments.of("Q75", 4, 4, 16, 4, 4),
                Arguments.of("Q76", 2, 2, 2, 2, 2),
                Arguments.of("Q77", 14, 26, 26, 26, 26),
                Arguments.of("Q78", 6, 6, 9, 6, 6),
                Arguments.of("Q79", 2, 2, 4, 2, 2),
                Arguments.of("Q80", 8, 8, 8, 8, 8),
                Arguments.of("Q81", 4, 4, 6, 4, 4),
                Arguments.of("Q82", 2, 6, 8, 6, 7),
                Arguments.of("Q83", 3, 9, 9, 9, 9),
                Arguments.of("Q84", 0, 0, 0, 0, 0),
                Arguments.of("Q85", 2, 2, 2, 2, 2),
                Arguments.of("Q86", 2, 2, 2, 2, 2),
                Arguments.of("Q87", 8, 12, 20, 12, 17),
                Arguments.of("Q88", 16, 16, 16, 16, 16),
                Arguments.of("Q89", 2, 2, 4, 4, 4),
                Arguments.of("Q90", 4, 4, 4, 4, 4),
                Arguments.of("Q91", 2, 2, 4, 2, 4),
                Arguments.of("Q92", 4, 4, 4, 4, 4),
                Arguments.of("Q93", 2, 2, 2, 2, 2),
                Arguments.of("Q94", 4, 4, 4, 4, 4),
                Arguments.of("Q95", 4, 4, 4, 4, 4),
                Arguments.of("Q96", 2, 2, 2, 2, 2),
                Arguments.of("Q97", 6, 10, 12, 10, 12),
                Arguments.of("Q98", 2, 4, 4, 4, 4),
                Arguments.of("Q99", 2, 2, 2, 2, 2),
        };

        return Arrays.stream(cases);
    }

}
