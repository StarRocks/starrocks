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
import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TPCDSCTERatioTest extends TPCDS1TTestBase {

    private static final List<String> CTE_SQL_NAME = Arrays.asList(
            "Q01", "Q02", "Q04", "Q11", "Q14_1", "Q14_2", "Q23_1", "Q23_2", "Q24_1",
            "Q24_2", "Q30", "Q31", "Q39_1", "Q39_2", "Q47", "Q57", "Q59", "Q64",
            "Q74", "Q75", "Q81", "Q95");
    private static final List<String> CTE_SQL = Arrays.asList(
            Q01, Q02, Q04, Q11, Q14_1, Q14_2, Q23_1, Q23_2, Q24_1,
            Q24_2, Q30, Q31, Q39_1, Q39_2, Q47, Q57, Q59, Q64,
            Q74, Q75, Q81, Q95);

    public void setRatio(double ratio) {
        connectContext.getSessionVariable().setCboCTERuseRatio(ratio);
    }

    public void testCTE(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
    }

    public void testInline(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("MultiCastDataSinks"), plan);
    }

    private static void print(List<Boolean> expect, List<Boolean> actual) {
        System.out.printf("%-10s%-10s%-10s\n", "Name", "Actual", "Expect");
        for (int i = 0; i < CTE_SQL_NAME.size(); i++) {
            System.out.printf("%-10s%-10s%-10s\n", CTE_SQL_NAME.get(i), actual.get(i), expect.get(i));
        }
    }

    private String logicalPlan(String sql) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        return plan.getExplainString(StatementBase.ExplainLevel.LOGICAL);
    }

    /*
        Performance test
        query     FORCE CTE(ms)   NONE CTE(ms)  FORCE/NONE
        QUERY95      34,681         9,384          3.70
        QUERY59      7,468          2,392          3.12
        QUERY39-2    646            348            1.86
        QUERY39-1    893            593            1.51
        QUERY11      57,774         52,200         1.11
        QUERY04      94,417         86,726         1.09
        QUERY74      34,964         34,936         1.00
        QUERY31      3,560          3,653          0.97
        QUERY02      4,088          4,205          0.97
        QUERY30      1,117          1,291          0.87
        QUERY75      16,970         19,781         0.86
        QUERY01      1,351          1,586          0.85
        QUERY81      1,281          2,081          0.62
        QUERY64      18,377         31,747         0.58
        QUERY23-1    76,103         136,791        0.56
        QUERY23-2    75,989         138,349        0.55
        QUERY14-2    19,626         36,485         0.54
        QUERY14-1    21,884         54,389         0.40
        QUERY24-2    4,844          15,153         0.32
        QUERY24-1    4,894          15,355         0.32
        QUERY57      4,294          16,763         0.26
        QUERY47      6,143          28,724         0.21
     */

    public void printCosts() throws Exception {
        System.out.printf("%-10s%-30s%-30s%-30s\n", "Name", "CTE", "Inline", "Inline/CTE");
        List<Boolean> expect = Lists.newArrayList();
        List<Boolean> actual = Lists.newArrayList();
        for (int i = 0; i < CTE_SQL_NAME.size(); i++) {
            setRatio(0);
            ExecPlan cte = getExecPlan(CTE_SQL.get(i));
            setRatio(-1);
            ExecPlan inline = getExecPlan(CTE_SQL.get(i));

            System.out.printf("%-10s%-30s%-30s%-30s\n", CTE_SQL_NAME.get(i),
                    String.format("%.2f", cte.getPhysicalPlan().getCost()),
                    String.format("%.2f", inline.getPhysicalPlan().getCost()),
                    String.format("%.2f", inline.getPhysicalPlan().getCost() / cte.getPhysicalPlan().getCost()));

            expect.add(cte.getExplainString(StatementBase.ExplainLevel.NORMAL).contains("MultiCastDataSinks"));
            actual.add(inline.getExplainString(StatementBase.ExplainLevel.NORMAL).contains("MultiCastDataSinks"));
        }

        System.out.println();
        print(expect, actual);
    }

    @Test
    public void testRatioQ01() throws Exception {
        testCTE(Q01);
    }

    @Test
    public void testRatioQ02() throws Exception {
        testCTE(Q02);
    }

    @Test
    public void testRatioQ04() throws Exception {
        testInline(Q04);
    }

    @Test
    public void testRatioQ11() throws Exception {
        testInline(Q11);
    }

    @Test
    public void testRatioQ14_1() throws Exception {
        testCTE(Q14_1);
    }

    @Test
    public void testRatioQ14_2() throws Exception {
        testCTE(Q14_2);
    }

    @Test
    public void testRatioQ23_1() throws Exception {
        testCTE(Q23_1);
    }

    @Test
    public void testRatioQ23_2() throws Exception {
        testCTE(Q23_2);
    }

    @Test
    public void testRatioQ24_1() throws Exception {
        testCTE(Q24_1);
    }

    @Test
    public void testRatioQ24_2() throws Exception {
        testCTE(Q24_2);
    }

    @Test
    public void testRatioQ30() throws Exception {
        testCTE(Q30);
    }

    @Test
    public void testRatioQ31() throws Exception {
        testCTE(Q31);
    }

    @Test
    public void testRatioQ39_1() throws Exception {
        testCTE(Q39_1);
    }

    @Test
    public void testRatioQ39_2() throws Exception {
        testCTE(Q39_2);
    }

    @Test
    public void testRatioQ47() throws Exception {
        testCTE(Q47);
    }

    @Test
    public void testRatioQ57() throws Exception {
        testCTE(Q57);
    }

    @Test
    public void testRatioQ59() throws Exception {
        testCTE(Q59);
    }

    @Test
    public void testRatioQ64() throws Exception {
        testCTE(Q64);
    }

    @Test
    public void testRatioQ74() throws Exception {
        testInline(Q74);
    }

    @Test
    public void testRatioQ75() throws Exception {
        testCTE(Q75);
    }

    @Test
    public void testRatioQ81() throws Exception {
        testCTE(Q81);
    }

    @Test
    public void testRatioQ95() throws Exception {
        testInline(Q95);
    }
}
