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

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TPCDS1TTest extends TPCDS1TTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
    }

    public void testAllInlineCTE(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    public void testCTE(String sql) throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));

        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    public void testForceCTE(String sql) throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));

        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testQ01() throws Exception {
        testCTE(Q01);
    }

    @Test
    public void testQ02() throws Exception {
        testCTE(Q02);
    }

    @Test
    public void testQ03() throws Exception {
        testAllInlineCTE(Q03);
    }

    @Test
    public void testQ04() throws Exception {
        testCTE(Q04);
    }

    @Test
    public void testQ05() throws Exception {
        testAllInlineCTE(Q05);
    }

    @Test
    public void testQ06() throws Exception {
        testAllInlineCTE(Q06);
    }

    @Test
    public void testQ07() throws Exception {
        testAllInlineCTE(Q07);
    }

    @Test
    public void testQ08() throws Exception {
        testAllInlineCTE(Q08);
    }

    @Test
    public void testQ09() throws Exception {
        testAllInlineCTE(Q09);
    }

    @Test
    public void testQ10() throws Exception {
        testAllInlineCTE(Q10);
    }

    @Test
    public void testQ11() throws Exception {
        testCTE(Q11);
    }

    @Test
    public void testQ12() throws Exception {
        testAllInlineCTE(Q12);
    }

    @Test
    public void testQ13() throws Exception {
        testAllInlineCTE(Q13);
    }

    @Test
    public void testQ14_1() throws Exception {
        testCTE(Q14_1);
    }

    @Test
    public void testQ14_2() throws Exception {
        testCTE(Q14_2);
    }

    @Test
    public void testQ15() throws Exception {
        testAllInlineCTE(Q15);
    }

    @Test
    public void testQ16() throws Exception {
        testAllInlineCTE(Q16);
    }

    @Test
    public void testQ17() throws Exception {
        testAllInlineCTE(Q17);
    }

    @Test
    public void testQ18() throws Exception {
        testAllInlineCTE(Q18);
    }

    @Test
    public void testQ19() throws Exception {
        testAllInlineCTE(Q19);
    }

    @Test
    public void testQ20() throws Exception {
        testAllInlineCTE(Q20);
    }

    @Test
    public void testQ21() throws Exception {
        testAllInlineCTE(Q21);
    }

    @Test
    public void testQ22() throws Exception {
        testAllInlineCTE(Q22);
    }

    @Test
    public void testQ23_1() throws Exception {
        testCTE(Q23_1);
    }

    @Test
    public void testQ23_2() throws Exception {
        testCTE(Q23_2);
    }

    @Test
    public void testQ24_1() throws Exception {
        testCTE(Q24_1);
    }

    @Test
    public void testQ24_2() throws Exception {
        testCTE(Q24_2);
    }

    @Test
    public void testQ25() throws Exception {
        testAllInlineCTE(Q25);
    }

    @Test
    public void testQ26() throws Exception {
        testAllInlineCTE(Q26);
    }

    @Test
    public void testQ27() throws Exception {
        testAllInlineCTE(Q27);
    }

    @Test
    public void testQ28() throws Exception {
        testAllInlineCTE(Q28);
    }

    @Test
    public void testQ29() throws Exception {
        testAllInlineCTE(Q29);
    }

    @Test
    public void testQ30() throws Exception {
        testCTE(Q30);
    }

    @Test
    public void testQ31() throws Exception {
        testCTE(Q31);
    }

    @Test
    public void testQ32() throws Exception {
        testAllInlineCTE(Q32);
    }

    @Test
    public void testQ33() throws Exception {
        testAllInlineCTE(Q33);
    }

    @Test
    public void testQ34() throws Exception {
        testAllInlineCTE(Q34);
    }

    @Test
    public void testQ35() throws Exception {
        testAllInlineCTE(Q35);
    }

    @Test
    public void testQ36() throws Exception {
        testAllInlineCTE(Q36);
    }

    @Test
    public void testQ37() throws Exception {
        testAllInlineCTE(Q37);
    }

    @Test
    public void testQ38() throws Exception {
        testAllInlineCTE(Q38);
    }

    @Test
    public void testQ39_1() throws Exception {
        testCTE(Q39_1);
    }

    @Test
    public void testQ39_2() throws Exception {
        testCTE(Q39_2);
    }

    @Test
    public void testQ40() throws Exception {
        testAllInlineCTE(Q40);
    }

    @Test
    public void testQ41() throws Exception {
        testAllInlineCTE(Q41);
    }

    @Test
    public void testQ42() throws Exception {
        testAllInlineCTE(Q42);
    }

    @Test
    public void testQ43() throws Exception {
        testAllInlineCTE(Q43);
    }

    @Test
    public void testQ44() throws Exception {
        testAllInlineCTE(Q44);
    }

    @Test
    public void testQ45() throws Exception {
        testForceCTE(Q45);
    }

    @Test
    public void testQ46() throws Exception {
        testAllInlineCTE(Q46);
    }

    @Test
    public void testQ47() throws Exception {
        testCTE(Q47);
    }

    @Test
    public void testQ48() throws Exception {
        testAllInlineCTE(Q48);
    }

    @Test
    public void testQ49() throws Exception {
        testAllInlineCTE(Q49);
    }

    @Test
    public void testQ50() throws Exception {
        testAllInlineCTE(Q50);
    }

    @Test
    public void testQ51() throws Exception {
        testAllInlineCTE(Q51);
    }

    @Test
    public void testQ52() throws Exception {
        testAllInlineCTE(Q52);
    }

    @Test
    public void testQ53() throws Exception {
        testAllInlineCTE(Q53);
    }

    @Test
    public void testQ54() throws Exception {
        testAllInlineCTE(Q54);
    }

    @Test
    public void testQ55() throws Exception {
        testAllInlineCTE(Q55);
    }

    @Test
    public void testQ56() throws Exception {
        testAllInlineCTE(Q56);
    }

    @Test
    public void testQ57() throws Exception {
        testCTE(Q57);
    }

    @Test
    public void testQ58() throws Exception {
        testAllInlineCTE(Q58);
    }

    @Test
    public void testQ59() throws Exception {
        testCTE(Q59);
    }

    @Test
    public void testQ60() throws Exception {
        testAllInlineCTE(Q60);
    }

    @Test
    public void testQ61() throws Exception {
        testAllInlineCTE(Q61);
    }

    @Test
    public void testQ62() throws Exception {
        testAllInlineCTE(Q62);
    }

    @Test
    public void testQ63() throws Exception {
        testAllInlineCTE(Q63);
    }

    @Test
    public void testQ64() throws Exception {
        testCTE(Q64);
    }

    @Test
    public void testQ65() throws Exception {
        testAllInlineCTE(Q65);
    }

    @Test
    public void testQ66() throws Exception {
        testAllInlineCTE(Q66);
    }

    @Test
    public void testQ67() throws Exception {
        testAllInlineCTE(Q67);
    }

    @Test
    public void testQ68() throws Exception {
        testAllInlineCTE(Q68);
    }

    @Test
    public void testQ69() throws Exception {
        testAllInlineCTE(Q69);
    }

    @Test
    public void testQ70() throws Exception {
        testAllInlineCTE(Q70);
    }

    @Test
    public void testQ71() throws Exception {
        testAllInlineCTE(Q71);
    }

    @Test
    public void testQ72() throws Exception {
        testAllInlineCTE(Q72);
    }

    @Test
    public void testQ73() throws Exception {
        testAllInlineCTE(Q73);
    }

    @Test
    public void testQ74() throws Exception {
        testCTE(Q74);
    }

    @Test
    public void testQ75() throws Exception {
        testCTE(Q75);
    }

    @Test
    public void testQ76() throws Exception {
        testAllInlineCTE(Q76);
    }

    @Test
    public void testQ77() throws Exception {
        testAllInlineCTE(Q77);
    }

    @Test
    public void testQ78() throws Exception {
        testAllInlineCTE(Q78);
    }

    @Test
    public void testQ79() throws Exception {
        testAllInlineCTE(Q79);
    }

    @Test
    public void testQ80() throws Exception {
        testAllInlineCTE(Q80);
    }

    @Test
    public void testQ81() throws Exception {
        testCTE(Q81);
    }

    @Test
    public void testQ82() throws Exception {
        testAllInlineCTE(Q82);
    }

    @Test
    public void testQ83() throws Exception {
        testAllInlineCTE(Q83);
    }

    @Test
    public void testQ84() throws Exception {
        testAllInlineCTE(Q84);
    }

    @Test
    public void testQ85() throws Exception {
        testAllInlineCTE(Q85);
    }

    @Test
    public void testQ86() throws Exception {
        testAllInlineCTE(Q86);
    }

    @Test
    public void testQ87() throws Exception {
        testAllInlineCTE(Q87);
    }

    @Test
    public void testQ88() throws Exception {
        testAllInlineCTE(Q88);
    }

    @Test
    public void testQ89() throws Exception {
        testAllInlineCTE(Q89);
    }

    @Test
    public void testQ90() throws Exception {
        testAllInlineCTE(Q90);
    }

    @Test
    public void testQ91() throws Exception {
        testAllInlineCTE(Q91);
    }

    @Test
    public void testQ92() throws Exception {
        testAllInlineCTE(Q92);
    }

    @Test
    public void testQ93() throws Exception {
        testAllInlineCTE(Q93);
    }

    @Test
    public void testQ94() throws Exception {
        testAllInlineCTE(Q94);
    }

    @Test
    public void testQ95() throws Exception {
        testCTE(Q95);
    }

    @Test
    public void testQ96() throws Exception {
        testAllInlineCTE(Q96);
    }

    @Test
    public void testQ97() throws Exception {
        testAllInlineCTE(Q97);
    }

    @Test
    public void testQ98() throws Exception {
        testAllInlineCTE(Q98);
    }

    @Test
    public void testQ99() throws Exception {
        testAllInlineCTE(Q99);
    }

    @Test
    public void testQuery14IntersectDistinct() throws Exception {
        String sql = "select iss.i_brand_id brand_id\n" +
                "     ,iss.i_class_id class_id\n" +
                "     ,iss.i_category_id category_id\n" +
                " from store_sales\n" +
                "     ,item iss\n" +
                "     ,date_dim d1\n" +
                " where ss_item_sk = iss.i_item_sk\n" +
                "   and ss_sold_date_sk = d1.d_date_sk\n" +
                "   and d1.d_year between 1999 AND 1999 + 2\n" +
                " intersect\n" +
                " select ics.i_brand_id\n" +
                "     ,ics.i_class_id\n" +
                "     ,ics.i_category_id\n" +
                " from catalog_sales\n" +
                "     ,item ics\n" +
                "     ,date_dim d2\n" +
                " where cs_item_sk = ics.i_item_sk\n" +
                "   and cs_sold_date_sk = d2.d_date_sk\n" +
                "   and d2.d_year between 1999 AND 1999 + 2\n" +
                " intersect\n" +
                " select iws.i_brand_id\n" +
                "     ,iws.i_class_id\n" +
                "     ,iws.i_category_id\n" +
                " from web_sales\n" +
                "     ,item iws\n" +
                "     ,date_dim d3\n" +
                " where ws_item_sk = iws.i_item_sk\n" +
                "   and ws_sold_date_sk = d3.d_date_sk\n" +
                "   and d3.d_year between 1999 AND 1999 + 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "41:AGGREGATE (merge finalize)\n" +
                "  |  group by: 199: i_brand_id, 201: i_class_id, 203: i_category_id");
        assertContains(plan, "27:AGGREGATE (merge finalize)\n" +
                "  |  group by: 115: i_brand_id, 117: i_class_id, 119: i_category_id");
        assertContains(plan, "13:AGGREGATE (merge finalize)\n" +
                "  |  group by: 31: i_brand_id, 33: i_class_id, 35: i_category_id");

    }
}
