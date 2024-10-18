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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class TPCDS1TExtractCTETest extends TPCDS1TTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        connectContext.getSessionVariable().setCboExtractCommonPlan(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setCboExtractCommonPlan(true);
    }

    @Test
    public void testQuery09() throws Exception {
        String plan = getFragmentPlan(Q09);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(461: expr, 1, NULL)), avg(if(461: expr, 394: ss_ext_discount_amt, NULL))");
    }

    @Test
    public void testQuery28() throws Exception {
        String plan = getFragmentPlan(Q28);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  output: avg(192: if), count(192: if), multi_distinct_count(192: if), " +
                "count(196: if), multi_distinct_count(196: if)");
    }

    @Test
    public void testQuery44() throws Exception {
        String plan = getFragmentPlan(Q44);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  4:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(192: avg)\n" +
                "  |  group by: 179: ss_item_sk");
        assertContains(plan, "  9:AGGREGATE (merge finalize)\n" +
                "  |  output: avg(168: avg)\n" +
                "  |  group by: 165: ss_store_sk");
    }

    @Test
    public void testQuery65() throws Exception {
        String plan = getFragmentPlan(Q65);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "  8:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(208: sum)\n" +
                "  |  group by: 177: ss_store_sk, 167: ss_item_sk");
    }

    @Test
    public void testQuery88() throws Exception {
        String plan = getFragmentPlan(Q88);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (merge finalize)\n" +
                "  |  output: count(641: count), count(643: count)");
    }

    @Test
    public void testQuery90() throws Exception {
        String plan = getFragmentPlan(Q90);
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "AGGREGATE (update serialize)\n" +
                "  |  output: count(if((172: t_hour >= 8)");
    }
}
