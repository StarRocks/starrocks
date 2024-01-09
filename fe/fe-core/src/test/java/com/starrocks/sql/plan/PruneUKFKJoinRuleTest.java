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

import com.starrocks.planner.TablePruningTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

public class PruneUKFKJoinRuleTest extends TPCDSPlanTestBase {
    Map<String, Long> tpcdsStats = null;

    private static void prepareUniqueKeys() {
        TablePruningTestBase.getSqlList("sql/tpcds_constraints/", "AddUniqueKeys")
                .forEach(stmts -> Arrays.stream(stmts.split("\n")).forEach(q -> {
                            try {
                                starRocksAssert.alterTableProperties(q);
                            } catch (Exception e) {
                                Assert.fail();
                            }
                        }
                ));
    }

    private static void prepareForeignKeys() {
        TablePruningTestBase.getSqlList("sql/tpcds_constraints/", "AddForeignKeys")
                .forEach(stmts -> Arrays.stream(stmts.split("\n")).forEach(q -> {
                            try {
                                starRocksAssert.alterTableProperties(q);
                            } catch (Exception e) {
                                Assert.fail();
                            }
                        }
                ));
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        prepareUniqueKeys();
        prepareForeignKeys();

        connectContext.getSessionVariable().setEnableUKFKOpt(true);

        starRocksAssert.withTable("CREATE TABLE `t_uk` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t_fk` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.alterTableProperties("alter table t_uk set (\"unique_constraints\" = \"v1\");");
        starRocksAssert.alterTableProperties(
                "ALTER TABLE t_fk SET(\"foreign_key_constraints\" = \"(v1) REFERENCES t_uk(v1)\");");
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableUKFKOpt(true);
    }

    @Before
    public void setUp() {
        super.setUp();
        tpcdsStats = getTPCDSTableStats();
    }

    @After
    public void tearDown() {
        setTPCDSTableStats(tpcdsStats);
    }

    private void assertMatches(String text, String pattern) {
        Pattern regex = Pattern.compile(pattern);
        Assert.assertTrue(text, regex.matcher(text).find());
    }

    private void assertNotMatches(String text, String pattern) {
        Pattern regex = Pattern.compile(pattern);
        Assert.assertFalse(text, regex.matcher(text).find());
    }

    private void assertPlans(String query, boolean equals, String... patterns) throws Exception {
        connectContext.getSessionVariable().setEnableUKFKOpt(false);
        String planDisabled = getFragmentPlan(query);
        connectContext.getSessionVariable().setEnableUKFKOpt(true);
        String planEnabled = getFragmentPlan(query);
        if (equals) {
            Assert.assertEquals(planDisabled, planEnabled);
        } else {
            Assert.assertNotEquals(planDisabled, planEnabled);
            for (String pattern : patterns) {
                assertMatches(planDisabled, pattern);
                assertNotMatches(planEnabled, pattern);
            }
        }
    }

    @Test
    public void canPrune() throws Exception {
        {
            String sql = "select t_fk.*, t_fk.v2 * 3 +5, t_uk.v1 * 5 from t_fk, t_uk where t_uk.v1 = t_fk.v1";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "HASH JOIN");
            assertContains(plan, "v1 IS NOT NULL");
        }
        {
            String sql =
                    "select t_fk.*, t_fk.v2 * 3 +5, t_uk.v1 * 5 from " +
                            "t_fk left outer join t_uk on t_uk.v1 = t_fk.v1";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "HASH JOIN");
            assertContains(plan, "v1 IS NOT NULL");
        }
        {
            String sql =
                    "select t_fk.*, t_fk.v2 * 3 +5, t_uk.v1 * 5 from " +
                            "t_uk right outer join t_fk on t_uk.v1 = t_fk.v1";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "HASH JOIN");
            assertContains(plan, "v1 IS NOT NULL");
        }
        {
            String sql = "select t_fk.*, t_fk.v2 * 3 +5 from " +
                    "t_fk left semi join t_uk on t_uk.v1 = t_fk.v1";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "HASH JOIN");
            assertContains(plan, "v1 IS NOT NULL");
        }
        {
            String sql = "select t_fk.*, t_fk.v2 * 3 +5 from " +
                    "t_uk right semi join t_fk on t_uk.v1 = t_fk.v1";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "HASH JOIN");
            assertContains(plan, "v1 IS NOT NULL");
        }
        {
            String sql = "select t_fk.*, t_fk.v2 * 3 +5 from " +
                    "t_uk join t_fk on t_uk.v1 = t_fk.v1 and t_fk.v1 = 5";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "HASH JOIN");
            assertContains(plan, "v1 IS NOT NULL, 4: v1 = 5");
        }
    }

    @Test
    public void cannotPrune() throws Exception {
        {
            String sql = "select t_uk.v1, t_uk.v2 from t_fk, t_uk where t_uk.v1 = t_fk.v1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "HASH JOIN");
        }
        {
            String sql = "select t_uk.v1 from t_fk, t_uk where t_uk.v1 = t_fk.v1 and t_uk.v2 = 3";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "HASH JOIN");
        }
    }

    @Test
    public void testQ1() throws Exception {
        assertPlans(Q01, true);
    }

    @Test
    public void testQ2() throws Exception {
        assertPlans(Q02, true);
    }

    @Test
    public void testQ3() throws Exception {
        assertPlans(Q03, true);
    }

    @Test
    public void testQ4() throws Exception {
        assertPlans(Q04, true);
    }

    @Test
    public void testQ5() throws Exception {
        assertPlans(Q05, true);
    }

    @Test
    public void testQ6() throws Exception {
        assertPlans(Q06, true);
    }

    @Test
    public void testQ7() throws Exception {
        assertPlans(Q07, true);
    }

    @Test
    public void testQ8() throws Exception {
        assertPlans(Q08, true);
    }

    @Test
    public void testQ9() throws Exception {
        assertPlans(Q09, true);
    }

    @Test
    public void testQ10() throws Exception {
        assertPlans(Q10, true);
    }

    @Test
    public void testQ11() throws Exception {
        assertPlans(Q11, true);
    }

    @Test
    public void testQ12() throws Exception {
        assertPlans(Q12, true);
    }

    @Test
    public void testQ13() throws Exception {
        assertPlans(Q13, false, "[0-9]+: ss_store_sk = [0-9]+: s_store_sk");
    }

    @Test
    public void testQ14() throws Exception {
        assertPlans(Q14_1, true);
        assertPlans(Q14_2, true);
    }

    @Test
    public void testQ15() throws Exception {
        assertPlans(Q15, true);
    }

    @Test
    public void testQ16() throws Exception {
        assertPlans(Q16, true);
    }

    @Test
    public void testQ17() throws Exception {
        assertPlans(Q17, true);
    }

    @Test
    public void testQ18() throws Exception {
        // TODO, if the prune rule is put in the physical stage, then this join can be pruned, how to fix it
        assertPlans(Q18, true, "[0-9]+: c_current_cdemo_sk = [0-9]+: cd_demo_sk");
    }

    @Test
    public void testQ19() throws Exception {
        assertPlans(Q19, true);
    }

    @Test
    public void testQ20() throws Exception {
        assertPlans(Q20, true);
    }

    @Test
    public void testQ21() throws Exception {
        assertPlans(Q21, true);
    }

    @Test
    public void testQ22() throws Exception {
        assertPlans(Q22, true);
    }

    @Test
    public void testQ23() throws Exception {
        assertPlans(Q23_1, false, "[0-9]+: ss_customer_sk = [0-9]+: c_customer_sk");
        assertPlans(Q23_2, false, "[0-9]+: ss_customer_sk = [0-9]+: c_customer_sk");
    }

    @Test
    public void testQ24() throws Exception {
        assertPlans(Q24_1, true);
        assertPlans(Q24_2, true);
    }

    @Test
    public void testQ25() throws Exception {
        assertPlans(Q25, true);
    }

    @Test
    public void testQ26() throws Exception {
        assertPlans(Q26, true);
    }

    @Test
    public void testQ27() throws Exception {
        assertPlans(Q27, true);
    }

    @Test
    public void testQ28() throws Exception {
        assertPlans(Q28, true);
    }

    @Test
    public void testQ29() throws Exception {
        assertPlans(Q29, true);
    }

    @Test
    public void testQ30() throws Exception {
        assertPlans(Q30, true);
    }

    @Test
    public void testQ31() throws Exception {
        assertPlans(Q31, true);
    }

    @Test
    public void testQ32() throws Exception {
        assertPlans(Q32, true);
    }

    @Test
    public void testQ33() throws Exception {
        assertPlans(Q33, true);
    }

    @Test
    public void testQ34() throws Exception {
        assertPlans(Q34, true);
    }

    @Test
    public void testQ35() throws Exception {
        assertPlans(Q35, true);
    }

    @Test
    public void testQ36() throws Exception {
        assertPlans(Q36, true);
    }

    @Test
    public void testQ37() throws Exception {
        assertPlans(Q37, true);
    }

    @Test
    public void testQ38() throws Exception {
        assertPlans(Q38, true);
    }

    @Test
    public void testQ39() throws Exception {
        assertPlans(Q39_1, false, "[0-9]+: inv_item_sk = [0-9]+: i_item_sk");
        assertPlans(Q39_2, false, "[0-9]+: inv_item_sk = [0-9]+: i_item_sk");
    }

    @Test
    public void testQ40() throws Exception {
        assertPlans(Q40, true);
    }

    @Test
    public void testQ41() throws Exception {
        assertPlans(Q41, true);
    }

    @Test
    public void testQ42() throws Exception {
        assertPlans(Q42, true);
    }

    @Test
    public void testQ43() throws Exception {
        assertPlans(Q43, true);
    }

    @Test
    public void testQ44() throws Exception {
        assertPlans(Q44, true);
    }

    @Test
    public void testQ45() throws Exception {
        assertPlans(Q45, true);
    }

    @Test
    public void testQ46() throws Exception {
        assertPlans(Q46, true);
    }

    @Test
    public void testQ47() throws Exception {
        assertPlans(Q47, true);
    }

    @Test
    public void testQ48() throws Exception {
        assertPlans(Q48, false, "[0-9]+: ss_store_sk = [0-9]+: s_store_sk");
    }

    @Test
    public void testQ49() throws Exception {
        assertPlans(Q49, true);
    }

    @Test
    public void testQ50() throws Exception {
        assertPlans(Q50, false, "[0-9]+: ss_sold_date_sk = [0-9]+: d_date_sk");
    }

    @Test
    public void testQ51() throws Exception {
        assertPlans(Q51, true);
    }

    @Test
    public void testQ52() throws Exception {
        assertPlans(Q52, true);
    }

    @Test
    public void testQ53() throws Exception {
        assertPlans(Q53, false, "[0-9]+: ss_store_sk = [0-9]+: s_store_sk");
    }

    @Test
    public void testQ54() throws Exception {
        assertPlans(Q54, true);
    }

    @Test
    public void testQ55() throws Exception {
        assertPlans(Q55, true);
    }

    @Test
    public void testQ56() throws Exception {
        assertPlans(Q56, true);
    }

    @Test
    public void testQ57() throws Exception {
        assertPlans(Q57, true);
    }

    @Test
    public void testQ58() throws Exception {
        assertPlans(Q58, true);
    }

    @Test
    public void testQ59() throws Exception {
        assertPlans(Q59, true);
    }

    @Test
    public void testQ60() throws Exception {
        assertPlans(Q60, true);
    }

    @Test
    public void testQ61() throws Exception {
        assertPlans(Q61, true);
    }

    @Test
    public void testQ62() throws Exception {
        assertPlans(Q62, true);
    }

    @Test
    public void testQ63() throws Exception {
        assertPlans(Q63, false, "[0-9]+: ss_store_sk = [0-9]+: s_store_sk");
    }

    @Test
    public void testQ64() throws Exception {
        assertPlans(Q64, false, "[0-9]+: hd_income_band_sk = [0-9]+: ib_income_band_sk",
                "[0-9]+: ss_promo_sk = [0-9]+: p_promo_sk");
    }

    @Test
    public void testQ65() throws Exception {
        assertPlans(Q65, true);
    }

    @Test
    public void testQ66() throws Exception {
        assertPlans(Q66, true);
    }

    @Test
    public void testQ67() throws Exception {
        assertPlans(Q67, true);
    }

    @Test
    public void testQ68() throws Exception {
        assertPlans(Q68, true);
    }

    @Test
    public void testQ69() throws Exception {
        assertPlans(Q69, true);
    }

    @Test
    public void testQ70() throws Exception {
        assertPlans(Q70, true);
    }

    @Test
    public void testQ71() throws Exception {
        assertPlans(Q71, true);
    }

    @Test
    public void testQ72() throws Exception {
        assertPlans(Q72, false, "[0-9]+: cs_promo_sk = [0-9]+: p_promo_sk");
    }

    @Test
    public void testQ73() throws Exception {
        assertPlans(Q73, true);
    }

    @Test
    public void testQ74() throws Exception {
        assertPlans(Q74, true);
    }

    @Test
    public void testQ75() throws Exception {
        assertPlans(Q75, true);
    }

    @Test
    public void testQ76() throws Exception {
        assertPlans(Q76, true);
    }

    @Test
    public void testQ77() throws Exception {
        assertPlans(Q77, false, "[0-9]+: wr_web_page_sk = [0-9]+: wp_web_page_sk",
                "[0-9]+: ws_web_page_sk = [0-9]+: wp_web_page_sk", "[0-9]+: sr_store_sk = [0-9]+: s_store_sk");
    }

    @Test
    public void testQ78() throws Exception {
        assertPlans(Q78, true);
    }

    @Test
    public void testQ79() throws Exception {
        assertPlans(Q79, true);
    }

    @Test
    public void testQ80() throws Exception {
        assertPlans(Q80, true);
    }

    @Test
    public void testQ81() throws Exception {
        assertPlans(Q81, true);
    }

    @Test
    public void testQ82() throws Exception {
        assertPlans(Q82, true);
    }

    @Test
    public void testQ83() throws Exception {
        assertPlans(Q83, true);
    }

    @Test
    public void testQ84() throws Exception {
        assertPlans(Q84, false, "[0-9]+: c_current_cdemo_sk = [0-9]+: cd_demo_sk");
    }

    @Test
    public void testQ85() throws Exception {
        assertPlans(Q85, false, "[0-9]+: ws_web_page_sk = [0-9]+: wp_web_page_sk");
    }

    @Test
    public void testQ86() throws Exception {
        assertPlans(Q86, true);
    }

    @Test
    public void testQ87() throws Exception {
        assertPlans(Q87, true);
    }

    @Test
    public void testQ88() throws Exception {
        assertPlans(Q88, true);
    }

    @Test
    public void testQ89() throws Exception {
        assertPlans(Q89, true);
    }

    @Test
    public void testQ90() throws Exception {
        assertPlans(Q90, true);
    }

    @Test
    public void testQ91() throws Exception {
        assertPlans(Q91, true);
    }

    @Test
    public void testQ92() throws Exception {
        assertPlans(Q92, true);
    }

    @Test
    public void testQ93() throws Exception {
        assertPlans(Q93, true);
    }

    @Test
    public void testQ94() throws Exception {
        assertPlans(Q94, true);
    }

    @Test
    public void testQ95() throws Exception {
        assertPlans(Q95, true);
    }

    @Test
    public void testQ96() throws Exception {
        assertPlans(Q96, true);
    }

    @Test
    public void testQ97() throws Exception {
        assertPlans(Q97, true);
    }

    @Test
    public void testQ98() throws Exception {
        assertPlans(Q98, true);
    }

    @Test
    public void testQ99() throws Exception {
        assertPlans(Q99, true);
    }
}
