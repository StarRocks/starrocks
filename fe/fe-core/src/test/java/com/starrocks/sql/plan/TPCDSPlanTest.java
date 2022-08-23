// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class TPCDSPlanTest extends TPCDSPlanTestBase {
    Map<String, Long> tpcdsStats = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
    }

    @Before
    public void setUp() {
        tpcdsStats = getTPCDSTableStats();
    }

    @After
    public void tearDown() {
        setTPCDSTableStats(tpcdsStats);
    }

    @Test
    public void testQ1() throws Exception {
        getFragmentPlan(Q1);
    }

    @Test
    public void testQ2() throws Exception {
        getFragmentPlan(Q2);
    }

    @Test
    public void testQ3() throws Exception {
        String costPlanFragment = getCostExplain(Q3);
        Assert.assertTrue(costPlanFragment.contains("hasNullableGenerateChild: true"));
        Assert.assertTrue(costPlanFragment.contains(" column statistics: \n" +
                "     * i_item_sk-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                "     * i_brand_id-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"));
    }

    @Test
    public void testQ4() throws Exception {
        getFragmentPlan(Q4);
    }

    @Test
    public void testQ5() throws Exception {
        getFragmentPlan(Q5);
    }

    @Test
    public void testQ6() throws Exception {
        getFragmentPlan(Q6);
    }

    @Test
    public void testQ7() throws Exception {
        getFragmentPlan(Q7);
    }

    @Test
    public void testQ8() throws Exception {
        getFragmentPlan(Q8);
    }

    @Test
    public void testQ9() throws Exception {
        getFragmentPlan(Q9);
    }

    @Test
    public void testQ10() throws Exception {
        getFragmentPlan(Q10);
    }

    @Test
    public void testQ11() throws Exception {
        getFragmentPlan(Q11);
    }

    @Test
    public void testQ12() throws Exception {
        getFragmentPlan(Q12);
    }

    @Test
    public void testQ13() throws Exception {
        getFragmentPlan(Q13);
    }

    @Test
    public void testQ14() throws Exception {
        getFragmentPlan(Q14);
    }

    @Test
    public void testQ15() throws Exception {
        getFragmentPlan(Q15);
    }

    @Test
    public void testQ16() throws Exception {
        getFragmentPlan(Q16);
    }

    @Test
    public void testQ17() throws Exception {
        getFragmentPlan(Q17);
    }

    @Test
    public void testQ18() throws Exception {
        getFragmentPlan(Q18);
    }

    @Test
    public void testQ19() throws Exception {
        getFragmentPlan(Q19);
    }

    @Test
    public void testQ20() throws Exception {
        getFragmentPlan(Q20);
    }

    @Test
    public void testQ21() throws Exception {
        getFragmentPlan(Q21);
    }

    @Test
    public void testQ22() throws Exception {
        getFragmentPlan(Q22);
    }

    @Test
    public void testQ23() throws Exception {
        getFragmentPlan(Q23);
    }

    @Test
    public void testQ24() throws Exception {
        getFragmentPlan(Q24);
    }

    @Test
    public void testQ25() throws Exception {
        getFragmentPlan(Q25);
    }

    @Test
    public void testQ26() throws Exception {
        getFragmentPlan(Q26);
    }

    @Test
    public void testQ27() throws Exception {
        getFragmentPlan(Q27);
    }

    @Test
    public void testQ28() throws Exception {
        getFragmentPlan(Q28);
    }

    @Test
    public void testQ29() throws Exception {
        getFragmentPlan(Q29);
    }

    @Test
    public void testQ30() throws Exception {
        getFragmentPlan(Q30);
    }

    @Test
    public void testQ31() throws Exception {
        getFragmentPlan(Q31);
    }

    @Test
    public void testQ32() throws Exception {
        getFragmentPlan(Q32);
    }

    @Test
    public void testQ33() throws Exception {
        getFragmentPlan(Q33);
    }

    @Test
    public void testQ34() throws Exception {
        getFragmentPlan(Q34);
    }

    @Test
    public void testQ35() throws Exception {
        getFragmentPlan(Q35);
    }

    @Test
    public void testQ36() throws Exception {
        getFragmentPlan(Q36);
    }

    @Test
    public void testQ37() throws Exception {
        getFragmentPlan(Q37);
    }

    @Test
    public void testQ38() throws Exception {
        getFragmentPlan(Q38);
    }

    @Test
    public void testQ39() throws Exception {
        getFragmentPlan(Q39);
    }

    @Test
    public void testQ40() throws Exception {
        getFragmentPlan(Q40);
    }

    @Test
    public void testQ41() throws Exception {
        getFragmentPlan(Q41);
    }

    @Test
    public void testQ42() throws Exception {
        getFragmentPlan(Q42);
    }

    @Test
    public void testQ43() throws Exception {
        getFragmentPlan(Q43);
    }

    @Test
    public void testQ44() throws Exception {
        getFragmentPlan(Q44);
    }

    @Test
    public void testQ45() throws Exception {
        getFragmentPlan(Q45);
    }

    @Test
    public void testQ46() throws Exception {
        getFragmentPlan(Q46);
    }

    @Test
    public void testQ47() throws Exception {
        getFragmentPlan(Q47);
    }

    @Test
    public void testQ48() throws Exception {
        getFragmentPlan(Q48);
    }

    // @Test
    public void testQ49() throws Exception {
        getFragmentPlan(Q49);
    }

    @Test
    public void testQ50() throws Exception {
        getFragmentPlan(Q50);
    }

    @Test
    public void testQ51() throws Exception {
        getFragmentPlan(Q51);
    }

    @Test
    public void testQ52() throws Exception {
        getFragmentPlan(Q52);
    }

    @Test
    public void testQ53() throws Exception {
        getFragmentPlan(Q53);
    }

    @Test
    public void testQ54() throws Exception {
        getFragmentPlan(Q54);
    }

    @Test
    public void testQ55() throws Exception {
        getFragmentPlan(Q55);
    }

    @Test
    public void testQ56() throws Exception {
        getFragmentPlan(Q56);
    }

    @Test
    public void testQ57() throws Exception {
        getFragmentPlan(Q57);
    }

    @Test
    public void testQ58() throws Exception {
        getFragmentPlan(Q58);
    }

    @Test
    public void testQ59() throws Exception {
        getFragmentPlan(Q59);
    }

    @Test
    public void testQ60() throws Exception {
        getFragmentPlan(Q60);
    }

    @Test
    public void testQ61() throws Exception {
        getFragmentPlan(Q61);
    }

    @Test
    public void testQ62() throws Exception {
        getFragmentPlan(Q62);
    }

    @Test
    public void testQ63() throws Exception {
        getFragmentPlan(Q63);
    }

    @Test
    public void testQ64() throws Exception {
        getFragmentPlan(Q64);
    }

    @Test
    public void testQ65() throws Exception {
        getFragmentPlan(Q65);
    }

    @Test
    public void testQ66() throws Exception {
        getFragmentPlan(Q66);
    }

    @Test
    public void testQ67() throws Exception {
        getFragmentPlan(Q67);
    }

    @Test
    public void testQ68() throws Exception {
        getFragmentPlan(Q68);
    }

    @Test
    public void testQ69() throws Exception {
        getFragmentPlan(Q69);
    }

    @Test
    public void testQ70() throws Exception {
        getFragmentPlan(Q70);
    }

    @Test
    public void testQ71() throws Exception {
        getFragmentPlan(Q71);
    }

    @Test
    public void testQ72() throws Exception {
        getFragmentPlan(Q72);
    }

    @Test
    public void testQ73() throws Exception {
        getFragmentPlan(Q73);
    }

    @Test
    public void testQ74() throws Exception {
        getFragmentPlan(Q74);
    }

    @Test
    public void testQ75() throws Exception {
        getFragmentPlan(Q75);
    }

    @Test
    public void testQ76() throws Exception {
        getFragmentPlan(Q76);
    }

    @Test
    public void testQ77() throws Exception {
        getFragmentPlan(Q77);
    }

    @Test
    public void testQ78() throws Exception {
        getFragmentPlan(Q78);
    }

    @Test
    public void testQ79() throws Exception {
        getFragmentPlan(Q79);
    }

    @Test
    public void testQ80() throws Exception {
        getFragmentPlan(Q80);
    }

    @Test
    public void testQ81() throws Exception {
        getFragmentPlan(Q81);
    }

    @Test
    public void testQ82() throws Exception {
        getFragmentPlan(Q82);
    }

    @Test
    public void testQ83() throws Exception {
        getFragmentPlan(Q83);
    }

    @Test
    public void testQ84() throws Exception {
        getFragmentPlan(Q84);
    }

    @Test
    public void testQ85() throws Exception {
        getFragmentPlan(Q85);
    }

    @Test
    public void testQ86() throws Exception {
        getFragmentPlan(Q86);
    }

    @Test
    public void testQ87() throws Exception {
        getFragmentPlan(Q87);
    }

    @Test
    public void testQ88() throws Exception {
        getFragmentPlan(Q88);
    }

    @Test
    public void testQ89() throws Exception {
        getFragmentPlan(Q89);
    }

    @Test
    public void testQ90() throws Exception {
        getFragmentPlan(Q90);
    }

    @Test
    public void testQ91() throws Exception {
        getFragmentPlan(Q91);
    }

    @Test
    public void testQ92() throws Exception {
        getFragmentPlan(Q92);
    }

    @Test
    public void testQ93() throws Exception {
        getFragmentPlan(Q93);
    }

    @Test
    public void testQ94() throws Exception {
        getFragmentPlan(Q94);
    }

    @Test
    public void testQ95() throws Exception {
        getFragmentPlan(Q95);
    }

    @Test
    public void testQ96() throws Exception {
        getFragmentPlan(Q96);
    }

    @Test
    public void testQ97() throws Exception {
        getFragmentPlan(Q97);
    }

    @Test
    public void testQ98() throws Exception {
        getFragmentPlan(Q98);
    }

    @Test
    public void testQ99() throws Exception {
        getFragmentPlan(Q99);
    }

    @Test
    public void testQ80_2() throws Exception {
        String planFragment = getFragmentPlan(Q80_2);
        Assert.assertFalse(planFragment.contains("cross join"));
    }

    @Test
    public void testQ95_2() throws Exception {
        String planFragment = getFragmentPlan(Q95_2);
        Assert.assertFalse(planFragment.contains("cross join"));
    }

    @Test
    public void testTPCDSDecodeRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        String sql = "select c_customer_id customer_id        ,c_first_name customer_first_name        " +
                ",c_last_name customer_last_name        ,c_preferred_cust_flag customer_preferred_cust_flag        " +
                ",c_birth_country customer_birth_country        ,d_year dyear        " +
                ",sum(ss_ext_list_price-ss_ext_discount_amt) year_total        ,'s' sale_type  from customer      " +
                ",store_sales      ,date_dim  where c_customer_sk = ss_customer_sk    " +
                "and ss_sold_date_sk = d_date_sk  group by c_customer_id          ,c_first_name          ,c_last_name          " +
                ",c_preferred_cust_flag           ,c_birth_country          ,d_year;";
        String plan = getCostExplain(sql);
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);

        Assert.assertTrue(plan.contains("dict_col=c_birth_country"));
        Assert.assertTrue(plan.contains("  13:Decode\n" +
                "  |  <dict id 73> : <string id 15>"));
    }

    @Test
    public void testTPCDSDecodeRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        String sql = "with year_total as (\n" +
                " select c_customer_id customer_id\n" +
                "       ,c_birth_country customer_birth_country\n" +
                "       ,d_year dyear\n" +
                " from customer\n" +
                "     ,store_sales\n" +
                "     ,date_dim\n" +
                " where c_customer_sk = ss_customer_sk\n" +
                "   and ss_sold_date_sk = d_date_sk\n" +
                " group by c_customer_id\n" +
                "         ,c_birth_country\n" +
                "         ,d_year \n" +
                " )\n" +
                " \n" +
                " select t_s_secyear.customer_id\n" +
                " from year_total t_s_firstyear\n" +
                "     ,year_total t_s_secyear\n" +
                " where t_s_secyear.customer_id = t_s_firstyear.customer_id\n" +
                "         and t_s_firstyear.dyear = 2001\n" +
                "         and t_s_secyear.dyear = 2001+1\n" +
                "limit 100;";
        String plan = getCostExplain(sql);
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);

        Assert.assertTrue(plan.contains("dict_col=c_birth_country"));
    }

    @Test
    public void expressionExtract() throws Exception {
        String sql = "\n" +
                "select count(1)\n" +
                " from store_sales\n" +
                "     ,customer_demographics\n" +
                "     ,customer_address\n" +
                " where 1=1\n" +
                " and((cd_demo_sk = ss_cdemo_sk\n" +
                "  and cd_marital_status = 'M'\n" +
                "  and cd_education_status = 'Advanced Degree'\n" +
                "  and ss_sales_price between 100.00 and 150.00\n" +
                "     )or\n" +
                "     (cd_demo_sk = ss_cdemo_sk\n" +
                "  and cd_marital_status = 'S'\n" +
                "  and cd_education_status = 'College'\n" +
                "  and ss_sales_price between 50.00 and 100.00\n" +
                "     ) or\n" +
                "     (cd_demo_sk = ss_cdemo_sk\n" +
                "  and cd_marital_status = 'W'\n" +
                "  and cd_education_status = '2 yr Degree'\n" +
                "  and ss_sales_price between 150.00 and 200.00\n" +
                "     ))\n" +
                " and((ss_addr_sk = ca_address_sk\n" +
                "  and ca_country = 'United States'\n" +
                "  and ca_state in ('TX', 'OH', 'TX')\n" +
                "  and ss_net_profit between 100 and 200\n" +
                "     ) or\n" +
                "     (ss_addr_sk = ca_address_sk\n" +
                "  and ca_country = 'United States'\n" +
                "  and ca_state in ('OR', 'NM', 'KY')\n" +
                "  and ss_net_profit between 150 and 300\n" +
                "     ) or\n" +
                "     (ss_addr_sk = ca_address_sk\n" +
                "  and ca_country = 'United States'\n" +
                "  and ca_state in ('VA', 'TX', 'MS')\n" +
                "  and ss_net_profit between 50 and 250\n" +
                "     ));";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 14: ss_sales_price >= 50.00, 14: ss_sales_price <= 200.00, 23: " +
                "ss_net_profit >= 50, 23: ss_net_profit <= 300");
    }

    @Test
    public void testQuery20LeftDeepJoinReorderNoCrossJoin() throws Exception {
        setTPCDSFactor(1);
        String plan = getFragmentPlan(Q20);
        assertNotContains(plan, "CROSS JOIN");
    }

    @Test
    public void testQuery33LeftDeepJoinReorderAvoidInnerJoinOnSameTable() throws Exception {
        setTPCDSFactor(1);
        String plan = getFragmentPlan(Q33);
        assertContains(plan, "  16:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 44: wr_returning_cdemo_sk = 82: cd_demo_sk\n" +
                "  |  equal join conjunct: 75: cd_marital_status = 84: cd_marital_status\n" +
                "  |  equal join conjunct: 76: cd_education_status = 85: cd_education_status\n" +
                "  |  \n" +
                "  |----15:EXCHANGE");
        assertContains(plan, "  12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 40: wr_refunded_cdemo_sk = 73: cd_demo_sk\n" +
                "  |  other join predicates: ((((75: cd_marital_status = 'D') " +
                "AND (76: cd_education_status = 'Primary')) AND ((22: ws_sales_price >= 100.00) " +
                "AND (22: ws_sales_price <= 150.00))) OR (((75: cd_marital_status = 'U') " +
                "AND (76: cd_education_status = 'Unknown')) AND ((22: ws_sales_price >= 50.00) " +
                "AND (22: ws_sales_price <= 100.00)))) OR (((75: cd_marital_status = 'M') " +
                "AND (76: cd_education_status = 'Advanced Degree')) AND ((22: ws_sales_price >= 150.00) " +
                "AND (22: ws_sales_price <= 200.00)))\n" +
                "  |  \n" +
                "  |----11:EXCHANGE");
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 15\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  14:OlapScanNode\n" +
                "     TABLE: customer_demographics");
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  10:OlapScanNode\n" +
                "     TABLE: customer_demographics");
    }
}
