// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedEnvTPCDSPlanTest extends TPCDSPlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDSPlanTest.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable customer_address =
                (OlapTable) globalStateMgr.getDb("test").getTable("customer_address");
        setTableStatistics(customer_address, 1000000);

        OlapTable customer = (OlapTable) globalStateMgr.getDb("test").getTable("customer");
        setTableStatistics(customer, 2000000);

        OlapTable store_sales = (OlapTable) globalStateMgr.getDb("test").getTable("store_sales");
        setTableStatistics(store_sales, 287997024);

        OlapTable date_dim = (OlapTable) globalStateMgr.getDb("test").getTable("date_dim");
        setTableStatistics(date_dim, 73048);

        OlapTable item = (OlapTable) globalStateMgr.getDb("test").getTable("item");
        setTableStatistics(item, 203999);

        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
    }

    @AfterClass
    public static void afterClass() {
        try {
            UtFrameUtils.dropMockBackend(10002);
            UtFrameUtils.dropMockBackend(10003);
        } catch (DdlException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQ06() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select  a.ca_state state, count(*) cnt\n" +
                " from customer_address a\n" +
                "     ,customer c\n" +
                "     ,store_sales s\n" +
                "     ,date_dim d\n" +
                "     ,item i\n" +
                " where       a.ca_address_sk = c.c_current_addr_sk\n" +
                " \tand c.c_customer_sk = s.ss_customer_sk\n" +
                " \tand s.ss_sold_date_sk = d.d_date_sk\n" +
                " \tand s.ss_item_sk = i.i_item_sk\n" +
                " \tand d.d_month_seq =\n" +
                " \t     (select distinct (d_month_seq)\n" +
                " \t      from date_dim\n" +
                "               where d_year = 2001\n" +
                " \t        and d_moy = 1 )\n" +
                " \tand i.i_current_price > 1.2 *\n" +
                "             (select avg(j.i_current_price)\n" +
                " \t     from item j\n" +
                " \t     where j.i_category = i.i_category)\n" +
                " group by a.ca_state\n" +
                " having count(*) >= 10\n" +
                " order by cnt, a.ca_state\n" +
                " limit 100;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("0:OlapScanNode\n" +
                "     TABLE: store_sales"));
        FeConstants.runningUnitTest = false;
    }
}
