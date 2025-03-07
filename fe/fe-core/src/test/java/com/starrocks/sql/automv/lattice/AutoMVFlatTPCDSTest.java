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

package com.starrocks.sql.automv.lattice;

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AutoMVFlatTPCDSTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        FeConstants.enablePruneEmptyOutputScan = false;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("flat_tpcds_db", TestUtil::getFlatTpcdsTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.mockTimelinessForAsyncMVTest(starRocksAssert.getCtx());
    }

    @Test
    public void testUniqueDimensions() {
        String q = "select  i_item_desc\n" +
                "      ,w_warehouse_name\n" +
                "      ,week(cs_sold_date) d_week_seq\n" +
                "      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo\n" +
                "      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo\n" +
                "      ,count(*) total_cnt\n" +
                "from catalog_sales_flat cs\n" +
                "join inventory_flat on (cs_item_sk = inv_item_sk)\n" +
                "join item_flat on (i_item_sk = cs_item_sk)\n" +
                "left outer join catalog_returns_flat " +
                "on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)\n" +
                "where date_trunc('week', cs_sold_date) =  date_trunc('week', inv_date)\n" +
                "  and inv_quantity_on_hand < cs_quantity \n" +
                "  and cs_ship_date > days_add(cs_sold_date, 5)\n" +
                "  and cs.bill_hd_buy_potential = '>10000'\n" +
                "  and year(cs_sold_date) = 1999\n" +
                "  and cs.bill_cd_marital_status = 'D'\n" +
                "group by i_item_desc,w_warehouse_name,cs_sold_date\n" +
                "order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq\n" +
                "limit 100;";
        AutoMVUtil.testSingleQueryHelper(STARROCKS_ASSERT.get(), q, sv -> {
            sv.setAutoMVDefaultPartitionByTimeGranule(TimeGranule.Unit.DAY.name());
            sv.setAutoMVEnableComplexDerivedMetrics(true);
            sv.setAutoMVCardRowCountRatioLWM(1.0);
            sv.setAutoMVCardRowCountRatioHWM(1.0);
        }, results -> {
            String mv = results.get(0).get(2);
            Assert.assertTrue(mv, mv.contains("PARTITION BY cs_sold_date"));
        });
    }

    @Test
    public void test1() {
        List<Pair<String, String>> queryList = TestUtil.getFlatTpcdsSqlList();
        AutoMVUtil.testOneOneMVHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    sv.setAutoMVDefaultPartitionByTimeGranule("month");
                    sv.setOptimizerExecuteTimeout(300000);
                },
                gv -> {
                },
                (pieces, results) -> {
                    Assert.assertEquals(results.size(), results.size(), 7);
                    return null;
                });
    }

    @Test
    public void testCollocated11MV() {
        String q = "select\n" +
                "  ws.bill_cd_gender,\n" +
                "  ws.bill_cd_marital_status,\n" +
                "  wr.returning_cd_education_status,\n" +
                "  count(1),\n" +
                "  avg(wr_refunded_cash),\n" +
                "  sum(wr_return_quantity)\n" +
                "from\n" +
                "  web_sales_flat ws\n" +
                "  left join web_returns_flat wr on ws.ws_item_sk = wr.wr_item_sk\n" +
                "  and ws.ws_order_number = wr.wr_order_number\n" +
                "where\n" +
                "  ws.web_name = 'site_0'\n" +
                "group by\n" +
                "  ws.bill_cd_gender,\n" +
                "  ws.bill_cd_marital_status,\n" +
                "  wr.returning_cd_education_status";
        AutoMVUtil.testOneOneMVHelper(getStarRocksAssert().getCtx(), Arrays.asList(Pair.create("q", q)),
                sv -> {
                    sv.setAutoMVDefaultPartitionByTimeGranule("month");
                },
                gv -> {
                    GlobalVariable.setAutoMVEnable11mvSelectivityEvaluation(false);
                },
                (pieces, results) -> {
                    String mv0 = results.get(0).get(2);
                    String mv1 = results.get(1).get(2);
                    if (!mv1.contains("web_sales_flat")) {
                        String tmp = mv0;
                        mv0 = mv1;
                        mv1 = tmp;
                    }
                    Assert.assertTrue(mv0, mv0.contains("PARTITION BY date_trunc(\"month\""));
                    Assert.assertTrue(mv1, mv1.contains("PARTITION BY date_trunc(\"month\""));
                    Assert.assertTrue(mv0, mv0.contains("DISTRIBUTED BY HASH (wr_item_sk, wr_order_number)"));
                    Assert.assertTrue(mv1, mv1.contains("DISTRIBUTED BY HASH (ws_item_sk, ws_order_number)"));
                    return null;
                });
    }
}
