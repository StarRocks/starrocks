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
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.Result;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AutoMVViewScanTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            StarRocksAssert starRocksAssert = TestUtil.prepareTables("tpcds", TestUtil::getTPCDSCreateTableSqlList);
            String viewSql = "create view cume_sales_view as \n" +
                    "WITH web_v1 as (\n" +
                    "select\n" +
                    "  ws_item_sk item_sk, ws_bill_customer_sk customer_sk, d_date,\n" +
                    "  sum(sum(ws_sales_price))\n" +
                    "      over (partition by ws_item_sk order by d_date " +
                    "rows between unbounded preceding and current row) cume_sales\n" +
                    "from web_sales\n" +
                    "    ,date_dim\n" +
                    "where ws_sold_date_sk=d_date_sk\n" +
                    "  and d_month_seq between 1200 and 1200+11\n" +
                    "  and ws_item_sk is not NULL\n" +
                    "group by ws_item_sk, ws_bill_customer_sk, d_date),\n" +
                    "store_v1 as (\n" +
                    "select\n" +
                    "  ss_item_sk item_sk, ss_customer_sk customer_sk, d_date,\n" +
                    "  sum(sum(ss_sales_price))\n" +
                    "      over (partition by ss_item_sk order by d_date " +
                    "rows between unbounded preceding and current row) cume_sales\n" +
                    "from store_sales\n" +
                    "    ,date_dim\n" +
                    "where ss_sold_date_sk=d_date_sk\n" +
                    "  and d_month_seq between 1200 and 1200+11\n" +
                    "  and ss_item_sk is not NULL\n" +
                    "group by ss_item_sk, ss_customer_sk, d_date)\n" +
                    "\n" +
                    "select item_sk,customer_sk, d_date,cume_sales\n" +
                    "from web_v1\n" +
                    "union all\n" +
                    "select item_sk,customer_sk, d_date,cume_sales\n" +
                    "from store_v1;";
            starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000);

            Result.wrap(() -> starRocksAssert.withView(viewSql));
            STARROCKS_ASSERT.set(starRocksAssert);
        }
        return STARROCKS_ASSERT.get();
    }

    @Test
    public void testSingleMV() {
        String q = "select d_date, sum(cume_sales) from cume_sales_view group by d_date";
        AutoMVUtil.testSingleQueryHelper(getStarRocksAssert(), q,
                sv -> {
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVEnableViewInline(false);
                },
                results -> {
                    Assert.assertFalse(results.isEmpty());
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("`tpcds`.`cume_sales_view`"));
                });
    }

    @Test
    public void testMultiMVMerge() {
        String q1 = "select d_date, sum(cume_sales) from cume_sales_view group by d_date";
        String q2 = "select d_date, customer_sk, sum(cume_sales) from cume_sales_view group by d_date,customer_sk";
        String q3 = "select d_date, count(item_sk), sum(cume_sales) from cume_sales_view group by d_date";
        List<Pair<String, String>> queryList = Lists.newArrayList(
                Pair.create("q1", q1),
                Pair.create("q2", q2),
                Pair.create("q3", q3)
        );
        AutoMVUtil.testHelper(getStarRocksAssert().getCtx(), queryList,
                sv -> {
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVEnableViewInline(false);
                }, results -> {
                    Assert.assertEquals(2, results.size());
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("SELECT\n" +
                            "  `tpcds`.`cume_sales_view`.d_date\n" +
                            "  ,`tpcds`.`cume_sales_view`.customer_sk\n" +
                            "  ,(count(`tpcds`.`cume_sales_view`.item_sk)) AS _ca0002\n" +
                            "  ,(sum(`tpcds`.`cume_sales_view`.cume_sales)) AS _ca0003\n" +
                            "FROM\n" +
                            "  `tpcds`.`cume_sales_view`\n" +
                            "GROUP BY\n" +
                            "  `tpcds`.`cume_sales_view`.d_date\n" +
                            "  ,`tpcds`.`cume_sales_view`.customer_sk"));
                });
    }

    @Test
    public void testTableJoinViewMV() {
        String q = "select cd_gender, cd_marital_status, cd_education_status, sum(cume_sales) \n" +
                "from cume_sales_view v1 \n" +
                "  inner join customer c on v1.customer_sk = c.c_customer_sk\n" +
                "  inner join customer_demographics cd on c.c_current_cdemo_sk = cd_demo_sk\n" +
                "group by cd_gender, cd_marital_status,cd_education_status";
        AutoMVUtil.testSingleQueryHelper(getStarRocksAssert(), q,
                sv -> {
                    sv.setAutoMVCardRowCountRatioLWM(1.0);
                    sv.setAutoMVCardRowCountRatioHWM(1.0);
                    sv.setAutoMVEnableViewInline(false);
                },
                results -> {
                    Assert.assertFalse(results.isEmpty());
                    String mv = results.get(0).get(2);
                    Assert.assertTrue(mv, mv.contains("    FROM\n" +
                            "      `tpcds`.`cume_sales_view`\n" +
                            "      INNER JOIN\n" +
                            "      `tpcds`.`customer`\n" +
                            "      ON (`tpcds`.`cume_sales_view`.customer_sk = `tpcds`.`customer`.c_customer_sk)\n" +
                            "      INNER JOIN\n" +
                            "      `tpcds`.`customer_demographics`\n" +
                            "      ON (`tpcds`.`customer`.c_current_cdemo_sk = " +
                            "`tpcds`.`customer_demographics`.cd_demo_sk)\n" +
                            "  ) _ta0000"));
                });
    }
}
