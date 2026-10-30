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

import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// PushDownAggregateGroupingSetsRule's union outputs the aggregation's group keys, which keep the scan columns'
// ids. A grouping() filter that folds the full-key branch away leaves a rolled-up key as a NULL constant under
// that id. Global late materialization must not take it for the scan column and defer it, or the fetch is
// pushed down to the constant and tryPushDownFetch fails its checkState.
public class GlobalLateMaterializeGroupingSetTest extends TPCDS1TTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDS1TTestBase.beforeClass();
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableGlobalLateMaterialization(true);
        sv.setEnableGlobalLateMaterializationCostBased(false);
        sv.setCboPushDownGroupingSet(true);
    }

    @AfterAll
    public static void afterClass() {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableGlobalLateMaterialization(false);
        sv.setEnableGlobalLateMaterializationCostBased(true);
        sv.setCboPushDownGroupingSet(false);
        TPCDS1TTestBase.afterClass();
    }

    // TPC-DS Q67's rollup; g = 1 prunes the push-down's branch that still has s_store_id
    private static final String DW1 = "(select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, " +
            "d_moy, s_store_id, sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales, grouping(s_store_id) g " +
            "from store_sales, date_dim, store, item " +
            "where ss_sold_date_sk=d_date_sk and ss_item_sk=i_item_sk and ss_store_sk = s_store_sk " +
            "and d_month_seq between 1200 and 1200+11 " +
            "group by rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)) dw1 ";

    // ranked as in Q67; the window keeps the NULL projection apart from any projection above it
    private static final String DW2 = "(select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, " +
            "d_moy, s_store_id, sumsales, rank() over (partition by i_category order by sumsales desc) rk " +
            "from " + DW1 + "where g = 1) dw2 ";

    // a projection above the window that computes other columns and only passes s_store_id through
    private static final String PASS_THROUGH = "(select s_store_id, i_category, concat(i_brand, '#') b, rk + 1 r " +
            "from " + DW2 + "where rk <= 100) t ";

    @Test
    public void testNullKeyUnderTopN() throws Exception {
        String sql = "select i_category, i_brand, s_store_id, sumsales from " + DW1 +
                "where g = 1 order by s_store_id, i_category limit 100";
        assertContains(getFragmentPlan(sql), "TOP-N");
    }

    @Test
    public void testRankedNullKeyUnderTopN() throws Exception {
        String sql = "select * from " + DW2 + "where rk <= 100 " +
                "order by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, " +
                "sumsales, rk limit 100";
        assertContains(getFragmentPlan(sql), "TOP-N");
    }

    // The projection above the window does not compute s_store_id, but it must not defer it again either: a
    // projection below has already turned it into a value.
    @Test
    public void testPassThroughNullKeyUnderTopN() throws Exception {
        String sql = "select s_store_id, i_category, b, r from " + PASS_THROUGH +
                "order by s_store_id, i_category, b, r limit 100";
        assertContains(getFragmentPlan(sql), "TOP-N");
    }

    @Test
    public void testPassThroughNullKeyInJoin() throws Exception {
        String sql = "select t.s_store_id, t.b, s.s_store_name from " + PASS_THROUGH +
                "join store s on t.s_store_id = s.s_store_id limit 100";
        assertContains(getFragmentPlan(sql), "HASH JOIN");
    }

    @Test
    public void testPassThroughNullKeyInWindowPartition() throws Exception {
        String sql = "select s_store_id, b, sum(r) over (partition by s_store_id) w from " + PASS_THROUGH +
                "limit 100";
        assertContains(getFragmentPlan(sql), "ANALYTIC");
    }

    // the default, cost-based gate
    @Test
    public void testNullKeyUnderTopNCostBased() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableGlobalLateMaterializationCostBased(true);
        try {
            String sql = "select s_store_id, i_category, b, r from " + PASS_THROUGH +
                    "order by s_store_id, i_category, b, r limit 100";
            assertContains(getFragmentPlan(sql), "TOP-N");
        } finally {
            sv.setEnableGlobalLateMaterializationCostBased(false);
        }
    }
}
