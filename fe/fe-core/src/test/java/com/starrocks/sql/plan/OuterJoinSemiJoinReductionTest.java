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

import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class OuterJoinSemiJoinReductionTest extends TPCDS1TTestBase {
    // a/b/c are each aggregated on (k1, k2), the shape of TPC-DS Q78
    private static final String WITH = "with " +
            "a as (select ss_item_sk k1, ss_customer_sk k2, sum(ss_quantity) q from store_sales group by 1, 2), " +
            "b as (select ws_item_sk k1, ws_bill_customer_sk k2, sum(ws_quantity) q from web_sales group by 1, 2), " +
            "c as (select cs_item_sk k1, cs_bill_customer_sk k2, sum(cs_quantity) q " +
            "      from catalog_sales group by 1, 2) ";
    private static final String ABC = WITH + "select a.k1, a.k2, a.q aq, b.q bq, c.q cq from a " +
            "left join b on a.k1 = b.k1 and a.k2 = b.k2 " +
            "left join c on a.k1 = c.k1 and a.k2 = c.k2 ";

    private String plan(String sql, boolean enable) throws Exception {
        boolean previous = connectContext.getSessionVariable().isEnableOuterJoinSemiJoinReduction();
        connectContext.getSessionVariable().setEnableOuterJoinSemiJoinReduction(enable);
        connectContext.getSessionVariable().setEnableOptimizerRuleDebug(true);
        try {
            String plan = getFragmentPlan(sql);
            // a query that fails analysis yields the same error text with the rule on and off
            Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0"), plan);
            return plan;
        } finally {
            connectContext.getSessionVariable().setEnableOuterJoinSemiJoinReduction(previous);
            connectContext.getSessionVariable().setEnableOptimizerRuleDebug(false);
        }
    }

    private void assertFires(String sql) throws Exception {
        Assertions.assertFalse(plan(sql, false).contains("LEFT SEMI JOIN"), sql);
        String plan = plan(sql, true);
        Assertions.assertTrue(plan.contains("LEFT SEMI JOIN"), plan);
        Assertions.assertTrue(plan.contains("MultiCastDataSinks"), plan);
    }

    private void assertNotFires(String sql) throws Exception {
        Assertions.assertEquals(plan(sql, false), plan(sql, true), sql);
    }

    @Test
    public void testQ78() throws Exception {
        assertFires(Q78);
        // d_year = 2000 is applied both inside the CTE and again on its consumers; counting it twice made the
        // key set look tiny and broadcast it
        Assertions.assertFalse(plan(Q78, true).contains("LEFT SEMI JOIN (BROADCAST)"));
    }

    @Test
    public void testFires() throws Exception {
        assertFires(ABC + "where coalesce(b.q, 0) > 0 or coalesce(c.q, 0) > 0");
        // a disjunct also referencing a: only its b-only conjunct filters the key set
        assertFires(ABC + "where (b.q > 0 and b.q > a.q) or c.q > 0");
        // a disjunct with no b-only conjunct still requires a match in b
        assertFires(ABC + "where b.q > a.q or c.q > 0");
        // only k1 is an equality key of both joins
        assertFires(WITH + "select a.k1, b.q bq, c.q cq from a left join b on a.k1 = b.k1 " +
                "left join c on a.k1 = c.k1 and a.k2 = c.k2 where b.q > 0 or c.q > 0");
    }

    @Test
    public void testNotFires() throws Exception {
        // the OR as a whole null-rejects b, so the outer join is already an inner join
        assertNotFires(WITH + "select a.k1, b.q from a left join b on a.k1 = b.k1 and a.k2 = b.k2 " +
                "where b.q > 10 or b.k1 < 5");
        // not null-rejecting
        assertNotFires(ABC + "where coalesce(b.q, 0) = 0 or c.q > 0");
        assertNotFires(ABC + "where b.q is null or c.q > 0");
        // a disjunct on the preserved side
        assertNotFires(ABC + "where b.q > 0 or a.q > 0");
        // non-deterministic
        assertNotFires(ABC + "where b.q > rand() or c.q > 0");
        // full outer join on the spine
        assertNotFires(WITH + "select a.k1, b.q bq, c.q cq from a full join b on a.k1 = b.k1 and a.k2 = b.k2 " +
                "left join c on a.k1 = c.k1 and a.k2 = c.k2 where b.q > 0 or c.q > 0");
        // null-safe equality can match NULL keys
        assertNotFires(WITH + "select a.k1, b.q bq, c.q cq from a left join b on a.k1 <=> b.k1 " +
                "left join c on a.k1 <=> c.k1 where b.q > 0 or c.q > 0");
        // the join key is an aggregated value, not a group key
        assertNotFires(WITH + "select a.k1, b.q bq, c.q cq from a left join b on a.q = b.q " +
                "left join c on a.q = c.q where b.k1 > 0 or c.k1 > 0");
        // no aggregation under the preserved side
        assertNotFires("select ss_item_sk from store_sales " +
                "left join web_sales on ss_item_sk = ws_item_sk " +
                "left join catalog_sales on ss_item_sk = cs_item_sk " +
                "where ws_quantity > 0 or cs_quantity > 0");
        // grouping sets
        assertNotFires("with a as (select ss_item_sk k1, sum(ss_quantity) q from store_sales " +
                "group by rollup(ss_item_sk)), b as (select ws_item_sk k1, sum(ws_quantity) q from web_sales group by 1) " +
                "select a.k1, b.q from a left join b on a.k1 = b.k1 where b.q > 0 or b.q < -10");
        // a non-deterministic expression on the spine
        assertNotFires(WITH + "select a.k1, b.q bq, c.q cq from a " +
                "left join b on a.k1 = b.k1 and a.k2 = b.k2 and rand() > 0.5 " +
                "left join c on a.k1 = c.k1 and a.k2 = c.k2 where b.q > 0 or c.q > 0");
        // a limit below the top join picks its rows before the new filter would
        assertNotFires(WITH + "select t.k1, t.bq, c.q cq from " +
                "(select a.k1, a.k2, b.q bq from a left join b on a.k1 = b.k1 and a.k2 = b.k2 limit 100) t " +
                "left join c on t.k1 = c.k1 and t.k2 = c.k2 where t.bq > 0 or c.q > 0");
        // string keys are excluded, and no other key is left
        assertNotFires("with " +
                "a as (select cast(ss_item_sk as varchar) k, sum(ss_quantity) q from store_sales group by 1), " +
                "b as (select cast(ws_item_sk as varchar) k, sum(ws_quantity) q from web_sales group by 1), " +
                "c as (select cast(cs_item_sk as varchar) k, sum(cs_quantity) q from catalog_sales group by 1) " +
                "select a.k, b.q bq, c.q cq from a left join b on a.k = b.k left join c on a.k = c.k " +
                "where b.q > 0 or c.q > 0");
        // the OR is a match condition of the outer join, not a filter on its result
        assertNotFires(WITH + "select a.k1, b.q from a left join b on a.k1 = b.k1 and (b.q > 0 or b.q < -10)");
    }

    @Test
    public void testNotFiresWithoutKeyStatistics() throws Exception {
        // a cold connector statistics cache, or a table that was never analyzed: the aggregation estimate is
        // meaningless, so the rule keeps today's plan
        Set<String> keys = Set.of("ss_item_sk", "ss_customer_sk");
        StatisticStorage previous = connectContext.getGlobalStateMgr().getStatisticStorage();
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTPCDSStatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table table, String columnName) {
                return keys.contains(columnName) ? ColumnStatistic.unknown() : super.getColumnStatistic(table, columnName);
            }
        });
        try {
            assertNotFires(Q78);
            assertNotFires(ABC + "where coalesce(b.q, 0) > 0 or coalesce(c.q, 0) > 0");
        } finally {
            connectContext.getGlobalStateMgr().setStatisticStorage(previous);
        }
        // The aggregation estimate of ABC is capped at its input (the item x customer NDV product exceeds the
        // rows), and testFires still expects the rewrite: a capped estimate alone does not stop it.
        assertFires(ABC + "where coalesce(b.q, 0) > 0 or coalesce(c.q, 0) > 0");
    }

    @Test
    public void testNotWorthRewriting() throws Exception {
        // a small preserved side whose every key is also on the right: the semi join would filter nothing while
        // the key set reads two far larger aggregations
        String sql = "with a as (select ss_item_sk k, sum(ss_quantity) q from store_sales " +
                "where ss_sold_date_sk = 2451000 group by 1), " +
                "b as (select ss_item_sk k, ss_customer_sk cu, sum(ss_quantity) q from store_sales group by 1, 2), " +
                "c as (select cs_item_sk k, cs_bill_customer_sk cu, sum(cs_quantity) q from catalog_sales group by 1, 2) " +
                "select a.k, b.q bq, c.q cq from a left join b on a.k = b.k left join c on a.k = c.k " +
                "where b.q > 0 or c.q > 0";
        // the aggregation is below the minimum size
        assertNotFires(sql);
        // and without that limit the key set is still larger than the rows it would filter
        long previous = connectContext.getSessionVariable().getOuterJoinSemiJoinReductionMinAggRows();
        connectContext.getSessionVariable().setOuterJoinSemiJoinReductionMinAggRows(0);
        try {
            assertNotFires(sql);
        } finally {
            connectContext.getSessionVariable().setOuterJoinSemiJoinReductionMinAggRows(previous);
        }
    }
}
