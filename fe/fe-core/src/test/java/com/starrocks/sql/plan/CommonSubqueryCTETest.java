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

import com.starrocks.sql.analyzer.CommonSubqueryCTEHoister;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class CommonSubqueryCTETest extends TPCDSPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
    }

    @BeforeEach
    public void enableRule() {
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(true);
        // Take the cost model out of the picture: these tests assert that the CTE is *found*, not that the
        // optimizer chooses to materialize it. testQ65InlinedPlanMatchesDisabledPlan covers the other side.
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @AfterEach
    public void restoreDefaults() {
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(1.15);
    }

    private int scanCount(String sql, String table) throws Exception {
        String plan = getFragmentPlan(sql);
        int count = 0;
        int from = 0;
        String needle = "TABLE: " + table + "\n";
        while (true) {
            int at = plan.indexOf(needle, from);
            if (at < 0) {
                return count;
            }
            count++;
            from = at + needle.length();
        }
    }

    @Test
    public void testQ65ScansStoreSalesOnce() throws Exception {
        assertEquals(1, scanCount(Q65, "store_sales"));
        assertTrue(getFragmentPlan(Q65).contains("MultiCastDataSinks"));
    }

    @Test
    public void testQ65ScansStoreSalesTwiceWhenDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(false);
        assertEquals(2, scanCount(Q65, "store_sales"));
    }

    @Test
    public void testQ44ScansStoreSalesTwiceNotFour() throws Exception {
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(false);
        int before = scanCount(Q44, "store_sales");
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(true);
        int after = scanCount(Q44, "store_sales");
        assertEquals(4, before);
        assertEquals(2, after);
    }

    /**
     * When the cost model inlines the synthesized CTE back into both places, the resulting plan must be
     * the plan we produce with the pass switched off. That reversibility is what bounds the regression
     * risk of turning the pass on.
     *
     * <p>Column-ref and plan-node ids do shift, because building the CTE allocates refs before the
     * inline pass removes it again. Comparing with every integer erased still pins down the operator
     * tree, fragment layout, join order and distributions - only literal numbers are out of scope.
     */
    @Test
    public void testQ65InlinedPlanMatchesDisabledPlan() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        String hoisted = getFragmentPlan(Q65);
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(false);
        String original = getFragmentPlan(Q65);
        assertEquals(eraseIds(original), eraseIds(hoisted));
    }

    private static String eraseIds(String plan) {
        return plan.replaceAll("\\d+", "N");
    }

    @Test
    public void testTwoIdenticalDerivedTables() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk, sum(ss_sales_price) rev from store_sales group by ss_store_sk) a, "
                + "(select ss_store_sk, sum(ss_sales_price) rev from store_sales group by ss_store_sk) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        assertEquals(1, scanCount(sql, "store_sales"));
    }

    @Test
    public void testDifferentDerivedTablesNotShared() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk, sum(ss_sales_price) rev from store_sales group by ss_store_sk) a, "
                + "(select ss_store_sk, sum(ss_net_profit) rev from store_sales group by ss_store_sk) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        assertEquals(2, scanCount(sql, "store_sales"));
    }

    @Test
    public void testNonDeterministicNotShared() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk, rand() r from store_sales) a, "
                + "(select ss_store_sk, rand() r from store_sales) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        assertEquals(2, scanCount(sql, "store_sales"));
    }

    @Test
    public void testLimitNotShared() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk from store_sales limit 10) a, "
                + "(select ss_store_sk from store_sales limit 10) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        assertEquals(2, scanCount(sql, "store_sales"));
    }

    /**
     * A LIMIT nested below the candidate is just as unsafe to share as one written on it directly: the
     * inner derived table keeps its LIMIT, so two occurrences may pick different rows today.
     */
    @Test
    public void testNestedLimitNotShared() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk from (select ss_store_sk from store_sales limit 1) i1) a, "
                + "(select ss_store_sk from (select ss_store_sk from store_sales limit 1) i2) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        assertEquals(2, scanCount(sql, "store_sales"));
    }

    /**
     * The pre-analysis guards read SQL text and cannot see through a view, so a view calling rand() would
     * otherwise be shared. The post-analysis re-check must catch it and revert.
     */
    @Test
    public void testNonDeterministicBehindViewNotShared() throws Exception {
        starRocksAssert.withView("create view cse_rand_view as select ss_store_sk, rand() r from store_sales");
        try {
            String sql = "select a.ss_store_sk, b.ss_store_sk from "
                    + "(select ss_store_sk, r from cse_rand_view) a, "
                    + "(select ss_store_sk, r from cse_rand_view) b "
                    + "where a.ss_store_sk = b.ss_store_sk";
            assertEquals(2, scanCount(sql, "store_sales"));
        } finally {
            starRocksAssert.dropView("cse_rand_view");
        }
    }

    /**
     * A prepared statement re-plans one cached AST per EXECUTE, so a rewrite whose grouping decision depends
     * on the current parameter bindings must not be baked in. Parameterized statements are skipped outright.
     */
    @Test
    public void testParameterizedStatementNotRewritten() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk from store_sales where ss_item_sk = ?) a, "
                + "(select ss_store_sk from store_sales where ss_item_sk = ?) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        // `?` parses into a PrepareStmt wrapper; the statement actually re-planned on every EXECUTE is the
        // inner one, which is what the guard has to inspect.
        PrepareStmt prepared = (PrepareStmt) com.starrocks.sql.parser.SqlParser
                .parse("prepare p from " + sql, connectContext.getSessionVariable()).get(0);
        QueryStatement stmt = (QueryStatement) prepared.getInnerStmt();
        assertTrue(CommonSubqueryCTEHoister.hoist(stmt).isEmpty());
    }

    /** Same shape without parameters is still rewritten, so the guard above is not over-broad. */
    @Test
    public void testSameShapeWithoutParametersIsRewritten() throws Exception {
        String sql = "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk from store_sales where ss_item_sk = 1) a, "
                + "(select ss_store_sk from store_sales where ss_item_sk = 1) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        QueryStatement stmt = (QueryStatement) com.starrocks.sql.parser.SqlParser
                .parse(sql, connectContext.getSessionVariable()).get(0);
        assertTrue(!CommonSubqueryCTEHoister.hoist(stmt).isEmpty());
    }

    @Test
    public void testExplicitColumnAliasesNotShared() throws Exception {
        String sql = "select a.k, b.k from "
                + "(select ss_store_sk from store_sales) a(k), "
                + "(select ss_store_sk from store_sales) b(k) "
                + "where a.k = b.k";
        assertEquals(2, scanCount(sql, "store_sales"));
    }

    /** A body that mentions a WITH-defined name may bind differently at each site, so it is left alone. */
    @Test
    public void testBodyReferencingCteNameNotShared() throws Exception {
        String sql = "with ss as (select ss_store_sk from store_sales) "
                + "select a.ss_store_sk, b.ss_store_sk from "
                + "(select ss_store_sk from ss) a, (select ss_store_sk from ss) b "
                + "where a.ss_store_sk = b.ss_store_sk";
        String hoisted = getFragmentPlan(sql);
        connectContext.getSessionVariable().setEnableCommonSubqueryCte(false);
        assertEquals(eraseIds(getFragmentPlan(sql)), eraseIds(hoisted));
    }

    /**
     * A correlated derived table cannot be hoisted to the outermost block: the outer column stops
     * resolving there. The caller must notice, revert, and still plan the query.
     */
    @Test
    public void testCorrelatedDerivedTableFallsBack() throws Exception {
        String sql = "select ss_store_sk from store_sales s1 where exists ("
                + "select 1 from "
                + "(select sr_item_sk from store_returns where sr_returned_date_sk = s1.ss_sold_date_sk) x, "
                + "(select sr_item_sk from store_returns where sr_returned_date_sk = s1.ss_sold_date_sk) y "
                + "where x.sr_item_sk = y.sr_item_sk)";

        connectContext.getSessionVariable().setEnableCommonSubqueryCte(false);
        String baseline;
        try {
            baseline = getFragmentPlan(sql);
        } catch (Exception e) {
            // StarRocks rejects this shape on its own; there is nothing for the fallback to protect.
            assumeTrue(false, "correlated derived tables are not supported: " + e.getMessage());
            return;
        }

        connectContext.getSessionVariable().setEnableCommonSubqueryCte(true);
        assertEquals(eraseIds(baseline), eraseIds(getFragmentPlan(sql)));
    }
}
