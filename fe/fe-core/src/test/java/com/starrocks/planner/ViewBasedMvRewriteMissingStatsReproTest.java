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

package com.starrocks.planner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Repro for the field issue (CelerData ENG-1162031, StarRocks 4.0.x and main):
 *   ERROR 1064: only found column statistics: {...}, but missing statistic of col: cast
 *
 * Mechanism:
 *  - enable_view_based_mv_rewrite folds views into LogicalViewScanOperator;
 *  - multi-stage rewrite PHASE1 runs mv rewrite early;
 *  - when only SOME view scans are rewritten (here: the aggregate branch matches mv_on_view,
 *    the other two branches do not), viewBasedMvRuleRewrite replaces the remaining view scans
 *    back via MvUtils.replaceLogicalViewScanOperator, whose predicate==null branch drops the
 *    view scan's merged projection;
 *  - the resulting plan's UNION childOutputColumns still references a column the child no
 *    longer defines, so memo statistics derivation (StatisticsCalculator.computeUnionNode ->
 *    Statistics.getColumnStatistic) throws at plan time.
 *
 * Matches the customer's bisection: three DISTINCT branches are required; any two pass.
 */
public class ViewBasedMvRewriteMissingStatsReproTest extends MaterializedViewTestBase {

    // Aggregate sub-query matching mv_on_view's definition -> rewritable view usage.
    private static final String Q_AGG_MATCH =
            "SELECT ns_tenant_id, instance, COUNT(1) AS cnt FROM v_union " +
                    "GROUP BY ns_tenant_id, instance";
    private static final String Q_AGG_BRANCH =
            "SELECT ns_tenant_id, instance, CAST(cnt AS VARCHAR), CAST(NULL AS VARCHAR), " +
                    "CAST(NULL AS VARCHAR) FROM (" + Q_AGG_MATCH + ") s";
    // Plain view usage projecting the view's expression columns (incl. the CAST NULL constant).
    private static final String Q_PLAIN_BRANCH =
            "SELECT ns_tenant_id, instance, c_coalesce, c_case, c_cast FROM v_union";
    // View used as the right side of a LEFT JOIN: the ON condition cannot be pushed down as the
    // view scan's predicate, so the replacement path hits the predicate==null branch.
    private static final String Q_JOIN_BRANCH =
            "SELECT v.ns_tenant_id, v.instance, v.c_coalesce, v.c_case, v.c_cast " +
                    "FROM v_union v LEFT JOIN t_extra e ON v.ns_tenant_id = e.ns_tenant_id";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        starRocksAssert.withTable("CREATE TABLE t_base (\n" +
                "  ns_tenant_id bigint NOT NULL,\n" +
                "  instance varchar(64),\n" +
                "  file_root_id varchar(64),\n" +
                "  x varchar(64),\n" +
                "  y varchar(64),\n" +
                "  v bigint\n" +
                ") DUPLICATE KEY(ns_tenant_id)\n" +
                "DISTRIBUTED BY HASH(ns_tenant_id) BUCKETS 3\n" +
                "PROPERTIES('replication_num'='1')");

        starRocksAssert.withTable("CREATE TABLE t_extra (\n" +
                "  ns_tenant_id bigint NOT NULL,\n" +
                "  instance varchar(64),\n" +
                "  tag varchar(64),\n" +
                "  arr array<varchar(64)>,\n" +
                "  v2 bigint\n" +
                ") DUPLICATE KEY(ns_tenant_id)\n" +
                "DISTRIBUTED BY HASH(ns_tenant_id) BUCKETS 3\n" +
                "PROPERTIES('replication_num'='1')");

        // Logical view shaped like the customer's drives_base_mv: UNION ALL branches where one
        // branch projects real columns plus coalesce/case expressions and the other synthesizes
        // CAST(NULL AS STRING) constants.
        starRocksAssert.withView("CREATE VIEW v_union AS\n" +
                "SELECT ns_tenant_id, instance, file_root_id,\n" +
                "       COALESCE(x, y) AS c_coalesce,\n" +
                "       CASE WHEN x IS NOT NULL THEN x ELSE y END AS c_case,\n" +
                "       CAST(NULL AS STRING) AS c_cast\n" +
                "FROM t_base\n" +
                "UNION ALL\n" +
                "SELECT ns_tenant_id, instance, CAST(NULL AS STRING),\n" +
                "       x, CAST(NULL AS STRING), CAST(NULL AS STRING)\n" +
                "FROM t_base");

        // SPJG MV over the view: makes the aggregate view usage rewritable so that
        // viewBasedMvRuleRewrite hits the partial-success path.
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_on_view " +
                "DISTRIBUTED BY RANDOM\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES('replication_num'='1')\n" +
                "AS SELECT ns_tenant_id, instance, COUNT(1) AS cnt\n" +
                "FROM v_union GROUP BY ns_tenant_id, instance");

        // Non-SPJG multi-table MV with UNNEST, mirroring the customer's finding_overview
        // candidate. Being multi-table it defers single-table mv rewrite into the memo phase
        // (MvRewriteStrategy), which is part of the customer's trigger conditions.
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_nonspjg " +
                "DISTRIBUTED BY RANDOM\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES('replication_num'='1')\n" +
                "AS SELECT b.ns_tenant_id, b.instance, t.unnest AS tag_item, COUNT(1) AS c\n" +
                "FROM t_base b JOIN t_extra e ON b.ns_tenant_id = e.ns_tenant_id\n" +
                "CROSS JOIN LATERAL UNNEST(e.arr) t\n" +
                "GROUP BY b.ns_tenant_id, b.instance, t.unnest");

        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
    }

    @Test
    public void testMissingColumnStatisticsWithPartialViewRewrite() {
        String threeBranches = Q_AGG_BRANCH
                + " UNION ALL " + Q_PLAIN_BRANCH
                + " UNION ALL " + Q_JOIN_BRANCH;
        Throwable t = Assertions.assertThrows(Throwable.class, () -> getQueryPlan(threeBranches));
        Assertions.assertTrue(t.getMessage() != null && t.getMessage().contains("missing statistic of col"),
                "expected 'missing statistic of col' but got: " + t);
    }

    @Test
    public void testControlWorkaroundDisableViewBasedRewrite() {
        // NOTE: 2-branch subsets are NOT used as controls here: whether they fail depends on the
        // warmth of the MV plan cache (they DO fail when planned first on a cold cache, and pass
        // when planned after another query already populated it), which matches how erratic the
        // customer's synthetic-repro attempts were. The only stable control is the verified
        // workaround: disabling view-based rewrite always plans fine.
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
        try {
            Assertions.assertDoesNotThrow(() -> getQueryPlan(Q_AGG_BRANCH
                    + " UNION ALL " + Q_PLAIN_BRANCH
                    + " UNION ALL " + Q_JOIN_BRANCH));
        } finally {
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        }
    }
}
