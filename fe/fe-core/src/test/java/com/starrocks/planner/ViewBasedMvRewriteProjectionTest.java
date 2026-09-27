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

// Regression for partial view-based mv rewrite dropping a view scan's merged projection.
// When only some usages of a view are rewritten to a candidate MV, the rest are replaced back to
// their inlined plans; a replaced-back usage with no predicate used to drop its projection, leaving
// a UNION whose childOutputColumns reference columns the inlined plan no longer produces, which then
// hard-errored during statistics derivation with "missing statistic of col".
public class ViewBasedMvRewriteProjectionTest extends MaterializedViewTestBase {

    private static final String Q_AGG_BRANCH =
            "SELECT ns_tenant_id, instance, CAST(cnt AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR) " +
                    "FROM (SELECT ns_tenant_id, instance, COUNT(1) AS cnt FROM v_union GROUP BY ns_tenant_id, instance) s";
    private static final String Q_PLAIN_BRANCH =
            "SELECT ns_tenant_id, instance, c_coalesce, c_case, c_cast FROM v_union";
    private static final String Q_JOIN_BRANCH =
            "SELECT v.ns_tenant_id, v.instance, v.c_coalesce, v.c_case, v.c_cast " +
                    "FROM v_union v LEFT JOIN t_extra e ON v.ns_tenant_id = e.ns_tenant_id";
    private static final String THREE_BRANCHES =
            Q_AGG_BRANCH + " UNION ALL " + Q_PLAIN_BRANCH + " UNION ALL " + Q_JOIN_BRANCH;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        starRocksAssert.withTable("CREATE TABLE t_base (ns_tenant_id bigint NOT NULL, instance varchar(64), " +
                "file_root_id varchar(64), x varchar(64), y varchar(64), v bigint) DUPLICATE KEY(ns_tenant_id) " +
                "DISTRIBUTED BY HASH(ns_tenant_id) BUCKETS 3 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE t_extra (ns_tenant_id bigint NOT NULL, instance varchar(64), " +
                "tag varchar(64), arr array<varchar(64)>, v2 bigint) DUPLICATE KEY(ns_tenant_id) " +
                "DISTRIBUTED BY HASH(ns_tenant_id) BUCKETS 3 PROPERTIES('replication_num'='1')");

        // View is a UNION ALL that projects coalesce/case expressions and a CAST(NULL) constant.
        starRocksAssert.withView("CREATE VIEW v_union AS " +
                "SELECT ns_tenant_id, instance, file_root_id, COALESCE(x, y) AS c_coalesce, " +
                "CASE WHEN x IS NOT NULL THEN x ELSE y END AS c_case, CAST(NULL AS STRING) AS c_cast FROM t_base " +
                "UNION ALL " +
                "SELECT ns_tenant_id, instance, CAST(NULL AS STRING), x, CAST(NULL AS STRING), CAST(NULL AS STRING) " +
                "FROM t_base");

        // SPJG MV over the view: makes the aggregate usage rewritable so rewrite partially succeeds.
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_on_view DISTRIBUTED BY RANDOM " +
                "REFRESH DEFERRED MANUAL PROPERTIES('replication_num'='1') AS " +
                "SELECT ns_tenant_id, instance, COUNT(1) AS cnt FROM v_union GROUP BY ns_tenant_id, instance");
        // Non-SPJG multi-table MV with UNNEST, evaluated and pruned in the memo phase.
        createAndRefreshMV(MATERIALIZED_DB_NAME, "CREATE MATERIALIZED VIEW mv_nonspjg DISTRIBUTED BY RANDOM " +
                "REFRESH DEFERRED MANUAL PROPERTIES('replication_num'='1') AS " +
                "SELECT b.ns_tenant_id, b.instance, t.unnest AS tag_item, COUNT(1) AS c " +
                "FROM t_base b JOIN t_extra e ON b.ns_tenant_id = e.ns_tenant_id " +
                "CROSS JOIN LATERAL UNNEST(e.arr) t GROUP BY b.ns_tenant_id, b.instance, t.unnest");

        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
    }

    @Test
    public void testPartialViewRewriteKeepsProjection() {
        String plan = Assertions.assertDoesNotThrow(() -> getQueryPlan(THREE_BRANCHES));
        Assertions.assertTrue(plan.contains("11:Project\n" +
                "  |  <slot 59> : 59: ns_tenant_id\n" +
                "  |  <slot 60> : 60: instance\n" +
                "  |  <slot 62> : 62: coalesce\n" +
                "  |  <slot 71> : 63: cast\n" +
                "  |  <slot 72> : 64: cast\n" +
                "  |  \n" +
                "  4:UNION\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |    \n" +
                "  7:EXCHANGE"));
    }

    @Test
    public void testDisableViewBasedRewriteWorkaround() {
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
        try {
            Assertions.assertDoesNotThrow(() -> getQueryPlan(THREE_BRANCHES));
        } finally {
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        }
    }
}
