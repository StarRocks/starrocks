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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan-shape regression for the MV rewrite staleness baseline fix: {@code MaterializedView#isStalenessSatisfied()}
 * uses {@code refreshScheme.getLastFreshnessConfirmedAt()} (the start time of the last COMPLETE refresh batch) as
 * the freshness baseline instead of {@code lastRefreshTime}. These tests exercise the query-rewrite layer directly
 * (not just the unit-level boolean) to prove the two plan shapes it produces:
 * <ul>
 *     <li>staleness NOT satisfied -&gt; checked-mode per-partition change detection runs, and the one partition
 *     that actually changed since the MV's last refresh is compensated from the base table via UNION;</li>
 *     <li>staleness satisfied -&gt; the whole MV is used directly, no per-partition check, no UNION.</li>
 * </ul>
 */
public class MvRewriteStalenessTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE t_staleness_base (\n"
                + "  dt date NOT NULL,\n"
                + "  id bigint NOT NULL,\n"
                + "  v bigint NOT NULL\n"
                + ") DUPLICATE KEY(dt, id)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p1 VALUES [('2026-07-01'), ('2026-07-02')),\n"
                + "  PARTITION p2 VALUES [('2026-07-02'), ('2026-07-03'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 2\n"
                + "PROPERTIES('replication_num'='1')");
        // Seed both partitions BEFORE any MV is created off this table, each with its own explicit PARTITION(...)
        // clause. Two reasons this matters:
        //  1) Keep every MV mirror partition non-empty so plan shapes stay comparable (empty partitions can
        //     be pruned into degenerate plans). Note: in LOOSE/FORCE_MV modes MVTimelinessArbiter's
        //     addEmptyPartitionsToRefresh would additionally force-flag empty MV partitions, but these tests
        //     run the default CHECKED mode, where that mechanism does not apply.
        //  2) An INSERT without an explicit partition clause is planned/published against every partition the
        //     statement's target-partition resolution touches; observed empirically to bump ALL partitions'
        //     visible version/version-time together even when only one partition's range matches the row, which
        //     would make p1 look "changed" too and defeat the very distinction (p1 untouched vs. p2 modified
        //     after refresh) these tests are trying to prove. Sibling tests in MvRewritePartialPartitionTest
        //     consistently scope such inserts with an explicit PARTITION(...) clause for the same reason.
        executeInsertSql(connectContext, "INSERT INTO t_staleness_base PARTITION(p1) VALUES ('2026-07-01', 100, 1)");
        executeInsertSql(connectContext, "INSERT INTO t_staleness_base PARTITION(p2) VALUES ('2026-07-02', 200, 1)");
    }

    /**
     * Cross-partition staleness masking regression: p2's base partition is modified AFTER the MV's last complete
     * refresh, but the last CONFIRMED complete refresh (the staleness baseline) is 2h old, past the 3600s
     * tolerance. isStalenessSatisfied() must be false, which forces checked-mode per-partition change detection:
     * p2 (changed) must be compensated from the base table (UNION); p1 (untouched) must still be served from the MV.
     */
    @Test
    public void testStalenessMaskedPartitionIsCompensated() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW mv_staleness_masked\n"
                + "PARTITION BY (dt)\n"
                + "DISTRIBUTED BY HASH(dt) BUCKETS 2\n"
                + "REFRESH MANUAL\n"
                + "PROPERTIES('mv_rewrite_staleness_second'='3600')\n"
                + "AS SELECT dt, sum(v) AS sv FROM t_staleness_base GROUP BY dt");
        try {
            // base p2 changes AFTER the full refresh -> its MV copy is now stale. Explicit PARTITION(p2): see the
            // comment in beforeClass about unscoped inserts bumping every partition's version together.
            executeInsertSql(connectContext, "INSERT INTO t_staleness_base PARTITION(p2) VALUES ('2026-07-02', 1, 5)");

            MaterializedView mv = getMv("test", "mv_staleness_masked");
            long now = System.currentTimeMillis();
            // Simulate the masking precondition: the last CONFIRMED complete refresh started 2h ago, well past the
            // 3600s tolerance. Deliberately NOT touching lastRefreshTime here: isStalenessSatisfied() has a
            // rollback-detection guard (base table's max refresh timestamp regressing below lastRefreshTime) that
            // is orthogonal to what this test is proving, and lastRefreshTime is already consistent (<= the base
            // table's real visible-version time) from the real refresh above.
            mv.getRefreshScheme().setLastFreshnessConfirmedAt(now - 7_200_000L);

            String plan = getFragmentPlan("SELECT dt, sum(v) FROM t_staleness_base "
                    + "WHERE dt >= '2026-07-01' AND dt < '2026-07-03' GROUP BY dt");
            PlanTestBase.assertContains(plan, "UNION");
            PlanTestBase.assertContains(plan, "t_staleness_base");
            PlanTestBase.assertContains(plan, "mv_staleness_masked");
        } finally {
            starRocksAssert.dropMaterializedView("mv_staleness_masked");
        }
    }

    /** Short-circuit still works: freshness confirmed within the budget -> direct MV scan, no UNION. */
    @Test
    public void testStalenessConfirmedWithinBudgetServesMvDirectly() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW mv_staleness_fresh\n"
                + "PARTITION BY (dt)\n"
                + "DISTRIBUTED BY HASH(dt) BUCKETS 2\n"
                + "REFRESH MANUAL\n"
                + "PROPERTIES('mv_rewrite_staleness_second'='3600')\n"
                + "AS SELECT dt, sum(v) AS sv FROM t_staleness_base GROUP BY dt");
        try {
            executeInsertSql(connectContext, "INSERT INTO t_staleness_base PARTITION(p2) VALUES ('2026-07-02', 2, 7)");

            MaterializedView mv = getMv("test", "mv_staleness_fresh");
            long now = System.currentTimeMillis();
            // Freshness confirmed "now" (>= the insert's commit time above) -> a negative/zero staleness gap ->
            // within the 3600s budget -> isStalenessSatisfied() == true -> whole MV served directly, no UNION.
            mv.getRefreshScheme().setLastFreshnessConfirmedAt(now);

            String plan = getFragmentPlan("SELECT dt, sum(v) FROM t_staleness_base "
                    + "WHERE dt >= '2026-07-01' AND dt < '2026-07-03' GROUP BY dt");
            PlanTestBase.assertContains(plan, "mv_staleness_fresh");
            PlanTestBase.assertNotContains(plan, "UNION");
        } finally {
            starRocksAssert.dropMaterializedView("mv_staleness_fresh");
        }
    }
}
