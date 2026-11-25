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
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A refresh records the base table versions it consumed, so its query may be answered by another materialized view
 * only when that view holds what the base tables hold, whatever staleness the view's owner tolerates for queries.
 */
public class MvRefreshRewriteConsistencyTest extends MVTestBase {

    @Test
    public void testRefreshReadsAFreshSibling() throws Exception {
        String fact = createFact("t_fresh_sibling");
        createAndRefreshMv(siblingDdl("sibling_fresh", fact, ""));

        PlanTestBase.assertContains(refreshPlanOf("mv_fresh_sibling", fact), "sibling_fresh");
    }

    @Test
    public void testRefreshDoesNotReadASiblingWithinItsStaleness() throws Exception {
        String fact = createFact("t_staleness");
        createAndRefreshMv(siblingDdl("sibling_staleness", fact, "'mv_rewrite_staleness_second' = '3600'"));
        loadEveryPartition(fact);
        PlanTestBase.assertContains(getFragmentPlan(rollupQuery(fact)), "sibling_staleness");

        PlanTestBase.assertNotContains(refreshPlanOf("mv_staleness", fact), "sibling_staleness");
    }

    @Test
    public void testRefreshDoesNotReadAStaleNocheckSibling() throws Exception {
        String fact = createFact("t_nocheck");
        createAndRefreshMv(siblingDdl("sibling_nocheck", fact, "'query_rewrite_consistency' = 'nocheck'"));
        loadEveryPartition(fact);
        PlanTestBase.assertContains(getFragmentPlan(rollupQuery(fact)), "sibling_nocheck");

        PlanTestBase.assertNotContains(refreshPlanOf("mv_nocheck", fact), "sibling_nocheck");
    }

    @Test
    public void testRefreshDoesNotReuseTheForceMvTimelinessOfAQuery() throws Exception {
        String fact = createFact("t_force_mv");
        createAndRefreshMv(siblingDdl("sibling_force_mv", fact, "'query_rewrite_consistency' = 'force_mv'"));
        loadEveryPartition(fact);
        PlanTestBase.assertContains(getFragmentPlan(rollupQuery(fact)), "sibling_force_mv");

        PlanTestBase.assertNotContains(refreshPlanOf("mv_force_mv", fact), "sibling_force_mv");
    }

    private static String createFact(String name) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + name + " (dt date NOT NULL, k bigint NOT NULL, v bigint NOT NULL) " +
                "DUPLICATE KEY(dt, k) PARTITION BY RANGE(dt) (" +
                "PARTITION p1 VALUES [('2026-07-01'), ('2026-07-02')), " +
                "PARTITION p2 VALUES [('2026-07-02'), ('2026-07-03'))) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 2 PROPERTIES('replication_num' = '1')");
        loadEveryPartition(name);
        return name;
    }

    private static void loadEveryPartition(String fact) throws Exception {
        executeInsertSql(connectContext, "INSERT INTO " + fact + " PARTITION(p1) VALUES ('2026-07-01', 1, 1)");
        executeInsertSql(connectContext, "INSERT INTO " + fact + " PARTITION(p2) VALUES ('2026-07-02', 2, 1)");
    }

    private static String siblingDdl(String name, String fact, String properties) {
        return "CREATE MATERIALIZED VIEW " + name + " PARTITION BY dt DISTRIBUTED BY HASH(dt) BUCKETS 2 " +
                "REFRESH DEFERRED MANUAL " + (properties.isEmpty() ? "" : "PROPERTIES (" + properties + ") ") +
                "AS SELECT dt, k, sum(v) AS sv FROM " + fact + " GROUP BY dt, k";
    }

    private static String rollupQuery(String fact) {
        return "SELECT dt, sum(v) AS sv FROM " + fact + " GROUP BY dt";
    }

    private String refreshPlanOf(String mvName, String fact) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " PARTITION BY dt " +
                "DISTRIBUTED BY HASH(dt) BUCKETS 2 REFRESH DEFERRED MANUAL AS " + rollupQuery(fact));
        MaterializedView mv = getMv(DB_NAME, mvName);
        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
        ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
        Assertions.assertNotNull(execPlan);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }
}
