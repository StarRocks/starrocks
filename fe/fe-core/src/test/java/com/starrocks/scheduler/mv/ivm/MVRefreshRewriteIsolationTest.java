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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A refresh that reads its base tables at a pinned snapshot cannot be answered by another materialized view, which
 * holds the base tables as of its own refresh.
 */
public class MVRefreshRewriteIsolationTest extends IvmIcebergRefreshJobTestBase {
    private static final String FACT_QUERY = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";

    @Test
    public void testPctRefreshReadsAFreshSibling() throws Exception {
        loadFact();
        MaterializedView sibling = createRefreshedSibling("sibling_fresh");
        MaterializedView mv = createFactMv("mv_fresh_sibling", "pct");

        String plan = refreshPlanOf(runFirstBatch(mv));
        Assertions.assertTrue(plan.contains(sibling.getName()), plan);
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testBootstrapReadsTheBaseTable(String refreshMode) throws Exception {
        loadFact();
        MaterializedView sibling = createRefreshedSibling("sibling_bootstrap_" + refreshMode);
        MaterializedView mv = createFactMv("mv_bootstrap_" + refreshMode, refreshMode);

        assertPinnedRefreshDoesNotRead(runFirstBatch(mv), sibling);
    }

    @Test
    public void testForceRebuildReadsTheBaseTable() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_force", "incremental");
        runJob(mv);
        MaterializedView sibling = createRefreshedSibling("sibling_force");

        assertPinnedRefreshDoesNotRead(runForceFirstBatch(mv), sibling);
    }

    @Test
    public void testSessionPropertiesDoNotReopenRewriteForAPinnedRefresh() throws Exception {
        loadFact();
        MaterializedView sibling = createRefreshedSibling("sibling_session_props");
        boolean refreshRewrite = Config.enable_mv_refresh_query_rewrite;
        Config.enable_mv_refresh_query_rewrite = false;
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`mv_session_props` " +
                    "PARTITION BY `date` REFRESH DEFERRED MANUAL " +
                    "PROPERTIES ('refresh_mode' = 'incremental', 'partition_refresh_number' = '1', " +
                    "'partition_refresh_strategy' = 'strict', " +
                    "'session.enable_materialized_view_rewrite' = 'true', " +
                    "'session.enable_materialized_view_for_insert' = 'true') AS " + FACT_QUERY);

            assertPinnedRefreshDoesNotRead(runFirstBatch(getMv("mv_session_props")), sibling);
        } finally {
            Config.enable_mv_refresh_query_rewrite = refreshRewrite;
        }
    }

    private MaterializedView createRefreshedSibling(String name) throws Exception {
        MaterializedView sibling = createFactMv(name, "pct");
        runJob(sibling);
        return sibling;
    }

    private static void assertPinnedRefreshDoesNotRead(TaskRun batch, MaterializedView sibling) throws Exception {
        MvTaskRunContext context = getMVTaskRunProcessor(batch).getMvTaskRunContext();
        Assertions.assertFalse(context.getRefreshRuntimeState().getPinnedTvrMap().isEmpty(),
                "the refresh is expected to read pinned base table snapshots");
        String plan = refreshPlanOf(batch);
        Assertions.assertFalse(plan.contains(sibling.getName()), plan);
        Assertions.assertFalse(context.getCtx().getSessionVariable().isEnableMaterializedViewRewriteForInsert());
    }

    private static String refreshPlanOf(TaskRun batch) {
        ExecPlan execPlan = getMVTaskRunProcessor(batch).getMvTaskRunContext().getExecPlan();
        Assertions.assertNotNull(execPlan);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }
}
