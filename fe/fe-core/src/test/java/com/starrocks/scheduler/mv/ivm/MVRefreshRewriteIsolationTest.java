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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.scheduler.TaskRun;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * A materialized view refresh may be rewritten to read another materialized view. That is only sound when the other
 * view holds exactly what its base tables hold: a refresh reading the base tables at a pinned snapshot cannot be
 * answered by a view at its own version, and a refresh records the base versions it consumed, so a view tolerated as
 * stale for queries must not feed it.
 */
public class MVRefreshRewriteIsolationTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_mv_refresh_rewrite_isolation";

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @Test
    public void testPctRefreshReadsAFreshSibling() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_fresh_sibling");
        loadEveryPartition(fact);
        MaterializedView sibling = createRefreshedSibling("sibling_fresh", fact, "");
        MaterializedView mv = createMv("mv_fresh_sibling", "PCT", projectionOf(fact));

        Assertions.assertEquals(Set.of(sibling.getName()), tablesReadByFirstBatch(mv));
    }

    @Test
    public void testIncrementalBootstrapReadsTheBaseTable() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_bootstrap");
        loadEveryPartition(fact);
        createRefreshedSibling("sibling_bootstrap", fact, "");
        MaterializedView mv = createMv("mv_bootstrap", "INCREMENTAL", projectionOf(fact));
        int scansBefore = SCANNED_VERSIONS.size();
        TaskRun batch = runFirstBatch(mv);

        assertPinnedRefreshReadsOnly(batch, scansBefore, fact);
    }

    @Test
    public void testAutoBootstrapReadsTheBaseTable() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_auto");
        loadEveryPartition(fact);
        createRefreshedSibling("sibling_auto", fact, "");
        MaterializedView mv = createMv("mv_auto", "AUTO", projectionOf(fact));
        int scansBefore = SCANNED_VERSIONS.size();
        TaskRun batch = runFirstBatch(mv);

        assertPinnedRefreshReadsOnly(batch, scansBefore, fact);
    }

    @Test
    public void testForceRebuildReadsTheBaseTable() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_force");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_force", "INCREMENTAL", projectionOf(fact));
        runJob(mv, fact);
        createRefreshedSibling("sibling_force", fact, "");

        int scansBefore = SCANNED_VERSIONS.size();
        TaskRun batch = runForceFirstBatch(mv);

        assertPinnedRefreshReadsOnly(batch, scansBefore, fact);
    }

    @Test
    public void testSessionPropertiesDoNotReopenRewriteForAPinnedRefresh() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_session_props");
        loadEveryPartition(fact);
        createRefreshedSibling("sibling_session_props", fact, "");
        boolean refreshRewrite = Config.enable_mv_refresh_query_rewrite;
        Config.enable_mv_refresh_query_rewrite = false;
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_session_props PARTITION BY dt " +
                    "DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                    "PROPERTIES ('refresh_mode' = 'INCREMENTAL', 'partition_refresh_number' = '1', " +
                    "'partition_refresh_strategy' = 'strict', " +
                    "'session.enable_materialized_view_rewrite' = 'true', " +
                    "'session.enable_materialized_view_for_insert' = 'true') AS " + projectionOf(fact));
            MaterializedView mv = (MaterializedView) db.getTable("mv_session_props");
            int scansBefore = SCANNED_VERSIONS.size();
            TaskRun batch = runFirstBatch(mv);

            assertPinnedRefreshReadsOnly(batch, scansBefore, fact);
        } finally {
            Config.enable_mv_refresh_query_rewrite = refreshRewrite;
        }
    }

    @Test
    public void testPctRefreshDoesNotReadASiblingWithinItsStaleness() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_staleness");
        loadEveryPartition(fact);
        MaterializedView sibling =
                createRefreshedSibling("sibling_staleness", fact, ", 'mv_rewrite_staleness_second' = '3600'");
        // A UT task run has no process start time, so its refresh never confirms the freshness staleness counts from.
        sibling.getRefreshScheme().setLastFreshnessConfirmedAt(System.currentTimeMillis());
        loadEveryPartition(fact);
        assertQueryReads(projectionOf(fact), sibling);
        MaterializedView mv = createMv("mv_staleness", "PCT", projectionOf(fact));

        Assertions.assertEquals(Set.of(fact.getName()), tablesReadByFirstBatch(mv));
    }

    @Test
    public void testPctRefreshDoesNotReadAStaleNocheckSibling() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_nocheck");
        loadEveryPartition(fact);
        MaterializedView sibling =
                createRefreshedSibling("sibling_nocheck", fact, ", 'query_rewrite_consistency' = 'nocheck'");
        loadEveryPartition(fact);
        assertQueryReads(projectionOf(fact), sibling);
        MaterializedView mv = createMv("mv_nocheck", "PCT", projectionOf(fact));

        Assertions.assertEquals(Set.of(fact.getName()), tablesReadByFirstBatch(mv));
    }

    @Test
    public void testPctRefreshDoesNotReuseTheForceMvTimelinessOfAQuery() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_force_mv");
        loadEveryPartition(fact);
        MaterializedView sibling =
                createRefreshedSibling("sibling_force_mv", fact, ", 'query_rewrite_consistency' = 'force_mv'");
        loadEveryPartition(fact);
        assertQueryReads(projectionOf(fact), sibling);
        MaterializedView mv = createMv("mv_force_mv", "PCT", projectionOf(fact));

        Assertions.assertEquals(Set.of(fact.getName()), tablesReadByFirstBatch(mv));
    }

    @Test
    public void testBookmarkReadIsNotRewritten() throws Exception {
        mockLakeDml();
        OlapTable fact = createFact("fact_bookmark");
        loadEveryPartition(fact);
        MaterializedView ivm = createMv("mv_bookmark_holder", "INCREMENTAL", projectionOf(fact));
        runJob(ivm, fact);
        long bookmarkId = bookmarkIdOf(tvrBaseline(ivm), fact);
        MaterializedView sibling = createRefreshedSibling("sibling_bookmark", fact, "");

        assertQueryReads(projectionOf(fact), sibling);
        String pinned = starRocksAssert.query(
                "SELECT dt, k, v FROM " + fact.getName() + " [_BOOKMARK_" + bookmarkId + "_]").explainQuery();
        Assertions.assertFalse(pinned.contains(sibling.getName()), pinned);
    }

    private static void assertQueryReads(String query, MaterializedView mv) throws Exception {
        String plan = starRocksAssert.query(query).explainQuery();
        Assertions.assertTrue(plan.contains(mv.getName()), plan);
    }

    private static String projectionOf(OlapTable fact) {
        return "SELECT dt, k, v FROM " + fact.getName();
    }

    private static MaterializedView createRefreshedSibling(String name, OlapTable fact, String extraProperties)
            throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " PARTITION BY dt " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'PCT'" + extraProperties + ") AS " + projectionOf(fact));
        MaterializedView sibling = (MaterializedView) db.getTable(name);
        runJob(sibling, fact);
        return sibling;
    }

    // Iceberg pins its scans with a snapshot the optimizer cannot tell apart from a live read, so the refresh session
    // itself must not rewrite.
    private static void assertPinnedRefreshReadsOnly(TaskRun batch, int scansBefore, OlapTable base) {
        Assertions.assertEquals(Set.of(base.getName()), tablesReadSince(scansBefore));
        Assertions.assertFalse(refreshProcessorOf(batch).getMvContext().getCtx().getSessionVariable()
                .isEnableMaterializedViewRewriteForInsert());
    }

    private static Set<String> tablesReadByFirstBatch(MaterializedView mv) throws Exception {
        int scansBefore = SCANNED_VERSIONS.size();
        runFirstBatch(mv);
        return tablesReadSince(scansBefore);
    }

    private static Set<String> tablesReadSince(int scansBefore) {
        Set<String> tables = new HashSet<>();
        for (int i = scansBefore; i < SCANNED_VERSIONS.size(); i++) {
            SCANNED_VERSIONS.get(i).forEach((table, partitionVersions) -> {
                if (!partitionVersions.isEmpty()) {
                    tables.add(table);
                }
            });
        }
        return tables;
    }
}
