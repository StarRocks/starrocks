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

package com.starrocks.scheduler;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;

/**
 * Regression test for the bug: DROP VIEW clears the version map of dependent MVs,
 * causing unnecessary full re-refresh of all partitions on the next refresh cycle.
 *
 * Bug reference: https://github.com/StarRocks/starrocks/issues/68875
 * "Data loss - DROP VIEW over another MV cleared all data from underlying MV"
 *
 * Root cause:
 *   doInactiveMaterializedViewOnly() in AlterMVJobExecutor unconditionally called
 *   clearVisibleVersionMap() regardless of WHY the MV is being inactivated.
 *   When the inactivation is triggered by DROP VIEW (not a schema change),
 *   the MV's existing partition data is still physically valid — clearing the version map
 *   is wrong because on next refresh ALL partitions are treated as stale and re-computed
 *   via INSERT OVERWRITE unnecessarily.
 *
 * Fix:
 *   View.onDrop() overrides Table.onDrop() and calls inactiveRelatedMaterializedViewsRecursive()
 *   with clearVersionMap=false, preserving the version map so that re-activation + refresh
 *   can be incremental rather than a full re-computation.
 *
 *   DROP TABLE still clears the version map (clearVersionMap=true) since the base table
 *   is physically gone and a full refresh is expected if it is ever re-created.
 *
 * Note on version maps:
 *   For native OLAP base tables, the refresh processor writes to getBaseTableVisibleVersionMap()
 *   (keyed by table ID as Long). getBaseTableInfoVisibleVersionMap() is used for external tables.
 *   clearVisibleVersionMap() clears BOTH maps.
 */
@TestMethodOrder(MethodName.class)
public class DropViewOrTableClearsVersionMapBugTest extends MVTestBase {

    private static final String BASE_TABLE_DDL =
            "CREATE TABLE `test`.`bug_base_table` (\n" +
            "  `k1` date,\n" +
            "  `k2` int,\n" +
            "  `v1` int\n" +
            ") PARTITION BY RANGE(`k1`) (\n" +
            "  PARTITION p1 VALUES [('2020-01-01'),('2020-02-01')),\n" +
            "  PARTITION p2 VALUES [('2020-02-01'),('2020-03-01'))\n" +
            ") DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
            "PROPERTIES('replication_num' = '1');";

    // MV directly over the base table
    private static final String MV_ON_TABLE_DDL =
            "CREATE MATERIALIZED VIEW `test`.`bug_mv_on_table`\n" +
            "PARTITION BY `k1`\n" +
            "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES('replication_num' = '1')\n" +
            "AS SELECT k1, k2, v1 FROM `test`.`bug_base_table`;";

    // View over the base table (same base as the MV above)
    private static final String VIEW_ON_TABLE_DDL =
            "CREATE VIEW `test`.`bug_view_on_table` AS\n" +
            "SELECT k1, k2, v1 FROM `test`.`bug_base_table`;";

    // MV over the view (view is the base of this MV — the exact issue #68875 scenario)
    private static final String MV_ON_VIEW_DDL =
            "CREATE MATERIALIZED VIEW `test`.`bug_mv_on_view`\n" +
            "PARTITION BY `k1`\n" +
            "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES('replication_num' = '1')\n" +
            "AS SELECT k1, k2, v1 FROM `test`.`bug_view_on_table`;";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    /**
     * Verifies DROP TABLE behavior:
     *
     * Setup:
     *   base_table → bug_mv_on_table (MV, refreshed, has version map entries)
     *
     * Action:
     *   DROP TABLE base_table
     *
     * Expected behavior:
     *   bug_mv_on_table is set INACTIVE (base table gone — correct).
     *   bug_mv_on_table version map is CLEARED — this is acceptable since the base table
     *   is physically gone; a full refresh is expected if the table is ever re-created
     *   and the MV re-activated.
     */
    @Test
    public void testDropBaseTableClearsVersionMap() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable(BASE_TABLE_DDL)
                .withMaterializedView(MV_ON_TABLE_DDL);

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(testDb.getFullName(), "bug_mv_on_table");

        // Refresh the MV so the version map is populated.
        refreshMVRange("bug_mv_on_table", true);

        // For native OLAP base tables, version info is in getBaseTableVisibleVersionMap().
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMapBeforeDrop =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertFalse(versionMapBeforeDrop.isEmpty(),
                "Version map should be populated after refresh");
        Assertions.assertTrue(mv.isActive(),
                "MV should be active after refresh");

        // Drop the base table.
        starRocksAssert.dropTable("bug_base_table");

        // MV must be inactive — base table is gone, this is correct.
        Assertions.assertFalse(mv.isActive(),
                "MV should be inactive after base table is dropped");

        // DROP TABLE clears the version map — this is acceptable since the base table is gone
        // and the MV will need a full refresh if the table is re-created and the MV re-activated.
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMapAfterDrop =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertTrue(versionMapAfterDrop.isEmpty(),
                "Version map is cleared after DROP TABLE — expected behavior.");

        // Cleanup.
        starRocksAssert.dropMaterializedView("bug_mv_on_table");
    }

    /**
     * Proves the exact bug from issue #68875:
     *
     * Setup:
     *   base_table → bug_view_on_table (VIEW over base table)
     *             → bug_mv_on_view     (MV whose base is the VIEW, refreshed)
     *
     * Action:
     *   DROP VIEW bug_view_on_table
     *
     * Expected (correct behavior):
     *   bug_mv_on_view is set INACTIVE (correct — its base view is gone)
     *   bug_mv_on_view version map is PRESERVED (existing partition data is still valid)
     *
     * Actual (buggy behavior):
     *   bug_mv_on_view version map is CLEARED → existing MV data treated as fully stale
     *   → on next refresh INSERT OVERWRITE wipes and re-computes all partitions
     */
    @Test
    public void testDropViewClearsVersionMapOfDependentMV() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable(BASE_TABLE_DDL)
                .withView(VIEW_ON_TABLE_DDL)
                .withMaterializedView(MV_ON_VIEW_DDL);

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(testDb.getFullName(), "bug_mv_on_view");

        // Refresh the MV so the version map is populated.
        refreshMVRange("bug_mv_on_view", true);

        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMapBeforeDrop =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertFalse(versionMapBeforeDrop.isEmpty(),
                "Version map should be populated after refresh");
        Assertions.assertTrue(mv.isActive(),
                "MV should be active before view drop");

        // Drop the base view.
        starRocksAssert.dropView("bug_view_on_table");

        // MV should be inactive — its base view is gone, this is correct.
        Assertions.assertFalse(mv.isActive(),
                "MV should be inactive after its base view is dropped");

        // After the fix: version map is preserved because DROP VIEW does not clear it.
        // The MV's physical partition data remains valid even though the MV is inactive.
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMapAfterDrop =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertFalse(versionMapAfterDrop.isEmpty(),
                "Version map must be preserved after DROP VIEW — existing MV partition data is still valid.");

        // Cleanup.
        starRocksAssert.dropMaterializedView("bug_mv_on_view");
        starRocksAssert.dropTable("bug_base_table");
    }

    /**
     * Proves the cascading form of the bug:
     *
     * Setup:
     *   base_table → bug_mv_on_table  (MV, refreshed)
     *             → bug_view_on_table (VIEW over same base table)
     *             → bug_mv_on_view    (MV whose base is the VIEW, refreshed)
     *
     * Action:
     *   DROP VIEW bug_view_on_table
     *
     * Expected (correct behavior):
     *   bug_mv_on_view is INACTIVE (its base view dropped — correct).
     *   bug_mv_on_view version map is PRESERVED.
     *   bug_mv_on_table is completely UNAFFECTED (it does not depend on the view).
     *
     * Actual (buggy behavior):
     *   bug_mv_on_view version map is CLEARED.
     *   bug_mv_on_table remains ACTIVE (correct — no dependency on dropped view),
     *   but bug_mv_on_view's version map being cleared is still the bug.
     */
    @Test
    public void testDropViewCascadeClearsVersionMaps() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable(BASE_TABLE_DDL)
                .withMaterializedView(MV_ON_TABLE_DDL)
                .withView(VIEW_ON_TABLE_DDL)
                .withMaterializedView(MV_ON_VIEW_DDL);

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mvOnTable = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(testDb.getFullName(), "bug_mv_on_table");
        MaterializedView mvOnView = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(testDb.getFullName(), "bug_mv_on_view");

        // Refresh both MVs.
        refreshMVRange("bug_mv_on_table", true);
        refreshMVRange("bug_mv_on_view", true);

        Assertions.assertFalse(
                mvOnTable.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableVisibleVersionMap().isEmpty(),
                "bug_mv_on_table version map should be populated after refresh");
        Assertions.assertFalse(
                mvOnView.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableVisibleVersionMap().isEmpty(),
                "bug_mv_on_view version map should be populated after refresh");

        // Drop the view.
        starRocksAssert.dropView("bug_view_on_table");

        // bug_mv_on_view directly depends on the dropped view — INACTIVE is correct.
        Assertions.assertFalse(mvOnView.isActive(),
                "bug_mv_on_view should be inactive since its base view was dropped");

        // After the fix: bug_mv_on_view's version map is preserved.
        Assertions.assertFalse(
                mvOnView.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableVisibleVersionMap().isEmpty(),
                "Version map must be preserved after DROP VIEW — existing MV partition data is still valid.");

        // bug_mv_on_table does NOT depend on the dropped view — it must remain unaffected.
        Assertions.assertTrue(mvOnTable.isActive(),
                "bug_mv_on_table should remain active — it has no dependency on the dropped view.");
        Assertions.assertFalse(
                mvOnTable.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableVisibleVersionMap().isEmpty(),
                "bug_mv_on_table version map should be intact — it has no relationship with the dropped view.");

        // Cleanup.
        starRocksAssert.dropMaterializedView("bug_mv_on_view");
        starRocksAssert.dropMaterializedView("bug_mv_on_table");
        starRocksAssert.dropTable("bug_base_table");
    }
}
