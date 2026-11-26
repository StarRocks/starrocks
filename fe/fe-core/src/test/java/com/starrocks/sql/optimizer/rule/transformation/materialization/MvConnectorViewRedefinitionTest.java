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

import com.google.common.collect.ImmutableMap;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ConnectorView;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.sys.SysObjectDependencies;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MVs defined on a view of an external catalog ({@link ConnectorView}) that the catalog redefines behind
 * StarRocks' back, e.g. a Unity Catalog view.
 *
 * <p>The tests only depend on the {@link ConnectorView} contract, so the view is a plain {@link HiveView}. As in
 * production, the catalog converts the view again on every lookup, so each query sees a new view object with a new
 * id and the view's current definition, and nothing stored on a previous object survives.
 *
 * <p>The view bodies read native tables, which stand in for the external tables behind a real view and avoid
 * mocking external snapshots or MV freshness.
 */
class MvConnectorViewRedefinitionTest extends MVTestBase {
    private static final String CATALOG = "unity_mv_catalog";
    private static final String DB = "unity_mv_db";
    private static final String TABLE_VIEW = CATALOG + "." + DB + ".customer_table_view";
    private static final String TABLE_1_SQL = "SELECT * FROM default_catalog.test.mv_view_table_1";
    private static final String TABLE_2_SQL = "SELECT * FROM default_catalog.test.mv_view_table_2";
    // The view's current definition in the external catalog; tests change it to simulate the view being redefined.
    private static volatile String tableViewSql = TABLE_1_SQL;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        // installs the MockedMetadataMgr the view catalog registers with
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("hive", CATALOG, "",
                ImmutableMap.of("type", "hive", "hive.metastore.uris", "thrift://127.0.0.1:9083"));
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(CATALOG, new RedefinableViewMetadata());
    }

    /** Serves the view, converted again from its current definition on every lookup. */
    private static class RedefinableViewMetadata extends MockedHiveMetadata {
        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            if (DB.equalsIgnoreCase(dbName) && "customer_table_view".equalsIgnoreCase(tblName)) {
                // names as the query spelled them, like a real connector
                return toConnectorView(dbName, tblName, tableViewSql);
            }
            return null;
        }
    }

    private static ConnectorView toConnectorView(String dbName, String viewName, String sql) {
        List<Column> schema = List.of(
                new Column("c_custkey", IntegerType.INT),
                new Column("c_name", StringType.DEFAULT_STRING),
                new Column("c_mktsegment", StringType.DEFAULT_STRING));
        return new HiveView(GlobalStateMgr.getCurrentState().getNextId(), CATALOG, dbName, viewName, schema, sql,
                HiveView.Type.Hive);
    }

    @Test
    void refreshedMvOnRedefinedViewDoesNotRewriteOldBaseTable() throws Exception {
        boolean enablePlanCache = connectContext.getSessionVariable().isEnableMaterializedViewPlanCache();
        boolean enableViewRewrite = connectContext.getSessionVariable().isEnableViewBasedMvRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        try {
            // Native base tables avoid mocking external snapshots or MV freshness. The view itself is a real
            // ConnectorView, with the same schema before and after replacement.
            for (String table : List.of("mv_view_table_1", "mv_view_table_2")) {
                starRocksAssert.withTable("CREATE TABLE " + table + " (" +
                        "c_custkey INT, c_name STRING, c_mktsegment STRING) " +
                        "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
            }
            cluster.runSql("test", "INSERT INTO mv_view_table_1 VALUES (1, 'table_1', 'BUILDING')");
            cluster.runSql("test", "INSERT INTO mv_view_table_2 VALUES (2, 'table_2', 'MACHINERY')");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW table_view_redefined_mv " +
                    "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\"replication_num\" = \"1\", \"query_rewrite_consistency\" = \"checked\") " +
                    "AS SELECT * FROM " + TABLE_VIEW);
            try {
                refreshMaterializedView("test", "table_view_redefined_mv");
                assertTrue(getMv("table_view_redefined_mv").getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableVisibleVersionMap().containsKey(getTable("test", "mv_view_table_1").getId()));

                // Warm the cached expanded and view-preserving plans while the view still reads table_1.
                PlanTestBase.assertContains(getFragmentPlan("SELECT * FROM " + TABLE_VIEW),
                        "TABLE: table_view_redefined_mv");
                PlanTestBase.assertContains(getFragmentPlan(TABLE_1_SQL), "TABLE: table_view_redefined_mv");
                PlanTestBase.assertNotContains(getFragmentPlan(TABLE_2_SQL), "TABLE: table_view_redefined_mv");

                // Simulate ALTER/CREATE OR REPLACE in the external catalog, without refreshing the MV.
                tableViewSql = TABLE_2_SQL;
                String viewPlan = getFragmentPlan("SELECT * FROM " + TABLE_VIEW);
                PlanTestBase.assertNotContains(viewPlan, "TABLE: table_view_redefined_mv");
                PlanTestBase.assertContains(viewPlan, "TABLE: mv_view_table_2");

                // Force a real rebuild: an ordinary refresh can skip it because table_1 has not changed.
                // Once the MV contains table_2 data, queries against table_1 must scan table_1.
                TaskRun refreshTask = buildMVTaskRun(getMv("table_view_redefined_mv"), "test");
                assertPlanContains(getMVRefreshExecPlan(refreshTask, true), "TABLE: mv_view_table_2");
                assertFalse(refreshTask.getRunCtx().getState().isError(),
                        refreshTask.getRunCtx().getState().getErrorMessage());
                for (boolean enableViewBasedRewrite : List.of(false, true)) {
                    connectContext.getSessionVariable().setEnableViewBasedMvRewrite(enableViewBasedRewrite);
                    String tablePlan = getFragmentPlan(TABLE_1_SQL);
                    PlanTestBase.assertNotContains(tablePlan, "TABLE: table_view_redefined_mv");
                    PlanTestBase.assertContains(tablePlan, "TABLE: mv_view_table_1");
                }
            } finally {
                dropMv("test", "table_view_redefined_mv");
            }
        } finally {
            tableViewSql = TABLE_1_SQL;
            connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(enablePlanCache);
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(enableViewRewrite);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    @Test
    void refreshedMvOnRedefinedViewUpdatesObjectDependencies() throws Exception {
        String mvName = "table_view_dependencies_mv";
        try {
            for (String table : List.of("mv_view_table_1", "mv_view_table_2")) {
                starRocksAssert.withTable("CREATE TABLE " + table + " (" +
                        "c_custkey INT, c_name STRING, c_mktsegment STRING) " +
                        "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
            }
            cluster.runSql("test", "INSERT INTO mv_view_table_1 VALUES (1, 'table_1', 'BUILDING')");
            cluster.runSql("test", "INSERT INTO mv_view_table_2 VALUES (2, 'table_2', 'MACHINERY')");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " " +
                    "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\"replication_num\" = \"1\", \"query_rewrite_consistency\" = \"checked\") " +
                    "AS SELECT * FROM " + TABLE_VIEW);
            try {
                refreshMaterializedView("test", mvName);
                assertEquals(Set.of(TABLE_VIEW, "default_catalog.test.mv_view_table_1"),
                        objectDependencyNames(mvName));

                // The external view keeps its name and schema, but now reads a different base table.
                tableViewSql = TABLE_2_SQL;
                TaskRun refreshTask = buildMVTaskRun(getMv(mvName), "test");
                assertPlanContains(getMVRefreshExecPlan(refreshTask, true), "TABLE: mv_view_table_2");
                assertFalse(refreshTask.getRunCtx().getState().isError(),
                        refreshTask.getRunCtx().getState().getErrorMessage());

                // Query the same frontend row provider used by sys.object_dependencies, rather than
                // inspecting the MV's internal list. It must report the view and its new base table only.
                assertEquals(Set.of(TABLE_VIEW, "default_catalog.test.mv_view_table_2"),
                        objectDependencyNames(mvName));
            } finally {
                dropMv("test", mvName);
            }
        } finally {
            tableViewSql = TABLE_1_SQL;
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    @Test
    void refreshAfterViewDropsAUnionBranchRebuildsTheMvAlthoughTheKeptTableIsUnchanged() throws Exception {
        String mvName = "table_view_dropped_branch_mv";
        boolean enablePlanCache = connectContext.getSessionVariable().isEnableMaterializedViewPlanCache();
        boolean enableViewRewrite = connectContext.getSessionVariable().isEnableViewBasedMvRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        createViewBaseTables("");
        tableViewSql = TABLE_1_SQL + " UNION ALL " + TABLE_2_SQL;
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " " +
                    "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\"replication_num\" = \"1\", \"query_rewrite_consistency\" = \"checked\") " +
                    "AS SELECT * FROM " + TABLE_VIEW);
            refreshMaterializedView("test", mvName);
            PlanTestBase.assertNotContains(getFragmentPlan(TABLE_1_SQL), "TABLE: " + mvName);

            // The view keeps table_1 only. table_1 is still read and unchanged, so its recorded version survives
            // the base-table sync, while the mv still holds table_2's rows of the old union.
            tableViewSql = TABLE_1_SQL;
            TaskRun refreshTask = buildMVTaskRun(getMv(mvName), "test");
            ExecPlan refreshPlan = getMVRefreshExecPlan(refreshTask);
            assertFalse(refreshTask.getRunCtx().getState().isError(),
                    refreshTask.getRunCtx().getState().getErrorMessage());
            assertEquals(Set.of(TABLE_VIEW, "default_catalog.test.mv_view_table_1"), objectDependencyNames(mvName));

            // Skipping the rebuild leaves table_2's rows materialized, while the mv now matches queries on table_1
            // alone, so these would return table_2's rows.
            assertNotNull(refreshPlan, "refresh was skipped although the mv still holds the old union's data");
            assertPlanContains(refreshPlan, "TABLE: mv_view_table_1");
            PlanTestBase.assertNotContains(refreshPlan.getExplainString(TExplainLevel.NORMAL),
                    "TABLE: mv_view_table_2");
        } finally {
            tableViewSql = TABLE_1_SQL;
            connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(enablePlanCache);
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(enableViewRewrite);
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    @Test
    void replayedBaseTableRedefinitionKeepsTheMvStaleUntilItIsRebuilt() throws Exception {
        String mvName = "table_view_replayed_dropped_branch_mv";
        boolean enablePlanCache = connectContext.getSessionVariable().isEnableMaterializedViewPlanCache();
        boolean enableViewRewrite = connectContext.getSessionVariable().isEnableViewBasedMvRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        createViewBaseTables("");
        tableViewSql = TABLE_1_SQL + " UNION ALL " + TABLE_2_SQL;
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " " +
                    "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\"replication_num\" = \"1\", \"query_rewrite_consistency\" = \"checked\") " +
                    "AS SELECT * FROM " + TABLE_VIEW);
            refreshMaterializedView("test", mvName);
            MaterializedView mv = getMv(mvName);
            long table1Id = getTable("test", "mv_view_table_1").getId();
            assertTrue(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                    .containsKey(table1Id));

            // A follower replays the leader's sync of the base tables, before the leader's refresh rebuilds the mv,
            // e.g. because that refresh failed or the leader went down.
            tableViewSql = TABLE_1_SQL;
            List<BaseTableInfo> newBaseTableInfos = mv.getBaseTableInfos().stream()
                    .filter(info -> !"mv_view_table_2".equalsIgnoreCase(info.getTableName()))
                    .collect(Collectors.toList());
            GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(),
                    new JournalEntity(OperationType.OP_ALTER_MATERIALIZED_VIEW_BASE_TABLE_INFOS,
                            AlterMaterializedViewBaseTableInfosLog.ofRedefinedBaseTables(
                                    mv.getDbId(), mv.getId(), newBaseTableInfos)));
            assertEquals(newBaseTableInfos, mv.getBaseTableInfos());
            assertFalse(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                    .containsKey(table1Id));
            assertEquals(0, mv.getLastRefreshTime());

            // The mv already matches queries on table_1 alone, but still holds table_2's rows.
            String tablePlan = getFragmentPlan(TABLE_1_SQL);
            PlanTestBase.assertNotContains(tablePlan, "TABLE: " + mvName);
            PlanTestBase.assertContains(tablePlan, "TABLE: mv_view_table_1");

            TaskRun refreshTask = buildMVTaskRun(mv, "test");
            ExecPlan refreshPlan = getMVRefreshExecPlan(refreshTask);
            assertFalse(refreshTask.getRunCtx().getState().isError(),
                    refreshTask.getRunCtx().getState().getErrorMessage());
            assertNotNull(refreshPlan, "refresh was skipped although the mv still holds the old union's data");
            assertPlanContains(refreshPlan, "TABLE: mv_view_table_1");
            assertTrue(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                    .containsKey(table1Id));
            PlanTestBase.assertContains(getFragmentPlan(TABLE_1_SQL), "TABLE: " + mvName);
        } finally {
            tableViewSql = TABLE_1_SQL;
            connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(enablePlanCache);
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(enableViewRewrite);
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    /**
     * <b>Given</b> an unpartitioned mv with a staleness budget on a view that reads table_1, refreshed from table_1,
     * and table_2 updated within that budget afterwards.
     * <br><b>When</b> the view is redefined to read table_2 and the refresh adopts table_2 as base table, but fails to
     * rebuild the mv.
     * <br><b>Then</b> queries on table_2 do not rewrite to the mv, which still holds table_1's rows, although the
     * staleness budget is not exceeded. Once a later refresh rebuilds the mv, they do.
     */
    @Test
    void failedRebuildAfterViewRedefinitionKeepsTheMvStaleWithinTheStalenessBudget() throws Exception {
        String mvName = "table_view_staleness_mv";
        boolean enablePlanCache = connectContext.getSessionVariable().isEnableMaterializedViewPlanCache();
        boolean enableViewRewrite = connectContext.getSessionVariable().isEnableViewBasedMvRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(true);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        createViewBaseTables("");
        boolean[] failRebuild = {false};
        new MockUp<MVTaskRunProcessor>() {
            @Mock
            public void executePlan(Invocation invocation, ExecPlan execPlan, InsertStmt insertStmt)
                    throws Exception {
                if (failRebuild[0]) {
                    throw new RuntimeException("rebuild failed");
                }
                invocation.proceed(execPlan, insertStmt);
            }
        };
        try {
            // given: an mv on the view reading table_1, refreshed and used for rewrite
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " " +
                    "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\"replication_num\" = \"1\", \"query_rewrite_consistency\" = \"checked\", " +
                    "\"mv_rewrite_staleness_second\" = \"300\") " +
                    "AS SELECT * FROM " + TABLE_VIEW);
            refreshMaterializedView("test", mvName);
            MaterializedView mv = getMv(mvName);
            PlanTestBase.assertContains(getFragmentPlan(TABLE_1_SQL), "TABLE: " + mvName);

            // given: table_2 changes within the staleness budget of the mv's last refresh
            cluster.runSql("test", "INSERT INTO mv_view_table_2 VALUES (3, 'table_2_new', 'MACHINERY')");

            // when: the view moves to table_2 and the refresh adopts it, but fails to rebuild the mv
            tableViewSql = TABLE_2_SQL;
            failRebuild[0] = true;
            assertThrows(Exception.class, () -> refreshMV("test", mv));
            assertTrue(mv.isActive(), mv.getInactiveReason());
            assertEquals(Set.of(TABLE_VIEW, "default_catalog.test.mv_view_table_2"), objectDependencyNames(mvName));

            // then: the mv already matches queries on table_2, but still holds table_1's rows. The staleness budget
            // must not make it look fresh, since its data was never built from table_2.
            String tablePlan = getFragmentPlan(TABLE_2_SQL);
            PlanTestBase.assertNotContains(tablePlan, "TABLE: " + mvName);
            PlanTestBase.assertContains(tablePlan, "TABLE: mv_view_table_2");

            // when: a later refresh rebuilds the mv from table_2
            failRebuild[0] = false;
            refreshMaterializedView("test", mvName);
            // then: queries on table_2 rewrite to the mv again
            PlanTestBase.assertContains(getFragmentPlan(TABLE_2_SQL), "TABLE: " + mvName);
        } finally {
            tableViewSql = TABLE_1_SQL;
            connectContext.getSessionVariable().setEnableMaterializedViewPlanCache(enablePlanCache);
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(enableViewRewrite);
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    @Test
    void partitionedMvOnRedefinedViewIsInactivatedInsteadOfRebound() throws Exception {
        // The mv's partition expression is bound to table_1. Rebinding it to the tables the redefined view reads is
        // not supported, so the refresh must stop and leave the mv inactive on its old base tables.
        String mvName = "table_view_partitioned_mv";
        createViewBaseTables("PARTITION BY (c_mktsegment)");
        try {
            MaterializedView mv = createRefreshedPartitionedMvOnTableView(mvName);
            List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();

            tableViewSql = TABLE_2_SQL;
            List<JournalEntity> journals = recordMvJournals();
            Exception refreshError = assertThrows(Exception.class, () -> refreshMV("test", mv));
            assertTrue(refreshError.getMessage().contains(PARTITION_REBINDING_UNSUPPORTED), refreshError.getMessage());
            assertInactiveOnBaseTables(mv, baseTableInfos);

            // a follower replays the leader's journal onto an mv that is still active
            mv.setActive();
            for (JournalEntity journal : journals) {
                GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), journal);
            }
            assertInactiveOnBaseTables(mv, baseTableInfos);
        } finally {
            tableViewSql = TABLE_1_SQL;
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    @Test
    void partitionedMvOnRedefinedViewBecomesActiveOnceTheViewIsRestored() throws Exception {
        String mvName = "table_view_partitioned_active_mv";
        createViewBaseTables("PARTITION BY (c_mktsegment)");
        try {
            MaterializedView mv = createRefreshedPartitionedMvOnTableView(mvName);
            tableViewSql = TABLE_2_SQL;
            assertThrows(Exception.class, () -> refreshMV("test", mv));

            // automatic and manual activation both run ALTER ... ACTIVE, which must not rebind the mv either
            String activate = "ALTER MATERIALIZED VIEW " + mvName + " ACTIVE";
            Exception activateError = assertThrows(Exception.class, () -> cluster.runSql("test", activate));
            assertTrue(activateError.getMessage().contains(PARTITION_REBINDING_UNSUPPORTED), activateError.getMessage());
            assertFalse(mv.isActive());

            tableViewSql = TABLE_1_SQL;
            cluster.runSql("test", activate);
            assertTrue(mv.isActive(), mv.getInactiveReason());
            refreshMaterializedView("test", mvName);
        } finally {
            tableViewSql = TABLE_1_SQL;
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    /**
     * An unpartitioned mv on a view reading table_1 UNION ALL table_2 is inactivated by the user, which keeps its
     * version map. While it is inactive, the view drops the table_2 branch. Activating it must then drop table_2 the
     * way a refresh does, see MaterializedView#replaceBaseTableInfos, and leave the mv stale although table_1 is
     * unchanged, since it still holds table_2's rows.
     */
    @Test
    void activationAfterViewDropsAUnionBranchRebuildsTheMv() throws Exception {
        String mvName = "table_view_activated_dropped_branch_mv";
        createViewBaseTables("");
        try {
            MaterializedView mv = createInactiveMvOnRedefinedTableViewUnion(mvName);
            cluster.runSql("test", "ALTER MATERIALIZED VIEW " + mvName + " ACTIVE");
            assertTrue(mv.isActive(), mv.getInactiveReason());
            assertAdoptedTable1AsStale(mv);
            PlanTestBase.assertNotContains(getFragmentPlan(TABLE_1_SQL), "TABLE: " + mvName);

            TaskRun refreshTask = buildMVTaskRun(mv, "test");
            ExecPlan refreshPlan = getMVRefreshExecPlan(refreshTask);
            assertNotNull(refreshPlan, "refresh was skipped although the mv still holds the old union's data");
            PlanTestBase.assertNotContains(refreshPlan.getExplainString(TExplainLevel.NORMAL),
                    "TABLE: mv_view_table_2");
        } finally {
            tableViewSql = TABLE_1_SQL;
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    /** Same as {@link #activationAfterViewDropsAUnionBranchRebuildsTheMv}, replaying the leader's activation. */
    @Test
    void replayedActivationAfterViewDropsAUnionBranchKeepsTheMvStale() throws Exception {
        String mvName = "table_view_replayed_activation_mv";
        createViewBaseTables("");
        try {
            MaterializedView mv = createInactiveMvOnRedefinedTableViewUnion(mvName);
            AlterMaterializedViewStatusLog activation = new AlterMaterializedViewStatusLog(mv.getDbId(), mv.getId(),
                    AlterMaterializedViewStatusClause.ACTIVE, "");
            activation.setBaseTableInfos(mv.getBaseTableInfos().stream()
                    .filter(info -> !"mv_view_table_2".equalsIgnoreCase(info.getTableName()))
                    .collect(Collectors.toList()));
            GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(),
                    new JournalEntity(OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS, activation));
            assertAdoptedTable1AsStale(mv);
        } finally {
            tableViewSql = TABLE_1_SQL;
            dropMv("test", mvName);
            starRocksAssert.dropTable("mv_view_table_2");
            starRocksAssert.dropTable("mv_view_table_1");
        }
    }

    /** A refreshed mv on the view's union, inactivated by the user, after which the view keeps table_1 only. */
    private MaterializedView createInactiveMvOnRedefinedTableViewUnion(String mvName) throws Exception {
        tableViewSql = TABLE_1_SQL + " UNION ALL " + TABLE_2_SQL;
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " " +
                "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"replication_num\" = \"1\", \"query_rewrite_consistency\" = \"checked\") " +
                "AS SELECT * FROM " + TABLE_VIEW);
        refreshMaterializedView("test", mvName);
        cluster.runSql("test", "ALTER MATERIALIZED VIEW " + mvName + " INACTIVE");
        MaterializedView mv = getMv(mvName);
        // the user's inactivation keeps the version map, unlike one caused by a base table change
        assertTrue(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                .containsKey(getTable("test", "mv_view_table_1").getId()));
        tableViewSql = TABLE_1_SQL;
        return mv;
    }

    private void assertAdoptedTable1AsStale(MaterializedView mv) {
        assertEquals(List.of("customer_table_view", "mv_view_table_1"), mv.getBaseTableInfos().stream()
                .map(BaseTableInfo::getTableName).sorted().collect(Collectors.toList()));
        assertFalse(getTable("test", "mv_view_table_2").getRelatedMaterializedViews().contains(mv.getMvId()));
        assertTrue(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap().isEmpty());
        assertEquals(0, mv.getLastRefreshTime());
    }

    private static final String PARTITION_REBINDING_UNSUPPORTED = "partition-expression rebinding is not supported";

    private MaterializedView createRefreshedPartitionedMvOnTableView(String mvName) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + " PARTITION BY c_mktsegment " +
                "DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"replication_num\" = \"1\") AS SELECT * FROM " + TABLE_VIEW);
        refreshMaterializedView("test", mvName);
        return getMv(mvName);
    }

    private static void createViewBaseTables(String partitionClause) throws Exception {
        for (String table : List.of("mv_view_table_1", "mv_view_table_2")) {
            starRocksAssert.withTable("CREATE TABLE " + table + " (c_custkey INT, c_name STRING, c_mktsegment STRING) " +
                    partitionClause + " DISTRIBUTED BY HASH(c_custkey) BUCKETS 1 " +
                    "PROPERTIES (\"replication_num\" = \"1\")");
        }
        cluster.runSql("test", "INSERT INTO mv_view_table_1 VALUES (1, 'table_1', 'BUILDING')");
        cluster.runSql("test", "INSERT INTO mv_view_table_2 VALUES (2, 'table_2', 'MACHINERY')");
    }

    private void assertInactiveOnBaseTables(MaterializedView mv, List<BaseTableInfo> baseTableInfos) {
        assertFalse(mv.isActive());
        assertTrue(mv.getInactiveReason().contains(PARTITION_REBINDING_UNSUPPORTED), mv.getInactiveReason());
        assertEquals(baseTableInfos, mv.getBaseTableInfos());
    }

    /** Records the mv journal entries written from now on, deserialized the way a follower reads them. */
    private static List<JournalEntity> recordMvJournals() {
        List<JournalEntity> journals = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObject(Invocation invocation, short op, Object obj) throws IOException {
                record(op, obj);
                invocation.proceed(op, obj);
            }

            @Mock
            public void logJsonObject(Invocation invocation, short op, Object obj, WALApplier applier)
                    throws IOException {
                record(op, obj);
                invocation.proceed(op, obj, applier);
            }

            private void record(short op, Object obj) throws IOException {
                if (op != OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS &&
                        op != OperationType.OP_ALTER_MATERIALIZED_VIEW_BASE_TABLE_INFOS) {
                    return;
                }
                DataOutputBuffer buffer = new DataOutputBuffer();
                Text.writeString(buffer, GsonUtils.GSON.toJson(obj));
                try (DataInputStream in = new DataInputStream(
                        new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength()))) {
                    journals.add(new JournalEntity(op, EditLogDeserializer.deserialize(op, in)));
                }
            }
        };
        return journals;
    }

    private Set<String> objectDependencyNames(String mvName) {
        TAuthInfo auth = new TAuthInfo().setCurrent_user_ident(
                UserIdentityUtils.toThrift(connectContext.getCurrentUserIdentity()));
        TObjectDependencyReq request = new TObjectDependencyReq().setAuth_info(auth);
        long mvId = getMv(mvName).getId();
        TObjectDependencyRes response = SysObjectDependencies.listObjectDependencies(request);
        if (!response.isSetItems()) {
            return Set.of();
        }
        return response.getItems().stream()
                .filter(item -> item.getObject_id() == mvId)
                .map(item -> item.getRef_catalog() + "." + item.getRef_database() + "." + item.getRef_object_name())
                .collect(Collectors.toSet());
    }
}
