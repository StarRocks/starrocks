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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MV rewrite for MVs defined on a view of an external catalog ({@link com.starrocks.catalog.ConnectorView}), here a
 * {@link com.starrocks.catalog.HiveView}.
 *
 * <p>The view is a UNION ALL, so an MV on it is not SPJG once the view is inlined; only view-based rewrite can match
 * it. Connectors convert their views with a new id from {@code CONNECTOR_ID_GENERATOR} (the Hive metastore cache only
 * delays that until its entry expires), so the mock converts the view again on every lookup: each query sees a new
 * view object with a new id, and nothing stored on a previous object survives.
 *
 * <p>The view reads the mocked Hive table {@code hive0.tpch.customer}.
 */
class HiveViewMvRewriteTest extends MVTestBase {
    private static final String VIEW_DB = "view_db";
    private static final String VIEW_NAME = "customer_union_view";
    private static final String VIEW = MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME + "." + VIEW_DB + "." + VIEW_NAME;
    private static final String NESTED_VIEW_NAME = "customer_nested_view";
    private static final String NESTED_VIEW =
            MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME + "." + VIEW_DB + "." + NESTED_VIEW_NAME;
    private static final String OUTER_VIEW_NAME = "customer_outer_view";
    private static final String OUTER_VIEW =
            MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME + "." + VIEW_DB + "." + OUTER_VIEW_NAME;
    private static final UserIdentity RESTRICTED_USER = new UserIdentity("policy_user", "%");

    private static final String VIEW_SQL =
            "SELECT c_custkey, c_name, c_mktsegment FROM hive0.tpch.customer WHERE c_mktsegment = 'BUILDING' " +
                    "UNION ALL " +
                    "SELECT c_custkey, c_name, c_mktsegment FROM hive0.tpch.customer WHERE c_mktsegment = 'MACHINERY'";

    // The view's current definition in the metastore; tests change it to simulate the view being redefined.
    private static volatile String viewSql = VIEW_SQL;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        ((MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr())
                .registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new UncachedViewHiveMetadata());
    }

    /** Converts the view from its metastore definition on every lookup, as after a metastore cache expiry. */
    private static final class UncachedViewHiveMetadata extends MockedHiveMetadata {
        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            boolean nested = NESTED_VIEW_NAME.equalsIgnoreCase(tblName);
            boolean outer = OUTER_VIEW_NAME.equalsIgnoreCase(tblName);
            if (!VIEW_DB.equalsIgnoreCase(dbName) || (!VIEW_NAME.equalsIgnoreCase(tblName) && !nested && !outer)) {
                return super.getTable(context, dbName, tblName);
            }
            List<FieldSchema> cols = Lists.newArrayList(
                    new FieldSchema("c_custkey", "int", null),
                    new FieldSchema("c_name", "string", null),
                    new FieldSchema("c_mktsegment", "string", null));
            StorageDescriptor sd = new StorageDescriptor(cols, "", "", "", false, -1, null, Lists.newArrayList(),
                    Lists.newArrayList(), Maps.newHashMap());
            // the metastore stores db and view names lower-cased
            org.apache.hadoop.hive.metastore.api.Table hmsView = new org.apache.hadoop.hive.metastore.api.Table(
                    outer ? OUTER_VIEW_NAME : nested ? NESTED_VIEW_NAME : VIEW_NAME,
                    VIEW_DB, null, 0, 0, 0, sd, Lists.newArrayList(),
                    Maps.newHashMap(), null, outer ? "SELECT * FROM " + NESTED_VIEW : nested ? "SELECT * FROM " + VIEW : viewSql,
                    "VIRTUAL_VIEW");
            return HiveMetastoreApiConverter.toHiveView(hmsView, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME);
        }
    }

    private static Table lookUpView() {
        return GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, VIEW_DB, VIEW_NAME);
    }

    private void createMvOnView(String mvName) throws Exception {
        createMvOnView(mvName, VIEW);
    }

    private void createMvOnView(String mvName, String view) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" +
                "DISTRIBUTED BY HASH(c_custkey)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT * FROM " + view);
        refreshMaterializedView(DB_NAME, mvName);
    }

    @Test
    void lookupsReturnANewViewObjectEachTime() {
        // Guards the premise of the tests below: if the mock ever caches the view, they stop covering the problem.
        Table first = lookUpView();
        Table second = lookUpView();
        assertTrue(first.isHiveView());
        assertNotSame(first, second);
        assertNotEquals(first.getId(), second.getId());
    }

    @Test
    void newViewObjectFromALaterLookupKnowsItsMvsUntilTheMvIsDropped() throws Exception {
        createMvOnView("related_mv");
        MvId mvId;
        try {
            mvId = getMv(DB_NAME, "related_mv").getMvId();
            assertTrue(lookUpView().getRelatedMaterializedViews().contains(mvId));
        } finally {
            dropMv(DB_NAME, "related_mv");
        }
        assertFalse(lookUpView().getRelatedMaterializedViews().contains(mvId));
    }

    @Test
    void queryOnUnionViewIsRewrittenToMvOnTheView() throws Exception {
        createMvOnView("union_view_mv");
        try {
            String query = "SELECT c_custkey, c_name FROM " + VIEW + " WHERE c_custkey > 10";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "union_view_mv");
            PlanTestBase.assertNotContains(plan, "HdfsScanNode");

            // the MV's query is a UNION ALL, so only view-based rewrite can match it
            connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
            try {
                PlanTestBase.assertNotContains(getFragmentPlan(query), "union_view_mv");
            } finally {
                connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
            }
        } finally {
            dropMv(DB_NAME, "union_view_mv");
        }
    }

    private String getPolicyCheckedPlan(String query) {
        StatementBase statement = SqlParser.parse(query, connectContext.getSessionVariable()).get(0);
        // ConnectProcessor enables policy checks on user queries before analysis.
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitRelation(Relation relation, Void context) {
                relation.setNeedRewrittenByPolicy(true);
                return null;
            }
        }.visit(statement);
        return StatementPlanner.plan(statement, connectContext).getExplainString(TExplainLevel.NORMAL);
    }

    private String getRestrictedPlan(String query) {
        UserIdentity originalUser = connectContext.getCurrentUserIdentity();
        connectContext.setCurrentUserIdentity(RESTRICTED_USER);
        Tracers.register(connectContext);
        Tracers.init(connectContext, "LOGS", "MV");
        try {
            String plan = getPolicyCheckedPlan(query);
            PlanTestBase.assertContains(Tracers.printLogs(), "contains a security policy rewrite");
            return plan;
        } finally {
            Tracers.close();
            connectContext.setCurrentUserIdentity(originalUser);
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void rowFilterOnUnderlyingTablePreventsMvRewrite(boolean nested, boolean textMatch) throws Exception {
        boolean originalTextMatch = connectContext.getSessionVariable().isEnableMaterializedViewTextMatchRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(textMatch);
        String view = nested ? NESTED_VIEW : VIEW;
        createMvOnView("union_view_row_policy_mv", view);
        try {
            assertNotNull(CachingMvPlanContextBuilder.getInstance()
                    .getOrLoadPlanContext(getMv(DB_NAME, "union_view_row_policy_mv"), 30_000));
            String query = "SELECT * FROM " + view;
            PlanTestBase.assertContains(getPolicyCheckedPlan(query), "union_view_row_policy_mv");

            try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
                authorizer.when(() -> Authorizer.getRowAccessPolicy(Mockito.argThat(
                                        ctx -> RESTRICTED_USER.equals(ctx.getCurrentUserIdentity())),
                                Mockito.argThat(name -> "customer".equalsIgnoreCase(name.getTbl()))))
                        .thenAnswer(invocation -> SqlParser.parseSqlToExpr("c_custkey < 100", 0));
                String plan = getRestrictedPlan(query);
                PlanTestBase.assertNotContains(plan, "union_view_row_policy_mv");
                PlanTestBase.assertContains(plan, "HdfsScanNode", "c_custkey < 100");
            }
        } finally {
            connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(originalTextMatch);
            dropMv(DB_NAME, "union_view_row_policy_mv");
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void columnMaskOnUnderlyingTablePreventsMvRewrite(boolean nested, boolean textMatch) throws Exception {
        boolean originalTextMatch = connectContext.getSessionVariable().isEnableMaterializedViewTextMatchRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(textMatch);
        String view = nested ? NESTED_VIEW : VIEW;
        createMvOnView("union_view_mask_policy_mv", view);
        try {
            assertNotNull(CachingMvPlanContextBuilder.getInstance()
                    .getOrLoadPlanContext(getMv(DB_NAME, "union_view_mask_policy_mv"), 30_000));
            String query = "SELECT * FROM " + view;
            PlanTestBase.assertContains(getPolicyCheckedPlan(query), "union_view_mask_policy_mv");

            try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
                authorizer.when(() -> Authorizer.getColumnMaskingPolicy(Mockito.argThat(
                                        ctx -> RESTRICTED_USER.equals(ctx.getCurrentUserIdentity())),
                                Mockito.argThat(name -> "customer".equalsIgnoreCase(name.getTbl())), Mockito.any()))
                        .thenAnswer(invocation -> Map.of("c_name", SqlParser.parseSqlToExpr("'masked_name'", 0)));
                String plan = getRestrictedPlan(query);
                PlanTestBase.assertNotContains(plan, "union_view_mask_policy_mv");
                PlanTestBase.assertContains(plan, "HdfsScanNode", "masked_name");
            }
        } finally {
            connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(originalTextMatch);
            dropMv(DB_NAME, "union_view_mask_policy_mv");
        }
    }

    @Test
    void dbAndViewNameCaseInTheQueryDoesNotMatter() throws Exception {
        createMvOnView("union_view_case_mv");
        try {
            String plan = getFragmentPlan("SELECT c_custkey, c_name FROM " + MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME +
                    "." + VIEW_DB.toUpperCase() + ".Customer_Union_View WHERE c_custkey > 10");
            PlanTestBase.assertContains(plan, "union_view_case_mv");
        } finally {
            dropMv(DB_NAME, "union_view_case_mv");
        }
    }

    @Test
    void cachedMvPlanStopsMatchingOnceTheViewIsRedefined() throws Exception {
        createMvOnView("union_view_redefined_mv");
        try {
            // The MV's plan is loaded asynchronously: wait for it, so it is built from the definition the MV was
            // refreshed with and not from the one below.
            assertNotNull(CachingMvPlanContextBuilder.getInstance()
                    .getOrLoadPlanContext(getMv(DB_NAME, "union_view_redefined_mv"), 30_000));

            // e.g. a branch added or changed in the metastore: the MV holds data for the old definition only
            viewSql = VIEW_SQL.replace("'MACHINERY'", "'AUTOMOBILE'");
            String plan = getFragmentPlan("SELECT c_custkey, c_name FROM " + VIEW + " WHERE c_custkey > 10");
            PlanTestBase.assertNotContains(plan, "union_view_redefined_mv");
            PlanTestBase.assertContains(plan, "AUTOMOBILE");
        } finally {
            viewSql = VIEW_SQL;
            dropMv(DB_NAME, "union_view_redefined_mv");
        }
    }

    @ParameterizedTest
    @CsvSource({"1, false", "1, true", "2, false", "2, true"})
    void cachedMvPlanStopsMatchingOnceAnInnerViewIsRedefined(int depth, boolean textMatch) throws Exception {
        boolean originalTextMatch = connectContext.getSessionVariable().isEnableMaterializedViewTextMatchRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(textMatch);
        String view = depth == 1 ? NESTED_VIEW : OUTER_VIEW;
        String mvName = "nested_view_redefined_mv";
        createMvOnView(mvName, view);
        try {
            List<MvPlanContext> cachedPlans = CachingMvPlanContextBuilder.getInstance()
                    .getOrLoadPlanContext(getMv(DB_NAME, mvName), 30_000);
            assertNotNull(cachedPlans);
            String query = "SELECT * FROM " + view;
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);

            // Only the innermost definition changes. Every enclosing view keeps the same SELECT * text.
            viewSql = VIEW_SQL.replace("'MACHINERY'", "'AUTOMOBILE'");
            var planAndTrace = UtFrameUtils.getFragmentPlanWithTrace(connectContext, query, "MV");
            String plan = planAndTrace.second.first.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertNotContains(plan, mvName);
            PlanTestBase.assertContains(plan, "HdfsScanNode", "AUTOMOBILE");
            PlanTestBase.assertContains(planAndTrace.second.second, "connector view definitions");
            assertSame(cachedPlans, CachingMvPlanContextBuilder.getInstance()
                    .getOrLoadPlanContext(getMv(DB_NAME, mvName), 30_000));

            // Restoring the definition can match the original snapshot again, without rebuilding the cache.
            viewSql = VIEW_SQL;
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        } finally {
            viewSql = VIEW_SQL;
            connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(originalTextMatch);
            dropMv(DB_NAME, mvName);
        }
    }

    @Test
    void aggregateOnUnionViewIsRewrittenToRollupMvNestedOnTheViewMv() throws Exception {
        createMvOnView("union_view_base_mv");
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW union_view_segment_counts_mv\n" +
                    "DISTRIBUTED BY HASH(c_mktsegment)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "PROPERTIES (\"replication_num\" = \"1\")\n" +
                    "AS SELECT c_mktsegment, count(*) AS cnt FROM union_view_base_mv GROUP BY c_mktsegment");
            try {
                refreshMaterializedView(DB_NAME, "union_view_segment_counts_mv");
                String plan = getFragmentPlan("SELECT c_mktsegment, count(*) FROM " + VIEW + " GROUP BY c_mktsegment");
                PlanTestBase.assertContains(plan, "union_view_segment_counts_mv");
                PlanTestBase.assertNotContains(plan, "HdfsScanNode");
            } finally {
                dropMv(DB_NAME, "union_view_segment_counts_mv");
            }
        } finally {
            dropMv(DB_NAME, "union_view_base_mv");
        }
    }
}
