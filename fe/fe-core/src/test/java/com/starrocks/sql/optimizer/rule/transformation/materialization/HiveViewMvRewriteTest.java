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
import com.starrocks.catalog.Table;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
            if (!VIEW_DB.equalsIgnoreCase(dbName) || !VIEW_NAME.equalsIgnoreCase(tblName)) {
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
                    VIEW_NAME, VIEW_DB, null, 0, 0, 0, sd, Lists.newArrayList(), Maps.newHashMap(), null, viewSql,
                    "VIRTUAL_VIEW");
            return HiveMetastoreApiConverter.toHiveView(hmsView, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME);
        }
    }

    private static Table lookUpView() {
        return GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, VIEW_DB, VIEW_NAME);
    }

    private void createMvOnView(String mvName) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" +
                "DISTRIBUTED BY HASH(c_custkey)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT * FROM " + VIEW);
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
