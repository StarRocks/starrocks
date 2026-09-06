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

package com.starrocks.sql.plan;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.adbc.MockedADBCMetadata;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ADBCScanPlanTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        Map<String, String> properties = Map.of(
                "type", "adbc", "driver", "adbc_driver_flightsql", "uri", "grpc+tcp://localhost:32010");
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        state.getCatalogMgr().createCatalog("adbc", MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME, "", properties);
        ((MockedMetadataMgr) state.getMetadataMgr()).registerMockedMetadata(
                MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME, new MockedADBCMetadata(properties));
    }

    @AfterAll
    public static void dropADBCCatalog() {
        dropCatalog(MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME);
    }

    @Test
    public void testADBCSelectAll() throws Exception {
        String sql = "select a, b, c, d from adbc0.test_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "TABLE: \"test_db0\".\"tbl0\"");
        assertContains(plan, "QUERY: SELECT \"a\", \"b\", \"c\", \"d\"");
    }

    @Test
    public void testADBCColumnPruning() throws Exception {
        String sql = "select a, c from adbc0.test_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "QUERY: SELECT \"a\", \"c\"");
    }

    @Test
    public void testADBCCountStarKeepsPhysicalColumn() throws Exception {
        String plan = getFragmentPlan("select count(*) from adbc0.test_db0.tbl0");
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "QUERY: SELECT \"c\"");
    }

    @Test
    public void testADBCPredicatePushdown() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0 where c > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "WHERE");
    }

    @Test
    public void testADBCLimitPushdown() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0 limit 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "LIMIT 5");
    }

    @Test
    public void testADBCCombinedPushdown() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0 where b = 'x' limit 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "WHERE");
        assertContains(plan, "LIMIT 10");
    }

    @Test
    public void testADBCCrossCatalogJoin() throws Exception {
        String sql = "select t1.a, t2.v1 from adbc0.test_db0.tbl0 t1 join test.t0 t2 on t1.c = t2.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "OlapScanNode");
    }

    @Test
    public void testADBCCatalogQueryMetrics() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0";
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        StmtExecutor executor = new StmtExecutor(connectContext, UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        Set<String> catalogTypes = Deencapsulation.invoke(executor, "extractCatalogTypes", plan);
        assertEquals(Set.of("adbc"), catalogTypes);

        String joinSql = "select t1.a, t2.v1 from adbc0.test_db0.tbl0 t1 join test.t0 t2 on t1.c = t2.v1";
        ExecPlan joinPlan = UtFrameUtils.getPlanAndFragment(connectContext, joinSql).second;
        catalogTypes = Deencapsulation.invoke(executor, "extractCatalogTypes", joinPlan);
        assertEquals(Set.of("adbc", "default"), catalogTypes);
    }

    @Test
    public void testADBCExplainShowsLogicalDriver() throws Exception {
        String sql = "EXPLAIN select a from adbc0.test_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "DRIVER: adbc_driver_flightsql");
    }

    @Test
    public void testADBCMaterializedViewRequiresFullRefresh() {
        String mvName = "mv_adbc_unknown_freshness";
        starRocksAssert.withMaterializedView("create materialized view " + mvName +
                " refresh deferred manual properties (\"force_external_table_query_rewrite\" = \"CHECKED\")" +
                " as select c from adbc0.test_db0.tbl0", () -> {
                    MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable("test", mvName);
                    BaseTableInfo baseInfo = mv.getBaseTableInfos().get(0);
                    Table baseTable = MvUtils.getTableChecked(baseInfo);
                    // Record a previous refresh so an unchanged synthetic version cannot imply freshness.
                    mv.getBaseTableRefreshInfo(baseInfo).put(baseTable.getName(),
                            mv.getBaseTableLatestPartitionInfo(baseTable).get(0));
                    long refreshTime = System.currentTimeMillis();
                    mv.getRefreshScheme().setLastRefreshTime(refreshTime);
                    mv.getRefreshScheme().setLastFreshnessConfirmedAt(refreshTime);
                    assertADBCMaterializedViewNeedsRefresh(mv);

                    QueryMaterializationContext previousContext = connectContext.getQueryMVContext();
                    QueryMaterializationContext cachedContext = new QueryMaterializationContext();
                    cachedContext.setEnableQueryContextCache(true);
                    connectContext.setQueryMVContext(cachedContext);
                    try {
                        assertADBCMaterializedViewNeedsRefresh(mv);
                        assertFalse(cachedContext.getQueryCacheStats().getCounter().isEmpty());
                    } finally {
                        connectContext.setQueryMVContext(previousContext);
                    }
                    assertContains(getFragmentPlan("select c from adbc0.test_db0.tbl0"), "SCAN ADBC");
                });
    }

    private void assertADBCMaterializedViewNeedsRefresh(MaterializedView mv) {
        for (int staleness : new int[] {0, 3600}) {
            mv.setMaxMVRewriteStaleness(staleness);
            assertFalse(mv.isStalenessSatisfied());
            for (boolean queryRewrite : new boolean[] {false, true}) {
                MvUpdateInfo info = MvRefreshArbiter.getMVTimelinessUpdateInfo(mv,
                        new MVTimelinessArbiter.QueryRewriteParams(queryRewrite, null));
                assertEquals(MvUpdateInfo.MvToRefreshType.FULL, info.getMVToRefreshType());
                assertFalse(info.isValidRewrite());
            }
        }
    }
}
