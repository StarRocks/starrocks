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

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.adbc.MockedADBCMetadata;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
