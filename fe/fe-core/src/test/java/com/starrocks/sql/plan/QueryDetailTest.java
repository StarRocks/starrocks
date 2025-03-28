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

import com.starrocks.common.Config;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.QueryDetail;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryDetailTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        ConnectorPlanTestBase.mockCatalog(connectContext, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME);

        Config.enable_collect_query_detail_info = true;
        Config.query_detail_explain_level = "NORMAL";
        QueryDetail startQueryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", false, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog");
        connectContext.setQueryDetail(startQueryDetail);
    }

    @Test
    public void testInsert() throws Exception {
        String plan = getFragmentPlan("insert into t0 select * from t0");
        Assert.assertEquals(plan, connectContext.getQueryDetail().getExplain());
        assertContains(plan, "  OLAP TABLE SINK\n" +
                "    TABLE: t0\n" +
                "    TUPLE ID: 1\n" +
                "    RANDOM\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testQuery() throws Exception {
        String plan = getFragmentPlan("SELECT DISTINCT t0.v1 FROM t0 RIGHT JOIN[BUCKET] t1 ON t0.v1 = t1.v4");
        assertContains(plan, "  6:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: v1");
        Assert.assertEquals(plan, connectContext.getQueryDetail().getExplain());
    }

    @Test
    public void testExternalTable() throws Exception {
        // Explaining external table always use `NORMAL` level regardless of the config.
        Config.query_detail_explain_level = "COSTS";
        try {
            String plan = getFragmentPlan("SELECT count(1) FROM hive0.tpch.lineitem t1 " +
                    "join [shuffle] hive0.tpch.lineitem t2 on t1.l_orderkey = t2.l_orderkey");
            assertContains(plan, "  6:AGGREGATE (update serialize)\n" +
                    "  |  output: count(1)\n" +
                    "  |  group by: ");
            Assert.assertEquals(plan, connectContext.getQueryDetail().getExplain());
        } finally {
            Config.query_detail_explain_level = "NORMAL";
        }
    }

    @Test
    public void testExplainCosts() throws Exception {
        Config.query_detail_explain_level = "COSTS";
        try {
            String plan = getCostExplain("SELECT DISTINCT t0.v1 FROM t0 RIGHT JOIN[BUCKET] t1 ON t0.v1 = t1.v4");
            Assert.assertEquals(plan, connectContext.getQueryDetail().getExplain());
            assertContains(plan, " 6:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  group by: [1: v1, BIGINT, true]\n" +
                    "  |  hasNullableGenerateChild: true\n" +
                    "  |  cardinality: 1\n" +
                    "  |  column statistics: \n" +
                    "  |  * v1-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");
        } finally {
            Config.query_detail_explain_level = "NORMAL";
        }
    }
}
