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

package com.starrocks.planner;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewTest extends PlanTestBase {
    private static final String MATERIALIZED_DB_NAME = "test_mv";

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext= UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setEnableQueryCache(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME)
                .useDatabase(MATERIALIZED_DB_NAME);
        String empsTable = "" +
                "CREATE TABLE emps\n" +
                "(\n" +
                "    empid      INT         NOT NULL,\n" +
                "    deptno     INT         NOT NULL,\n" +
                "    locationid INT         NOT NULL,\n" +
                "    empname    VARCHAR(20) NOT NULL,\n" +
                "    salary     DECIMAL(18, 2)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`empid`)\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(empsTable);
    }

    private boolean testRewrite(String mv, String query) throws Exception {
        String mvSQL = "CREATE MATERIALIZED VIEW mv0\n" +
                "   DISTRIBUTED BY HASH(`col1`) BUCKETS 12\n" +
                " AS " +
                mv;
        starRocksAssert.withNewMaterializedView(mvSQL);
        String rewritePlan = getFragmentPlan(query);
        System.out.println(rewritePlan);
        starRocksAssert.dropMaterializedView("mv0");
        return rewritePlan.contains("TABLE: mv0");
    }
    private void testRewriteOK(String mv, String query) throws Exception {
        Assert.assertTrue(testRewrite(mv, query));
    }

    private void testRewriteFail(String mv, String query) throws Exception {
        Assert.assertTrue(!testRewrite(mv, query));
    }

    @Test
    public void testFilter() throws Exception {
        String mv = "select empid + 1 as col1 from emps where deptno = 10";
        testRewriteOK(mv, "select empid + 1 from emps where deptno = 10");
        testRewriteOK(mv, "select max(empid + 1) from emps where deptno = 10");
        testRewriteFail(mv, "select max(empid) from emps where deptno = 10");
        testRewriteFail(mv, "select max(empid) from emps where deptno = 11");
        testRewriteFail(mv, "select max(empid) from emps");
        testRewriteFail(mv, "select empid from emps where deptno = 10");
    }
}
