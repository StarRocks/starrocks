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

import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MaterializedViewTPCHTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        executeSqlFile("sql/materialized-view/tpch/ddl_tpch.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv1.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv2.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv3.sql");
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);

        int scale = 1;
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, scale));
        OlapTable t4 = (OlapTable) globalStateMgr.getDb(MATERIALIZED_DB_NAME).getTable("customer");
        setTableStatistics(t4, 150000 * scale);
        OlapTable t7 = (OlapTable) globalStateMgr.getDb(MATERIALIZED_DB_NAME).getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);
    }

    @Test
    public void testQuery1() {
        runFileUnitTest("materialized-view/tpch/q1");
    }

    @Test
    public void testQuery2() {
        runFileUnitTest("materialized-view/tpch/q2");
    }

    @Test
    public void testQuery3() {
        runFileUnitTest("materialized-view/tpch/q3");
    }

    @Test
    public void testQuery4() {
        runFileUnitTest("materialized-view/tpch/q4");
    }

    @Test
    public void testQuery5() {
        runFileUnitTest("materialized-view/tpch/q5");
    }

    @Test
    public void testQuery6() {
        runFileUnitTest("materialized-view/tpch/q6");
    }

    @Test
    public void testQuery7() {
        runFileUnitTest("materialized-view/tpch/q7");
    }

    @Test
    @Ignore
    public void testQuery8() {
        runFileUnitTest("materialized-view/tpch/q8");
    }

    @Test
    public void testQuery9() {
        runFileUnitTest("materialized-view/tpch/q9");
    }

    @Test
    public void testQuery10() {
        runFileUnitTest("materialized-view/tpch/q10");
    }

    @Test
    public void testQuery11() {
        runFileUnitTest("materialized-view/tpch/q11");
    }

    @Test
    public void testQuery12() {
        runFileUnitTest("materialized-view/tpch/q12");
    }

    @Test
    public void testQuery13() {
        runFileUnitTest("materialized-view/tpch/q13");
    }

    @Test
    public void testQuery14() {
        runFileUnitTest("materialized-view/tpch/q14");
    }

    @Test
    public void testQuery15() {
        runFileUnitTest("materialized-view/tpch/q15");
    }

    @Test
    public void testQuery16() {
        connectContext.getSessionVariable().setEnableMaterializedViewRewriteGreedyMode(true);
        runFileUnitTest("materialized-view/tpch/q16");
        connectContext.getSessionVariable().setEnableMaterializedViewRewriteGreedyMode(false);
    }

    @Test
    public void testQuery17() {
        runFileUnitTest("materialized-view/tpch/q17");
    }

    @Test
    public void testQuery18() {
        runFileUnitTest("materialized-view/tpch/q18");
    }

    @Test
    public void testQuery19() {
        runFileUnitTest("materialized-view/tpch/q19");
    }

    @Test
    public void testQuery20() {
        runFileUnitTest("materialized-view/tpch/q20");
    }

    @Test
    public void testQuery21() {
        runFileUnitTest("materialized-view/tpch/q21");
    }

    @Test
    public void testQuery22() {
        runFileUnitTest("materialized-view/tpch/q22");
    }
}
