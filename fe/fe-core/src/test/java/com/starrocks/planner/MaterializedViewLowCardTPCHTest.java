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
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MaterializedViewLowCardTPCHTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        executeSqlFile("sql/materialized-view/tpch/ddl_tpch.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv1.sql");
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);

        int scale = 1;
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, scale));
        OlapTable t4 = (OlapTable) globalStateMgr.getDb(MATERIALIZED_DB_NAME).getTable("customer");
        setTableStatistics(t4, 150000 * scale);
        OlapTable t7 = (OlapTable) globalStateMgr.getDb(MATERIALIZED_DB_NAME).getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);

        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
    }

    @Test
    public void testQuery7() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        String plan = getVerboseExplain(TpchSQL.Q07);
        assertContainsIgnoreColRefs(plan, "group by: [425: n_name, INT, true], " +
                "[426: n_name, INT, true], " +
                "[49: year, SMALLINT, false]");
    }

    @Test
    public void testQuery12() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        String plan = getVerboseExplain(TpchSQL.Q12);
        assertCContains(plan, "group by: [90: l_shipmode, INT, true]");
    }
}
