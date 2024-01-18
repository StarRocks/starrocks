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

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class MaterializedViewTPCHTest extends MaterializedViewTestBase {
    @BeforeAll
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

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "materialized-view/tpch/" + entry.getKey()));
        }
        return cases.stream();
    }
}
