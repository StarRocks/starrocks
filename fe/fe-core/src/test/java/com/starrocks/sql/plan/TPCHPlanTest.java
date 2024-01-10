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

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.TpchSQL;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TPCHPlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
        connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.DOUBLE);
    }

    @Test
    public void testJoin() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table1 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(table1, 10000);
        runFileUnitTest("optimized-plan/join");
        setTableStatistics(table1, 0);
    }

    @Test
    public void testSelect() {
        runFileUnitTest("optimized-plan/select");
    }

    @Test
    public void testPredicatePushDown() {
        runFileUnitTest("optimized-plan/predicate-pushdown");
    }

    @Test
    public void testInSubquery() {
        runFileUnitTest("subquery/in-subquery");
    }

    @Test
    public void testExists() {
        runFileUnitTest("subquery/exists-subquery");
    }

    @Test
    public void testScalar() {
        runFileUnitTest("subquery/scalar-subquery");
    }

    @Test
    public void testWindow() {
        runFileUnitTest("optimized-plan/window");
    }

    @Test
    public void testExtractPredicate() {
        runFileUnitTest("optimized-plan/predicate-extract");
    }

    @Test
    public void testConstant() {
        runFileUnitTest("optimized-plan/constant-query");
    }

    @Test
    public void testSetOperation() {
        runFileUnitTest("optimized-plan/set-operation");
    }

    @Test
    public void testGroupingSets() {
        runFileUnitTest("optimized-plan/grouping-sets");
    }

    @Test
    public void testLateral() {
        runFileUnitTest("optimized-plan/lateral");
    }

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "tpch/" + entry.getKey()));
        }
        return cases.stream();
    }
}
