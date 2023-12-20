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

package com.starrocks.connector.parser.trino;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.TpchSQL;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TrinoTPCHTest extends TrinoTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, 1));
        GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().clear();

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);
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

    @Test
    public void testTPCH4_2() {
        runFileUnitTest("tpch/q4-2");
    }

    @Test
    public void testTPCH5_2() {
        runFileUnitTest("tpch/q5-2");
    }

    @Test
    public void testTPCH6_2() {
        runFileUnitTest("tpch/q6-2");
    }

    @Test
    public void testTPCH12_2() {
        runFileUnitTest("tpch/q12-2");
    }

    @Test
    public void testTPCH14_2() {
        runFileUnitTest("tpch/q14-2");
    }

    @Test
    public void testTPCH20_2() {
        runFileUnitTest("tpch/q20-2");
    }
}
