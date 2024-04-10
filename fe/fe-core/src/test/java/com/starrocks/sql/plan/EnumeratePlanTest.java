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
import com.starrocks.common.FeConstants;
import com.starrocks.planner.TpchSQL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class EnumeratePlanTest extends DistributedEnvPlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
        connectContext.getSessionVariable().setCboPruneShuffleColumnRate(0);
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setUseNthExecPlan(0);
    }

    @Test
    public void testThreeTableJoinEnumPlan() {
        runFileUnitTest("enumerate-plan/three-join");
    }

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "enumerate-plan/tpch-" + entry.getKey()));
        }
        return cases.stream();
    }

    @Test
    public void testFullOuterJoinPlan_1() {
        runFileUnitTest("enumerate-plan/full-outer-join-1");
    }

    @Test
    public void testFullOuterJoinPlan_2() {
        runFileUnitTest("enumerate-plan/full-outer-join-2");
    }
}
