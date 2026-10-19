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

package com.starrocks.sql.optimizer.task;

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

// Whether a physical property is satisfied is not a pure function: HashDistributionSpec.isSatisfy()
// asks ColocateTableIndex whether the colocate group is stable, and the ColocateTableBalancer daemon
// flips that state concurrently. EnforceAndCostTask used to ask twice -- once to decide that the
// property needs enforcing, once inside enforceProperty() to pick the enforcer -- so a flip between
// the two made enforceProperty() fall through all of its branches and return null, and the caller
// dereferenced it.
public class EnforcePropertyColocateRaceTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("colocate_race_db").useDatabase("colocate_race_db");
        starRocksAssert.withTable("" +
                "CREATE TABLE t1 (\n" +
                "dt DATE NOT NULL,\n" +
                "c1 INT NOT NULL,\n" +
                "v1 BIGINT NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(dt, c1)\n" +
                "PARTITION BY RANGE(dt) (\n" +
                "  START (\"2022-01-01\") END (\"2022-02-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(c1) BUCKETS 4\n" +
                "PROPERTIES(\"replication_num\" = \"1\", \"colocate_with\" = \"cg_colocate_race\");");
    }

    @Test
    public void testPlanningSurvivesColocateGroupFlippingStability() throws Exception {
        // Every other question about the group's stability gets the opposite answer, which is what a
        // balancer running concurrently with the optimizer looks like from the planner's side.
        AtomicInteger calls = new AtomicInteger();
        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isGroupUnstable(ColocateTableIndex.GroupId groupId) {
                return calls.getAndIncrement() % 2 == 0;
            }
        };

        String sql = "select count(*), sum(s), min(s), max(s) from " +
                "(select c1, sum(v1) s from t1 group by c1 limit 10) x";
        // The plan itself is whatever the flipping answers make it; all that matters is that
        // planning completes instead of dereferencing a null enforced property.
        for (int i = 0; i < 16; i++) {
            final int round = i;
            Assertions.assertDoesNotThrow(() -> UtFrameUtils.getPlanAndFragment(ctx, sql),
                    "planning must not fail when the colocate group's stability flips, round " + round);
        }
    }
}
