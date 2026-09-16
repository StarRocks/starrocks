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

import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * enable_ukfk_join_reorder is an independent switch from enable_ukfk_opt. Turning only the former on used to make
 * JoinOrder.buildJoinExpr dereference UK/FK constraints that UKFKConstraintsCollector had refused to collect
 * (the collector short-circuits on enable_ukfk_opt), so every join under an aggregate died with
 * NullPointerException: Cannot read field "uniqueKeys" because "other" is null.
 */
public class UKFKJoinReorderTest extends PlanTestBase {
    private boolean prevUKFKJoinReorder;
    private boolean prevUKFKOpt;
    private int prevPushDownAggregateMode;

    @BeforeEach
    public void setUp() {
        SessionVariable sv = connectContext.getSessionVariable();
        prevUKFKJoinReorder = sv.isEnableUKFKJoinReorder();
        prevUKFKOpt = sv.isEnableUKFKOpt();
        prevPushDownAggregateMode = sv.getCboPushDownAggregateMode();
        // PlanTestBase disables aggregate push-down, but ReorderJoinRule is only reached from
        // QueryOptimizer.pushDownAggregation, so restore the production default here.
        sv.setCboPushDownAggregateMode(0);
    }

    @AfterEach
    public void tearDown() {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableUKFKJoinReorder(prevUKFKJoinReorder);
        sv.setEnableUKFKOpt(prevUKFKOpt);
        sv.setCboPushDownAggregateMode(prevPushDownAggregateMode);
    }

    @Test
    public void testJoinReorderWithoutUKFKOpt() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableUKFKJoinReorder(true);
        // Left at its default on purpose: this is the combination that used to crash.
        sv.setEnableUKFKOpt(false);

        // Cross join: no equal-on predicate, so the collector fails while inheriting relaxed unique keys.
        String plan = getFragmentPlan("select count(1) from t0 a, t0 b");
        assertContains(plan, "CROSS JOIN");

        plan = getFragmentPlan("select count(1) from t0 a, t0 b, t0 c");
        assertContains(plan, "CROSS JOIN");

        // Inner join: there is an equal-on predicate, so the collector fails earlier, while looking the
        // unique constraint of the left child up.
        plan = getFragmentPlan("select count(1) from t0 a join t0 b on a.v1 = b.v1");
        assertContains(plan, "JOIN");

        plan = getFragmentPlan("select count(1) from t0 a join t0 b on a.v1 = b.v1 join t0 c on b.v2 = c.v2");
        assertContains(plan, "JOIN");
    }

    @Test
    public void testJoinReorderWithUKFKOpt() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableUKFKJoinReorder(true);
        sv.setEnableUKFKOpt(true);

        String plan = getFragmentPlan("select count(1) from t0 a, t0 b");
        assertContains(plan, "CROSS JOIN");

        plan = getFragmentPlan("select count(1) from t0 a join t0 b on a.v1 = b.v1");
        assertContains(plan, "JOIN");
    }
}
