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

import com.starrocks.qe.SqlModeHelper;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// BE evaluates a projection's common expressions for every chunk before its outputs. In ALLOW_THROW_EXCEPTION mode
// an expression that the original plan only evaluated conditionally must therefore not become a common expression:
// it could fail the query on rows that never needed it.
public class ExprReuseThrowModeTest extends PlanTestBase {
    private long sqlMode;

    @BeforeEach
    public void before() {
        sqlMode = connectContext.getSessionVariable().getSqlMode();
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setSqlMode(sqlMode);
    }

    private String planInThrowMode(String sql) throws Exception {
        connectContext.getSessionVariable().setSqlMode(sqlMode | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
        return getFragmentPlan(sql);
    }

    @Test
    public void testIfBranches() throws Exception {
        String sql = "select if(v1 > 0, 0, abs(v1 + v2)) + if(v1 > 1, 0, abs(v1 + v2) + 1) from tarray";
        assertContains(getFragmentPlan(sql), "common expressions");
        assertNotContains(planInThrowMode(sql), "common expressions");
    }

    @Test
    public void testCaseBranches() throws Exception {
        String sql = "select case when v1 > 0 then abs(v1 + v2) else 0 end, " +
                "case when v1 > 1 then 0 else abs(v1 + v2) end from tarray";
        assertContains(getFragmentPlan(sql), "common expressions");
        assertNotContains(planInThrowMode(sql), "common expressions");
    }

    @Test
    public void testCoalesceAndOrRightSide() throws Exception {
        String sql = "select coalesce(v1, abs(v1 + v2)), v2 > 0 or abs(v1 + v2) > 1 from tarray";
        assertContains(getFragmentPlan(sql), "common expressions");
        assertNotContains(planInThrowMode(sql), "common expressions");
    }

    @Test
    public void testUnconditionalOccurrenceStillReused() throws Exception {
        // the first occurrence is always evaluated, so evaluating it up front changes nothing
        String sql = "select abs(v1 + v2), if(v1 > 0, abs(v1 + v2), 0) from tarray";
        assertContains(planInThrowMode(sql), "common expressions");
        // the condition of IF and the left side of AND are always evaluated too
        sql = "select if(abs(v1 + v2) > 0, 1, 2), abs(v1 + v2) > 1 and v2 > 0 from tarray";
        assertContains(planInThrowMode(sql), "common expressions");
    }

    @Test
    public void testSharedNonDeterministicCallStillReused() throws Exception {
        // Both IF branches reference the same rand() occurrence: it must stay one evaluation, or the two outputs
        // differ for a row that takes both branches.
        String plan = planInThrowMode("select if(v1 > 0, r, 0), if(v1 > 1, r, 0) " +
                "from (select v1, rand() as r from tarray) s");
        assertContains(plan, "common expressions");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "rand()"), plan);
        // same when the call is wrapped in an expression the reuse rewriter does not descend into: the wrapping
        // expression is reused as in the default mode, so both outputs read one array_sum
        String sql = "select if(v1 > 0, r, 0), if(v1 > 1, r, 0) " +
                "from (select v1, array_sum([rand(), cast(v2 as double)]) as r from tarray) s";
        plan = planInThrowMode(sql);
        assertContains(plan, "if(1: v1 > 0, 9: array_sum, 0.0)", "if(1: v1 > 1, 9: array_sum, 0.0)",
                "<slot 9> : array_sum([rand(),CAST(2: v2 AS DOUBLE)])");
        // two separate rand() calls stay two evaluations
        plan = planInThrowMode("select if(v1 > 0, rand(), 0), if(v1 > 1, rand(), 0) from tarray");
        Assertions.assertEquals(2, StringUtils.countMatches(plan, "rand()"), plan);
    }

    @Test
    public void testLambdaBodyNotHoisted() throws Exception {
        // array_map skips the lambda for an empty or NULL array; a lambda-argument-independent expression in the
        // body must stay inside the lambda.
        String sql = "select array_map(x -> x + abs(v1 + v2), v3) from tarray";
        assertContains(getFragmentPlan(sql), "common expressions");
        String plan = planInThrowMode(sql);
        assertNotContains(plan, "common expressions");
        assertContains(plan, "array_map(<slot 4> -> CAST(<slot 4> AS LARGEINT) + abs(1: v1 + 2: v2), 3: v3)");
    }
}
