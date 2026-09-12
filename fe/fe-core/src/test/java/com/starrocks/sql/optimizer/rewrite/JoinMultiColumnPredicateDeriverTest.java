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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

/**
 * Regression tests for {@link JoinMultiColumnPredicateDeriver} (invoked from {@link JoinPredicatePushdown}).
 */
public class JoinMultiColumnPredicateDeriverTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testDeriveMultiColumnExpressionAcrossJoinKeys() throws Exception {
        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 where t0.v1 + t0.v2 > 2";
        String plan = getFragmentPlan(sql);
        // Derived predicate may sit on a SELECT above the scan (not only OlapScanNode PREDICATES).
        PlanTestBase.assertContains(plan, "predicates: 4: v4 + 5: v5 > 2");
    }

    @Test
    public void testDeriveMixedSidePredicateToRight() throws Exception {
        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 where t0.v1 + t1.v5 > 2";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "predicates: 4: v4 + 5: v5 > 2");
    }

    @Test
    public void testDeriveMixedSidePredicateToLeft() throws Exception {
        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 where t1.v4 + t0.v2 > 2";
        String plan = getFragmentPlan(sql);
        // v4 rewrites to v1; v2 stays on the left.
        PlanTestBase.assertContains(plan, "predicates: 1: v1 + 2: v2 > 2");
    }

    @Test
    public void testIncompleteMappingDoesNotDeriveOppositeMultiColumnPredicate() throws Exception {
        // Only t0.v1 = t1.v4; t0.v2 has no mapping to t1, so cannot derive v4 + v5 form.
        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 where t0.v1 + t0.v2 > 2";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertNotContains(plan, "predicates: 4: v4 + 5: v5 > 2");
    }

    @Test
    public void testConflictingSourceMappingDoesNotDeriveMultiColumnPredicate() throws Exception {
        // t0.v1 maps to both t1.v4 and t1.v5: conflict drops mapping for v1, so no v4+v5 derivation.
        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 and t0.v1 = t1.v5 where t0.v1 + t0.v2 > 2";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertNotContains(plan, "predicates: 4: v4 + 5: v5 > 2");
    }

    @Test
    public void testExistOnClauseNotRemappedAsJoinKeyEqualsApplyArtifact() throws Exception {
        String sql = "select * from t0 join t1 on t0.v1 = t1.v4 "
                + "and t0.v1 = (exists (select v7 from t2 where t2.v8 = t1.v5))";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(
                Pattern.compile("v4\\s*=\\s*CAST\\([^\\n]*countRows").matcher(plan).find(),
                "must not derive v4 = CAST(...countRows...) via join mapping; got plan:\n" + plan);
        PlanTestBase.assertContains(plan, "countRows IS NOT NULL AS BIGINT) IS NOT NULL");
    }
}
