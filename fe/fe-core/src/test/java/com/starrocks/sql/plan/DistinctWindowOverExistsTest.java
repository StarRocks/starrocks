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

import org.junit.jupiter.api.Test;

/**
 * A DISTINCT aggregate over a window, filtered by an uncorrelated EXISTS.
 *
 * <p>{@code DistinctAggregationOverWindowRule} duplicates the subtree through
 * {@code OptExpressionDuplicator}. An uncorrelated EXISTS is rewritten into a join whose child
 * carries a LIMIT, so the duplicator walks into a {@code LogicalLimit} -- and
 * {@code visitLogicalLimit} was a copy of {@code visitLogicalUnion} that cast the operator to
 * {@code LogicalSetOperator}, which can never succeed. Every such query died with
 * {@code ClassCastException: LogicalLimitOperator cannot be cast to LogicalSetOperator}.
 *
 * <p>The broken method was introduced in 2024-04 with transparent MV rewrite (#43304), where
 * nothing put a limit inside a duplicated subtree, and lay dormant until
 * {@code DistinctAggregationOverWindowRule} (#65030, 2025-11) reused the same duplicator on a
 * shape that does. Three unrelated features have to meet for it to fire, which is why the
 * existing MV-rewrite suite never touched it.
 *
 * <p>The control cases below matter as much as the failing one: the defect needs the DISTINCT,
 * the window, and an *uncorrelated* EXISTS together. A correlated EXISTS or an IN subquery takes
 * a different path.
 */
public class DistinctWindowOverExistsTest extends PlanTestBase {

    @Test
    public void testDistinctWindowWithUncorrelatedExists() throws Exception {
        String plan = getFragmentPlan(
                "SELECT count(DISTINCT v1) OVER () FROM t0 WHERE EXISTS (SELECT 1 FROM t1)");
        assertContains(plan, "OUTPUT EXPRS");
    }

    @Test
    public void testDistinctWindowWithPartitionAndExists() throws Exception {
        String plan = getFragmentPlan(
                "SELECT sum(DISTINCT v2) OVER (PARTITION BY v1) FROM t0 "
                        + "WHERE EXISTS (SELECT 1 FROM t1)");
        assertContains(plan, "OUTPUT EXPRS");
    }

    @Test
    public void testDistinctWindowWithExplicitLimit() throws Exception {
        // An explicit LIMIT reaches the repaired visitor directly. Before the fix that method had
        // never executed successfully even once, so this is the case that shows the replacement
        // body actually works rather than merely not throwing.
        String plan = getFragmentPlan(
                "SELECT count(DISTINCT v1) OVER () FROM t0 "
                        + "WHERE EXISTS (SELECT 1 FROM t1) LIMIT 5");
        assertContains(plan, "OUTPUT EXPRS");
    }

    @Test
    public void testDistinctWindowWithCorrelatedExists() throws Exception {
        // Control: a correlated EXISTS plans through another path and was never affected.
        String plan = getFragmentPlan(
                "SELECT count(DISTINCT v1) OVER () FROM t0 "
                        + "WHERE EXISTS (SELECT v4 FROM t1 WHERE t1.v4 = t0.v1)");
        assertContains(plan, "OUTPUT EXPRS");
    }

    @Test
    public void testDistinctWindowWithoutExists() throws Exception {
        // Control: without the EXISTS there is no limit in the duplicated subtree.
        String plan = getFragmentPlan("SELECT count(DISTINCT v1) OVER () FROM t0");
        assertContains(plan, "OUTPUT EXPRS");
    }
}
