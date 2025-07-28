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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for OuterJoinEliminationRule.
 */
public class OuterJoinEliminationRuleTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeAll();
    }

    @AfterAll
    public static void afterClass() {
        PlanTestBase.afterClass();
    }

    @Test
    public void testLeftJoinCanBeConvertedToInner() throws Exception {
        String sql = "SELECT * FROM join1 LEFT JOIN join2 ON join1.id = join2.id WHERE join2.id > 1";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN"));
    }

    @Test
    public void testRightJoinCanBeConvertedToInner() throws Exception {
        String sql = "SELECT * FROM join1 RIGHT JOIN join2 ON join1.id = join2.id WHERE join1.id > 1";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN"));
    }

    @Test
    public void testFullOuterJoinCanBeConvertedToInner() throws Exception {
        String sql = "SELECT * FROM join1 FULL OUTER JOIN join2 ON join1.id = join2.id WHERE join1.id > 10 AND join2.id > 1";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN"));
    }

    @Test
    public void testNoEliminateLeftJoinIfFilterOnNull() throws Exception {
        String sql = "SELECT * FROM join1 LEFT JOIN join2 ON join1.id = join2.id WHERE join2.id IS NULL";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("LEFT OUTER JOIN"));
    }

    @Test
    public void testNoEliminateLeftJoinIfHasPostJoinFilterOnInnerTable() throws Exception {
        String sql = "SELECT MIN(join1.dt) FROM join1 LEFT JOIN join2 ON join1.id = join2.id AND join1.dt > join2.dt";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("TABLE: join2"));
    }

    @Test
    public void testEliminateLeftJoinWithDuplicateInsensitiveAggregatesOnly() throws Exception {
        String sql = "SELECT MIN(join1.dt), MAX(join1.dt) FROM join1 LEFT JOIN join2 ON join1.id = join2.id";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("TABLE: join2"));
    }

    @Test
    public void testEliminateLeftJoinWithGroupByOnPreservedSide() throws Exception {
        String sql = "SELECT MIN(join1.dt) FROM join1 LEFT JOIN join2 ON join1.id = join2.id GROUP BY join1.id";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("TABLE: join2"));
    }

    @Test
    public void testNoEliminateLeftJoinIfReferencingInnerColumnsInSelect() throws Exception {
        String sql = "SELECT MIN(join2.dt) FROM join1 LEFT JOIN join2 ON join1.id = join2.id";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("TABLE: join2"));
    }

    @Test
    public void testNoEliminateLeftJoinIfUsingDuplicateSensitiveAggregates() throws Exception {
        String sql = "SELECT SUM(join1.dt), AVG(join1.dt) FROM join1 LEFT JOIN join2 ON join1.id = join2.id";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("LEFT OUTER JOIN"));
    }

    @Test
    public void testEliminateRightJoinWithAggregateOnPreservedSide() throws Exception {
        String sql = "SELECT MIN(join2.dt) FROM join1 RIGHT JOIN join2 ON join1.id = join2.id";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("TABLE: join1"));
    }

    @Test
    public void testEliminateLeftJoinAndAddNotNullPredicateIfFKCanBeNull() throws Exception {
        String sql = "SELECT MIN(join1.dt) FROM join1 LEFT JOIN join2 ON join1.id = join2.id WHERE join1.id IS NOT NULL";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("TABLE: join2"));
    }

    @Test
    public void testNoEliminateLeftJoinIfHavingClauseReferencesInnerColumn() throws Exception {
        String sql = "SELECT COUNT(*) FROM join1 LEFT JOIN join2 ON join1.id = join2.id GROUP BY join2.id HAVING join2.id > 1";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN"));
    }

    @Test
    public void testNoEliminateLeftJoinIfJoinConditionIsNotStrictEquality() throws Exception {
        String sql = "SELECT MIN(join1.dt) FROM join1 LEFT JOIN join2 ON join1.id < join2.id";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("LEFT OUTER JOIN"));
    }
}