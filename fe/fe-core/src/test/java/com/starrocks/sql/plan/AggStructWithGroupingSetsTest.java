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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Aggregating a full STRUCT column with ROLLUP/CUBE/GROUPING SETS used to fail at plan time with
 * "StructType SlotRef must have an non-empty usedStructFiledPos!". Plain GROUP BY is the control.
 */
public class AggStructWithGroupingSetsTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("create table t_agg_struct(c0 INT, c2 struct<a int, b int>) "
                + "duplicate key(c0) distributed by hash(c0) buckets 1 "
                + "properties('replication_num'='1');");
    }

    @Test
    public void testAnyValueStructWithRollup() throws Exception {
        String plan = getFragmentPlan(
                "select c0, any_value(c2) from t_agg_struct group by rollup(c0)");
        assertContains(plan, "REPEAT_NODE");
        assertContains(plan, "any_value");
    }

    @Test
    public void testAnyValueStructWithCube() throws Exception {
        String plan = getFragmentPlan(
                "select c0, any_value(c2) from t_agg_struct group by cube(c0)");
        assertContains(plan, "REPEAT_NODE");
        assertContains(plan, "any_value");
    }

    @Test
    public void testAnyValueStructWithGroupingSets() throws Exception {
        String plan = getFragmentPlan(
                "select c0, any_value(c2) from t_agg_struct group by grouping sets((c0), ())");
        assertContains(plan, "REPEAT_NODE");
        assertContains(plan, "any_value");
    }

    @Test
    public void testArrayAggStructWithRollup() throws Exception {
        String plan = getFragmentPlan(
                "select c0, array_agg(c2) from t_agg_struct group by rollup(c0)");
        assertContains(plan, "REPEAT_NODE");
        assertContains(plan, "array_agg");
    }

    @Test
    public void testPlainGroupByStructStillWorks() throws Exception {
        String plan = getFragmentPlan(
                "select c0, any_value(c2) from t_agg_struct group by c0");
        assertContains(plan, "any_value");
    }
}
