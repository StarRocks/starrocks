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

import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Test;

public class TableFunctionTest extends PlanTestBase {
    @Test
    public void testTableFunctionAlias() throws Exception {
        String sql = "select t.*, unnest from test_all_type t, unnest(split(t1a, ','))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        sql = "select table_function_unnest.*, unnest from test_all_type table_function_unnest, unnest(split(t1a, ','))";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        sql = "select t.*, unnest from test_all_type t, unnest(split(t1a, ',')) unnest";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        sql = "select t.*, unnest.v1, unnest.v2 from (select * from test_all_type join t0_not_null) t, " +
                "unnest(split(t1a, ','), v3) as unnest (v1, v2)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        Exception e = Assert.assertThrows(SemanticException.class, () -> {
            String sql1 = "select table_function_unnest.*, unnest.v1, unnest.v2 from " +
                    "(select * from test_all_type join t0_not_null) " +
                    "table_function_unnest, unnest(split(t1a, ','))";
            getFragmentPlan(sql1);
        });

        Assert.assertTrue(e.getMessage().contains("Not unique table/alias: 'table_function_unnest'"));
    }
}
