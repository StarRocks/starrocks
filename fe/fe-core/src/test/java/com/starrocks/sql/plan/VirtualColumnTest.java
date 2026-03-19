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

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test virtual columns' functionality.
 * Virtual columns are dynamically computed metadata columns that exist only during query execution.
 * They are managed by VirtualColumnRegistry for scalability.
 */
public class VirtualColumnTest extends PlanTestBase {
    
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.enable_virtual_columns = true;
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_virtual_columns = false;
    }

    @Test
    public void testTabletIdVirtualColumn() throws Exception {
        // Test that _tablet_id_ is recognized in SELECT clause
        String sql = "select v1, _tablet_id_ from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "_tablet_id_");
        
        // Test that _tablet_id_ can be used in WHERE clause
        sql = "select v1 from t0 where _tablet_id_ = 10001";
        plan = getFragmentPlan(sql);
        assertContains(plan, "_tablet_id_");
        
        // Test that _tablet_id_ works with joins
        sql = "select t0.v1, t0._tablet_id_, t1.v4 from t0 join t1 on t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "_tablet_id_");
    }
    
    @Test
    public void testTabletIdNotInSelectStar() throws Exception {
        // Test that _tablet_id_ is not included in SELECT *
        String sql = "select * from t0";
        String plan = getFragmentPlan(sql);
        // Virtual columns should not appear in SELECT * output
        assertNotContains(plan, "_tablet_id_");
    }
    
    @Test
    public void testTabletIdWithAggregation() throws Exception {
        // Test that _tablet_id_ can be used with aggregation
        String sql = "select _tablet_id_, count(*) from t0 group by _tablet_id_";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "_tablet_id_");
    }

    @Test
    public void testRowIdWithAggregation() throws Exception {
        // Test that _row_id_ can be used with aggregation
        String sql = "select _row_id_, _segment_id_, _tablet_id_ from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "_row_id_");
        assertContains(plan, "_segment_id_");
        assertContains(plan, "_tablet_id_");
    }
}
