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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AsofJoinStatisticsTest extends PlanWithCostTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanWithCostTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        setTableStatistics(t0, 1000);
        
        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");
        setTableStatistics(t1, 200000);
    }

    @Test
    public void testAsofInnerJoinStatistics() throws Exception {
        // Test ASOF INNER JOIN statistics calculation
        // Expected: output row count should be <= probe table row count (1000)
        String sql = "SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5";
        String plan = getCostExplain(sql);

        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: ASOF INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [4: v4, BIGINT, true]\n" +
                "  |  asof join conjunct: [2: v2, BIGINT, true] <= [5: v5, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (4: v4), remote = false\n" +
                "  |  output columns: 1\n" +
                "  |  cardinality: 900");
    }

    @Test  
    public void testAsofLeftJoinStatistics() throws Exception {
        // Expected: output row count should equal probe table row count (1000)
        String sql = "SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 >= t1.v5";
        String plan = getCostExplain(sql);
        
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: ASOF LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [4: v4, BIGINT, true]\n" +
                "  |  asof join conjunct: [2: v2, BIGINT, true] >= [5: v5, BIGINT, true]\n" +
                "  |  output columns: 1\n" +
                "  |  cardinality: 1000");
    }
}
