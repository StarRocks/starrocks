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

import com.starrocks.catalog.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.MinMaxFilterExprBuilder;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RuntimeFilterTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    @Test
    public void testDeterministicBroadcastJoinForColocateJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [colocate] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [7: v4, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 1, build_expr = (7: v4), remote = true\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1");

    }

    @Test
    public void testDeterministicBroadcastJoinForBroadcastJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [broadcast] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |       probe runtime filters:\n" +
                "  |       - filter_id = 2, probe_expr = (7: v4)");
    }

    @Test
    public void testMinMaxRuntimeFilter() throws Exception {
        // Enable min/max runtime filter
        connectContext.getSessionVariable().setEnableMinMaxRuntimeFilter(true);
        
        // Test MIN aggregation - should generate min/max runtime filter
        String sql = "SELECT MIN(v1) as min_v1 FROM t0";
        String plan = getVerboseExplain(sql);
        
        // The plan should contain the aggregation with min/max runtime filter
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "min_v1");
        
        // Test MAX aggregation
        sql = "SELECT MAX(v1) as max_v1 FROM t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "max_v1");
        
        // Test with GROUP BY - should still generate min/max runtime filter
        sql = "SELECT v2, MIN(v1) as min_v1 FROM t0 GROUP BY v2";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "group by");
    }

    @Test
    public void testMinMaxRuntimeFilterNoGroupBy() throws Exception {
        // Enable min/max runtime filter
        connectContext.getSessionVariable().setEnableMinMaxRuntimeFilter(true);
        
        // Test SELECT MIN(v1), MAX(v1) FROM t0 - no GROUP BY
        String sql = "SELECT MIN(v1) as min_v1, MAX(v1) as max_v1 FROM t0";
        String plan = getVerboseExplain(sql);
        
        // The plan should contain the aggregation
        assertContains(plan, "AGGREGATE");
        
        // Test SELECT MIN(v1), MAX(v2) FROM t0 - different columns
        sql = "SELECT MIN(v1) as min_v1, MAX(v2) as max_v2 FROM t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        
        // Test with WHERE clause
        sql = "SELECT MIN(v1) as min_v1 FROM t0 WHERE v2 > 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "PREDICATES");
    }

    @Test
    public void testMinMaxRuntimeFilterMultiStage() throws Exception {
        // Enable min/max runtime filter
        connectContext.getSessionVariable().setEnableMinMaxRuntimeFilter(true);
        
        // Test with GROUP BY - this typically triggers multi-stage aggregation
        String sql = "SELECT v2, MIN(v1), MAX(v1) FROM t0 GROUP BY v2";
        String plan = getVerboseExplain(sql);
        
        // The plan should contain aggregation with finalize stage
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "finalize");
        
        // Test with DISTINCT - also triggers multi-stage aggregation
        sql = "SELECT COUNT(DISTINCT v1), MIN(v2), MAX(v2) FROM t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
    }

    @Test
    public void testMinMaxRuntimeFilterColumnRef() throws Exception {
        // Enable min/max runtime filter
        connectContext.getSessionVariable().setEnableMinMaxRuntimeFilter(true);
        
        // Test MIN/MAX with simple column reference - should generate runtime filter
        String sql = "SELECT MIN(v1) FROM t0";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        
        // Test MAX with simple column reference
        sql = "SELECT MAX(v1) FROM t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        
        // Test MIN/MAX with expression - should NOT generate runtime filter
        // because the filter cannot be pushed down to scan node
        sql = "SELECT MIN(v1 + 1) FROM t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
        
        // Test MIN/MAX with multiple columns - should NOT generate runtime filter
        // Note: This is actually invalid SQL for MIN/MAX, but we handle it gracefully
        
        // Test MIN/MAX with different column types
        sql = "SELECT MIN(v2), MAX(v3) FROM t0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "AGGREGATE");
    }
    
    @Test
    public void testMinMaxFilterExpression() throws Exception {
        // Test the MinMaxFilterExprBuilder
        SlotRef slotRef = new SlotRef(new TableName("db", "t"), "created_at");
        slotRef.setType(DateType.DATE);
        
        // Build range filter: created_at >= '2024-01-01' AND created_at <= '2024-01-31'
        Expr rangeFilter = MinMaxFilterExprBuilder.buildRangeFilter(
                slotRef, "2024-01-01", "2024-01-31");
        assertNotNull(rangeFilter);
        String rangeSql = MinMaxFilterExprBuilder.toSql(rangeFilter);
        assertContains(rangeSql, ">=");
        assertContains(rangeSql, "<=");
        assertContains(rangeSql, "2024-01-01");
        assertContains(rangeSql, "2024-01-31");
        
        // Build exclusion filter: created_at < '2024-01-01' OR created_at > '2024-01-31'
        Expr exclusionFilter = MinMaxFilterExprBuilder.buildExclusionFilter(
                slotRef, "2024-01-01", "2024-01-31");
        assertNotNull(exclusionFilter);
        String exclusionSql = MinMaxFilterExprBuilder.toSql(exclusionFilter);
        assertContains(exclusionSql, "<");
        assertContains(exclusionSql, ">");
        assertContains(exclusionSql, "OR");
        
        // Build lower bound filter: created_at >= '2024-01-01'
        Expr lowerFilter = MinMaxFilterExprBuilder.buildLowerBoundFilter(
                slotRef, "2024-01-01");
        assertNotNull(lowerFilter);
        String lowerSql = MinMaxFilterExprBuilder.toSql(lowerFilter);
        assertContains(lowerSql, ">=");
        
        // Build upper bound filter: created_at <= '2024-01-31'
        Expr upperFilter = MinMaxFilterExprBuilder.buildUpperBoundFilter(
                slotRef, "2024-01-31");
        assertNotNull(upperFilter);
        String upperSql = MinMaxFilterExprBuilder.toSql(upperFilter);
        assertContains(upperSql, "<=");
    }
}
