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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Tests for the TopN pre-agg runtime filter optimization on Iceberg (non-OLAP) scans.
// The optimization is triggered by PushDownTopNToPreAggRule when:
//   TopN(PARTIAL) -> Agg(GLOBAL) -> Agg(LOCAL) -> IcebergScan
// which causes AggregationNode.buildRuntimeFilters() to push a TOPN_FILTER to the scan.
public class IcebergTopNRuntimeFilterTest extends ConnectorPlanTestBase {
    private static boolean originalRunningUnitTest;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    // ORDER BY grouping key — basic case.
    @Test
    public void testTopNAggOrderByGroupKeyGeneratesRuntimeFilter() throws Exception {
        String sql = "SELECT id, SUM(c1) FROM iceberg0.unpartitioned_db.t_numeric " +
                "GROUP BY id ORDER BY id LIMIT 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: id)");
    }

    // ORDER BY grouping key via alias — the column mapping must be threaded through the Project
    // that column pruning inserts between TopN and Agg.  This is the main regression target for
    // the alias/project fix: without the fix isTopNOnGroupKeyAgg() returns false for this shape
    // and no runtime filter is generated.
    @Test
    public void testTopNAggOrderByAliasedGroupKeyGeneratesRuntimeFilter() throws Exception {
        String sql = "SELECT id AS k, SUM(c1) FROM iceberg0.unpartitioned_db.t_numeric " +
                "GROUP BY id ORDER BY k LIMIT 10";
        String plan = getVerboseExplain(sql);
        // The filter must probe the underlying 'id' column, not the alias 'k'.
        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: id)");
    }

    // ORDER BY aggregate result — should NOT generate a pre-agg TopN runtime filter because
    // the ordering column depends on the aggregation output, not a grouping key.
    @Test
    public void testTopNAggOrderByAggregateResultNoRuntimeFilter() throws Exception {
        String sql = "SELECT id, SUM(c1) AS revenue FROM iceberg0.unpartitioned_db.t_numeric " +
                "GROUP BY id ORDER BY revenue LIMIT 10";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "probe runtime filters");
    }
}
