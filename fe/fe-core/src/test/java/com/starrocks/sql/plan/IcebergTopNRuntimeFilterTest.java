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

    @Test
    public void testTopNAggOrderByGroupKeyGeneratesRuntimeFilter() throws Exception {
        String sql = "SELECT id, SUM(c1) FROM iceberg0.unpartitioned_db.t_numeric " +
                "GROUP BY id ORDER BY id LIMIT 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "2:TOP-N");
        assertContains(plan, "build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: id), remote = false");
        assertContains(plan, "probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 1> 1: id)");
    }

    @Test
    public void testTopNAggOrderByAggregateResultDoesNotUsePreAggTopNRuntimeFilter() throws Exception {
        String sql = "SELECT id, SUM(c1) AS revenue FROM iceberg0.unpartitioned_db.t_numeric " +
                "GROUP BY id ORDER BY revenue LIMIT 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "IcebergScanNode");
        assertNotContains(plan, "probe runtime filters");
    }
}
