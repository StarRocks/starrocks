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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RewriteAliasTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        PlanTestBase.beforeClass();
    }

    @Test
    public void testAggregateNotNested() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableGroupbyUseOutputAlias(true);
        String sql = "select \n" +
                "sum(v1) v1,\n" +
                "sum(v2) v2,\n" +
                "sum(case when v3>0 then v1 else v2 end) v3\n" +
                "from t0";

        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1), sum(2: v2), sum(if(3: v3 > 0, 1: v1, 2: v2))\n" +
                "  |  group by: ");
    }

    @Test
    public void testAnalyticalNotNested() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableGroupbyUseOutputAlias(true);
        String sql = "select \n" +
                "sum(v1) over() v1,\n" +
                "sum(v2) over() v2,\n" +
                "sum(case when v3>0 then v1 else v2 end) over() v3\n" +
                "from t0";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ], [, sum(2: v2), ], [, sum(if(3: v3 > 0, 1: v1, 2: v2)), ]");
    }
}