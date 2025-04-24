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

import org.junit.Test;

public class DeferProjectAfterTopNTest extends PlanTestBase {

    @Test
    public void testDeferProjectAfterTopN() throws Exception {
        {
            String sql = "select v1, hex(v2), v2 + v3 from t0 order by v1 limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:TOP-N\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  3:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : hex(2: v2)\n" +
                    "  |  <slot 5> : 2: v2 + 3: v3\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  2:MERGING-EXCHANGE");
        }
        {
            String sql = "select v1,hex(v1) from t0 order by hex(v1) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:TOP-N\n" +
                    "  |  order by: <slot 4> 4: hex ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : hex(1: v1)");
        }
        {
            String sql = "select v1, hex(v2), hex(v3) from t0 order by v1,hex(v2) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:TOP-N\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 4> 4: hex ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 4> : hex(2: v2)\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  4:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: hex\n" +
                    "  |  <slot 5> : hex(3: v3)\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  3:MERGING-EXCHANGE");
        }
        {
            String sql = "select v1, hex(v2), length(hex(v2)) from t0 order by hex(v2) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:TOP-N\n" +
                    "  |  order by: <slot 4> 4: hex ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 4> : hex(2: v2)\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  4:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: hex\n" +
                    "  |  <slot 5> : length(hex(2: v2))\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  3:MERGING-EXCHANGE");
        }

    }
}
