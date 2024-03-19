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

public class SelectHintTest extends PlanTestBase {

    @Test
    public void testHintSql() throws Exception {
        String sql = "select /*+ set_user_variable(@a = 1, @b = (select max(v4) from t1))*/ @a, @b, v1 from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1\n" +
                "  |  <slot 5> : 'MOCK_HINT_VALUE'\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");

        sql = "select @a, @b, v1 from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : NULL\n" +
                "  |  <slot 5> : NULL\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }
}
