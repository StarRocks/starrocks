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

public class DistinctAggTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testDistinctConstants() throws Exception {
        String sql = "select count(distinct v3, 1) from t0 group by v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(3: v3 IS NULL, NULL, 1))");

        sql = "select  distinct 111 as id, 111 as id from t0 order by id + 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:Project\n" +
                "  |  <slot 5> : 5: expr\n" +
                "  |  <slot 6> : CAST(5: expr AS SMALLINT) + 1\n" +
                "  |  \n" +
                "  4:AGGREGATE (merge finalize)\n" +
                "  |  group by: 5: expr\n" +
                "  |  \n" +
                "  3:EXCHANGE");
    }
}
