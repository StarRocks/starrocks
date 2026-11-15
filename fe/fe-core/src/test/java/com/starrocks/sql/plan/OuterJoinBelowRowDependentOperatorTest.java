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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// A join rejecting NULLs of a column records `col IS NOT NULL` for the whole rewrite pass, and an outer join feeding
// that column becomes an inner join. Below an operator whose output rows depend on other input rows, dropping the
// NULL-extended rows changes which rows are kept or what is computed for them, so the outer join must stay.
public class OuterJoinBelowRowDependentOperatorTest extends PlanTestBase {

    private static final String OUTER_JOIN = "select t0.v1, t0.v2, t0.v3, t1.v5 from t0 left join t1 on t0.v1 = t1.v4";

    @Test
    public void testNullRejectedAboveConvertsOuterJoin() throws Exception {
        String sql = "select * from (" + OUTER_JOIN + " where t0.v2 > 0) t join t2 on t.v5 = t2.v7";
        assertNotContains(getFragmentPlan(sql), "OUTER JOIN");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            OUTER_JOIN + " where t0.v2 > 0 order by t0.v3 limit 10",
            OUTER_JOIN + " where t0.v2 > 0 limit 10",
            // the local half of the limit is merged into the outer join itself
            OUTER_JOIN + " limit 10",
            "select v1, v5, count(*) over (partition by v2) from (" + OUTER_JOIN + ") x where v2 > 0",
    })
    public void testNullRejectedAboveRowDependentOperator(String below) throws Exception {
        String sql = "select * from (" + below + ") t join t2 on t.v5 = t2.v7";
        assertContains(getFragmentPlan(sql), "LEFT OUTER JOIN");
    }

    @Test
    public void testFilterAboveOuterJoinWithLimit() throws Exception {
        String sql = "select * from (" + OUTER_JOIN + " limit 10) t where t.v5 > 0";
        assertContains(getFragmentPlan(sql), "LEFT OUTER JOIN");
    }
}
