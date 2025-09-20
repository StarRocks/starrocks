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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Test;

public class JoinPredicateExprReuseTest extends PlanTestBase {
    @Test
    public void testHashJoin() throws Exception {
        {
            String sql = "select * from t0 left join t1 on t0.v1 = t1.v4 where " +
                    "abs(t0.v1 + t1.v4) = abs(t0.v2 + t1.v5) and abs(t0.v1 + t1.v4) > 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                    "  |  other predicates: 8: abs = abs(2: v2 + 5: v5), 8: abs > 5\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : abs(7: add)");
        }
        
        {
            String sql = "select * from t0 left join t1 on t0.v1 = t1.v4 where " +
                    "bit_shift_left(t0.v1 + t1.v4, 1) = 10 or bit_shift_left(t0.v1 + t1.v4, 1) = 20";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                    "  |  other predicates: (8: bit_shift_left = 10) OR (8: bit_shift_left = 20), " +
                    "8: bit_shift_left IN (10, 20)\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : 7: add BITSHIFTLEFT 1");
        }
        {
            String sql = "select * from t0 right join t1 on t0.v1 = t1.v4 where " +
                    "abs(t0.v1 + t1.v4) = abs(t0.v2 + t1.v5) and abs(t0.v1 + t1.v4) > 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                    "  |  other predicates: 8: abs = abs(2: v2 + 5: v5), 8: abs > 5\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : abs(7: add)");
        }

        {
            String sql = "select * from t0 right join t1 on t0.v1 = t1.v4 where " +
                    "bit_shift_left(t0.v1 + t1.v4, 1) = 10 or bit_shift_left(t0.v1 + t1.v4, 1) = 20";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                    "  |  other predicates: (8: bit_shift_left = 10) OR (8: bit_shift_left = 20), " +
                    "8: bit_shift_left IN (10, 20)\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : 7: add BITSHIFTLEFT 1");
        }
    }

    @Test
    public void testNestLoopJoin() throws Exception {
        {
            String sql = "select * from t0 left join t1 on t0.v1 > t1.v4 where " +
                    "abs(t0.v1 + t1.v4) = abs(t0.v2 + t1.v5) and abs(t0.v1 + t1.v4) > 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  other join predicates: 1: v1 > 4: v4\n" +
                    "  |  other predicates: 8: abs = abs(2: v2 + 5: v5), 8: abs > 5\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : abs(7: add)");
        }
        
        {
            String sql = "select * from t0 left join t1 on t0.v1 > t1.v4 where " +
                    "bit_shift_left(t0.v1 + t1.v4, 1) = 10 or bit_shift_left(t0.v1 + t1.v4, 1) = 20";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  other join predicates: 1: v1 > 4: v4\n" +
                    "  |  other predicates: (8: bit_shift_left = 10) OR (8: bit_shift_left = 20), " +
                    "8: bit_shift_left IN (10, 20)\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : 7: add BITSHIFTLEFT 1");
        }
        
        {
            String sql = "select * from t0 right join t1 on t0.v1 > t1.v4 and t0.v2 = t1.v5 where " +
                    "abs(t0.v1 + t1.v4) = abs(t0.v2 + t1.v5) and abs(t0.v1 + t1.v4) > 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  other join predicates: 1: v1 > 4: v4\n" +
                    "  |  other predicates: 8: abs = abs(2: v2 + 5: v5), 8: abs > 5\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : abs(7: add)");
        }
        
        {
            String sql = "select * from t0 right join t1 on t0.v1 > t1.v4 where " +
                    "bit_shift_left(t0.v1 + t1.v4, 1) = 10 or bit_shift_left(t0.v1 + t1.v4, 1) = 20";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  other join predicates: 1: v1 > 4: v4\n" +
                    "  |  other predicates: (8: bit_shift_left = 10) OR (8: bit_shift_left = 20), " +
                    "8: bit_shift_left IN (10, 20)\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : 7: add BITSHIFTLEFT 1");
        }
        
        {
            String sql = "select * from t0 left join t1 on t0.v1 = t1.v4 where " +
                    "abs(t0.v1 + t1.v4) > 5 and abs(t0.v1 + t1.v4) < 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                    "  |  other predicates: 8: abs > 5, 8: abs < 10\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 7> : 1: v1 + 4: v4\n" +
                    "  |    <slot 8> : abs(7: add)");
        }
    }

}
