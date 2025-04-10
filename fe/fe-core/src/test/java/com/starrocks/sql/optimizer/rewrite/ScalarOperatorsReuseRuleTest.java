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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Test;

public class ScalarOperatorsReuseRuleTest extends PlanTestBase {
    @Test
    public void testRandReuse() throws Exception {
        {
            String query = "select (rnd + 1) as rnd1, (rnd + 2) as rnd2 from (select rand() as rnd) sub";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 3> : 5: rand + 1.0\n" +
                    "  |  <slot 4> : 5: rand + 2.0\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 5> : rand()");
        }

        {
            String query = "select rand() as rnd1, rand() as rnd2";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 2> : rand()\n" +
                    "  |  <slot 3> : rand()");
        }
    }

    @Test
    public void testRandomReuse() throws Exception {
        {
            String query = "select (rnd + 1) as rnd1, (rnd + 2) as rnd2 from (select random() as rnd) sub";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 3> : 5: random + 1.0\n" +
                    "  |  <slot 4> : 5: random + 2.0\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 5> : random()");
        }

        {
            String query = "select random() as rnd1, random() as rnd2";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 2> : random()\n" +
                    "  |  <slot 3> : random()");
        }

        {
            String query = "select a, b, a + b from (select random() * 1000 a, random() * 1000 b from t0) t";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 4> : 9: multiply\n" +
                    "  |  <slot 5> : 10: multiply\n" +
                    "  |  <slot 6> : 9: multiply + 10: multiply\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 7> : random()\n" +
                    "  |  <slot 8> : random()\n" +
                    "  |  <slot 9> : 7: random * 1000.0\n" +
                    "  |  <slot 10> : 8: random * 1000.0");
        }
    }

    @Test
    public void testUUIDReuse() throws Exception {
        {
            String query = "select lower(id) as id1, upper(id) as id2 from (select uuid() as id) sub";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 3> : lower(5: uuid)\n" +
                    "  |  <slot 4> : upper(5: uuid)\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 5> : uuid()");
        }

        {
            String query = "select uuid() as id1, uuid() as id2";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "1:Project\n" +
                    "  |  <slot 2> : uuid()\n" +
                    "  |  <slot 3> : uuid()");
        }
    }
}
