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

package com.starrocks.sql.optimizer.cost;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.cost.feature.FeatureExtractor;
import com.starrocks.sql.optimizer.cost.feature.PlanFeatures;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PlanFeaturesTest extends PlanTestBase {

    @Test
    public void testBasic() throws Exception {
        ExecPlan execPlan = getExecPlan("select count(*) from t0 where v1 < 100 limit 100");
        OptExpression physicalPlan = execPlan.getPhysicalPlan();
        PlanFeatures planFeatures = FeatureExtractor.extractFeatures(physicalPlan);
        String string = planFeatures.toFeatureString();
        Assertions.assertTrue(string.startsWith("tables=[0,0,10003]"), string);
        Assertions.assertTrue(string.contains("38,0,0,0,0,0"), string);
        Assertions.assertTrue(string.contains("40,1,1,8,9,100"), string);
        Assertions.assertTrue(string.contains("44,1,1,9,9,0,100,50,1,1"), string);
    }

}