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

import com.google.common.base.Splitter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.cost.feature.FeatureExtractor;
import com.starrocks.sql.optimizer.cost.feature.PlanFeatures;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PlanFeaturesTest extends PlanTestBase {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "select count(*) from t0 where v1 < 100 limit 100 " +
                    "| tables=[0,0,10003] " +
                    "| 38,0,0,0,0,0;40,1,1,8,9,100;44,1,1,9,9,0,100,50,1,1",
            "select max(v1) from t0 where v1 < 100 limit 100" +
                    "|tables=[0,0,10003] " +
                    "|38,0,0,0,0,0;40,1,1,8,8,100;44,1,1,8,8,0,100,50,1,1",
            "select v1, count(*) from t0 group by v1 " +
                    "| tables=[0,0,10003] " +
                    "| 38,0,0,0,0,0;,40,1,1,16,8,0;44,1,1,8,8,0,100,50,0,0",
            "select count(*) from t0 a join t0 b on a.v1 = b.v2" +
                    "| tables=[0,0,10003] " +
                    "| 38,0,0,0,0,0;39,2,2,8,16,0;,40,2,2,8,9,0,2,0,2,2;44,2,2,16,16,0,200,100,2,0",

    })
    public void testBasic(String query, String expectedTables, String expected) throws Exception {
        expectedTables = StringUtils.trim(expectedTables);
        expected = StringUtils.trim(expected);

        ExecPlan execPlan = getExecPlan(query);
        OptExpression physicalPlan = execPlan.getPhysicalPlan();
        PlanFeatures planFeatures = FeatureExtractor.extractFeatures(physicalPlan);
        String string = planFeatures.toFeatureString();
        Assertions.assertTrue(string.startsWith(expectedTables), string);
        Splitter.on(";").splitToList(expected).forEach(slice -> {
            Assertions.assertTrue(string.contains(slice), string);
        });
    }

}