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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PlanFeaturesTest extends PlanTestBase {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "select count(*) from t0 where v1 < 100 limit 100 " +
                    "| tables=[0,0,10003] " +
                    "| 39,1,0,8,0,2,0,3;40,1,0,8,2,2,4,0,0,1,1;44,1,0,9,0,2,0,0,0,1,1",
            "select max(v1) from t0 where v1 < 100 limit 100" +
                    "|tables=[0,0,10003] " +
                    "| [39,1,0,8,0,2,0,3;40,1,0,8,2,2,4,0,0,1,1;44,1,0,8,0,2,0,0,0,1,1",
            "select v1, count(*) from t0 group by v1 " +
                    "| tables=[0,0,10003] " +
                    "| 40,1,0,16,2,2,0,0,1,1,1;44,1,0,8,0,2,0,0,0,0,0",
            "select count(*) from t0 a join t0 b on a.v1 = b.v2" +
                    "| tables=[0,0,10003] " +
                    "| 39,2,0,16,2,4,0,4;40,2,0,16,2,2,0,0,0,2,2;44,2,0,16,0,4,0,0,0,2,0",

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
            Assertions.assertTrue(string.contains(slice), "slice is " + slice + ", feature is " + string);
        });
    }

    @Test
    public void testHeader() {
        String header = PlanFeatures.featuresHeader();
        Assertions.assertEquals("""
                        tables_0,tables_1,tables_2,env_0,env_1,env_2,var_0,operators_0,\
                        operators_1,operators_2,operators_3,operators_4,operators_5,operators_6,operators_7,\
                        operators_8,operators_9,operators_10,operators_11,operators_12,operators_13,operators_14,\
                        operators_15,operators_16,operators_17,operators_18,operators_19,operators_20,operators_21,\
                        operators_22,operators_23,operators_24,operators_25,operators_26,operators_27,operators_28,\
                        operators_29,operators_30,operators_31,operators_32,operators_33,operators_34,operators_35,\
                        operators_36,operators_37,operators_38,operators_39,operators_40,operators_41,operators_42,\
                        operators_43,operators_44,operators_45,operators_46,operators_47,operators_48,operators_49,\
                        operators_50,operators_51,operators_52,operators_53,operators_54,operators_55,operators_56,\
                        operators_57,operators_58,operators_59,operators_60,operators_61,operators_62,operators_63,\
                        operators_64,operators_65,operators_66,operators_67,operators_68,operators_69,operators_70,\
                        operators_71,operators_72,operators_73,operators_74,operators_75,operators_76,operators_77,\
                        operators_78,operators_79,operators_80,operators_81,operators_82,operators_83,operators_84,\
                        operators_85,operators_86,operators_87,operators_88,operators_89,operators_90,operators_91,\
                        operators_92,operators_93,operators_94,operators_95,operators_96,operators_97,operators_98,operators_99,operators_100,operators_101,operators_102,operators_103,operators_104,operators_105,operators_106,operators_107,operators_108,operators_109,operators_110,operators_111,operators_112,operators_113,operators_114,operators_115,operators_116,operators_117,operators_118,operators_119,operators_120,operators_121,operators_122,operators_123,operators_124,operators_125,operators_126""",
                header);
    }

}