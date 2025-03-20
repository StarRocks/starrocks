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

package com.starrocks.statistic.sample;

import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PrimitiveTypeColumnStatsTest extends PlanTestBase {

    @ParameterizedTest
    @CsvSource(delimiterString = "|", value = {
            "DUJ1|IFNULL(SUM(t1.count) * COUNT(1) / (SUM(t1.count) - SUM(IF(t1.count = 1, 1, 0)) + SUM(IF(t1.count = " +
                    "1, 1, 0)) * 0.01), COUNT(1))",
            "LINEAR|COUNT(1) / 0.01",
            "POLYNOMIAL|COUNT(1) / 0.029700999999999977",
            "INVALID|IFNULL(SUM(t1.count) * COUNT(1) / (SUM(t1.count) - SUM(IF(t1.count = 1, 1, 0)) + SUM(IF(t1.count" +
                    " = 1, 1, 0)) * 0.01), COUNT(1))",
            "GEE|IFNULL(COUNT(1) + 9 * SUM(IF(t1.count = 1, 1, 0)), COUNT(1))"
    })
    public void getDistinctCount(String estimator, String expectedQuery) {
        PrimitiveTypeColumnStats c1 = new PrimitiveTypeColumnStats("c1", Type.CHAR);
        final double sampleRatio = 0.01;

        Config.statistics_sample_ndv_estimator = estimator;
        String query = c1.getDistinctCount(sampleRatio);
        Assertions.assertEquals(expectedQuery, query);
    }
}