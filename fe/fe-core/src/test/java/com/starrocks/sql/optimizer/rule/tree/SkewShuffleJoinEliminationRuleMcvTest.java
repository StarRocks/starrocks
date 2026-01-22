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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class SkewShuffleJoinEliminationRuleMcvTest {

    @Test
    public void testSelectSkewValuesFromMcv_scalesByNonNullFraction() {
        // H=400 (histogram/non-null domain), nullFraction=0.6 => nonNullFraction=0.4
        // "1" count=200 => p_single_total=(200/400)*0.4=0.2
        // topK_total=(200+40+10)/400*0.4=0.25
        Histogram hist = new Histogram(List.of(), Map.of("1", 200L, "2", 40L, "3", 10L));

        List<ScalarOperator> skewValues = SkewShuffleJoinEliminationRule.selectSkewValuesFromMcv(
                hist,
                /*nullFraction*/ 0.6,
                /*targetType*/ IntegerType.INT,
                /*topK*/ 3,
                /*topKTotalThreshold*/ 0.2,
                /*singleTotalThreshold*/ 0.05);

        Assertions.assertEquals(1, skewValues.size());
        Assertions.assertTrue(skewValues.get(0) instanceof ConstantOperator);
        Assertions.assertEquals("1", skewValues.get(0).toString());
    }

    @Test
    public void testSelectSkewValuesFromMcv_notTriggeredWhenTopKTooSmall() {
        Histogram hist = new Histogram(List.of(), Map.of("1", 10L, "2", 10L));
        List<ScalarOperator> skewValues = SkewShuffleJoinEliminationRule.selectSkewValuesFromMcv(
                hist,
                /*nullFraction*/ 0.0,
                /*targetType*/ IntegerType.INT,
                /*topK*/ 2,
                /*topKTotalThreshold*/ 0.9,
                /*singleTotalThreshold*/ 0.05);
        Assertions.assertTrue(skewValues.isEmpty());
    }
}
