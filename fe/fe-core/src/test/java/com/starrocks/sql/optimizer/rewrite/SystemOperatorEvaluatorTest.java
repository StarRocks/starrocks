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

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.SystemFunction;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.function.CboStatsShowExclusion;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SystemOperatorEvaluatorTest {
    @Test
    public void evaluationNotConstant() {
        try {
            CallOperator operator = new CallOperator(FunctionSet.CBO_STATS_ADD_EXCLUSION, Type.VARCHAR,
                    Lists.newArrayList(new ColumnRefOperator(1, Type.INT, "test", true)));
            SystemOperatorEvaluator.INSTANCE.evaluation(operator);
        } catch (StarRocksPlannerException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("System Function's args does't match."));
        } catch (Exception e) {
            Assert.fail("evaluation exception: " + e);
        }
    }

    @Test
    public void evaluationFunctionNull() {
        try {
            CallOperator operator = new CallOperator(FunctionSet.CBO_STATS_ADD_EXCLUSION, Type.VARCHAR,
                    Lists.newArrayList(ConstantOperator.createNull(Type.VARCHAR)));
            SystemOperatorEvaluator.INSTANCE.evaluation(operator);
        } catch (StarRocksPlannerException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().
                    contains("No matching system function: system$cbo_stats_add_exclusion"));
        } catch (Exception e) {
            Assert.fail("evaluation exception: " + e);
        }
    }

    @Test
    public void evaluationParamNull() {
        SystemFunction fn = SystemFunction.createSystemFunctionBuiltin(CboStatsShowExclusion.FN_NAME,
                new ArrayList<Type>((Arrays.asList(Type.VARCHAR))), false, Type.VARCHAR);

        CallOperator operator = new CallOperator(FunctionSet.CBO_STATS_ADD_EXCLUSION, Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createNull(Type.VARCHAR)), fn);

        ScalarOperator result = SystemOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertTrue(((ConstantOperator) result).isNull());
    }
}
