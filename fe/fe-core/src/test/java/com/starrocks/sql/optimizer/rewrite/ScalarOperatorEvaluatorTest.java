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
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScalarOperatorEvaluatorTest {
    @Test
    public void evaluationNotConstant() {
        CallOperator operator = new CallOperator(FunctionSet.IFNULL, Type.INT,
                Lists.newArrayList(new ColumnRefOperator(1, Type.INT, "test", true), ConstantOperator.createInt(2)));

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

    @Test
    public void evaluationNull() {
        CallOperator operator = new CallOperator(FunctionSet.CONCAT, Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createNull(Type.VARCHAR)));

        Function fn =
                new Function(new FunctionName(FunctionSet.CONCAT), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertTrue(((ConstantOperator) result).isNull());
    }

    @Test
    public void evaluationArrayArgs() {
        CallOperator operator = new CallOperator(FunctionSet.CONCAT, Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createVarchar("123")));

        Function fn =
                new Function(new FunctionName(FunctionSet.CONCAT), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertEquals("test123", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void evaluationFromUtc() {
        CallOperator operator = new CallOperator(FunctionSet.STR_TO_DATE, Type.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("2003-10-11 23:56:25"),
                ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")
        ));

        Function fn =
                new Function(new FunctionName(FunctionSet.STR_TO_DATE), new Type[] {Type.VARCHAR, Type.VARCHAR},
                        Type.DATETIME,
                        false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(LocalDateTime.of(2003, 10, 11, 23, 56, 25), ((ConstantOperator) result).getDatetime());
    }

    @Test
    public void evaluationNonNullableFunc() {
        CallOperator operator = new CallOperator(FunctionSet.BITMAP_COUNT, Type.BIGINT,
                Lists.newArrayList(ConstantOperator.createNull(Type.BITMAP)));

        Function fn =
                new Function(new FunctionName(FunctionSet.BITMAP_COUNT), new Type[] {Type.BITMAP}, Type.BIGINT, false);
        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

}