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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.gson.Gson;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ScalarOperatorSerializerTest {
    private static final Gson GSON = new Gson();

    @Test
    public void testNormalizesReversedBinaryPredicate() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.LT, ConstantOperator.createInt(10), column);

        Assertions.assertEquals(
                "{\"o\":\"b\",\"b\":\"GT\",\"c\":[{\"o\":\"cr\",\"t\":\"int\",\"n\":\"k\"},"
                        + "{\"o\":\"co\",\"t\":\"int\",\"v\":10}]}",
                GSON.toJson(ScalarOperatorSerializer.toJson(predicate)));
    }

    @Test
    public void testScalarFamilies() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        InPredicateOperator in = new InPredicateOperator(true, column,
                ConstantOperator.createInt(1), ConstantOperator.createInt(2));
        IsNullPredicateOperator isNull = new IsNullPredicateOperator(false, column);
        CallOperator startsWith = new CallOperator(FunctionSet.STARTS_WITH, BooleanType.BOOLEAN,
                List.of(new ColumnRefOperator(2, com.starrocks.type.VarcharType.VARCHAR, "s", true),
                        ConstantOperator.createVarchar("x")));

        String json = GSON.toJson(ScalarOperatorSerializer.toJson(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, in,
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                isNull, startsWith))));
        Assertions.assertTrue(json.contains("\"o\":\"ip\""));
        Assertions.assertTrue(json.contains("\"ng\":true"));
        Assertions.assertTrue(json.contains("\"o\":\"isn\""));
        Assertions.assertTrue(json.contains("\"f\":\"starts_with\""));
    }

    @Test
    public void testVectorCallNormalizesColumnFirstAndSerializesArray() {
        ColumnRefOperator vector = new ColumnRefOperator(2, ArrayType.ARRAY_FLOAT, "embedding", true);
        ArrayOperator query = new ArrayOperator(ArrayType.ARRAY_FLOAT, false,
                List.of(ConstantOperator.createFloat(1.25), ConstantOperator.createFloat(2.5)));
        CallOperator cosine = new CallOperator(FunctionSet.APPROX_COSINE_SIMILARITY, FloatType.FLOAT,
                List.of(query, vector));

        Map<String, Object> serialized = ScalarOperatorSerializer.toJson(cosine);
        List<?> arguments = (List<?>) serialized.get(ScalarOperatorSerializer.ARGUMENTS);
        Map<?, ?> column = (Map<?, ?>) arguments.get(0);
        Map<?, ?> array = (Map<?, ?>) arguments.get(1);

        Assertions.assertEquals(FunctionSet.APPROX_COSINE_SIMILARITY,
                serialized.get(ScalarOperatorSerializer.FN_NAME));
        Assertions.assertEquals("embedding", column.get(ScalarOperatorSerializer.NAME));
        Assertions.assertEquals("array<float>", column.get(ScalarOperatorSerializer.TYPE));
        Assertions.assertEquals("float", array.get(ScalarOperatorSerializer.ITEM_TYPE));
        Assertions.assertEquals(2, ((List<?>) array.get(ScalarOperatorSerializer.CHILDREN)).size());
    }
}
