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

package com.starrocks.connector.starrocks;

import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StarRocksRemotePredicateSerializerTest {
    @Test
    public void testStructSubfieldPredicateSerializesToPushdownSql() {
        StructType structType = new StructType(new ArrayList<>(
                Collections.singletonList(new StructField("f1", IntegerType.INT))));
        ColumnRefOperator structRef = new ColumnRefOperator(1, structType, "struct_col", true);
        SubfieldOperator subfield = new SubfieldOperator(structRef, IntegerType.INT, Collections.singletonList("f1"));
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, subfield, ConstantOperator.createInt(10));
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(structRef, new Column("struct_col", structType, true));

        StarRocksRemotePredicateSerializer.Result result =
                StarRocksRemotePredicateSerializer.serialize(predicate, columns);

        Assertions.assertNotNull(result.getPushdownSql());
        Assertions.assertTrue(result.getPushdownSql().contains("`struct_col`.`f1`"), result.getPushdownSql());
        Assertions.assertTrue(result.getPushdownSql().contains("10"), result.getPushdownSql());
        Assertions.assertTrue(result.getResidualPredicates().isEmpty(), result.getResidualPredicates().toString());
        Assertions.assertTrue(result.getUnsupportedReasons().isEmpty(), result.getUnsupportedReasons().toString());
    }

    @Test
    public void testToSqlFailureReasonIncludesExceptionClass() {
        StructType structType = new StructType(new ArrayList<>(
                Collections.singletonList(new StructField("f1", IntegerType.INT))));
        ColumnRefOperator structRef = new ColumnRefOperator(1, structType, "struct_col", true);
        SubfieldOperator subfield = new SubfieldOperator(structRef, IntegerType.INT, Collections.singletonList("f1"));
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, subfield, ConstantOperator.createInt(10));
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(structRef, new Column("struct_col", VarcharType.VARCHAR, true));

        StarRocksRemotePredicateSerializer.Result result =
                StarRocksRemotePredicateSerializer.serialize(predicate, columns);

        Assertions.assertNull(result.getPushdownSql());
        Assertions.assertEquals(1, result.getResidualPredicates().size());
        Assertions.assertEquals(1, result.getUnsupportedReasons().size());
        Assertions.assertTrue(result.getUnsupportedReasons().get(0).contains("java.lang.IllegalArgumentException"),
                result.getUnsupportedReasons().toString());
    }
}
