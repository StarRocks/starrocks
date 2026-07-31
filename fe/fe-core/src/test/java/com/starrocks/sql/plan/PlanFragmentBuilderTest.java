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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlanFragmentBuilderTest {
    @Test
    public void testStarRocksRemoteScanOnlyKeepsStructAccessPaths() {
        StructType structType = structType(
                new StructField("f1", IntegerType.INT),
                new StructField("f2", VarcharType.VARCHAR));
        ArrayType arrayType = new ArrayType(IntegerType.INT);

        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        columns.put(new ColumnRefOperator(1, structType, "struct_col", true),
                new Column("struct_col", structType));
        columns.put(new ColumnRefOperator(2, JsonType.JSON, "json_col", true),
                new Column("json_col", JsonType.JSON));
        columns.put(new ColumnRefOperator(3, arrayType, "arr_col", true),
                new Column("arr_col", arrayType));

        ColumnAccessPath structOutputPath = rootPath("struct_col", structType,
                fieldPath("f2", VarcharType.VARCHAR));
        ColumnAccessPath structPredicatePath = rootPath("struct_col", structType,
                fieldPath("f1", IntegerType.INT));
        structPredicatePath.setFromPredicate(true);
        ColumnAccessPath jsonPath = rootPath("json_col", JsonType.JSON,
                fieldPath("age", IntegerType.BIGINT));
        jsonPath.setExtended(true);
        ColumnAccessPath arrayPath = rootPath("arr_col", arrayType,
                fieldPath("1", IntegerType.INT));

        List<ColumnAccessPath> filtered = PlanFragmentBuilder.filterStarRocksRemoteScanColumnAccessPaths(
                columns, Arrays.asList(jsonPath, structOutputPath, arrayPath, structPredicatePath));

        Assertions.assertEquals(Arrays.asList(structOutputPath, structPredicatePath), filtered);
    }

    private static StructType structType(StructField... fields) {
        return new StructType(new ArrayList<>(Arrays.asList(fields)));
    }

    private static ColumnAccessPath rootPath(String rootColumn, com.starrocks.type.Type type,
                                             ColumnAccessPath... children) {
        ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.ROOT, rootColumn, type);
        Arrays.stream(children).forEach(path::addChildPath);
        return path;
    }

    private static ColumnAccessPath fieldPath(String field, com.starrocks.type.Type type) {
        return new ColumnAccessPath(TAccessPathType.FIELD, field, type);
    }
}
