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

package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.connector.ColumnTypeConverter.fromDeltaLakeType;

public class DeltaLakeApiConverterTest {
    @Test
    public void testArray() {
        io.delta.standalone.types.DataType deltaType = new io.delta.standalone.types.ArrayType(
                new io.delta.standalone.types.IntegerType(),
                true
        );

        Type srType = fromDeltaLakeType(deltaType);
        Assert.assertEquals(srType, new ArrayType(ScalarType.createType(PrimitiveType.INT)));
    }

    @Test
    public void testUnsupported() {
        io.delta.standalone.types.StructField[] fields = {
                new io.delta.standalone.types.StructField("k1", new io.delta.standalone.types.IntegerType()),
                new io.delta.standalone.types.StructField("k2", new io.delta.standalone.types.StringType())
        };
        io.delta.standalone.types.DataType innerType = new io.delta.standalone.types.StructType(fields);

        io.delta.standalone.types.DataType deltaType = new io.delta.standalone.types.MapType(
                innerType,
                innerType,
                true
        );

        Type srType = fromDeltaLakeType(deltaType);
        Assert.assertTrue(srType.isUnknown());
    }

    @Test
    public void testMap() {
        io.delta.standalone.types.DataType deltaType = new io.delta.standalone.types.MapType(
                new io.delta.standalone.types.IntegerType(),
                new io.delta.standalone.types.BinaryType(),
                true
        );

        Type srType = fromDeltaLakeType(deltaType);
        Assert.assertEquals(srType,
                new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.VARBINARY)));
    }

    @Test
    public void testStruct() {
        io.delta.standalone.types.StructField[] fields = {
                new io.delta.standalone.types.StructField("col1", new io.delta.standalone.types.IntegerType()),
                new io.delta.standalone.types.StructField("col2", new io.delta.standalone.types.NullType())
        };
        io.delta.standalone.types.DataType deltaType = new io.delta.standalone.types.StructType(fields);

        Type srType = fromDeltaLakeType(deltaType);
        Assert.assertEquals(srType, new StructType(ImmutableList.of(
                ScalarType.createType(PrimitiveType.INT),
                ScalarType.createType(PrimitiveType.NULL_TYPE))));
    }
}
