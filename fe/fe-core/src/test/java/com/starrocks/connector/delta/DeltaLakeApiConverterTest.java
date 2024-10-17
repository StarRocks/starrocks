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
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.starrocks.connector.ColumnTypeConverter.fromDeltaLakeType;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NONE;

public class DeltaLakeApiConverterTest {
    @Test
    public void testArray() {
        DataType deltaType = new io.delta.kernel.types.ArrayType(
                IntegerType.INTEGER,
                true
        );

        Type srType = fromDeltaLakeType(deltaType, COLUMN_MAPPING_MODE_NONE);
        Assert.assertEquals(srType, new ArrayType(ScalarType.createType(PrimitiveType.INT)));
    }

    @Test
    public void testUnsupported() {
        List<StructField> fields = ImmutableList.of(
                new StructField("k1", IntegerType.INTEGER, true),
                new StructField("k2", StringType.STRING, true)
        );
        DataType innerType = new io.delta.kernel.types.StructType(fields);

        DataType deltaType = new io.delta.kernel.types.MapType(
                innerType,
                innerType,
                true
        );

        Type srType = fromDeltaLakeType(deltaType, COLUMN_MAPPING_MODE_NONE);
        Assert.assertTrue(srType.isUnknown());
    }

    @Test
    public void testMap() {
        DataType deltaType = new io.delta.kernel.types.MapType(
                IntegerType.INTEGER,
                BinaryType.BINARY,
                true
        );

        Type srType = fromDeltaLakeType(deltaType, COLUMN_MAPPING_MODE_NONE);
        Assert.assertEquals(srType,
                new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.VARBINARY)));
    }

    @Test
    public void testStruct() {
        List<StructField> fields = ImmutableList.of(
                new StructField("col1", IntegerType.INTEGER, true),
                new StructField("col2", StringType.STRING, true)
        );
        DataType deltaType = new io.delta.kernel.types.StructType(fields);

        Type srType = fromDeltaLakeType(deltaType, COLUMN_MAPPING_MODE_NONE);
        Assert.assertEquals(srType, new StructType(ImmutableList.of(
                ScalarType.createType(PrimitiveType.INT),
                ScalarType.createDefaultCatalogString())));
    }
}
