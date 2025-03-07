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

package com.starrocks.connector.kudu;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.catalog.KuduTableTest.genColumnSchema;
import static com.starrocks.catalog.ScalarType.CATALOG_MAX_VARCHAR_LENGTH;

public class KuduColumnConverterTest {

    @Test
    public void testConvertBoolean() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.BOOL);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.BOOLEAN);
    }

    @Test
    public void testConvertTinyint() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.INT8);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.TINYINT);
    }

    @Test
    public void testConvertSmallint() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.INT16);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.SMALLINT);
    }

    @Test
    public void testConvertInt() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.INT32);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.INT);
    }

    @Test
    public void testConvertBigint() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.INT64);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.BIGINT);
    }

    @Test
    public void testConvertFloat() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.FLOAT);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.FLOAT);
    }

    @Test
    public void testConvertDouble() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.DOUBLE);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.DOUBLE);
    }

    @Test
    public void testConvertDate() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.DATE);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.DATE);
    }

    @Test
    public void testConvertDatetime() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.UNIXTIME_MICROS);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.DATETIME);
    }

    @Test
    public void testConvertString() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.STRING);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, ScalarType.createVarcharType(CATALOG_MAX_VARCHAR_LENGTH));
    }

    @Test
    public void testConvertVarchar() {
        int len = 11;
        ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("test", org.apache.kudu.Type.VARCHAR)
                        .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(len).build())
                        .build();
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, ScalarType.createVarcharType(len));
    }

    @Test
    public void testConvertBinary() {
        ColumnSchema columnSchema = genColumnSchema("test", org.apache.kudu.Type.BINARY);
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, Type.VARBINARY);
    }

    @Test
    public void testConvertDecimal() {
        int p = 10;
        int s = 2;
        ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("test", org.apache.kudu.Type.DECIMAL)
                .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(p).scale(s).build())
                .build();
        Type result = ColumnTypeConverter.fromKuduType(columnSchema);
        Assert.assertEquals(result, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, p, s));
    }
}
