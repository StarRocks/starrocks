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


package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;

public class IcebergApiConverterTest {

    @Test
    public void testGetHdfsFileFormat() {
        RemoteFileInputFormat fileFormat = IcebergApiConverter.getHdfsFileFormat(FileFormat.PARQUET);
        Assert.assertTrue(fileFormat.equals(RemoteFileInputFormat.PARQUET));
        Assert.assertThrows("Unexpected file format: %s", StarRocksConnectorException.class, () -> {
            IcebergApiConverter.getHdfsFileFormat(FileFormat.AVRO);
        });
    }

    @Test
    public void testDecimal() {
        int precision = 9;
        int scale = 5;
        Type decimalType = ScalarType.createUnifiedDecimalType(precision, scale);
        org.apache.iceberg.types.Type icebergType = Types.DecimalType.of(precision, scale);
        Type resType = fromIcebergType(icebergType);
        Assert.assertEquals(resType, decimalType);
    }

    @Test
    public void testString() {
        Type stringType = ScalarType.createDefaultExternalTableString();
        org.apache.iceberg.types.Type icebergType = Types.StringType.get();
        Type resType = fromIcebergType(icebergType);
        Assert.assertEquals(resType, stringType);
    }

    @Test
    public void testArray() {
        Assert.assertEquals(fromIcebergType(Types.ListType.ofRequired(136, Types.IntegerType.get())),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertEquals(fromIcebergType(Types.ListType.ofRequired(136,
                        Types.ListType.ofRequired(136, Types.IntegerType.get()))),
                new ArrayType(new ArrayType(ScalarType.createType(PrimitiveType.INT))));
    }

    @Test
    public void testUnsupported() {
        org.apache.iceberg.types.Type icebergType = Types.MapType.ofRequired(1, 2,
                Types.ListType.ofRequired(136, Types.IntegerType.get()), Types.StringType.get());
        Type resType = fromIcebergType(icebergType);
        Assert.assertTrue(resType.isUnknown());

        org.apache.iceberg.types.Type keyUnknownMapType = Types.MapType.ofRequired(1, 2,
                Types.TimeType.get(), Types.StringType.get());
        Type resKeyUnknowType = fromIcebergType(keyUnknownMapType);
        Assert.assertTrue(resKeyUnknowType.isUnknown());

        org.apache.iceberg.types.Type valueUnknownMapType = Types.MapType.ofRequired(1, 2,
                Types.StringType.get(), Types.TimeType.get());
        Type resValueUnknowType = fromIcebergType(valueUnknownMapType);
        Assert.assertTrue(resValueUnknowType.isUnknown());

        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.optional(1, "a", Types.IntegerType.get()));
        fields.add(Types.NestedField.required(1, "b", Types.TimeType.get()));
        org.apache.iceberg.types.Type unknownSubfieldStructType = Types.StructType.of(fields);
        Type unknownStructType = fromIcebergType(unknownSubfieldStructType);
        Assert.assertTrue(unknownStructType.isUnknown());
    }

    @Test
    public void testMap() {
        org.apache.iceberg.types.Type icebergType = Types.MapType.ofRequired(1, 2,
                Types.StringType.get(), Types.IntegerType.get());
        Type resType = fromIcebergType(icebergType);
        Assert.assertEquals(resType,
                new MapType(ScalarType.createDefaultExternalTableString(), ScalarType.createType(PrimitiveType.INT)));
    }

    @Test
    public void testStruct() {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.optional(1, "a", Types.IntegerType.get()));
        fields.add(Types.NestedField.required(1, "b", Types.StringType.get()));
        org.apache.iceberg.types.Type icebergType = Types.StructType.of(fields);
        Type resType = fromIcebergType(icebergType);
        Assert.assertTrue(resType.isStructType());
    }

    @Test
    public void testIdentityPartitionNames() {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional(1, "id", new Types.IntegerType()));
        fields.add(Types.NestedField.optional(2, "ts", new Types.DateType()));
        fields.add(Types.NestedField.optional(3, "data", new Types.StringType()));

        Schema schema = new Schema(fields);
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        PartitionSpec partitionSpec = builder.identity("ts").build();
        String partitionName = convertIcebergPartitionToPartitionName(partitionSpec, DataFiles.data(partitionSpec,
                "ts=2022-08-01"));
        Assert.assertEquals("ts=2022-08-01", partitionName);

        builder = PartitionSpec.builderFor(schema);
        partitionSpec = builder.identity("id").identity("ts").build();
        partitionName = convertIcebergPartitionToPartitionName(partitionSpec, DataFiles.data(partitionSpec,
                "id=1/ts=2022-08-01"));
        Assert.assertEquals("id=1/ts=2022-08-01", partitionName);
    }
}
