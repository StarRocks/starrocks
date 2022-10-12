// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.external.hive.RemoteFileInputFormat;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.external.iceberg.IcebergUtil.convertColumnType;
import static com.starrocks.external.iceberg.IcebergUtil.convertIcebergPartitionToPartitionName;

public class IcebergUtilTest {

    @Test
    public void testGetIcebergTableIdentifier() {
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier("database", "table");
        Assert.assertTrue(identifier.toString().equals("database.table"));
    }

    @Test
    public void testGetHdfsFileFormat() {
        RemoteFileInputFormat fileFormat = IcebergUtil.getHdfsFileFormat(FileFormat.PARQUET);
        Assert.assertTrue(fileFormat.equals(RemoteFileInputFormat.PARQUET));
        Assert.assertThrows("Unexpected file format: %s", StarRocksIcebergException.class, () -> {
            IcebergUtil.getHdfsFileFormat(FileFormat.AVRO);
        });
    }

    @Test
    public void testGetIcebergHiveCatalog() {
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergCatalog catalog = IcebergUtil.getIcebergHiveCatalog("thrift://test:9030", icebergProperties);
        Assert.assertTrue(catalog instanceof IcebergHiveCatalog);
    }

    @Test
    public void testDecimal() {
        int precision = 9;
        int scale = 5;
        Type decimalType = ScalarType.createUnifiedDecimalType(precision, scale);
        org.apache.iceberg.types.Type icebergType = Types.DecimalType.of(precision, scale);
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, decimalType);
    }

    @Test
    public void testString() {
        Type stringType = ScalarType.createDefaultString();
        org.apache.iceberg.types.Type icebergType = Types.StringType.get();
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, stringType);
    }

    @Test
    public void testArray() {
        Assert.assertEquals(convertColumnType(Types.ListType.ofRequired(136, Types.IntegerType.get())),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertEquals(convertColumnType(Types.ListType.ofRequired(136,
                        Types.ListType.ofRequired(136, Types.IntegerType.get()))),
                new ArrayType(new ArrayType(ScalarType.createType(PrimitiveType.INT))));
    }

    @Test
    public void testUnsupported() {
        org.apache.iceberg.types.Type icebergType = Types.MapType.ofRequired(1, 2,
                Types.StringType.get(), Types.StringType.get());
        Type resType = convertColumnType(icebergType);
        Assert.assertEquals(resType, ScalarType.createType(PrimitiveType.UNKNOWN_TYPE));
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
