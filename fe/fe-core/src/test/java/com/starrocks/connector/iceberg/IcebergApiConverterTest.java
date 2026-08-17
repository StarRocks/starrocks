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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import com.starrocks.type.VariantType;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergApiConverterTest {

    @Test
    public void testGetHdfsFileFormat() {
        RemoteFileInputFormat fileFormat = IcebergApiConverter.getHdfsFileFormat(FileFormat.PARQUET);
        assertTrue(fileFormat.equals(RemoteFileInputFormat.PARQUET));
        assertThrows(StarRocksConnectorException.class, () -> IcebergApiConverter.getHdfsFileFormat(FileFormat.AVRO),
                "Unexpected file format: %s");
    }

    @Test
    public void testDecimal() {
        int precision = 9;
        int scale = 5;
        org.apache.iceberg.types.Type icebergType = Types.DecimalType.of(precision, scale);
        Type resType = fromIcebergType(icebergType);
        assertEquals(resType, TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, scale));
        resType = fromIcebergType(Types.DecimalType.of(10, scale));
        assertEquals(resType, TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, scale));
        resType = fromIcebergType(Types.DecimalType.of(19, scale));
        assertEquals(resType, TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, scale));
    }

    @Test
    public void testString() {
        Type stringType = TypeFactory.createDefaultCatalogString();
        org.apache.iceberg.types.Type icebergType = Types.StringType.get();
        Type resType = fromIcebergType(icebergType);
        assertEquals(resType, stringType);
    }

    @Test
    public void testUUID() {
        org.apache.iceberg.types.Type icebergType = Types.UUIDType.get();
        Type resType = fromIcebergType(icebergType);
        assertTrue(resType.isStringType());
        assertEquals(resType, TypeFactory.createVarcharType(36));
    }

    @Test
    public void testArray() {
        assertEquals(fromIcebergType(Types.ListType.ofRequired(136, Types.IntegerType.get())),
                new ArrayType(IntegerType.INT));
        assertEquals(fromIcebergType(Types.ListType.ofRequired(136,
                        Types.ListType.ofRequired(136, Types.IntegerType.get()))),
                new ArrayType(new ArrayType(IntegerType.INT)));
    }

    @Test
    public void testVariant() {
        Type variantType = fromIcebergType(Types.VariantType.get());
        Assertions.assertTrue(variantType.isVariantType());
        Assertions.assertEquals(VariantType.VARIANT, variantType);
    }

    @Test
    public void testUnsupported() {
        org.apache.iceberg.types.Type icebergType = Types.MapType.ofRequired(1, 2,
                Types.ListType.ofRequired(136, Types.IntegerType.get()), Types.StringType.get());
        Type resType = fromIcebergType(icebergType);
        assertTrue(resType.isUnknown());

        org.apache.iceberg.types.Type keyUnknownMapType = Types.MapType.ofRequired(1, 2,
                Types.FixedType.ofLength(1), Types.StringType.get());
        Type resKeyUnknowType = fromIcebergType(keyUnknownMapType);
        assertTrue(resKeyUnknowType.isUnknown());

        org.apache.iceberg.types.Type valueUnknownMapType = Types.MapType.ofRequired(1, 2,
                Types.StringType.get(), Types.FixedType.ofLength(1));
        Type resValueUnknowType = fromIcebergType(valueUnknownMapType);
        assertTrue(resValueUnknowType.isUnknown());

        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.optional(1, "a", Types.IntegerType.get()));
        fields.add(Types.NestedField.required(1, "b", Types.FixedType.ofLength(1)));
        org.apache.iceberg.types.Type unknownSubfieldStructType = Types.StructType.of(fields);
        Type unknownStructType = fromIcebergType(unknownSubfieldStructType);
        assertTrue(unknownStructType.isUnknown());
    }

    @Test
    public void testMap() {
        org.apache.iceberg.types.Type icebergType = Types.MapType.ofRequired(1, 2,
                Types.StringType.get(), Types.IntegerType.get());
        Type resType = fromIcebergType(icebergType);
        assertEquals(resType,
                new MapType(TypeFactory.createDefaultCatalogString(), IntegerType.INT));
    }

    @Test
    public void testStruct() {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.optional(1, "a", Types.IntegerType.get()));
        fields.add(Types.NestedField.required(1, "b", Types.StringType.get()));
        org.apache.iceberg.types.Type icebergType = Types.StructType.of(fields);
        Type resType = fromIcebergType(icebergType);
        assertTrue(resType.isStructType());
    }

    @Test
    public void testIdentityPartitionNames() throws Exception {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional(1, "id", new Types.IntegerType()));
        fields.add(Types.NestedField.optional(2, "dt", new Types.DateType()));
        fields.add(Types.NestedField.optional(3, "data", new Types.StringType()));

        Schema schema = new Schema(fields);
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        PartitionSpec partitionSpec = builder.identity("dt").build();
        org.apache.iceberg.Table table = mock(org.apache.iceberg.Table.class);
        when(table.spec()).thenReturn(partitionSpec);
        when(table.schema()).thenReturn(schema);
        try (MockedStatic<Partitioning> mockedStatic = Mockito.mockStatic(Partitioning.class)) {
            mockedStatic.when(() -> Partitioning.partitionType(table)).thenReturn(partitionSpec.partitionType());

            StructLike partitionData = DataFiles.data(partitionSpec, "dt=2022-08-01");
            StructProjection partition = StructProjection.create(schema, schema);
            partition.wrap(partitionData);
            String partitionName = convertIcebergPartitionToPartitionName(table, partitionSpec, partition);
            assertEquals("dt=2022-08-01", partitionName);
        }

        // Second test case
        builder = PartitionSpec.builderFor(schema);
        partitionSpec = builder.identity("id").identity("dt").build();
        when(table.spec()).thenReturn(partitionSpec);
        try (MockedStatic<Partitioning> mockedStatic = Mockito.mockStatic(Partitioning.class)) {
            mockedStatic.when(() -> Partitioning.partitionType(table)).thenReturn(partitionSpec.partitionType());

            StructLike partitionData = DataFiles.data(partitionSpec, "id=1/dt=2022-08-01");
            StructProjection partition = StructProjection.create(schema, schema);
            partition.wrap(partitionData);
            String partitionName = convertIcebergPartitionToPartitionName(table, partitionSpec, partition);
            assertEquals("id=1/dt=2022-08-01", partitionName);
        }
    }

    @Test
    public void testNonIdentityPartitionNames() throws Exception {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional(1, "id", new Types.IntegerType()));
        fields.add(Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));
        fields.add(Types.NestedField.optional(3, "data", new Types.StringType()));

        Schema schema = new Schema(fields);
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        PartitionSpec partitionSpec = builder.hour("ts").build();
        org.apache.iceberg.Table table = mock(org.apache.iceberg.Table.class);
        when(table.spec()).thenReturn(partitionSpec);
        when(table.schema()).thenReturn(schema);

        try (MockedStatic<Partitioning> mockedStatic = Mockito.mockStatic(Partitioning.class)) {
            mockedStatic.when(() -> Partitioning.partitionType(table)).thenReturn(partitionSpec.partitionType());

            StructLike partitionData = DataFiles.data(partitionSpec, "ts_hour=62255");
            StructProjection partition = StructProjection.create(schema, schema);
            partition.wrap(partitionData);
            String partitionName = convertIcebergPartitionToPartitionName(table, partitionSpec, partition);
            assertEquals("ts_hour=1977-02-06-23", partitionName);
        }

        // Second test case
        builder = PartitionSpec.builderFor(schema);
        partitionSpec = builder.hour("ts").truncate("data", 2).build();
        when(table.spec()).thenReturn(partitionSpec);

        try (MockedStatic<Partitioning> mockedStatic = Mockito.mockStatic(Partitioning.class)) {
            mockedStatic.when(() -> Partitioning.partitionType(table)).thenReturn(partitionSpec.partitionType());

            StructLike partitionData = DataFiles.data(partitionSpec, "ts_hour=365/data_trunc=xy");
            StructProjection partition = StructProjection.create(schema, schema);
            partition.wrap(partitionData);
            String partitionName = convertIcebergPartitionToPartitionName(table, partitionSpec, partition);
            assertEquals("ts_hour=1970-01-16-05/data_trunc=xy", partitionName);
        }
    }

    @Test
    public void testToIcebergApiSchema() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", BooleanType.BOOLEAN));
        columns.add(new Column("c2", IntegerType.INT));
        columns.add(new Column("c3", IntegerType.BIGINT));
        columns.add(new Column("c4", FloatType.FLOAT));
        columns.add(new Column("c5", FloatType.DOUBLE));
        columns.add(new Column("c6", DateType.DATE));
        columns.add(new Column("c7", DateType.DATETIME));
        columns.add(new Column("c8", VarcharType.VARCHAR));
        columns.add(new Column("c9", CharType.CHAR));
        columns.add(new Column("c10", DecimalType.DECIMAL32));
        columns.add(new Column("c11", DecimalType.DECIMAL64));
        columns.add(new Column("c12", DecimalType.DECIMAL128));
        columns.add(new Column("c13", new ArrayType(IntegerType.INT)));
        columns.add(new Column("c14", new MapType(IntegerType.INT, IntegerType.INT)));
        columns.add(new Column("c15", new StructType(ImmutableList.of(IntegerType.INT))));
        columns.add(new Column("c16", DateType.TIME));
        columns.add(new Column("c17", VariantType.VARIANT));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);
        assertEquals("table {\n" +
                "  1: c1: required boolean\n" +
                "  2: c2: required int\n" +
                "  3: c3: required long\n" +
                "  4: c4: required float\n" +
                "  5: c5: required double\n" +
                "  6: c6: required date\n" +
                "  7: c7: required timestamp\n" +
                "  8: c8: required string\n" +
                "  9: c9: required string\n" +
                "  10: c10: required decimal(-1, -1)\n" +
                "  11: c11: required decimal(-1, -1)\n" +
                "  12: c12: required decimal(-1, -1)\n" +
                "  13: c13: required list<int>\n" +
                "  14: c14: required map<int, int>\n" +
                "  15: c15: required struct<21: col1: optional int>\n" +
                "  16: c16: required time\n" +
                "  17: c17: required variant\n" +
                "}", schema.toString());
        ListPartitionDesc partDesc = new ListPartitionDesc(Lists.newArrayList("c1"), null);
        PartitionSpec spec = IcebergApiConverter.parsePartitionFields(schema, partDesc);
        assertTrue(spec.isPartitioned());
        assertEquals(1, spec.fields().size());
    }

    @Test
    public void testRebuildCreateTableProperties() {
        Map<String, String> source = ImmutableMap.of("file_format", "orc");
        Map<String, String> target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("orc", target.get(DEFAULT_FILE_FORMAT));

        source = ImmutableMap.of("file_format", "orc", "compression_codec", "snappy");
        target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("snappy", target.get(ORC_COMPRESSION));

        source = ImmutableMap.of("file_format", "parquet", "compression_codec", "snappy");
        target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("snappy", target.get(PARQUET_COMPRESSION));

        source = ImmutableMap.of("file_format", "avro", "compression_codec", "zstd");
        target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("zstd", target.get(AVRO_COMPRESSION));

        source = ImmutableMap.of("write.format.default", "orc");
        target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("orc", target.get(DEFAULT_FILE_FORMAT));

        source = ImmutableMap.of("write.format.default", "parquet", "compression_codec", "snappy");
        target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("parquet", target.get(DEFAULT_FILE_FORMAT));
        assertEquals("snappy", target.get(PARQUET_COMPRESSION));

        source = ImmutableMap.of("write.format.default", "parquet", "write.parquet.compression-codec", "snappy");
        target = IcebergApiConverter.rebuildCreateTableProperties(source);
        assertEquals("parquet", target.get(DEFAULT_FILE_FORMAT));
        assertEquals("snappy", target.get(PARQUET_COMPRESSION));
    }

    @Test
    public void testReverseByteBuffer() {
        byte[] bytes = new byte[] {1, 2, 3, 4, 5};
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        IcebergApiConverter.reverseBuffer(byteBuffer);
        assertEquals(5, byteBuffer.remaining());
        assertEquals(5, byteBuffer.get(0));
        assertEquals(4, byteBuffer.get(1));
        assertEquals(3, byteBuffer.get(2));
        assertEquals(2, byteBuffer.get(3));
        assertEquals(1, byteBuffer.get(4));
    }

    @Test
    public void testTime() {
        Type timeType = DateType.TIME;
        org.apache.iceberg.types.Type icebergType = Types.TimeType.get();
        Type resType = fromIcebergType(icebergType);
        assertEquals(resType, timeType);
    }

    @Test
    public void testConvertDbNameToNamespace() {
        assertEquals(Namespace.of(""), IcebergApiConverter.convertDbNameToNamespace(""));
        assertEquals(Namespace.of("a"), IcebergApiConverter.convertDbNameToNamespace("a"));
        assertEquals(Namespace.of("a", "b", "c"), IcebergApiConverter.convertDbNameToNamespace("a.b.c"));
    }

    @Test
    public void testToIcebergPropsWithProps() {
        Map<String, String> toBeIcebergProps = ImmutableMap.of("k1", "v1", "k2", "v2");
        Map<String, String> toBeIcebergPropsWithCatType = ImmutableMap.of("k1", "v1", "k2", "v2",
                ICEBERG_CATALOG_TYPE, "rest");
        Map<String, String> icebergPropsWithCatType = IcebergApiConverter.toIcebergProps(
                Optional.of(toBeIcebergProps), "rest");

        assertEquals(toBeIcebergPropsWithCatType, icebergPropsWithCatType);
    }

    @Test
    public void testToIcebergPropsWithoutProps() {
        Map<String, String> toBeIcebergPropsWithCatType = ImmutableMap.of(ICEBERG_CATALOG_TYPE, "rest");
        Map<String, String> icebergPropsWithCatType = IcebergApiConverter.toIcebergProps(
                Optional.empty(), "rest");

        assertEquals(toBeIcebergPropsWithCatType, icebergPropsWithCatType);
    }

    @Test
    public void testToIcebergApiSchema2() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", BooleanType.BOOLEAN));
        columns.add(new Column("c2", IntegerType.INT));
        columns.add(new Column("c3", IntegerType.BIGINT));
        columns.add(new Column("c4", FloatType.FLOAT));
        columns.add(new Column("c5", FloatType.DOUBLE));
        columns.add(new Column("c6", DateType.DATE));
        columns.add(new Column("c7", DateType.DATETIME));
        columns.add(new Column("c8", VarcharType.VARCHAR));
        columns.add(new Column("c9", CharType.CHAR));
        columns.add(new Column("c10", DecimalType.DECIMAL32));
        columns.add(new Column("c11", DecimalType.DECIMAL64));
        columns.add(new Column("c12", DecimalType.DECIMAL128));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);

        List<String> exprStr = Lists.newArrayList("bucket(c12,1)",
                "truncate(c11,1)", "bucket(c10,1)",
                "truncate(c9,1)", "truncate(c8,1)",
                "year(c7)", "month(c6)", "void(c5)"
        );

        List<String> colNames = Lists.newArrayList("__generated_partition_column_(c12,1)",
                "__generated_partition_column_(c11,1)", "__generated_partition_column_(c10,1)",
                "__generated_partition_column_(c9,1)", "__generated_partition_column_(c8,1)",
                "__generated_partition_column_(c7)", "__generated_partition_column_(c6)",
                "__generated_partition_column_(c5)"
        );

        List<Expr> partitionExprs = exprStr.stream()
                .map(expr -> ColumnIdExpr.fromSql(expr).getExpr())
                .collect(Collectors.toList());

        ListPartitionDesc partDesc = new ListPartitionDesc(colNames, null);
        partDesc.setPartitionExprs(partitionExprs);
        PartitionSpec spec = IcebergApiConverter.parsePartitionFields(schema, partDesc);
        assertTrue(spec.isPartitioned());
        assertEquals(8, spec.fields().size());
    }

    @Test
    public void testToIcebergApiSchema3() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", BooleanType.BOOLEAN));
        columns.add(new Column("c2", IntegerType.INT));
        columns.add(new Column("c3", IntegerType.BIGINT));
        columns.add(new Column("c4", FloatType.FLOAT));
        columns.add(new Column("c5", FloatType.DOUBLE));
        columns.add(new Column("c6", DateType.DATE));
        columns.add(new Column("c7", DateType.DATETIME));
        columns.add(new Column("c8", VarcharType.VARCHAR));
        columns.add(new Column("c9", CharType.CHAR));
        columns.add(new Column("c10", DecimalType.DECIMAL32));
        columns.add(new Column("c11", DecimalType.DECIMAL64));
        columns.add(new Column("c12", DecimalType.DECIMAL128));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);

        List<String> exprStr = Lists.newArrayList("degrees(c1)"
        );

        List<String> colNames = Lists.newArrayList("__generated_partition_column_(c1)"
        );

        List<Expr> partitionExprs = exprStr.stream()
                .map(expr -> ColumnIdExpr.fromSql(expr).getExpr())
                .collect(Collectors.toList());

        ListPartitionDesc partDesc = new ListPartitionDesc(colNames, null);
        partDesc.setPartitionExprs(partitionExprs);
        assertThrows(SemanticException.class, () -> IcebergApiConverter.parsePartitionFields(schema, partDesc),
                "Unsupported partition transform degrees for column c1");
    }

    @Test
    public void testToIcebergApiSchema4() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", BooleanType.BOOLEAN));
        columns.add(new Column("c2", IntegerType.INT));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);

        List<String> exprStr = Lists.newArrayList("c6"
        );

        List<String> colNames = Lists.newArrayList("c6"
        );

        List<Expr> partitionExprs = exprStr.stream()
                .map(expr -> ColumnIdExpr.fromSql(expr).getExpr())
                .collect(Collectors.toList());

        ListPartitionDesc partDesc = new ListPartitionDesc(colNames, null);
        partDesc.setPartitionExprs(partitionExprs);
        assertThrows(SemanticException.class, () -> IcebergApiConverter.parsePartitionFields(schema, partDesc),
                "Column c6 not found in schema");
    }

    @Test
    public void testToIcebergApiSchema5() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c6", DateType.DATE));
        columns.add(new Column("c7", DateType.DATETIME));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);

        List<String> exprStr = Lists.newArrayList(
                "hour(c7)", "day(c6)"
        );

        List<String> colNames = Lists.newArrayList(
                "__generated_partition_column_(c7)", "__generated_partition_column_(c6)"
        );

        List<Expr> partitionExprs = exprStr.stream()
                .map(expr -> ColumnIdExpr.fromSql(expr).getExpr())
                .collect(Collectors.toList());

        ListPartitionDesc partDesc = new ListPartitionDesc(colNames, null);
        partDesc.setPartitionExprs(partitionExprs);
        PartitionSpec spec = IcebergApiConverter.parsePartitionFields(schema, partDesc);
        assertTrue(spec.isPartitioned());
        assertEquals(2, spec.fields().size());
    }

    @Test
    public void testToIcebergApiSchema6() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c6", DateType.DATE));
        columns.add(new Column("c7", DateType.DATETIME));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);

        List<String> exprStr = Lists.newArrayList(
                "hour(1)", "day(1)"
        );

        List<String> colNames = Lists.newArrayList(
                "__generated_partition_column_(c7)", "__generated_partition_column_(c6)"
        );

        List<Expr> partitionExprs = exprStr.stream()
                .map(expr -> ColumnIdExpr.fromSql(expr).getExpr())
                .collect(Collectors.toList());

        ListPartitionDesc partDesc = new ListPartitionDesc(colNames, null);
        partDesc.setPartitionExprs(partitionExprs);
        assertThrows(SemanticException.class, () -> IcebergApiConverter.parsePartitionFields(schema, partDesc),
                "Unsupported partition transform hour for arguments");
    }

    @Test
    public void testToIcebergApiSchema7() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c6", DateType.DATE));
        columns.add(new Column("c7", DateType.DATETIME));

        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);

        List<String> exprStr = Lists.newArrayList(
                "1", "1"
        );

        List<String> colNames = Lists.newArrayList(
                "__generated_partition_column_(c7)", "__generated_partition_column_(c6)"
        );

        List<Expr> partitionExprs = exprStr.stream()
                .map(expr -> ColumnIdExpr.fromSql(expr).getExpr())
                .collect(Collectors.toList());

        ListPartitionDesc partDesc = new ListPartitionDesc(colNames, null);
        partDesc.setPartitionExprs(partitionExprs);

        assertThrows(SemanticException.class, () -> IcebergApiConverter.parsePartitionFields(schema, partDesc),
                "Unsupported partition definition");
    }

    @Test
    public void testIcebergColumnType() {
        org.apache.iceberg.types.Type type;
        type = IcebergApiConverter.toIcebergColumnType(IntegerType.INT);
        assertEquals(org.apache.iceberg.types.Type.TypeID.INTEGER, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(BooleanType.BOOLEAN);
        assertEquals(org.apache.iceberg.types.Type.TypeID.BOOLEAN, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(IntegerType.TINYINT);
        assertEquals(org.apache.iceberg.types.Type.TypeID.INTEGER, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(DecimalType.DECIMAL64);
        assertEquals(org.apache.iceberg.types.Type.TypeID.DECIMAL, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(IntegerType.BIGINT);
        assertEquals(org.apache.iceberg.types.Type.TypeID.LONG, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(FloatType.DOUBLE);
        assertEquals(org.apache.iceberg.types.Type.TypeID.DOUBLE, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(FloatType.FLOAT);
        assertEquals(org.apache.iceberg.types.Type.TypeID.FLOAT, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(DateType.DATE);
        assertEquals(org.apache.iceberg.types.Type.TypeID.DATE, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(DateType.DATETIME);
        assertEquals(org.apache.iceberg.types.Type.TypeID.TIMESTAMP, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(DateType.TIME);
        assertEquals(org.apache.iceberg.types.Type.TypeID.TIME, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(VarcharType.VARCHAR);
        assertEquals(org.apache.iceberg.types.Type.TypeID.STRING, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(VarbinaryType.VARBINARY);
        assertEquals(org.apache.iceberg.types.Type.TypeID.BINARY, type.typeId());
    }

    @Test
    public void testToIcebergSortOrder() {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional(1, "id", new Types.IntegerType()));
        fields.add(Types.NestedField.optional(2, "dt", new Types.DateType()));
        fields.add(Types.NestedField.optional(3, "data", new Types.StringType()));
        Schema schema = new Schema(fields);

        try {
            SortOrder nullSortOrder = IcebergApiConverter.toIcebergSortOrder(schema, null);
            assertEquals(nullSortOrder, null);
        } catch (DdlException e) {
            assertTrue(false);
        }

        List<OrderByElement> orderByElements = new ArrayList<>();
        Expr expr1 = ColumnIdExpr.fromSql("id").getExpr();
        orderByElements.add(new OrderByElement(expr1, true, true));
        Expr expr2 = ColumnIdExpr.fromSql("dt").getExpr();
        orderByElements.add(new OrderByElement(expr2, false, false));

        SortOrder sortOrder = null;
        try {
            sortOrder = IcebergApiConverter.toIcebergSortOrder(schema, orderByElements);
        } catch (DdlException e) {
            assertTrue(false);
        }
        List<SortField> sortFields = sortOrder.fields();
        assertEquals(sortFields.size(), 2);

        // duplicate sorted columns
        Expr expr3 = ColumnIdExpr.fromSql("id").getExpr();
        orderByElements.add(new OrderByElement(expr3, true, true));

        sortOrder = null;
        try {
            sortOrder = IcebergApiConverter.toIcebergSortOrder(schema, orderByElements);
        } catch (DdlException e) {
            assertTrue(true);
        }
        assertEquals(sortOrder, null);
    }

    @Test
    public void testToFullSchemasWithInitialDefaultValue() {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional("c_bool").withId(1)
                .ofType(Types.BooleanType.get()).withInitialDefault(true).build());
        fields.add(Types.NestedField.optional("c_int").withId(2)
                .ofType(Types.IntegerType.get()).withInitialDefault(7).build());
        fields.add(Types.NestedField.optional("c_date").withId(3)
                .ofType(Types.DateType.get()).withInitialDefault(2).build());
        long tsMicros = DateTimeUtil.microsFromTimestamp(LocalDateTime.of(2024, 1, 2, 3, 4, 5));
        fields.add(Types.NestedField.optional("c_ts").withId(4)
                .ofType(Types.TimestampType.withoutZone()).withInitialDefault(tsMicros).build());
        fields.add(Types.NestedField.optional("c_no_default").withId(5)
                .ofType(Types.StringType.get()).build());

        Schema schema = new Schema(fields);
        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);

        assertEquals("1", columns.get(0).getDefaultValue());
        assertEquals("7", columns.get(1).getDefaultValue());
        assertEquals("1970-01-03", columns.get(2).getDefaultValue());
        assertEquals("2024-01-02 03:04:05", columns.get(3).getDefaultValue());
        Assertions.assertNull(columns.get(4).getDefaultValue());
    }

    @Test
    public void testToIcebergApiSchemaWithColumnDefaultValue() {
        Column boolCol = new Column("c_bool", BooleanType.BOOLEAN, true);
        boolCol.setDefaultValue("1");
        Column intCol = new Column("c_int", IntegerType.INT, true);
        intCol.setDefaultValue("7");
        Column tsCol = new Column("c_ts", DateType.DATETIME, true);
        tsCol.setDefaultValue("2024-01-02 03:04:05");

        Schema schema = IcebergApiConverter.toIcebergApiSchema(List.of(boolCol, intCol, tsCol));
        Types.NestedField boolField = schema.findField("c_bool");
        Types.NestedField intField = schema.findField("c_int");
        Types.NestedField tsField = schema.findField("c_ts");

        assertTrue((Boolean) boolField.initialDefault());
        assertTrue((Boolean) boolField.writeDefault());
        assertEquals(7, intField.initialDefault());
        assertEquals(7, intField.writeDefault());
        long expectedMicros = DateTimeUtil.microsFromTimestamp(LocalDateTime.of(2024, 1, 2, 3, 4, 5));
        assertEquals(expectedMicros, ((Number) tsField.initialDefault()).longValue());
        assertEquals(expectedMicros, ((Number) tsField.writeDefault()).longValue());
    }

    @Test
    public void testToFullSchemasUsesIcebergNullabilityAndWriteDefaultLookup() {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.required("c_required").withId(1).ofType(Types.IntegerType.get()).build());
        fields.add(Types.NestedField.optional("c_optional").withId(2)
                .ofType(Types.IntegerType.get()).withWriteDefault(9).build());
        Schema schema = new Schema(fields);

        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);
        assertFalse(columns.get(0).isAllowNull());
        assertTrue(columns.get(1).isAllowNull());
        assertEquals("9", IcebergApiConverter.getWriteDefaultValue(schema, "c_optional"));
    }

    @Test
    public void testToDefaultValueStringWithWriteDefault() {
        List<Types.NestedField> fields = Lists.newArrayList();
        // Boolean with write default
        fields.add(Types.NestedField.optional("c_bool").withId(1)
                .ofType(Types.BooleanType.get()).withWriteDefault(false).build());
        // Long with write default
        fields.add(Types.NestedField.optional("c_long").withId(2)
                .ofType(Types.LongType.get()).withWriteDefault(100L).build());
        // Float with write default
        fields.add(Types.NestedField.optional("c_float").withId(3)
                .ofType(Types.FloatType.get()).withWriteDefault(1.5f).build());
        // Double with write default
        fields.add(Types.NestedField.optional("c_double").withId(4)
                .ofType(Types.DoubleType.get()).withWriteDefault(3.14159).build());
        // Decimal with write default
        fields.add(Types.NestedField.optional("c_decimal").withId(5)
                .ofType(Types.DecimalType.of(10, 2)).withWriteDefault(new java.math.BigDecimal("99.99")).build());
        // String with write default
        fields.add(Types.NestedField.optional("c_string").withId(6)
                .ofType(Types.StringType.get()).withWriteDefault("default_value").build());

        Schema schema = new Schema(fields);
        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);

        assertEquals("0", columns.get(0).getDefaultValue()); // false -> "0"
        assertEquals("100", columns.get(1).getDefaultValue());
        assertEquals("1.5", columns.get(2).getDefaultValue());
        assertEquals("3.14159", columns.get(3).getDefaultValue());
        assertEquals("99.99", columns.get(4).getDefaultValue());
        assertEquals("default_value", columns.get(5).getDefaultValue());
    }

    @Test
    public void testToDefaultValueStringWithTime() {
        List<Types.NestedField> fields = Lists.newArrayList();
        // Time type
        long timeMicros = 3661000000L; // 01:01:01.000000
        fields.add(Types.NestedField.optional("c_time").withId(1)
                .ofType(Types.TimeType.get()).withInitialDefault(timeMicros).build());

        Schema schema = new Schema(fields);
        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);
        assertNotNull(columns.get(0).getDefaultValue());
    }

    @Test
    public void testToDefaultValueStringWithTimestampTz() {
        List<Types.NestedField> fields = Lists.newArrayList();
        // Timestamp with timezone
        long tsMicros = DateTimeUtil.microsFromTimestamp(LocalDateTime.of(2024, 6, 15, 10, 30, 45));
        fields.add(Types.NestedField.optional("c_ts_tz").withId(1)
                .ofType(Types.TimestampType.withZone()).withInitialDefault(tsMicros).build());

        Schema schema = new Schema(fields);
        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);
        assertNotNull(columns.get(0).getDefaultValue());
        assertTrue(columns.get(0).getDefaultValue().contains("2024"));
    }

    @Test
    public void testToIcebergDefaultLiteralWithVariousTypes() {
        // Test TINYINT
        Column tinyIntCol = new Column("c_tinyint", IntegerType.TINYINT, true);
        tinyIntCol.setDefaultValue("10");
        Schema schema1 = IcebergApiConverter.toIcebergApiSchema(List.of(tinyIntCol));
        Types.NestedField tinyIntField = schema1.findField("c_tinyint");
        assertNotNull(tinyIntField.initialDefault());
        assertEquals(10, tinyIntField.initialDefault());

        // Test SMALLINT
        Column smallIntCol = new Column("c_smallint", IntegerType.SMALLINT, true);
        smallIntCol.setDefaultValue("100");
        Schema schema2 = IcebergApiConverter.toIcebergApiSchema(List.of(smallIntCol));
        Types.NestedField smallIntField = schema2.findField("c_smallint");
        assertEquals(100, smallIntField.initialDefault());

        // Test BIGINT
        Column bigIntCol = new Column("c_bigint", IntegerType.BIGINT, true);
        bigIntCol.setDefaultValue("1000000");
        Schema schema3 = IcebergApiConverter.toIcebergApiSchema(List.of(bigIntCol));
        Types.NestedField bigIntField = schema3.findField("c_bigint");
        assertEquals(1000000L, bigIntField.initialDefault());

        // Test FLOAT
        Column floatCol = new Column("c_float", FloatType.FLOAT, true);
        floatCol.setDefaultValue("3.14");
        Schema schema4 = IcebergApiConverter.toIcebergApiSchema(List.of(floatCol));
        Types.NestedField floatField = schema4.findField("c_float");
        assertEquals(3.14f, floatField.initialDefault());

        // Test CHAR
        Column charCol = new Column("c_char", new CharType(10), true);
        charCol.setDefaultValue("abc");
        Schema schema5 = IcebergApiConverter.toIcebergApiSchema(List.of(charCol));
        Types.NestedField charField = schema5.findField("c_char");
        assertEquals("abc", charField.initialDefault());
    }

    @Test
    public void testToIcebergDefaultLiteralBooleanFormats() {
        // Test "1" for true
        Column col1 = new Column("c1", BooleanType.BOOLEAN, true);
        col1.setDefaultValue("1");
        Schema schema1 = IcebergApiConverter.toIcebergApiSchema(List.of(col1));
        assertTrue((Boolean) schema1.findField("c1").initialDefault());

        // Test "true" for true
        Column col2 = new Column("c2", BooleanType.BOOLEAN, true);
        col2.setDefaultValue("true");
        Schema schema2 = IcebergApiConverter.toIcebergApiSchema(List.of(col2));
        assertTrue((Boolean) schema2.findField("c2").initialDefault());

        // Test "TRUE" for true
        Column col3 = new Column("c3", BooleanType.BOOLEAN, true);
        col3.setDefaultValue("TRUE");
        Schema schema3 = IcebergApiConverter.toIcebergApiSchema(List.of(col3));
        assertTrue((Boolean) schema3.findField("c3").initialDefault());

        // Test "0" for false
        Column col4 = new Column("c4", BooleanType.BOOLEAN, true);
        col4.setDefaultValue("0");
        Schema schema4 = IcebergApiConverter.toIcebergApiSchema(List.of(col4));
        assertFalse((Boolean) schema4.findField("c4").initialDefault());

        // Test "false" for false
        Column col5 = new Column("c5", BooleanType.BOOLEAN, true);
        col5.setDefaultValue("false");
        Schema schema5 = IcebergApiConverter.toIcebergApiSchema(List.of(col5));
        assertFalse((Boolean) schema5.findField("c5").initialDefault());

        // Test "FALSE" for false
        Column col6 = new Column("c6", BooleanType.BOOLEAN, true);
        col6.setDefaultValue("FALSE");
        Schema schema6 = IcebergApiConverter.toIcebergApiSchema(List.of(col6));
        assertFalse((Boolean) schema6.findField("c6").initialDefault());
    }

    @Test
    public void testToIcebergDefaultLiteralInvalidBoolean() {
        Column col = new Column("c_bool", BooleanType.BOOLEAN, true);
        col.setDefaultValue("invalid");
        assertThrows(StarRocksConnectorException.class, () -> {
            IcebergApiConverter.toIcebergApiSchema(List.of(col));
        });
    }

    @Test
    public void testGetWriteDefaultValueCaseInsensitive() {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional("MyColumn").withId(1)
                .ofType(Types.IntegerType.get()).withWriteDefault(42).build());
        Schema schema = new Schema(fields);

        // Test case insensitive lookup
        assertEquals("42", IcebergApiConverter.getWriteDefaultValue(schema, "MyColumn"));
        assertEquals("42", IcebergApiConverter.getWriteDefaultValue(schema, "mycolumn"));
        assertEquals("42", IcebergApiConverter.getWriteDefaultValue(schema, "MYCOLUMN"));
    }

    @Test
    public void testGetWriteDefaultValueNotFound() {
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional("col1").withId(1)
                .ofType(Types.IntegerType.get()).build());
        Schema schema = new Schema(fields);

        assertNull(IcebergApiConverter.getWriteDefaultValue(schema, "nonexistent"));
    }

    @Test
    public void testToIcebergApiSchemaWithNullDefaultValue() {
        Column col1 = new Column("c1", IntegerType.INT, true);
        // No default value set
        col1.setDefaultValue(null);

        Schema schema = IcebergApiConverter.toIcebergApiSchema(List.of(col1));
        Types.NestedField field = schema.findField("c1");

        // Should not have default value
        assertNull(field.initialDefaultLiteral());
        assertNull(field.writeDefaultLiteral());
    }

    @Test
    public void testToIcebergApiSchemaWithDecimalTypes() {
        Column decimal32Col = new Column("c_d32", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2), true);
        decimal32Col.setDefaultValue("123.45");

        Column decimal64Col = new Column("c_d64", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4), true);
        decimal64Col.setDefaultValue("123456.7890");

        Column decimal128Col = new Column("c_d128", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 28, 8), true);
        decimal128Col.setDefaultValue("123456789.12345678");

        Schema schema = IcebergApiConverter.toIcebergApiSchema(List.of(decimal32Col, decimal64Col, decimal128Col));

        assertNotNull(schema.findField("c_d32").initialDefault());
        assertNotNull(schema.findField("c_d64").initialDefault());
        assertNotNull(schema.findField("c_d128").initialDefault());
    }

    @Test
    public void testToIcebergApiSchemaWithDateDefault() {
        Column dateCol = new Column("c_date", DateType.DATE, true);
        dateCol.setDefaultValue("2024-06-15");

        Schema schema = IcebergApiConverter.toIcebergApiSchema(List.of(dateCol));
        Types.NestedField field = schema.findField("c_date");

        assertNotNull(field.initialDefault());
    }

    @Test
    public void testToIcebergApiSchemaWithDatetimeDefault() {
        Column datetimeCol = new Column("c_datetime", DateType.DATETIME, true);
        datetimeCol.setDefaultValue("2024-06-15 10:30:45");

        Schema schema = IcebergApiConverter.toIcebergApiSchema(List.of(datetimeCol));
        Types.NestedField field = schema.findField("c_datetime");

        assertNotNull(field.initialDefault());
    }

    @Test
    public void testToIcebergApiSchemaWithDatetimeDefaultWithT() {
        // Test datetime with 'T' separator
        Column datetimeCol = new Column("c_datetime", DateType.DATETIME, true);
        datetimeCol.setDefaultValue("2024-06-15T10:30:45");

        Schema schema = IcebergApiConverter.toIcebergApiSchema(List.of(datetimeCol));
        Types.NestedField field = schema.findField("c_datetime");

        assertNotNull(field.initialDefault());
    }

    @Test
    public void testDifferingInitialAndWriteDefaults() {
        // Case A: Both initial-default and write-default set to different values
        List<Types.NestedField> fields = Lists.newArrayList();
        fields.add(Types.NestedField.optional("c_both").withId(1)
                .ofType(Types.IntegerType.get()).withInitialDefault(5).withWriteDefault(10).build());
        // Case B: Only write-default (no initial-default)
        fields.add(Types.NestedField.optional("c_write_only").withId(2)
                .ofType(Types.IntegerType.get()).withWriteDefault(10).build());
        // Case C: Only initial-default (no write-default)
        fields.add(Types.NestedField.optional("c_initial_only").withId(3)
                .ofType(Types.IntegerType.get()).withInitialDefault(5).build());

        Schema schema = new Schema(fields);
        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);

        // toFullSchemas stores write-default (for INSERT path), falling back to initial-default
        assertEquals("10", columns.get(0).getDefaultValue()); // write-default wins
        assertEquals("10", columns.get(1).getDefaultValue()); // write-default only
        assertEquals("5", columns.get(2).getDefaultValue());  // initial-default as fallback

        // toInitialDefaultValueString returns ONLY the initial-default
        assertEquals("5", IcebergApiConverter.toInitialDefaultValueString(schema.findField("c_both")));
        assertNull(IcebergApiConverter.toInitialDefaultValueString(schema.findField("c_write_only")));
        assertEquals("5", IcebergApiConverter.toInitialDefaultValueString(schema.findField("c_initial_only")));
    }

}
