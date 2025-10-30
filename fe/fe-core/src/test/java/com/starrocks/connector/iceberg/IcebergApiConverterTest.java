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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ListPartitionDesc;
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
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
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
        assertEquals(resType, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, scale));
        resType = fromIcebergType(Types.DecimalType.of(10, scale));
        assertEquals(resType, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, scale));
        resType = fromIcebergType(Types.DecimalType.of(19, scale));
        assertEquals(resType, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, scale));
    }

    @Test
    public void testString() {
        Type stringType = ScalarType.createDefaultCatalogString();
        org.apache.iceberg.types.Type icebergType = Types.StringType.get();
        Type resType = fromIcebergType(icebergType);
        assertEquals(resType, stringType);
    }

    @Test
    public void testUUID() {
        org.apache.iceberg.types.Type icebergType = Types.UUIDType.get();
        Type resType = fromIcebergType(icebergType);
        assertTrue(resType.isBinaryType());
    }

    @Test
    public void testArray() {
        assertEquals(fromIcebergType(Types.ListType.ofRequired(136, Types.IntegerType.get())),
                new ArrayType(ScalarType.createType(PrimitiveType.INT)));
        assertEquals(fromIcebergType(Types.ListType.ofRequired(136,
                        Types.ListType.ofRequired(136, Types.IntegerType.get()))),
                new ArrayType(new ArrayType(ScalarType.createType(PrimitiveType.INT))));
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
                new MapType(ScalarType.createDefaultCatalogString(), ScalarType.createType(PrimitiveType.INT)));
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
        columns.add(new Column("c1", Type.BOOLEAN));
        columns.add(new Column("c2", Type.INT));
        columns.add(new Column("c3", Type.BIGINT));
        columns.add(new Column("c4", Type.FLOAT));
        columns.add(new Column("c5", Type.DOUBLE));
        columns.add(new Column("c6", Type.DATE));
        columns.add(new Column("c7", Type.DATETIME));
        columns.add(new Column("c8", Type.VARCHAR));
        columns.add(new Column("c9", Type.CHAR));
        columns.add(new Column("c10", Type.DECIMAL32));
        columns.add(new Column("c11", Type.DECIMAL64));
        columns.add(new Column("c12", Type.DECIMAL128));
        columns.add(new Column("c13", new ArrayType(Type.INT)));
        columns.add(new Column("c14", new MapType(Type.INT, Type.INT)));
        columns.add(new Column("c15", new StructType(ImmutableList.of(Type.INT))));
        columns.add(new Column("c16", Type.TIME));

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
                "  15: c15: required struct<20: col1: optional int>\n" +
                "  16: c16: required time\n" +
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
        Type timeType = ScalarType.createType(PrimitiveType.TIME);
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
        columns.add(new Column("c1", Type.BOOLEAN));
        columns.add(new Column("c2", Type.INT));
        columns.add(new Column("c3", Type.BIGINT));
        columns.add(new Column("c4", Type.FLOAT));
        columns.add(new Column("c5", Type.DOUBLE));
        columns.add(new Column("c6", Type.DATE));
        columns.add(new Column("c7", Type.DATETIME));
        columns.add(new Column("c8", Type.VARCHAR));
        columns.add(new Column("c9", Type.CHAR));
        columns.add(new Column("c10", Type.DECIMAL32));
        columns.add(new Column("c11", Type.DECIMAL64));
        columns.add(new Column("c12", Type.DECIMAL128));

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
        columns.add(new Column("c1", Type.BOOLEAN));
        columns.add(new Column("c2", Type.INT));
        columns.add(new Column("c3", Type.BIGINT));
        columns.add(new Column("c4", Type.FLOAT));
        columns.add(new Column("c5", Type.DOUBLE));
        columns.add(new Column("c6", Type.DATE));
        columns.add(new Column("c7", Type.DATETIME));
        columns.add(new Column("c8", Type.VARCHAR));
        columns.add(new Column("c9", Type.CHAR));
        columns.add(new Column("c10", Type.DECIMAL32));
        columns.add(new Column("c11", Type.DECIMAL64));
        columns.add(new Column("c12", Type.DECIMAL128));

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
        columns.add(new Column("c1", Type.BOOLEAN));
        columns.add(new Column("c2", Type.INT));

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
        columns.add(new Column("c6", Type.DATE));
        columns.add(new Column("c7", Type.DATETIME));

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
        columns.add(new Column("c6", Type.DATE));
        columns.add(new Column("c7", Type.DATETIME));

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
        columns.add(new Column("c6", Type.DATE));
        columns.add(new Column("c7", Type.DATETIME));

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
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.INT));
        assertEquals(org.apache.iceberg.types.Type.TypeID.INTEGER, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.BOOLEAN));
        assertEquals(org.apache.iceberg.types.Type.TypeID.BOOLEAN, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.TINYINT));
        assertEquals(org.apache.iceberg.types.Type.TypeID.INTEGER, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.DECIMAL64));
        assertEquals(org.apache.iceberg.types.Type.TypeID.DECIMAL, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.BIGINT));
        assertEquals(org.apache.iceberg.types.Type.TypeID.LONG, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.DOUBLE));
        assertEquals(org.apache.iceberg.types.Type.TypeID.DOUBLE, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.FLOAT));
        assertEquals(org.apache.iceberg.types.Type.TypeID.FLOAT, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.DATE));
        assertEquals(org.apache.iceberg.types.Type.TypeID.DATE, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.DATETIME));
        assertEquals(org.apache.iceberg.types.Type.TypeID.TIMESTAMP, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.TIME));
        assertEquals(org.apache.iceberg.types.Type.TypeID.TIME, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.VARCHAR));
        assertEquals(org.apache.iceberg.types.Type.TypeID.STRING, type.typeId());
        type = IcebergApiConverter.toIcebergColumnType(ScalarType.createType(PrimitiveType.VARBINARY));
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

}
