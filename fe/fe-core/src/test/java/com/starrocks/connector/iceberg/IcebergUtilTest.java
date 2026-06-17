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

import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TExprMinMaxValue;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IcebergUtilTest {

    @Test
    public void testFileName() {
        assertEquals("1.orc", IcebergUtil.fileName("hdfs://tans/user/hive/warehouse/max-test/1.orc"));
        assertEquals("2.orc", IcebergUtil.fileName("cos://tans/user/hive/warehouse/max-test/2.orc"));
        assertEquals("3.orc", IcebergUtil.fileName("s3://tans/user/hive/warehouse/max-test/3.orc"));
        assertEquals("4.orc", IcebergUtil.fileName("gs://tans/user/hive/warehouse/max-test/4.orc"));
    }

    @Test
    public void testParseMinMaxValueBySlots() {
        Schema schema =
                new Schema(required(3, "id", Types.IntegerType.get()),
                        required(5, "date", Types.StringType.get()));
        List<SlotDescriptor> slots = List.of(
                new SlotDescriptor(new SlotId(3), "id", IntegerType.INT, true),
                new SlotDescriptor(new SlotId(5), "date", StringType.STRING, true)
        );
        slots.get(0).setColumn(new Column("id", IntegerType.INT, true));
        slots.get(1).setColumn(new Column("date", StringType.STRING, true));
        var lowerBounds = Map.of(3, ByteBuffer.wrap(new byte[] {1, 0, 0, 0}),
                5, ByteBuffer.wrap("2023-01-01".getBytes()));
        var upperBounds = Map.of(3, ByteBuffer.wrap(new byte[] {10, 0, 0, 0}),
                5, ByteBuffer.wrap("2023-01-10".getBytes()));
        var valueCounts = Map.of(3, (long) 10, 5, (long) 10);

        {
            var nullValueCounts = Map.of(3, (long) 0, 5, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(0, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = Map.of(3, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(0, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = new HashMap<Integer, Long>();
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(0, result.size());
        }
        {
            var nullValueCounts = Map.of(3, (long) 1, 5, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(1, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
    }

    @Test
    public void testTableDataLocationDefaultAndCustom() {
        org.apache.iceberg.Table table = org.mockito.Mockito.mock(org.apache.iceberg.Table.class);

        org.mockito.Mockito.when(table.location()).thenReturn("s3://bucket/path/");
        Map<String, String> properties = new HashMap<>();
        org.mockito.Mockito.when(table.properties()).thenReturn(properties);

        String defaultLocation = IcebergUtil.tableDataLocation(table);
        assertEquals("s3://bucket/path/data", defaultLocation);

        properties.put(TableProperties.WRITE_DATA_LOCATION, "s3://bucket/custom_data");
        String customLocation = IcebergUtil.tableDataLocation(table);
        assertEquals("s3://bucket/custom_data", customLocation);
    }

    @Test
    public void testParseMinMaxValuesFromDisorderedSlots() {
        Schema schema =
                new Schema(required(3, "id", Types.IntegerType.get()),
                        required(5, "date", Types.StringType.get()));
        List<SlotDescriptor> slots = List.of(
                new SlotDescriptor(new SlotId(5), "id", IntegerType.INT, true),
                new SlotDescriptor(new SlotId(3), "date", StringType.STRING, true)
        );
        slots.get(0).setColumn(new Column("id", IntegerType.INT, true));
        slots.get(1).setColumn(new Column("date", StringType.STRING, true));
        var lowerBounds = Map.of(3, ByteBuffer.wrap(new byte[] {1, 0, 0, 0}),
                5, ByteBuffer.wrap("2023-01-01".getBytes()));
        var upperBounds = Map.of(3, ByteBuffer.wrap(new byte[] {10, 0, 0, 0}),
                5, ByteBuffer.wrap("2023-01-10".getBytes()));
        var valueCounts = Map.of(3, (long) 10, 5, (long) 10);

        {
            var nullValueCounts = Map.of(3, (long) 0, 5, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(0, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = Map.of(3, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(0, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = new HashMap<Integer, Long>();
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(0, result.size());
        }
        {
            var nullValueCounts = Map.of(3, (long) 1, 5, (long) 0);
            var result =
                    IcebergUtil.parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(1, result.size());
            assertEquals(1, result.get(3).minValue);
            assertEquals(10, result.get(3).maxValue);
            assertEquals(1, result.get(3).nullValueCount);
            assertEquals(10, result.get(3).valueCount);
        }
        {
            var nullValueCounts = Map.of(3, (long) 1, 5, (long) 0);
            Map<Integer, TExprMinMaxValue> tExprMinMaxValueMap = IcebergUtil.toThriftMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds,
                    nullValueCounts, valueCounts, slots);
            assertEquals(1, tExprMinMaxValueMap.size());
            assertEquals(1, tExprMinMaxValueMap.get(5).min_int_value);
            assertEquals(10, tExprMinMaxValueMap.get(5).max_int_value);
        }
    }

    @Test
    public void testCheckFileFormatSupportedDelete() {
        FileScanTask parquetFileScanTask = createMockFileScanTask(FileFormat.PARQUET);
        FileScanTask orcFileScanTask = createMockFileScanTask(FileFormat.ORC);
        FileScanTask avroFileScanTask = createMockFileScanTask(FileFormat.AVRO);

        IcebergUtil.checkFileFormatSupportedDelete(parquetFileScanTask, true);

        StarRocksConnectorException orcException = assertThrows(StarRocksConnectorException.class,
                () -> IcebergUtil.checkFileFormatSupportedDelete(orcFileScanTask, true));
        assertEquals("Delete operations on Iceberg tables are only supported for Parquet format files. " +
                "Found ORC format file: /test/orc/file.orc", orcException.getMessage());

        StarRocksConnectorException avroException = assertThrows(StarRocksConnectorException.class,
                () -> IcebergUtil.checkFileFormatSupportedDelete(avroFileScanTask, true));
        assertEquals("Delete operations on Iceberg tables are only supported for Parquet format files. " +
                "Found AVRO format file: /test/avro/file.avro", avroException.getMessage());

        IcebergUtil.checkFileFormatSupportedDelete(orcFileScanTask, false);
        IcebergUtil.checkFileFormatSupportedDelete(avroFileScanTask, false);
        IcebergUtil.checkFileFormatSupportedDelete(parquetFileScanTask, false);
    }

    @Test
    public void testParseMinMaxValueBySlotsWithIcebergTimestampTypes() {
        Schema schema = new Schema(
                required(3, "ts_ntz", Types.TimestampType.withoutZone()),
                required(5, "ts_tz", Types.TimestampType.withZone()));
        List<SlotDescriptor> slots = List.of(
                new SlotDescriptor(new SlotId(3), "ts_ntz", DateType.DATETIME, true),
                new SlotDescriptor(new SlotId(5), "ts_tz", DateType.DATETIME, true)
        );
        slots.get(0).setColumn(new Column("ts_ntz", DateType.DATETIME, true));
        slots.get(1).setColumn(new Column("ts_tz", DateType.DATETIME, true));

        long minMicros = 0L;
        long maxMicros = 1_000_000L;
        Map<Integer, ByteBuffer> lowerBounds = Map.of(
                3, org.apache.iceberg.types.Conversions.toByteBuffer(Types.TimestampType.withoutZone(), minMicros),
                5, org.apache.iceberg.types.Conversions.toByteBuffer(Types.TimestampType.withZone(), minMicros));
        Map<Integer, ByteBuffer> upperBounds = Map.of(
                3, org.apache.iceberg.types.Conversions.toByteBuffer(Types.TimestampType.withoutZone(), maxMicros),
                5, org.apache.iceberg.types.Conversions.toByteBuffer(Types.TimestampType.withZone(), maxMicros));
        Map<Integer, Long> nullValueCounts = Map.of(3, 0L, 5, 0L);
        Map<Integer, Long> valueCounts = Map.of(3, 2L, 5, 2L);

        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("Asia/Shanghai");
        ctx.setThreadLocalInfo();
        try {
            Map<Integer, IcebergUtil.MinMaxValue> result = IcebergUtil.parseMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(2, result.size());
            assertEquals(minMicros, result.get(3).minValue);
            assertEquals(maxMicros, result.get(3).maxValue);
            assertEquals(minMicros + TimeZone.getTimeZone("Asia/Shanghai").getOffset(0) * 1000L, result.get(5).minValue);
            assertEquals(maxMicros + TimeZone.getTimeZone("Asia/Shanghai").getOffset(1000L) * 1000L,
                    result.get(5).maxValue);

            Map<Integer, TExprMinMaxValue> thriftValues = IcebergUtil.toThriftMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
            assertEquals(minMicros, thriftValues.get(3).min_int_value);
            assertEquals(maxMicros, thriftValues.get(3).max_int_value);
            assertEquals(minMicros + TimeZone.getTimeZone("Asia/Shanghai").getOffset(0) * 1000L,
                    thriftValues.get(5).min_int_value);
            assertEquals(maxMicros + TimeZone.getTimeZone("Asia/Shanghai").getOffset(1000L) * 1000L,
                    thriftValues.get(5).max_int_value);
        } finally {
            ConnectContext.remove();
        }
    }

    private static org.apache.iceberg.Table tableWithProperty(String key, String value) {
        org.apache.iceberg.Table table = org.mockito.Mockito.mock(org.apache.iceberg.Table.class);
        Map<String, String> props = new HashMap<>();
        if (key != null) {
            props.put(key, value);
        }
        org.mockito.Mockito.when(table.properties()).thenReturn(props);
        return table;
    }

    private static SessionVariable sessionWithTargetMaxFileSize(long value) {
        // SessionVariable doesn't expose a setter for this field (it's bound via @VarAttr),
        // so we mock just the one getter we care about.
        SessionVariable sv = org.mockito.Mockito.mock(SessionVariable.class);
        org.mockito.Mockito.when(sv.getConnectorSinkTargetMaxFileSize()).thenReturn(value);
        return sv;
    }

    @Test
    public void testResolveTargetMaxFileSizePrefersTableProperty() {
        // Table property wins over session value.
        org.apache.iceberg.Table table = tableWithProperty(
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "262144000"); // 250 MiB
        SessionVariable sv = sessionWithTargetMaxFileSize(123456L);
        assertEquals(262144000L, IcebergUtil.resolveTargetMaxFileSize(table, sv));
    }

    @Test
    public void testResolveTargetMaxFileSizeFallsBackToSession() {
        // Table property absent → session value used.
        org.apache.iceberg.Table table = tableWithProperty(null, null);
        SessionVariable sv = sessionWithTargetMaxFileSize(999_999L);
        assertEquals(999_999L, IcebergUtil.resolveTargetMaxFileSize(table, sv));
    }

    @Test
    public void testResolveTargetMaxFileSizeDefaultsToHardcoded() {
        // Neither property set nor session > 0 → IcebergUtil.DEFAULT_TARGET_FILE_SIZE_BYTES.
        org.apache.iceberg.Table table = tableWithProperty(null, null);
        SessionVariable sv = sessionWithTargetMaxFileSize(0L);
        assertEquals(IcebergUtil.DEFAULT_TARGET_FILE_SIZE_BYTES,
                IcebergUtil.resolveTargetMaxFileSize(table, sv));
    }

    @Test
    public void testResolveTargetMaxFileSizeIgnoresUnparseableProperty() {
        // Garbage property value → WARN logged, falls back through the chain.
        org.apache.iceberg.Table table = tableWithProperty(
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "not-a-number");
        SessionVariable sv = sessionWithTargetMaxFileSize(555L);
        assertEquals(555L, IcebergUtil.resolveTargetMaxFileSize(table, sv));
    }

    @Test
    public void testResolveTargetMaxFileSizeIgnoresNonPositiveProperty() {
        // Zero / negative are treated like unset.
        org.apache.iceberg.Table table = tableWithProperty(
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "0");
        SessionVariable sv = sessionWithTargetMaxFileSize(777L);
        assertEquals(777L, IcebergUtil.resolveTargetMaxFileSize(table, sv));
    }

    private FileScanTask createMockFileScanTask(FileFormat fileFormat) {
        FileScanTask mockTask = org.mockito.Mockito.mock(FileScanTask.class);
        org.apache.iceberg.DataFile mockFile = org.mockito.Mockito.mock(org.apache.iceberg.DataFile.class);
        org.mockito.Mockito.when(mockTask.file()).thenReturn(mockFile);
        org.mockito.Mockito.when(mockFile.format()).thenReturn(fileFormat);
        String location = "/test/" + fileFormat.name().toLowerCase() + "/file." +
                fileFormat.name().toLowerCase();
        org.mockito.Mockito.when(mockFile.location()).thenReturn(location);
        return mockTask;
    }
}
