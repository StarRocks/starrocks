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
import com.starrocks.connector.iceberg.cost.IcebergFileStats;
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
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
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
    public void testPromotedDateBoundsAreDropped() {
        // A date column promoted to timestamp keeps 4-byte day-count bounds in old files. Decoding them as
        // micros gives values near 1970, so neither the min/max fast path nor CBO stats may use them.
        Schema schema = new Schema(required(1, "ts", Types.TimestampType.withoutZone()));
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), "ts", DateType.DATETIME, true);
        slot.setColumn(new Column("ts", DateType.DATETIME, true));
        // 19723 days = 2024-01-01, 19733 days = 2024-01-11
        var lowerBounds = Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), 19723));
        var upperBounds = Map.of(1, Conversions.toByteBuffer(Types.DateType.get(), 19733));
        var counts = Map.of(1, (long) 10);
        var nullCounts = Map.of(1, (long) 0);
        Assertions.assertTrue(IcebergUtil.isPromotedDateBound(Types.TimestampType.withoutZone(), lowerBounds.get(1)));
        Assertions.assertTrue(IcebergUtil.parseMinMaxValueBySlots(
                schema, lowerBounds, upperBounds, nullCounts, counts, List.of(slot)).isEmpty());

        // Another column of the same file loses its min/max too: BE would otherwise substitute it per batch
        // while reading the promoted column from the file.
        Schema twoCols = new Schema(required(1, "ts", Types.TimestampType.withoutZone()),
                required(2, "id", Types.IntegerType.get()));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(2), "id", IntegerType.INT, true);
        idSlot.setColumn(new Column("id", IntegerType.INT, true));
        Assertions.assertTrue(IcebergUtil.parseMinMaxValueBySlots(twoCols,
                Map.of(1, lowerBounds.get(1), 2, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
                Map.of(1, upperBounds.get(1), 2, Conversions.toByteBuffer(Types.IntegerType.get(), 9)),
                Map.of(1, (long) 0, 2, (long) 0), Map.of(1, (long) 10, 2, (long) 10),
                List.of(idSlot, slot)).isEmpty());
        Assertions.assertTrue(IcebergFileStats.toMap(
                Map.of(1, Types.TimestampType.withoutZone()), lowerBounds).isEmpty());

        // 8-byte timestamp bounds and 4-byte int->long bounds are still decoded.
        var tsBound = Conversions.toByteBuffer(Types.TimestampType.withoutZone(), 1_700_000_000_000_000L);
        Assertions.assertFalse(IcebergUtil.isPromotedDateBound(Types.TimestampType.withoutZone(), tsBound));
        Assertions.assertEquals(1_700_000_000_000_000L, IcebergFileStats.toMap(
                Map.of(1, Types.TimestampType.withoutZone()), Map.of(1, tsBound)).get(1));
        Assertions.assertFalse(IcebergUtil.isPromotedDateBound(Types.LongType.get(),
                Conversions.toByteBuffer(Types.IntegerType.get(), 7)));
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

        IcebergUtil.checkFileFormatSupportedDelete(parquetFileScanTask, true, false);
        IcebergUtil.checkFileFormatSupportedDelete(parquetFileScanTask, true, true);
        IcebergUtil.checkFileFormatSupportedDelete(orcFileScanTask, true, false);

        StarRocksConnectorException orcException = assertThrows(StarRocksConnectorException.class,
                () -> IcebergUtil.checkFileFormatSupportedDelete(orcFileScanTask, true, true));
        assertEquals("Iceberg row lineage is not supported on ORC data files yet. " +
                "Found ORC format file: /test/orc/file.orc", orcException.getMessage());

        StarRocksConnectorException avroException = assertThrows(StarRocksConnectorException.class,
                () -> IcebergUtil.checkFileFormatSupportedDelete(avroFileScanTask, true, false));
        assertEquals("Delete operations on Iceberg tables are only supported for Parquet and ORC format files. " +
                "Found AVRO format file: /test/avro/file.avro", avroException.getMessage());

        IcebergUtil.checkFileFormatSupportedDelete(orcFileScanTask, false, true);
        IcebergUtil.checkFileFormatSupportedDelete(avroFileScanTask, false, true);
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

    // At the 2024-11-03 fall-back in New York, 05:30 UTC is 01:30 EDT and 06:10 UTC is 01:10 EST: converting the two
    // ends gives a minimum above a value of the file, while the widened bounds cover it.
    @Test
    public void testWidenedTimestampTzBoundsCoverOffsetChanges() {
        Schema schema = new Schema(required(5, "ts_tz", Types.TimestampType.withZone()));
        List<SlotDescriptor> slots = List.of(new SlotDescriptor(new SlotId(5), "ts_tz", DateType.DATETIME, true));
        slots.get(0).setColumn(new Column("ts_tz", DateType.DATETIME, true));
        long low = java.time.Instant.parse("2024-11-03T05:30:00Z").getEpochSecond() * 1_000_000L;
        long high = java.time.Instant.parse("2024-11-03T06:10:00Z").getEpochSecond() * 1_000_000L;
        Map<Integer, ByteBuffer> lowerBounds = Map.of(5, Conversions.toByteBuffer(Types.TimestampType.withZone(), low));
        Map<Integer, ByteBuffer> upperBounds =
                Map.of(5, Conversions.toByteBuffer(Types.TimestampType.withZone(), high));
        Map<Integer, Long> nullValueCounts = Map.of(5, 0L);
        Map<Integer, Long> valueCounts = Map.of(5, 2L);
        long localOfHigh = java.time.LocalDateTime.parse("2024-11-03T01:10:00")
                .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L;

        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            ctx.getSessionVariable().setTimeZone("America/New_York");
            // the endpoint conversion would put min above a value of the file, so the min/max optimization gives up
            Assertions.assertTrue(IcebergUtil.parseMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots, false).isEmpty());

            IcebergUtil.MinMaxValue widened = IcebergUtil.parseMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots, true).get(5);
            // the range crosses the fall-back at 06:00Z: low + EST (-5h) .. high + EDT (-4h)
            long hour = 3600 * 1_000_000L;
            assertEquals(low - 5 * hour, widened.minValue);
            assertEquals(high - 4 * hour, widened.maxValue);
            Assertions.assertTrue((Long) widened.minValue <= localOfHigh);
            Map<Integer, TExprMinMaxValue> thrift = IcebergUtil.toThriftMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots, true);
            assertEquals(low - 5 * hour, thrift.get(5).min_int_value);
            assertEquals(high - 4 * hour, thrift.get(5).max_int_value);

            // every local value of every instant in a range lies within the bounds
            java.time.ZoneId zone = java.time.ZoneId.of("America/New_York");
            java.time.Instant start = java.time.Instant.parse("2024-03-09T00:00:00Z");
            for (int i = 0; i < 400; i++) {
                for (int len : new int[] {0, 7, 61, 130, 1500}) {
                    java.time.Instant a = start.plusSeconds(i * 1800L);
                    java.time.Instant b = a.plusSeconds(len * 60L);
                    long[] bounds = IcebergUtil.widenedSessionTzBounds(a.getEpochSecond() * 1_000_000L,
                            b.getEpochSecond() * 1_000_000L);
                    for (java.time.Instant t = a; !t.isAfter(b); t = t.plusSeconds(300)) {
                        long local = java.time.LocalDateTime.ofInstant(t, zone)
                                .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L;
                        Assertions.assertTrue(bounds[0] <= local && local <= bounds[1], a + ".." + b + " " + t);
                    }
                }
                if (i == 199) {
                    start = java.time.Instant.parse("2024-11-02T00:00:00Z").minusSeconds(200 * 1800L);
                }
            }

            // a fixed offset converts exactly
            ctx.getSessionVariable().setTimeZone("+08:00");
            IcebergUtil.MinMaxValue fixed = IcebergUtil.parseMinMaxValueBySlots(
                    schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots, true).get(5);
            assertEquals(low + 8 * 3600 * 1_000_000L, fixed.minValue);
            assertEquals(high + 8 * 3600 * 1_000_000L, fixed.maxValue);

            ctx.getSessionVariable().setTimeZone("America/New_York");
            // saturates at the long range, and falls back to the widest offset over centuries of transitions
            long[] extreme = IcebergUtil.widenedSessionTzBounds(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1);
            assertEquals(Long.MIN_VALUE, extreme[0]);
            assertEquals(Long.MAX_VALUE, extreme[1]);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testMinMaxOptGivesUpTimestampTzFileAcrossFallBack() {
        Schema schema = new Schema(required(5, "ts_tz", Types.TimestampType.withZone()));
        List<SlotDescriptor> slots = List.of(new SlotDescriptor(new SlotId(5), "ts_tz", DateType.DATETIME, true));
        slots.get(0).setColumn(new Column("ts_tz", DateType.DATETIME, true));
        java.util.function.BiFunction<String, String, Map<Integer, IcebergUtil.MinMaxValue>> parse = (lo, hi) -> {
            long low = java.time.Instant.parse(lo).getEpochSecond() * 1_000_000L;
            long high = java.time.Instant.parse(hi).getEpochSecond() * 1_000_000L;
            return IcebergUtil.parseMinMaxValueBySlots(schema,
                    Map.of(5, Conversions.toByteBuffer(Types.TimestampType.withZone(), low)),
                    Map.of(5, Conversions.toByteBuffer(Types.TimestampType.withZone(), high)),
                    Map.of(5, 0L), Map.of(5, 2L), slots, false);
        };
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            ctx.getSessionVariable().setTimeZone("America/New_York");
            // New York falls back at 2024-11-03 06:00Z: 01:10 EDT .. 01:20 EST may hold 01:00 EST, below 01:10
            Assertions.assertTrue(parse.apply("2024-11-03T05:10:00Z", "2024-11-03T06:20:00Z").isEmpty());
            // 00:30 EDT .. 01:30 EST may hold 01:59 EDT, above 01:30
            Assertions.assertTrue(parse.apply("2024-11-03T04:30:00Z", "2024-11-03T06:30:00Z").isEmpty());
            // a fall-back far from both ends cannot reach past them
            IcebergUtil.MinMaxValue wide = parse.apply("2024-11-02T00:00:00Z", "2024-11-04T00:00:00Z").get(5);
            assertEquals(java.time.LocalDateTime.parse("2024-11-01T20:00:00").toEpochSecond(java.time.ZoneOffset.UTC)
                    * 1_000_000L, wide.minValue);
            // spring-forward keeps the order
            Assertions.assertFalse(parse.apply("2024-03-10T06:30:00Z", "2024-03-10T07:30:00Z").isEmpty());

            // the endpoints are the local min and max exactly when kept
            java.time.ZoneId zone = java.time.ZoneId.of("America/New_York");
            java.time.Instant base = java.time.Instant.parse("2024-11-03T02:00:00Z");
            for (int start = 0; start <= 300; start += 10) {
                for (int len : new int[] {0, 20, 50, 70, 130, 300}) {
                    java.time.Instant a = base.plusSeconds(start * 60L);
                    java.time.Instant b = a.plusSeconds(len * 60L);
                    long localA = java.time.LocalDateTime.ofInstant(a, zone).toEpochSecond(java.time.ZoneOffset.UTC);
                    long localB = java.time.LocalDateTime.ofInstant(b, zone).toEpochSecond(java.time.ZoneOffset.UTC);
                    // the ends and the half minutes between them, which reach past a shift on the minute grid
                    boolean inside = true;
                    for (java.time.Instant t = a.plusSeconds(30); t.isBefore(b); t = t.plusSeconds(60)) {
                        long local = java.time.LocalDateTime.ofInstant(t, zone).toEpochSecond(java.time.ZoneOffset.UTC);
                        inside &= localA <= local && local <= localB;
                    }
                    assertEquals(inside, IcebergUtil.sessionTzEndpointsExact(a.getEpochSecond() * 1_000_000L,
                            b.getEpochSecond() * 1_000_000L), a + ".." + b);
                }
            }

            // a fixed offset never gives up
            ctx.getSessionVariable().setTimeZone("+08:00");
            Assertions.assertFalse(parse.apply("2024-11-03T05:10:00Z", "2024-11-03T06:20:00Z").isEmpty());
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
