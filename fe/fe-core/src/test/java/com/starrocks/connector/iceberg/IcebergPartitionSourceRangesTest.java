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

import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Utils;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergPartitionSourceRangesTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "tstz", Types.TimestampType.withZone()),
            Types.NestedField.optional(4, "dt", Types.DateType.get()),
            Types.NestedField.optional(5, "name", Types.StringType.get()),
            Types.NestedField.optional(6, "amount", Types.DecimalType.of(10, 2)));

    private static double wall(String dateTime) {
        return Utils.getLongFromDateTime(LocalDateTime.parse(dateTime));
    }

    private static int day(String date) {
        return (int) LocalDate.parse(date).toEpochDay();
    }

    private static ManifestFile.PartitionFieldSummary summary(Type type, Object lower, Object upper) {
        ManifestFile.PartitionFieldSummary summary = mock(ManifestFile.PartitionFieldSummary.class);
        when(summary.lowerBound()).thenReturn(lower == null ? null : Conversions.toByteBuffer(type, lower));
        when(summary.upperBound()).thenReturn(upper == null ? null : Conversions.toByteBuffer(type, upper));
        return summary;
    }

    private static ManifestFile manifest(PartitionSpec spec, boolean live, ManifestFile.PartitionFieldSummary... fields) {
        ManifestFile manifest = mock(ManifestFile.class);
        when(manifest.partitionSpecId()).thenReturn(spec.specId());
        when(manifest.hasAddedFiles()).thenReturn(live);
        when(manifest.hasExistingFiles()).thenReturn(false);
        when(manifest.partitions()).thenReturn(fields.length == 0 ? null : List.of(fields));
        when(manifest.path()).thenReturn("m.avro");
        return manifest;
    }

    private static Map<String, Pair<Double, Double>> compute(Schema schema, PartitionSpec spec, ManifestFile... manifests) {
        return IcebergPartitionSourceRanges.compute(schema, Map.of(spec.specId(), spec), List.of(manifests));
    }

    @Test
    public void testDayTransformUnionAcrossManifests() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").build();
        Type dayType = spec.partitionType().fields().get(0).type();
        Map<String, Pair<Double, Double>> ranges = compute(SCHEMA, spec,
                manifest(spec, true, summary(dayType, day("2026-10-20"), day("2026-10-22"))),
                manifest(spec, true, summary(dayType, day("2026-10-26"), day("2026-10-26"))));
        // day=10-26 covers [10-26 00:00, 10-27 00:00)
        assertEquals(wall("2026-10-20T00:00:00"), ranges.get("ts").first);
        assertEquals(wall("2026-10-26T23:59:59"), ranges.get("ts").second);
    }

    @Test
    public void testHourMonthYearTransforms() {
        PartitionSpec hour = PartitionSpec.builderFor(SCHEMA).hour("ts").build();
        int h = (int) (LocalDateTime.parse("2026-10-26T17:00:00").toEpochSecond(ZoneOffset.UTC) / 3600);
        Map<String, Pair<Double, Double>> ranges = compute(SCHEMA, hour,
                manifest(hour, true, summary(Types.IntegerType.get(), h, h)));
        assertEquals(wall("2026-10-26T17:00:00"), ranges.get("ts").first);
        assertEquals(wall("2026-10-26T17:59:59"), ranges.get("ts").second);

        // month on a date column: 2026-02 covers 02-01 .. 02-28
        PartitionSpec month = PartitionSpec.builderFor(SCHEMA).month("dt").build();
        int m = (2026 - 1970) * 12 + 1;
        ranges = compute(SCHEMA, month, manifest(month, true, summary(Types.IntegerType.get(), m, m)));
        assertEquals(wall("2026-02-01T00:00:00"), ranges.get("dt").first);
        assertEquals(wall("2026-02-28T00:00:00"), ranges.get("dt").second);

        PartitionSpec year = PartitionSpec.builderFor(SCHEMA).year("ts").build();
        ranges = compute(SCHEMA, year, manifest(year, true, summary(Types.IntegerType.get(), 56, 56)));
        assertEquals(wall("2026-01-01T00:00:00"), ranges.get("ts").first);
        assertEquals(wall("2026-12-31T23:59:59"), ranges.get("ts").second);
    }

    @Test
    public void testIdentityTransforms() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
                .identity("id").identity("dt").identity("amount").identity("name").build();
        List<Types.NestedField> types = spec.partitionType().fields();
        Map<String, Pair<Double, Double>> ranges = compute(SCHEMA, spec, manifest(spec, true,
                summary(types.get(0).type(), 3, 9),
                summary(types.get(1).type(), day("2026-10-01"), day("2026-10-26")),
                summary(types.get(2).type(), new BigDecimal("1.50"), new BigDecimal("8.25")),
                summary(types.get(3).type(), "a", "z")));
        assertEquals(Pair.create(3.0, 9.0), ranges.get("id"));
        assertEquals(Pair.create(wall("2026-10-01T00:00:00"), wall("2026-10-26T00:00:00")), ranges.get("dt"));
        assertEquals(Pair.create(1.5, 8.25), ranges.get("amount"));
        // a string column has no numeric statistics range
        assertFalse(ranges.containsKey("name"));
    }

    @Test
    public void testSkipsUnboundingTransformsMissingBoundsAndDeadManifests() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 8).day("ts").build();
        List<Types.NestedField> types = spec.partitionType().fields();
        Map<String, Pair<Double, Double>> ranges = compute(SCHEMA, spec,
                // a manifest whose partition values are all null carries no bounds
                manifest(spec, true, summary(types.get(0).type(), 0, 7), summary(types.get(1).type(), null, null)),
                // a manifest with no live file does not count
                manifest(spec, false, summary(types.get(0).type(), 0, 7),
                        summary(types.get(1).type(), day("2027-01-01"), day("2027-01-01"))),
                // a manifest list without partition summaries
                manifest(spec, true),
                manifest(spec, true, summary(types.get(0).type(), 0, 7),
                        summary(types.get(1).type(), day("2026-10-26"), day("2026-10-26"))));
        assertFalse(ranges.containsKey("id"));
        assertEquals(Pair.create(wall("2026-10-26T00:00:00"), wall("2026-10-26T23:59:59")), ranges.get("ts"));
    }

    @Test
    public void testSkipsFieldWhoseSourceLeftTheSchema() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("id").build();
        List<Types.NestedField> types = spec.partitionType().fields();
        List<Types.NestedField> current = new ArrayList<>(SCHEMA.columns());
        current.removeIf(f -> f.fieldId() == 2);
        Map<String, Pair<Double, Double>> ranges = compute(new Schema(current), spec, manifest(spec, true,
                summary(types.get(0).type(), day("2026-10-26"), day("2026-10-26")),
                summary(types.get(1).type(), 1, 2)));
        assertFalse(ranges.containsKey("ts"));
        assertEquals(Pair.create(1.0, 2.0), ranges.get("id"));
    }

    @Test
    public void testTimestampWithZoneUsesSessionTimeZone() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("Asia/Shanghai");
        ctx.setThreadLocalInfo();
        try {
            PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("tstz").build();
            Type dayType = spec.partitionType().fields().get(0).type();
            Map<String, Pair<Double, Double>> ranges = compute(SCHEMA, spec,
                    manifest(spec, true, summary(dayType, day("2026-10-26"), day("2026-10-26"))));
            // UTC day 10-26 is [10-26 08:00, 10-27 08:00) in +08:00
            assertEquals(wall("2026-10-26T08:00:00"), ranges.get("tstz").first);
            assertEquals(wall("2026-10-27T07:59:59"), ranges.get("tstz").second);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testTimestampWithZoneCoversDstFallBack() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("America/New_York");
        ctx.setThreadLocalInfo();
        try {
            PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("tstz").build();
            Type type = spec.partitionType().fields().get(0).type();
            long lo = LocalDateTime.parse("2026-11-01T05:15:00").toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
            long hi = LocalDateTime.parse("2026-11-01T06:45:00").toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
            Pair<Double, Double> range = compute(SCHEMA, spec, manifest(spec, true, summary(type, lo, hi))).get("tstz");
            // 05:55Z is 01:55 EDT, later than the 01:45 EST the upper endpoint maps to
            assertTrue(range.first <= wall("2026-11-01T01:00:00"));
            assertTrue(range.second >= wall("2026-11-01T01:59:59"));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testPromotedDateBoundIsSkipped() {
        // ts was a DATE when these manifests were written: their identity bounds are 4-byte day counts
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("ts").build();
        Map<String, Pair<Double, Double>> ranges = compute(SCHEMA, spec, manifest(spec, true,
                summary(Types.DateType.get(), day("2024-01-01"), day("2024-01-31"))));
        assertFalse(ranges.containsKey("ts"));
    }

    @Test
    public void testUndecodableBoundIsSkipped() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
        ManifestFile.PartitionFieldSummary bad = mock(ManifestFile.PartitionFieldSummary.class);
        when(bad.lowerBound()).thenReturn(ByteBuffer.allocate(1));
        when(bad.upperBound()).thenReturn(ByteBuffer.allocate(1));
        assertTrue(compute(SCHEMA, spec, manifest(spec, true, bad)).isEmpty());
    }
}
