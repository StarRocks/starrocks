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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;

class ParquetRowGroupStatisticsReaderTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void readsBigintStatisticsAcrossWholeFile() throws Exception {
        // Tiny block size to force the writer to split into multiple row
        // groups; the exact count is implementation-defined so the assertions
        // exercise aggregate invariants only.
        Path parquetPath = writeParquet(
                "message schema { required int64 sort_key; }",
                /*rowCount=*/ 64,
                (group, rowIndex) -> group.append("sort_key", (long) rowIndex));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        long totalRowCount = 0L;
        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (RowGroupStatistics rowGroup : rowGroupStatistics) {
            Assertions.assertNotNull(rowGroup.getMinTuple());
            Assertions.assertNotNull(rowGroup.getMaxTuple());
            Assertions.assertFalse(rowGroup.isTruncated());
            long minValue = Long.parseLong(rowGroup.getMinTuple().getValues().get(0).getStringValue());
            long maxValue = Long.parseLong(rowGroup.getMaxTuple().getValues().get(0).getStringValue());
            Assertions.assertTrue(minValue <= maxValue,
                    "row-group min " + minValue + " > max " + maxValue);
            totalRowCount += rowGroup.getRowCount();
            globalMin = Math.min(globalMin, minValue);
            globalMax = Math.max(globalMax, maxValue);
        }
        Assertions.assertEquals(64L, totalRowCount);
        Assertions.assertEquals(0L, globalMin);
        Assertions.assertEquals(63L, globalMax);
    }

    @Test
    void readsVarcharStatistics() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required binary tenant (UTF8); }",
                /*rowCount=*/ 16,
                (group, rowIndex) -> group.append("tenant", String.format("tenant-%02d", rowIndex)));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("tenant", VarcharType.VARCHAR));

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        String globalMin = null;
        String globalMax = null;
        for (RowGroupStatistics rowGroup : rowGroupStatistics) {
            // Binary stats are conservatively marked truncated so string sort keys
            // route through data tier — see the class javadoc for rationale.
            Assertions.assertTrue(rowGroup.isTruncated());
            String minValue = rowGroup.getMinTuple().getValues().get(0).getStringValue();
            String maxValue = rowGroup.getMaxTuple().getValues().get(0).getStringValue();
            Assertions.assertTrue(minValue.compareTo(maxValue) <= 0);
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals("tenant-00", globalMin);
        Assertions.assertEquals("tenant-15", globalMax);
    }

    @Test
    void unannotatedBinaryColumnFallsBackToDataTier() throws Exception {
        // Parquet BINARY without a UTF8/string annotation could hold arbitrary bytes;
        // toStringUsingUTF8 would corrupt non-UTF8 data and change ordering. Meta tier
        // only admits BINARY when the string annotation is explicit.
        Path parquetPath = writeParquet(
                "message schema { required binary opaque_bytes; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("opaque_bytes", "value-" + rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("opaque_bytes", VarcharType.VARCHAR)));
    }

    @Test
    void readsIntStatisticsForParquetInt32() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int32 region_id; }",
                /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("region_id", rowIndex + 100));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("region_id", IntegerType.INT));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertEquals("100", rowGroupStatistics.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("104", rowGroupStatistics.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsBooleanStatistics() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required boolean flag; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("flag", rowIndex % 2 == 0));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("flag", BooleanType.BOOLEAN));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertEquals(2L, rowGroupStatistics.get(0).getRowCount());
    }

    @Test
    void columnAbsentFromSchemaFallsBackToDataTier() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int64 other; }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("other", (long) rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("missing_sort_key", IntegerType.BIGINT)));
    }

    @Test
    void unsupportedParquetTypeFallsBackToDataTier() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required double payload; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("payload", rowIndex * 1.5));

        // Even with a numeric StarRocks sort key, DOUBLE is outside the meta-tier
        // mapping window — caller should fall through to reservoir sampling.
        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("payload", IntegerType.BIGINT)));
    }

    @Test
    void mismatchedStarRocksTypeFallsBackToDataTier() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int32 region_id; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("region_id", rowIndex));

        // Parquet INT32 cannot route into a VARCHAR sort-key column.
        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("region_id", VarcharType.VARCHAR)));
    }

    @Test
    void caseVariantDuplicatesFallBackToDataTier() throws Exception {
        // Parquet's schema spec permits sibling fields that differ only in case;
        // StarRocks column names are case-insensitive, so the reader cannot pick
        // one silently. Both must be rejected as ambiguous.
        Path parquetPath = writeParquet(
                "message schema { required int64 sort_key; required int64 SORT_KEY; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> {
                    group.append("sort_key", (long) rowIndex);
                    group.append("SORT_KEY", (long) -rowIndex);
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("sort_key", IntegerType.BIGINT)));
    }

    @Test
    void outOfRangeStatsValueFallsBackToDataTier() throws Exception {
        // INT64 stats (260) outside StarRocks TINYINT range. IntVariant's
        // Preconditions.checkArgument fires, the reader's catch wraps it as
        // MetaTierUnavailableException so the pipeline retries with data tier.
        Path parquetPath = writeParquet(
                "message schema { required int64 wide_value; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("wide_value", 260L + rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("wide_value", IntegerType.TINYINT)));
    }

    @Test
    void allNullRowGroupReportsAbsentStatistics() throws Exception {
        // Group factory's `.append(name, value)` requires a value, so leaving
        // the optional sort_key column unset makes every row's value null.
        // Parquet then writes the row group with hasNonNullValue() == false.
        Path parquetPath = writeParquet(
                "message schema { optional int64 sort_key; required int64 keepalive; }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("keepalive", (long) rowIndex));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        RowGroupStatistics only = rowGroupStatistics.get(0);
        Assertions.assertEquals(3L, only.getRowCount());
        Assertions.assertNull(only.getMinTuple());
        Assertions.assertNull(only.getMaxTuple());
    }

    @Test
    void readsDateStatisticsForInt32DateColumn() throws Exception {
        // INT32+DATE is days-since-epoch. Day 0 = 1970-01-01 (canonical anchor,
        // hand-verifiable). With a StarRocks DATE sort key the reader renders
        // canonical "yyyy-MM-dd" boundary text.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("event_day", rowIndex));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertFalse(rowGroupStatistics.get(0).isTruncated());
        Assertions.assertEquals("1970-01-01",
                rowGroupStatistics.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("1970-01-05",
                rowGroupStatistics.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void dateAnnotatedColumnIntoNonDateSortKeyFallsBackToDataTier() throws Exception {
        // INT32+DATE only maps to a StarRocks DATE column; routing it into a BIGINT
        // sort key would publish day-counts as integer boundaries — reject.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("event_day", rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("event_day", IntegerType.BIGINT)));
    }

    @Test
    void pre1970DateFallsBackToDataTier() throws Exception {
        // epochDay -1 = 1969-12-31 < 1970-01-01: outside the safe window (pre-1970 +
        // pre-1582 + year-0 parity traps). Meta tier must defer to data tier.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_day", -1 - rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("event_day", DateType.DATE)));
    }

    @Test
    void maxSupportedDateIsAccepted() throws Exception {
        // epochDay 2932896 = 9999-12-31, the upper edge of the safe window — accepted.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_day", 2932896));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertEquals("9999-12-31",
                rowGroupStatistics.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void postYear9999DateFallsBackToDataTier() throws Exception {
        // epochDay 2932897 = 10000-01-01, year > 9999: above the safe window → data tier.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_day", 2932897));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("event_day", DateType.DATE)));
    }

    private static MessageType timestampSchema(LogicalTypeAnnotation.TimeUnit unit, boolean isAdjustedToUTC) {
        return Types.buildMessage()
                .required(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(isAdjustedToUTC, unit))
                .named("event_ts")
                .named("schema");
    }

    private Path writeParquet(MessageType schema, int rowCount,
            java.util.function.BiConsumer<org.apache.parquet.example.data.Group, Integer> rowFiller)
            throws IOException {
        return PresplitTestSupport.writeParquetFixture(tempDirectory, schema, rowCount, rowFiller);
    }

    @Test
    void readsDatetimeStatisticsForNonUtcMillisTimestamp() throws Exception {
        // epoch milli 0 = 1970-01-01 00:00:00 UTC; +1000ms per row. isAdjustedToUTC=false
        // means the stored ticks ARE the wall clock, so no tz math is needed.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("event_ts", rowIndex * 1000L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_ts", DateType.DATETIME));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        Assertions.assertEquals("1970-01-01 00:00:00",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("1970-01-01 00:00:02",
                stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsDatetimeStatisticsForNonUtcMicrosTimestampWithFraction() throws Exception {
        // 1_500_000 micros = 1.5s → sub-second boundary text exercises the micros render path.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", 1_500_000L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_ts", DateType.DATETIME));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals("1970-01-01 00:00:01.500000",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsDatetimeStatisticsForNonUtcNanosTruncatedToMicros() throws Exception {
        // 1_500_000_500 ns = 1.5000005 s. StarRocks DATETIME is microsecond-precision and BE
        // truncates nanos→micros at storage (of_epoch_second divides nanos by 1000), so the
        // boundary must render ".500000" (the trailing 500 ns dropped), matching the loaded value.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.NANOS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", 1_500_000_500L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_ts", DateType.DATETIME));

        Assertions.assertEquals("1970-01-01 00:00:01.500000",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void pre1970TimestampFallsBackToDataTier() throws Exception {
        // -1000 ms = 1969-12-31 23:59:59 < 1970-01-01: outside the safe window, where the FE
        // floorDiv/floorMod split is not proven equal to BE's signed C++ division. Defer to data tier.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_ts", -1000L - rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("event_ts", DateType.DATETIME)));
    }

    @Test
    void utcAdjustedTimestampFallsBackToDataTier() throws Exception {
        // isAdjustedToUTC=true: the load applies a session-tz offset the FE reader cannot
        // reproduce here, so meta tier must defer to data tier rather than risk an offset boundary.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_ts", rowIndex * 1000L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("event_ts", DateType.DATETIME)));
    }

    @Test
    void timestampIntoNonDatetimeSortKeyFallsBackToDataTier() throws Exception {
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_ts", rowIndex * 1000L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("event_ts", IntegerType.BIGINT)));
    }

    @Test
    void readsDecimalStatisticsForInt32Decimal() throws Exception {
        // INT32-backed DECIMAL(9,2): precision 9 fits int32. Unscaled (rowIndex+1)*100 → 1.00..3.00.
        Path parquetPath = writeParquet(
                "message schema { required int32 d (DECIMAL(9,2)); }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("d", (rowIndex + 1) * 100));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2)));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals("1.00", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("3.00", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsDecimalStatisticsForInt64Decimal() throws Exception {
        // INT64-backed DECIMAL(18,2): unscaled (rowIndex+1)*100 → 1.00, 2.00, ... 5.00.
        // Signed INT64 order == decimal order, so footer min/max are safe to use.
        Path parquetPath = writeParquet(
                "message schema { required int64 d (DECIMAL(18,2)); }",
                /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("d", (rowIndex + 1) * 100L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        Assertions.assertEquals("1.00", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("5.00", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    private Path writeParquet(
            String schemaText, int rowCount,
            java.util.function.BiConsumer<org.apache.parquet.example.data.Group, Integer> rowFiller)
            throws IOException {
        return PresplitTestSupport.writeParquetFixture(tempDirectory, schemaText, rowCount, rowFiller);
    }
}
