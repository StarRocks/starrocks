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
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
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
    void pre1970DateIsAccepted() throws Exception {
        // epochDay -1 = 1969-12-31. A DATE has no sub-second part and BE's day-of-epoch load is
        // proleptic-Gregorian end to end, so a pre-1970 DATE boundary is FE/BE-identical and stays
        // on the meta tier (only DATETIME keeps the 1970 lower bound).
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_day", -1 - rowIndex));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertEquals("1969-12-31",
                rowGroupStatistics.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void pre1582DateIsAccepted() throws Exception {
        // epochDay -171499 = 1500-06-15, before the 1582 Gregorian cutover. BE's calendar is
        // proleptic Gregorian (no Julian switch), so this still aligns and stays on the meta tier.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_day", -171499));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertEquals("1500-06-15",
                rowGroupStatistics.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void minSupportedDateIsAccepted() throws Exception {
        // epochDay -719162 = 0001-01-01, the lower edge of the DATE window — accepted.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_day", -719162));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertEquals(1, rowGroupStatistics.size());
        Assertions.assertEquals("0001-01-01",
                rowGroupStatistics.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void belowMinSupportedDateFallsBackToDataTier() throws Exception {
        // epochDay -719163 = 0000-12-31, below 0001-01-01: outside the DATE window → data tier.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_day", -719163));

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
        Assertions.assertFalse(stats.get(0).isTruncated());
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

    @Test
    void readsNegativeDecimalPreservesSignedOrder() throws Exception {
        // INT64-backed DECIMAL(18,2) spanning negative→positive: -2.00, -1.00, 0.00, 1.00.
        // parquet-mr orders INT64 stats by SIGNED comparison, so min is the most negative.
        Path parquetPath = writeParquet(
                "message schema { required int64 d (DECIMAL(18,2)); }",
                /*rowCount=*/ 4,
                (group, rowIndex) -> group.append("d", (rowIndex - 2) * 100L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals("-2.00", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("1.00", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void decimalScaleMismatchFallsBackToDataTier() throws Exception {
        // Source DECIMAL(18,2) into a StarRocks DECIMAL64(18,4): scales differ → reject.
        Path parquetPath = writeParquet(
                "message schema { required int64 d (DECIMAL(18,2)); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("d", (rowIndex + 1) * 100L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))));
    }

    @Test
    void decimalPrecisionMismatchFallsBackToDataTier() throws Exception {
        // Source DECIMAL(18,2) into a StarRocks DECIMAL64(10,2): precisions differ → reject.
        Path parquetPath = writeParquet(
                "message schema { required int64 d (DECIMAL(18,2)); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("d", (rowIndex + 1) * 100L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2))));
    }

    @Test
    void decimalIntoNonDecimalSortKeyFallsBackToDataTier() throws Exception {
        // Source DECIMAL into a BIGINT sort key → reject (publishing unscaled ints would be wrong).
        Path parquetPath = writeParquet(
                "message schema { required int64 d (DECIMAL(18,2)); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("d", (rowIndex + 1) * 100L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("d", IntegerType.BIGINT)));
    }

    @Test
    void readsFixedLenByteArrayDecimalStatistics() throws Exception {
        // FLBA(8)-backed DECIMAL(18,2) spanning negative→positive: -2.00, -1.00, 0.00, 1.00.
        // parquet-mr writes column_orders=TypeDefinedOrder, so the byte-array min/max are ordered by
        // the signed BINARY_AS_SIGNED_INTEGER_COMPARATOR — the gate accepts and decodes them. The
        // negative span proves signed (not unsigned) ordering end-to-end, including the raw-footer read.
        Path parquetPath = writeParquet(
                "message schema { required fixed_len_byte_array(8) d (DECIMAL(18,2)); }",
                /*rowCount=*/ 4,
                (group, rowIndex) -> group.append("d", flbaDecimal(BigInteger.valueOf((rowIndex - 2) * 100L), 8)));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        Assertions.assertEquals("-2.00", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("1.00", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    /**
     * Encode {@code unscaled} as a fixed-length, big-endian two's-complement byte array of
     * {@code byteLen} bytes (sign-extended) — the FIXED_LEN_BYTE_ARRAY layout parquet uses for
     * DECIMAL. parquet-mr computes the footer min/max with its signed byte-array comparator.
     */
    private static Binary flbaDecimal(BigInteger unscaled, int byteLen) {
        byte[] full = new byte[byteLen];
        java.util.Arrays.fill(full, (byte) (unscaled.signum() < 0 ? 0xFF : 0x00));
        byte[] minimal = unscaled.toByteArray();
        System.arraycopy(minimal, 0, full, byteLen - minimal.length, minimal.length);
        return Binary.fromConstantByteArray(full);
    }

    @Test
    void readsBinaryBackedDecimalStatistics() throws Exception {
        // BINARY-backed DECIMAL128(20,2) spanning negative→positive: a negative value and a 20-digit
        // unscaled value (> 64-bit) exercise the variable-length signed decode, the 128-bit path, and
        // signed (not unsigned) ordering. parquet-mr's column_orders=TypeDefinedOrder makes the
        // byte-array min/max signed; a decimal stat must NOT be marked truncated (reserved for strings).
        BigInteger negative = BigInteger.valueOf(-100L);             // -1.00
        BigInteger large = new BigInteger("99999999999999999999");   // 20-digit unscaled
        Path parquetPath = writeParquet(
                "message schema { required binary d (DECIMAL(20,2)); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("d", binaryDecimal(rowIndex == 0 ? negative : large)));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 2)));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        // min is the negative value (proves signed order); max asserted by VALUE (unscaled `large`).
        Assertions.assertEquals("-1.00", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals(0, new BigDecimal(large, 2).compareTo(
                new BigDecimal(stats.get(0).getMaxTuple().getValues().get(0).getStringValue())));
    }

    /** Encode {@code unscaled} as a minimal big-endian two's-complement byte array (variable-length
     * BINARY DECIMAL layout). parquet-mr computes the footer min/max with its signed comparator. */
    private static Binary binaryDecimal(BigInteger unscaled) {
        return Binary.fromConstantByteArray(unscaled.toByteArray());
    }

    @Test
    void readsFixedLenByteArrayDecimal128Statistics() throws Exception {
        // Mirrors StarRocks' own unload: BE encodes DECIMAL128 as FLBA(16). DECIMAL(38,2) with a
        // 38-digit unscaled value exercises the full 16-byte signed decode — the case that fell to
        // the data tier before this phase.
        BigInteger small = BigInteger.valueOf(100L);                                       // 1.00
        BigInteger large = new BigInteger("99999999999999999999999999999999999999");       // 38-digit unscaled
        Path parquetPath = writeParquet(
                "message schema { required fixed_len_byte_array(16) d (DECIMAL(38,2)); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("d", flbaDecimal(rowIndex == 0 ? small : large, 16)));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 2)));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        Assertions.assertEquals("1.00", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        // Assert the max by VALUE (the unscaled `large` at scale 2), not a hand-counted digit string.
        Assertions.assertEquals(0, new BigDecimal(large, 2).compareTo(
                new BigDecimal(stats.get(0).getMaxTuple().getValues().get(0).getStringValue())));
    }

    @Test
    void fixedLenByteArrayDecimalScaleMismatchFallsBackToDataTier() throws Exception {
        // A byte-array decimal still requires an EXACT precision/scale match, independent of the
        // (TypeDefinedOrder) column order: source DECIMAL(18,2) into a StarRocks DECIMAL64(18,4).
        Path parquetPath = writeParquet(
                "message schema { required fixed_len_byte_array(8) d (DECIMAL(18,2)); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("d", flbaDecimal(BigInteger.valueOf((rowIndex + 1) * 100L), 8)));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))));
    }

    @Test
    void declaresTypeDefinedColumnOrderRequiresTypeOrderEntry() {
        // parquet-mr's high-level writer always emits column_orders, so the "no column_orders /
        // unknown order → data tier" rejection is verified at the raw-footer-mapping predicate level.
        ColumnOrder typeOrder = ColumnOrder.TYPE_ORDER(new TypeDefinedOrder());
        // Footer positively declares TypeDefinedOrder for the leaf → signed order confirmed.
        Assertions.assertTrue(
                ParquetRowGroupStatisticsReader.declaresTypeDefinedColumnOrder(List.of(typeOrder), 0));
        // Legacy file with no column_orders → not confirmed.
        Assertions.assertFalse(
                ParquetRowGroupStatisticsReader.declaresTypeDefinedColumnOrder(null, 0));
        // Entry present but no TypeDefinedOrder set (UNDEFINED order) → not confirmed.
        Assertions.assertFalse(
                ParquetRowGroupStatisticsReader.declaresTypeDefinedColumnOrder(List.of(new ColumnOrder()), 0));
        // Leaf index past the end of the list → not confirmed.
        Assertions.assertFalse(
                ParquetRowGroupStatisticsReader.declaresTypeDefinedColumnOrder(List.of(typeOrder), 5));
    }

    private static MessageType unsignedIntSchema(int bitWidth) {
        return Types.buildMessage()
                .required(bitWidth <= 32 ? PrimitiveTypeName.INT32 : PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.intType(bitWidth, /*signed=*/ false))
                .named("u")
                .named("schema");
    }

    @Test
    void readsUint8Statistics() throws Exception {
        // UINT_8 (INT32-backed), values 250..254 — all < 2^31, decode identical to signed.
        Path parquetPath = writeParquet(unsignedIntSchema(8), /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("u", 250 + rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("u", IntegerType.SMALLINT));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        Assertions.assertEquals("250", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("254", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsUint16Statistics() throws Exception {
        // UINT_16 (INT32-backed), values 65530..65534 — all < 2^31.
        Path parquetPath = writeParquet(unsignedIntSchema(16), /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("u", 65530 + rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("u", IntegerType.INT));

        Assertions.assertEquals("65530", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("65534", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsUint32StatisticsWithHighBitSet() throws Exception {
        // UINT_32 values ~3e9 are stored as NEGATIVE int32 bit patterns. Integer.toString would
        // sign-extend them; the unsigned decode must surface the true magnitude (~3e9), monotone.
        // (Integer) 3_000_000_000 overflows int literal range, so write via (int) of a long.
        Path parquetPath = writeParquet(unsignedIntSchema(32), /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("u", (int) (3_000_000_000L + rowIndex)));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("u", IntegerType.BIGINT));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertFalse(stats.get(0).isTruncated());
        Assertions.assertEquals("3000000000", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("3000000002", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void uint64FallsBackToDataTier() throws Exception {
        // UINT_64 (INT64-backed): no signed-64 StarRocks fit (BIGINT max < 2^64), so it is rejected.
        Path parquetPath = writeParquet(unsignedIntSchema(64), /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("u", (long) rowIndex));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("u", IntegerType.BIGINT)));
    }

    @Test
    void uint32IntoTooNarrowTargetFallsBackToDataTier() throws Exception {
        // UINT_32 ~3e9 into a StarRocks INT (max 2^31-1): IntVariant range-check fails → data tier.
        Path parquetPath = writeParquet(unsignedIntSchema(32), /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("u", (int) (3_000_000_000L + rowIndex)));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        new Column("u", IntegerType.INT)));
    }

    @Test
    void readsUint32IntoIntWithinRange() throws Exception {
        // UINT_32 values <= Integer.MAX_VALUE map into a StarRocks INT at the meta tier (the BE
        // INT32->INT pass-through equals the unsigned magnitude below 2^31). Admitted, not rejected.
        Path parquetPath = writeParquet(unsignedIntSchema(32), /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("u", 2_000_000_000 + rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("u", IntegerType.INT));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals("2000000000", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("2000000002", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsUint8IntoTinyint() throws Exception {
        // UINT_8 values 0..127 map into a StarRocks TINYINT at the meta tier (narrowed admitted path:
        // int8(magnitude) == magnitude within range). Admitted, not rejected.
        Path parquetPath = writeParquet(unsignedIntSchema(8), /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("u", 100 + rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), new Column("u", IntegerType.TINYINT));

        Assertions.assertEquals("100", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("104", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    private Path writeParquet(
            String schemaText, int rowCount,
            java.util.function.BiConsumer<org.apache.parquet.example.data.Group, Integer> rowFiller)
            throws IOException {
        return PresplitTestSupport.writeParquetFixture(tempDirectory, schemaText, rowCount, rowFiller);
    }
}
