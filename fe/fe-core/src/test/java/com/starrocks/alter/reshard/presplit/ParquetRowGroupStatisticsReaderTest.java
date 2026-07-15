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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("sort_key", IntegerType.BIGINT)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", VarcharType.VARCHAR)), null);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        String globalMin = null;
        String globalMax = null;
        for (RowGroupStatistics rowGroup : rowGroupStatistics) {
            // String chunk stats are exact in practice (parquet-cpp/parquet-mr do not
            // truncate chunk-level Statistics); trust them for a meta-tier split.
            Assertions.assertFalse(rowGroup.isTruncated());
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
    void readsCharStatistics() throws Exception {
        // A CHAR(N) sort key is meta-tier-eligible: the BE right-pads a CHAR routing key with '\0'
        // to its fixed width before routing, but '\0'-padding is order-preserving under the BE
        // unsigned memcmp + shorter-prefix tiebreak and the boundary is stored stripped, so a
        // NUL-free CHAR boundary separates rows exactly as VARCHAR does. Same footer min/max as
        // readsVarcharStatistics, just with a CHAR(16) target column.
        Path parquetPath = writeParquet(
                "message schema { required binary tenant (UTF8); }",
                /*rowCount=*/ 16,
                (group, rowIndex) -> group.append("tenant", String.format("tenant-%02d", rowIndex)));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", TypeFactory.createCharType(16))), null);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        String globalMin = null;
        String globalMax = null;
        for (RowGroupStatistics rowGroup : rowGroupStatistics) {
            Assertions.assertFalse(rowGroup.isTruncated());
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
    void charSortKeyWithNulCanonicalizedToPrefix() throws Exception {
        // A CHAR value is defined only up to its first '\0' (the BE strnlen-truncates it), so a CHAR
        // StringVariant canonicalizes a boundary to the prefix; VARCHAR keeps the raw bytes.
        Path parquetPath = writeParquet(
                "message schema { required binary tenant (UTF8); }",
                /*rowCount=*/ 4,
                (group, rowIndex) -> group.append("tenant", "a\u0000z-" + rowIndex));

        // CHAR target: min/max canonicalized to the prefix before the first NUL ("a"), no NUL kept.
        List<RowGroupStatistics> charStats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", TypeFactory.createCharType(16))), null);
        Assertions.assertFalse(charStats.isEmpty());
        for (RowGroupStatistics rowGroup : charStats) {
            Assertions.assertEquals("a", rowGroup.getMinTuple().getValues().get(0).getStringValue());
            Assertions.assertEquals("a", rowGroup.getMaxTuple().getValues().get(0).getStringValue());
        }

        // VARCHAR target: same NUL data keeps the raw bytes (BE does not strnlen a VARCHAR boundary).
        List<RowGroupStatistics> varcharStats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", VarcharType.VARCHAR)), null);
        Assertions.assertFalse(varcharStats.isEmpty());
        for (RowGroupStatistics rowGroup : varcharStats) {
            Assertions.assertTrue(rowGroup.getMinTuple().getValues().get(0).getStringValue().indexOf('\0') >= 0);
        }
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
                        List.of(new Column("opaque_bytes", VarcharType.VARCHAR)), null));
    }

    @Test
    void readsIntStatisticsForParquetInt32() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int32 region_id; }",
                /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("region_id", rowIndex + 100));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("region_id", IntegerType.INT)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("flag", BooleanType.BOOLEAN)), null);

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
                        List.of(new Column("missing_sort_key", IntegerType.BIGINT)), null));
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
                        List.of(new Column("payload", IntegerType.BIGINT)), null));
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
                        List.of(new Column("region_id", VarcharType.VARCHAR)), null));
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
                        List.of(new Column("sort_key", IntegerType.BIGINT)), null));
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
                        List.of(new Column("wide_value", IntegerType.TINYINT)), null));
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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("sort_key", IntegerType.BIGINT)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_day", DateType.DATE)), null);

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
                        List.of(new Column("event_day", IntegerType.BIGINT)), null));
    }

    @Test
    void pre1970DateIsAccepted() throws Exception {
        // epochDay -1 = 1969-12-31. A DATE has no sub-second part and BE's day-of-epoch load is
        // proleptic-Gregorian end to end, so a pre-1970 DATE boundary is FE/BE-identical and stays
        // on the meta tier. DATE and DATETIME share the [0001-01-01, 9999-12-31] window.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_day", -1 - rowIndex));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_day", DateType.DATE)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_day", DateType.DATE)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_day", DateType.DATE)), null);

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
                        List.of(new Column("event_day", DateType.DATE)), null));
    }

    @Test
    void maxSupportedDateIsAccepted() throws Exception {
        // epochDay 2932896 = 9999-12-31, the upper edge of the safe window — accepted.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_day", 2932896));

        List<RowGroupStatistics> rowGroupStatistics = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_day", DateType.DATE)), null);

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
                        List.of(new Column("event_day", DateType.DATE)), null));
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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), null);

        Assertions.assertEquals("1970-01-01 00:00:01.500000",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void pre1970TimestampIsAccepted() throws Exception {
        // -2000 ms = 1969-12-31 23:59:58, -1000 ms = 1969-12-31 23:59:59: both before the epoch but
        // inside [0001-01-01, 9999-12-31]. The FE floorDiv/floorMod split equals the BE timestamp
        // load (which borrows a whole second for a negative sub-second remainder), so a pre-1970
        // DATETIME boundary is FE/BE-identical and stays on the meta tier.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_ts", -2000L + rowIndex * 1000L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), null);

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals("1969-12-31 23:59:58",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("1969-12-31 23:59:59",
                stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void pre1970TimestampWithSubSecondIsAccepted() throws Exception {
        // -500 ms = 1969-12-31 23:59:59.500000: the negative sub-second case the BE floor-borrow fix
        // makes load-correct. floorDiv(-500, 1000) = -1 s, floorMod(-500, 1000) = 500 ms → .500000.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", -500L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), null);

        Assertions.assertEquals("1969-12-31 23:59:59.500000",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void pre1582TimestampIsAccepted() throws Exception {
        // 1500-06-15 12:00:00 UTC, before the 1582 Gregorian cutover. BE's calendar is proleptic
        // Gregorian end to end (load and boundary parse both pack through the same from_date), so a
        // pre-1582 DATETIME boundary stays on the meta tier.
        long millis = LocalDateTime.of(1500, 6, 15, 12, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1000L;
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", millis));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), null);

        Assertions.assertEquals("1500-06-15 12:00:00",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void postYear9999TimestampFallsBackToDataTier() throws Exception {
        // 253402300800000 ms = 10000-01-01 00:00:00 UTC, year > 9999: above the window → data tier.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ false),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", 253402300800000L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        List.of(new Column("event_ts", DateType.DATETIME)), null));
    }

    @Test
    void utcAdjustedTimestampFallsBackToDataTier() throws Exception {
        // No load timezone (null) -> no fixed offset resolvable -> the meta tier cannot match the BE's
        // session-tz offset for a UTC-adjusted timestamp, so it defers to the data tier.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_ts", rowIndex * 1000L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        List.of(new Column("event_ts", DateType.DATETIME)), null));
    }

    @Test
    void utcAdjustedTimestampWithFixedOffsetReachesMetaTier() throws Exception {
        // isAdjustedToUTC=true stores the UTC instant; a +08:00 load adds a constant +8h offset (the BE
        // does the same for a fixed-offset zone). epoch milli 0 -> 1970-01-01 08:00:00 local; +1000ms/row.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("event_ts", rowIndex * 1000L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), "+08:00");

        Assertions.assertFalse(stats.isEmpty());
        String globalMin = null;
        String globalMax = null;
        for (RowGroupStatistics rowGroup : stats) {
            Assertions.assertFalse(rowGroup.isTruncated());
            String minValue = rowGroup.getMinTuple().getValues().get(0).getStringValue();
            String maxValue = rowGroup.getMaxTuple().getValues().get(0).getStringValue();
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals("1970-01-01 08:00:00", globalMin);
        Assertions.assertEquals("1970-01-01 08:00:02", globalMax);
    }

    @Test
    void utcAdjustedTimestampWithFixedOffsetKeepsMicroseconds() throws Exception {
        // MICROS sub-second must survive the offset add. tick 1_500_123 us = 1.500123 s UTC; +8h ->
        // 1970-01-01 08:00:01.500123 local.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", 1_500_123L));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_ts", DateType.DATETIME)), "+08:00");

        Assertions.assertEquals("1970-01-01 08:00:01.500123",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void utcAdjustedTimestampWithDstZoneFallsBackToDataTier() throws Exception {
        // A DST zone applies a per-instant offset the meta tier cannot reproduce with one scalar -> data tier.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("event_ts", rowIndex * 1000L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        List.of(new Column("event_ts", DateType.DATETIME)), "America/New_York"));
    }

    @Test
    void utcAdjustedTimestampOffsetCrossesDate() throws Exception {
        // The offset must roll the calendar date, not just the time. UTC 1969-12-31 20:00:00
        // (tick -14400000 ms) + 08:00 -> 1970-01-01 04:00:00 local; and a negative offset rolls back:
        // UTC 1970-01-01 02:00:00 (tick 7200000) + (-05:00) -> 1969-12-31 21:00:00 local.
        Path plusPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", -14_400_000L));
        Assertions.assertEquals("1970-01-01 04:00:00",
                ParquetRowGroupStatisticsReader.read(PresplitTestSupport.statusOf(plusPath), new Configuration(),
                        List.of(new Column("event_ts", DateType.DATETIME)), "+08:00")
                        .get(0).getMinTuple().getValues().get(0).getStringValue());

        Path minusPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", 7_200_000L));
        Assertions.assertEquals("1969-12-31 21:00:00",
                ParquetRowGroupStatisticsReader.read(PresplitTestSupport.statusOf(minusPath), new Configuration(),
                        List.of(new Column("event_ts", DateType.DATETIME)), "-05:00")
                        .get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void utcAdjustedTimestampOutsideWindowAfterOffsetFallsBackToDataTier() throws Exception {
        // The window gate runs on the OFFSET-ADJUSTED local date. A UTC value inside [0001,9999] can be
        // pushed past 9999-12-31 by a positive offset -> data tier (the BE would store an out-of-domain
        // DATETIME). MAX MILLIS tick 253402300799000 = 9999-12-31 23:59:59 UTC; +08:00 -> year 10000.
        Path parquetPath = writeParquet(
                timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, /*isAdjustedToUTC=*/ true),
                /*rowCount=*/ 1,
                (group, rowIndex) -> group.append("event_ts", 253_402_300_799_000L));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        List.of(new Column("event_ts", DateType.DATETIME)), "+08:00"));
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
                        List.of(new Column("event_ts", IntegerType.BIGINT)), null));
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
                List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2))), null);

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
                List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2))), null);

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
                List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2))), null);

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
                        List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))), null));
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
                        List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2))), null));
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
                        List.of(new Column("d", IntegerType.BIGINT)), null));
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
                List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2))), null);

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
                List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 2))), null);

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
                List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 2))), null);

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
                        List.of(new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))), null));
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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("u", IntegerType.SMALLINT)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), List.of(new Column("u", IntegerType.INT)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("u", IntegerType.BIGINT)), null);

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
                        List.of(new Column("u", IntegerType.BIGINT)), null));
    }

    @Test
    void uint32IntoTooNarrowTargetFallsBackToDataTier() throws Exception {
        // UINT_32 ~3e9 into a StarRocks INT (max 2^31-1): IntVariant range-check fails → data tier.
        Path parquetPath = writeParquet(unsignedIntSchema(32), /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("u", (int) (3_000_000_000L + rowIndex)));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                        List.of(new Column("u", IntegerType.INT)), null));
    }

    @Test
    void readsUint32IntoIntWithinRange() throws Exception {
        // UINT_32 values <= Integer.MAX_VALUE map into a StarRocks INT at the meta tier (the BE
        // INT32->INT pass-through equals the unsigned magnitude below 2^31). Admitted, not rejected.
        Path parquetPath = writeParquet(unsignedIntSchema(32), /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("u", 2_000_000_000 + rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(), List.of(new Column("u", IntegerType.INT)), null);

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
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("u", IntegerType.TINYINT)), null);

        Assertions.assertEquals("100", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("104", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsCompositeBoundingBoxAcrossColumns() throws Exception {
        // (tenant VARCHAR, position BIGINT) composite sort key. tenant changes slowly,
        // position ascends; the per-column footer stats compose a bounding-box tuple.
        Path parquetPath = writeParquet(
                "message schema { required binary tenant (UTF8); required int64 position; }",
                /*rowCount=*/ 64,
                (group, rowIndex) -> {
                    group.append("tenant", String.format("tenant-%02d", rowIndex / 16));
                    group.append("position", (long) rowIndex);
                });

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", VarcharType.VARCHAR), new Column("position", IntegerType.BIGINT)),
                null);

        Assertions.assertFalse(stats.isEmpty());
        String globalMinTenant = null;
        long globalMinPos = Long.MAX_VALUE;
        long globalMaxPos = Long.MIN_VALUE;
        for (RowGroupStatistics rg : stats) {
            Assertions.assertNotNull(rg.getMinTuple());
            Assertions.assertNotNull(rg.getMaxTuple());
            // arity 2, in sort-key order (tenant, position)
            Assertions.assertEquals(2, rg.getMinTuple().getValues().size());
            Assertions.assertEquals(2, rg.getMaxTuple().getValues().size());
            String minTenant = rg.getMinTuple().getValues().get(0).getStringValue();
            String maxTenant = rg.getMaxTuple().getValues().get(0).getStringValue();
            long minPos = Long.parseLong(rg.getMinTuple().getValues().get(1).getStringValue());
            long maxPos = Long.parseLong(rg.getMaxTuple().getValues().get(1).getStringValue());
            Assertions.assertTrue(minTenant.compareTo(maxTenant) <= 0);
            Assertions.assertTrue(minPos <= maxPos);
            globalMinTenant = (globalMinTenant == null || minTenant.compareTo(globalMinTenant) < 0)
                    ? minTenant : globalMinTenant;
            globalMinPos = Math.min(globalMinPos, minPos);
            globalMaxPos = Math.max(globalMaxPos, maxPos);
        }
        Assertions.assertEquals("tenant-00", globalMinTenant);
        Assertions.assertEquals(0L, globalMinPos);
        Assertions.assertEquals(63L, globalMaxPos);
    }

    @Test
    void compositeRejectsWhenAnyColumnUnsupported() {
        // tenant maps fine, but a LARGEINT sort-key column is outside the meta-tier integer
        // window (isIntegerType() is exactly {TINYINT, SMALLINT, INT, BIGINT}; LARGEINT is
        // excluded) -> whole file falls back to data tier. LARGEINT is a real, DDL-legal sort
        // key, so this is a reachable rejection (not an impossible-scenario test).
        Assertions.assertThrows(MetaTierUnavailableException.class, () -> {
            Path parquetPath = writeParquet(
                    "message schema { required binary tenant (UTF8); required int64 position; }",
                    /*rowCount=*/ 8,
                    (group, rowIndex) -> {
                        group.append("tenant", String.format("t-%02d", rowIndex));
                        group.append("position", (long) rowIndex);
                    });
            ParquetRowGroupStatisticsReader.read(
                    PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                    List.of(new Column("tenant", VarcharType.VARCHAR),
                            new Column("position", IntegerType.LARGEINT)),
                    null);
        });
    }

    @Test
    void compositeEmitsNullTupleWhenAnyColumnHasNoStats() throws Exception {
        // position is all-null (optional, never appended) -> its chunk has hasNonNullValue()=false
        // -> the whole row-group tuple is null (data-tier fallback downstream).
        Path parquetPath = writeParquet(
                "message schema { required binary tenant (UTF8); optional int64 position; }",
                /*rowCount=*/ 8,
                (group, rowIndex) -> group.append("tenant", String.format("t-%02d", rowIndex)));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", VarcharType.VARCHAR), new Column("position", IntegerType.BIGINT)),
                null);

        Assertions.assertFalse(stats.isEmpty());
        for (RowGroupStatistics rg : stats) {
            Assertions.assertNull(rg.getMinTuple(), "any-column-missing must null the whole tuple");
            Assertions.assertNull(rg.getMaxTuple());
        }
    }

    @Test
    void readsCompositeDateThenIntBoundingBox() throws Exception {
        // Mixed (DATE, INT) key exercises per-column DATE-window gate + integer column together.
        // Assert AGGREGATE global min/max across row groups (the writer may emit several; a
        // per-row-group "min == 2024-01-01" assertion would be false for later row groups).
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); required int32 region; }",
                /*rowCount=*/ 8,
                (group, rowIndex) -> {
                    group.append("event_day", (int) java.time.LocalDate.of(2024, 1, 1 + rowIndex).toEpochDay());
                    group.append("region", rowIndex + 10);
                });

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("event_day", DateType.DATE), new Column("region", IntegerType.INT)),
                null);

        Assertions.assertFalse(stats.isEmpty());
        String globalMinDate = null;
        long globalMinRegion = Long.MAX_VALUE;
        for (RowGroupStatistics rg : stats) {
            Assertions.assertEquals(2, rg.getMinTuple().getValues().size());
            String minDate = rg.getMinTuple().getValues().get(0).getStringValue();
            long minRegion = Long.parseLong(rg.getMinTuple().getValues().get(1).getStringValue());
            globalMinDate = (globalMinDate == null || minDate.compareTo(globalMinDate) < 0) ? minDate : globalMinDate;
            globalMinRegion = Math.min(globalMinRegion, minRegion);
        }
        Assertions.assertEquals("2024-01-01", globalMinDate);
        Assertions.assertEquals(10L, globalMinRegion);
    }

    @Test
    void compositeUsesNonNullFooterMinMaxWhenALeadingColumnHasNulls() throws Exception {
        // Parity with the single-column meta tier: a partial-null column does NOT trigger a
        // fallback -- the reader uses the non-null footer min/max. Data safety comes from the
        // BE routing every row (incl. nulls) by its true value, not from the box being a
        // strict lower bound. Here tenant is optional and null on even rows; the emitted box
        // still carries the non-null tenant min ("tenant-000") and full position range.
        Path parquetPath = writeParquet(
                "message schema { optional binary tenant (UTF8); required int64 position; }",
                /*rowCount=*/ 16,
                (group, rowIndex) -> {
                    if (rowIndex % 2 == 1) {
                        group.append("tenant", String.format("tenant-%03d", rowIndex));
                    }
                    group.append("position", (long) rowIndex);
                });

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                PresplitTestSupport.statusOf(parquetPath), new Configuration(),
                List.of(new Column("tenant", VarcharType.VARCHAR), new Column("position", IntegerType.BIGINT)),
                null);

        Assertions.assertFalse(stats.isEmpty());
        for (RowGroupStatistics rg : stats) {
            // A row group whose tenant chunk still has at least one non-null value produces a
            // non-null arity-2 box; a row group whose tenant chunk is entirely null yields a
            // null tuple (hasNonNullValue()==false), which is the existing all-null fallback.
            if (rg.getMinTuple() != null) {
                Assertions.assertEquals(2, rg.getMinTuple().getValues().size());
                Assertions.assertNotNull(rg.getMinTuple().getValues().get(0).getStringValue());
            }
        }
    }

    private Path writeParquet(
            String schemaText, int rowCount,
            java.util.function.BiConsumer<org.apache.parquet.example.data.Group, Integer> rowFiller)
            throws IOException {
        return PresplitTestSupport.writeParquetFixture(tempDirectory, schemaText, rowCount, rowFiller);
    }
}
