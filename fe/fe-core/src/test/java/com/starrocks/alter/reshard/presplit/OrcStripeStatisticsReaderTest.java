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
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

class OrcStripeStatisticsReaderTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void readsBigintStatisticsAcrossWholeFile() throws Exception {
        // Tiny stripe size (see writeOrcFixture) coaxes the writer into multiple
        // stripes; the exact count is implementation-defined so the assertions
        // exercise aggregate invariants only.
        Path orcPath = writeOrc(
                "struct<sort_key:bigint>",
                /*rowCount=*/ 64,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertFalse(stripeStatistics.isEmpty());
        long totalRowCount = 0L;
        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (RowGroupStatistics stripe : stripeStatistics) {
            Assertions.assertNotNull(stripe.getMinTuple());
            Assertions.assertNotNull(stripe.getMaxTuple());
            Assertions.assertFalse(stripe.isTruncated());
            long minValue = Long.parseLong(stripe.getMinTuple().getValues().get(0).getStringValue());
            long maxValue = Long.parseLong(stripe.getMaxTuple().getValues().get(0).getStringValue());
            Assertions.assertTrue(minValue <= maxValue, "stripe min " + minValue + " > max " + maxValue);
            totalRowCount += stripe.getRowCount();
            globalMin = Math.min(globalMin, minValue);
            globalMax = Math.max(globalMax, maxValue);
        }
        Assertions.assertEquals(64L, totalRowCount);
        Assertions.assertEquals(0L, globalMin);
        Assertions.assertEquals(63L, globalMax);
    }

    @Test
    void readsIntStatistics() throws Exception {
        Path orcPath = writeOrc(
                "struct<region_id:int>",
                /*rowCount=*/ 5,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex + 100);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("region_id", IntegerType.INT));

        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (RowGroupStatistics stripe : stripeStatistics) {
            globalMin = Math.min(globalMin, Long.parseLong(stripe.getMinTuple().getValues().get(0).getStringValue()));
            globalMax = Math.max(globalMax, Long.parseLong(stripe.getMaxTuple().getValues().get(0).getStringValue()));
        }
        Assertions.assertEquals(100L, globalMin);
        Assertions.assertEquals(104L, globalMax);
    }

    @Test
    void caseInsensitiveColumnMatchResolves() throws Exception {
        // ORC field "Sort_Key" must match StarRocks column "sort_key" (names are
        // case-insensitive in StarRocks).
        Path orcPath = writeOrc(
                "struct<Sort_Key:bigint>",
                /*rowCount=*/ 4,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertFalse(stripeStatistics.isEmpty());
        Assertions.assertNotNull(stripeStatistics.get(0).getMinTuple());
    }

    @Test
    void stringColumnFallsBackToDataTier() throws Exception {
        // ORC string stats would always need data-tier fallback; v1 rejects the
        // type eagerly rather than wiring a string-stats path.
        Path orcPath = writeOrc(
                "struct<tenant:string>",
                /*rowCount=*/ 4,
                (batch, batchRow, rowIndex) -> ((BytesColumnVector) batch.cols[0])
                        .setVal(batchRow, ("tenant-" + rowIndex).getBytes(StandardCharsets.UTF_8)));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("tenant", VarcharType.VARCHAR)));
    }

    @Test
    void unsupportedOrcTypeFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<payload:double>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((DoubleColumnVector) batch.cols[0]).vector[batchRow] = rowIndex * 1.5);

        // DOUBLE is outside the meta-tier mapping window even for a numeric sort key.
        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("payload", IntegerType.BIGINT)));
    }

    @Test
    void mismatchedStarRocksTypeFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<region_id:bigint>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        // ORC integer stats cannot route into a VARCHAR sort-key column.
        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("region_id", VarcharType.VARCHAR)));
    }

    @Test
    void columnAbsentFromSchemaFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<other:bigint>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("missing_sort_key", IntegerType.BIGINT)));
    }

    @Test
    void caseVariantDuplicatesFallBackToDataTier() throws Exception {
        // StarRocks column names are case-insensitive, so two ORC fields differing
        // only by case are ambiguous and the reader cannot pick one silently.
        Path orcPath = writeOrc(
                "struct<sort_key:bigint,SORT_KEY:bigint>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> {
                    ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex;
                    ((LongColumnVector) batch.cols[1]).vector[batchRow] = -rowIndex;
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT)));
    }

    @Test
    void outOfRangeStatsValueFallsBackToDataTier() throws Exception {
        // ORC bigint stats (260) outside StarRocks TINYINT range; the value-conversion
        // failure must surface as a meta-tier fallback, not a hard error.
        Path orcPath = writeOrc(
                "struct<wide_value:bigint>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = 260L + rowIndex);

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("wide_value", IntegerType.TINYINT)));
    }

    @Test
    void allNullStripeReportsAbsentStatistics() throws Exception {
        // sort_key is left null on every row (keepalive keeps the file non-empty).
        // ORC integer stats expose no presence flag, so the reader must consult
        // getNumberOfValues() and emit absent min/max rather than a bogus 0.
        Path orcPath = writeOrc(
                "struct<sort_key:bigint,keepalive:bigint>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) -> {
                    LongColumnVector sortKey = (LongColumnVector) batch.cols[0];
                    sortKey.noNulls = false;
                    sortKey.isNull[batchRow] = true;
                    ((LongColumnVector) batch.cols[1]).vector[batchRow] = rowIndex;
                });

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        long totalRowCount = 0L;
        for (RowGroupStatistics stripe : stripeStatistics) {
            Assertions.assertNull(stripe.getMinTuple());
            Assertions.assertNull(stripe.getMaxTuple());
            totalRowCount += stripe.getRowCount();
        }
        Assertions.assertEquals(3L, totalRowCount);
    }

    @Test
    void emptyFileReturnsNoStripes() throws Exception {
        Path orcPath = writeOrc(
                "struct<sort_key:bigint>",
                /*rowCount=*/ 0,
                (batch, batchRow, rowIndex) -> { });

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertTrue(stripeStatistics.isEmpty());
    }

    @Test
    void readsDateStatistics() throws Exception {
        // ORC DATE is stored in a LongColumnVector as day-of-epoch. Day 0 = 1970-01-01.
        Path orcPath = writeOrc(
                "struct<event_day:date>",
                /*rowCount=*/ 5,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertFalse(stripeStatistics.isEmpty());
        String globalMin = null;
        String globalMax = null;
        long totalRowCount = 0L;
        for (RowGroupStatistics stripe : stripeStatistics) {
            Assertions.assertFalse(stripe.isTruncated());
            String minValue = stripe.getMinTuple().getValues().get(0).getStringValue();
            String maxValue = stripe.getMaxTuple().getValues().get(0).getStringValue();
            Assertions.assertTrue(minValue.compareTo(maxValue) <= 0);
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
            totalRowCount += stripe.getRowCount();
        }
        Assertions.assertEquals(5L, totalRowCount);
        Assertions.assertEquals("1970-01-01", globalMin);
        Assertions.assertEquals("1970-01-05", globalMax);
    }

    @Test
    void dateColumnIntoNonDateSortKeyFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<event_day:date>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = rowIndex);

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("event_day", IntegerType.BIGINT)));
    }

    @Test
    void allNullDateStripeReportsAbsentStatistics() throws Exception {
        // ORC DATE column written with only nulls → DateColumnStatistics.getNumberOfValues() == 0,
        // so the stripe reports absent min/max (same contract as the integer all-null path).
        Path orcPath = writeOrc(
                "struct<event_day:date>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) -> {
                    batch.cols[0].noNulls = false;
                    batch.cols[0].isNull[batchRow] = true;
                });

        List<RowGroupStatistics> stripeStatistics = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("event_day", DateType.DATE));

        Assertions.assertFalse(stripeStatistics.isEmpty());
        long totalRowCount = 0L;
        for (RowGroupStatistics stripe : stripeStatistics) {
            Assertions.assertNull(stripe.getMinTuple());
            Assertions.assertNull(stripe.getMaxTuple());
            totalRowCount += stripe.getRowCount();
        }
        Assertions.assertEquals(3L, totalRowCount);
    }

    @Test
    void pre1970DateStripeFallsBackToDataTier() throws Exception {
        // day-of-epoch -1 = 1969-12-31 < 1970-01-01: outside the safe window → data tier.
        Path orcPath = writeOrc(
                "struct<event_day:date>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = -1 - rowIndex);

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("event_day", DateType.DATE)));
    }

    @Test
    void readsTimestampStatistics() throws Exception {
        // Plain ORC TIMESTAMP → StarRocks DATETIME. setIsUTC(true): time[] is UTC epoch millis, so
        // minimumUtc == the written millis. 1000 ms = 1970-01-01 00:00:01 UTC; +1000 ms per row.
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) -> {
                    TimestampColumnVector vector = (TimestampColumnVector) batch.cols[0];
                    vector.setIsUTC(true);
                    vector.time[batchRow] = (rowIndex + 1) * 1000L;
                    vector.nanos[batchRow] = 0;
                });

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("event_ts", DateType.DATETIME));

        Assertions.assertFalse(stats.isEmpty());
        String globalMin = null;
        String globalMax = null;
        for (RowGroupStatistics stripe : stats) {
            Assertions.assertFalse(stripe.isTruncated());
            String minValue = stripe.getMinTuple().getValues().get(0).getStringValue();
            String maxValue = stripe.getMaxTuple().getValues().get(0).getStringValue();
            Assertions.assertTrue(minValue.compareTo(maxValue) <= 0);
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals("1970-01-01 00:00:01", globalMin);
        // Whole-second max; assert the second prefix to stay robust to a sub-millisecond stats
        // ceiling if the ORC writer leaves maxNanos at its sentinel for a 0-nanos value.
        Assertions.assertTrue(globalMax.startsWith("1970-01-01 00:00:03"), "unexpected max " + globalMax);
    }

    @Test
    void readsTimestampStatisticsWithMicroseconds() throws Exception {
        // Sub-second precision must survive: BE keeps microseconds for a plain TIMESTAMP, so the
        // boundary must too. 1.500123 s = time=1000 ms (second 1) + nanos=500123000: a Hive
        // TimestampColumnVector adds nanos[] (the full sub-second, confirmed for orc-core 1.9.1) onto
        // the whole second of time[].
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp>",
                /*rowCount=*/ 1,
                (batch, batchRow, rowIndex) -> {
                    TimestampColumnVector vector = (TimestampColumnVector) batch.cols[0];
                    vector.setIsUTC(true);
                    vector.time[batchRow] = 1000L;
                    vector.nanos[batchRow] = 500_123_000;
                });

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("event_ts", DateType.DATETIME));

        Assertions.assertEquals("1970-01-01 00:00:01.500123",
                stats.get(0).getMinTuple().getValues().get(0).getStringValue());
    }

    @Test
    void timestampInstantFallsBackToDataTier() throws Exception {
        // TIMESTAMP_INSTANT (timestamp with local time zone) gets a session-tz offset at load time
        // that this reader cannot reproduce → defer to data tier.
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp with local time zone>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> {
                    TimestampColumnVector vector = (TimestampColumnVector) batch.cols[0];
                    vector.setIsUTC(true);
                    vector.time[batchRow] = (rowIndex + 1) * 1000L;
                    vector.nanos[batchRow] = 0;
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("event_ts", DateType.DATETIME)));
    }

    @Test
    void timestampIntoNonDatetimeSortKeyFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> {
                    TimestampColumnVector vector = (TimestampColumnVector) batch.cols[0];
                    vector.setIsUTC(true);
                    vector.time[batchRow] = (rowIndex + 1) * 1000L;
                    vector.nanos[batchRow] = 0;
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("event_ts", IntegerType.BIGINT)));
    }

    @Test
    void pre1970TimestampFallsBackToDataTier() throws Exception {
        // -1000 ms = 1969-12-31 23:59:59 < 1970-01-01: outside the safe window → data tier.
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> {
                    TimestampColumnVector vector = (TimestampColumnVector) batch.cols[0];
                    vector.setIsUTC(true);
                    vector.time[batchRow] = -1000L - rowIndex;
                    vector.nanos[batchRow] = 0;
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("event_ts", DateType.DATETIME)));
    }

    @Test
    void postYear9999TimestampFallsBackToDataTier() throws Exception {
        // 253402300800000 ms = 10000-01-01 00:00:00 UTC, year > 9999 → above the safe window.
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp>",
                /*rowCount=*/ 1,
                (batch, batchRow, rowIndex) -> {
                    TimestampColumnVector vector = (TimestampColumnVector) batch.cols[0];
                    vector.setIsUTC(true);
                    vector.time[batchRow] = 253402300800000L;
                    vector.nanos[batchRow] = 0;
                });

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("event_ts", DateType.DATETIME)));
    }

    @Test
    void allNullTimestampStripeReportsAbsentStatistics() throws Exception {
        Path orcPath = writeOrc(
                "struct<event_ts:timestamp>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) -> {
                    batch.cols[0].noNulls = false;
                    batch.cols[0].isNull[batchRow] = true;
                });

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(), new Column("event_ts", DateType.DATETIME));

        Assertions.assertFalse(stats.isEmpty());
        long totalRowCount = 0L;
        for (RowGroupStatistics stripe : stats) {
            Assertions.assertNull(stripe.getMinTuple());
            Assertions.assertNull(stripe.getMaxTuple());
            totalRowCount += stripe.getRowCount();
        }
        Assertions.assertEquals(3L, totalRowCount);
    }

    @Test
    void readsDecimalStatistics() throws Exception {
        // ORC decimal(18,2): write 1.00, 2.00, ... 5.00. ORC orders decimal stats correctly.
        Path orcPath = writeOrc(
                "struct<d:decimal(18,2)>",
                /*rowCount=*/ 5,
                (batch, batchRow, rowIndex) -> ((DecimalColumnVector) batch.cols[0])
                        .set(batchRow, HiveDecimal.create(BigDecimal.valueOf((rowIndex + 1) * 100L, 2))));

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));

        // Assert on BigDecimal VALUE, not the exact string: ORC's decimal-stats round-trip may
        // normalize trailing-zero scale ("1.00" vs "1"). Both are accepted downstream because the
        // Variant carries the column's ScalarType precision/scale regardless of the rendered text.
        Assertions.assertFalse(stats.isEmpty());
        BigDecimal globalMin = null;
        BigDecimal globalMax = null;
        for (RowGroupStatistics stripe : stats) {
            Assertions.assertFalse(stripe.isTruncated());
            BigDecimal minValue = new BigDecimal(stripe.getMinTuple().getValues().get(0).getStringValue());
            BigDecimal maxValue = new BigDecimal(stripe.getMaxTuple().getValues().get(0).getStringValue());
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals(0, globalMin.compareTo(new BigDecimal("1.00")));
        Assertions.assertEquals(0, globalMax.compareTo(new BigDecimal("5.00")));
    }

    @Test
    void readsNegativeDecimalStatistics() throws Exception {
        // ORC decimal(18,2) spanning negative→positive: -2.00, -1.00, 0.00, 1.00. ORC orders by value.
        Path orcPath = writeOrc(
                "struct<d:decimal(18,2)>",
                /*rowCount=*/ 4,
                (batch, batchRow, rowIndex) -> ((DecimalColumnVector) batch.cols[0])
                        .set(batchRow, HiveDecimal.create(BigDecimal.valueOf((rowIndex - 2) * 100L, 2))));

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));

        // Value-based comparison (ORC may render scale as "1" vs "1.00"); proves signed order.
        BigDecimal globalMin = null;
        BigDecimal globalMax = null;
        for (RowGroupStatistics stripe : stats) {
            Assertions.assertFalse(stripe.isTruncated());
            BigDecimal minValue = new BigDecimal(stripe.getMinTuple().getValues().get(0).getStringValue());
            BigDecimal maxValue = new BigDecimal(stripe.getMaxTuple().getValues().get(0).getStringValue());
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals(0, globalMin.compareTo(new BigDecimal("-2.00")));
        Assertions.assertEquals(0, globalMax.compareTo(new BigDecimal("1.00")));
    }

    @Test
    void readsDecimal128Statistics() throws Exception {
        // Accepted non-DECIMAL64 case: ORC decimal(20,2) → StarRocks DECIMAL128(20,2). The max
        // value's unscaled form (20 digits) exceeds 64-bit range, so this genuinely exercises the
        // 128-bit path through Type.isDecimalOfAnyVersion()/Variant.of/DecimalVariant.
        BigDecimal[] values = {
                new BigDecimal("1.00"),
                new BigDecimal("2.00"),
                new BigDecimal("999999999999999999.99"),
        };
        Path orcPath = writeOrc(
                "struct<d:decimal(20,2)>",
                /*rowCount=*/ values.length,
                (batch, batchRow, rowIndex) -> ((DecimalColumnVector) batch.cols[0])
                        .set(batchRow, HiveDecimal.create(values[rowIndex])));

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 2)));

        Assertions.assertFalse(stats.isEmpty());
        BigDecimal globalMin = null;
        BigDecimal globalMax = null;
        for (RowGroupStatistics stripe : stats) {
            Assertions.assertFalse(stripe.isTruncated());
            BigDecimal minValue = new BigDecimal(stripe.getMinTuple().getValues().get(0).getStringValue());
            BigDecimal maxValue = new BigDecimal(stripe.getMaxTuple().getValues().get(0).getStringValue());
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals(0, globalMin.compareTo(new BigDecimal("1.00")));
        Assertions.assertEquals(0, globalMax.compareTo(new BigDecimal("999999999999999999.99")));
    }

    @Test
    void decimalScaleMismatchFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<d:decimal(18,2)>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> ((DecimalColumnVector) batch.cols[0])
                        .set(batchRow, HiveDecimal.create(BigDecimal.valueOf((rowIndex + 1) * 100L, 2))));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))));
    }

    @Test
    void decimalPrecisionMismatchFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<d:decimal(18,2)>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> ((DecimalColumnVector) batch.cols[0])
                        .set(batchRow, HiveDecimal.create(BigDecimal.valueOf((rowIndex + 1) * 100L, 2))));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2))));
    }

    @Test
    void decimalIntoNonDecimalSortKeyFallsBackToDataTier() throws Exception {
        Path orcPath = writeOrc(
                "struct<d:decimal(18,2)>",
                /*rowCount=*/ 2,
                (batch, batchRow, rowIndex) -> ((DecimalColumnVector) batch.cols[0])
                        .set(batchRow, HiveDecimal.create(BigDecimal.valueOf((rowIndex + 1) * 100L, 2))));

        Assertions.assertThrows(MetaTierUnavailableException.class, () ->
                OrcStripeStatisticsReader.read(
                        PresplitTestSupport.statusOf(orcPath), new Configuration(),
                        new Column("d", IntegerType.BIGINT)));
    }

    @Test
    void allNullDecimalStripeReportsAbsentStatistics() throws Exception {
        // All-null decimal column → DecimalColumnStatistics.getNumberOfValues() == 0 → absent min/max.
        Path orcPath = writeOrc(
                "struct<d:decimal(18,2)>",
                /*rowCount=*/ 3,
                (batch, batchRow, rowIndex) -> {
                    batch.cols[0].noNulls = false;
                    batch.cols[0].isNull[batchRow] = true;
                });

        List<RowGroupStatistics> stats = OrcStripeStatisticsReader.read(
                PresplitTestSupport.statusOf(orcPath), new Configuration(),
                new Column("d", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));

        Assertions.assertFalse(stats.isEmpty());
        long totalRowCount = 0L;
        for (RowGroupStatistics stripe : stats) {
            Assertions.assertNull(stripe.getMinTuple());
            Assertions.assertNull(stripe.getMaxTuple());
            totalRowCount += stripe.getRowCount();
        }
        Assertions.assertEquals(3L, totalRowCount);
    }

    private Path writeOrc(
            String schemaText, int rowCount, PresplitTestSupport.OrcRowFiller rowFiller) throws IOException {
        return PresplitTestSupport.writeOrcFixture(tempDirectory, schemaText, rowCount, rowFiller);
    }
}
