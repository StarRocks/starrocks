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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

/**
 * Unit tests that directly exercise the individual helpers of {@link ConvertTzStatisticUtils}.
 * End-to-end statistics assertions (through the expression calculator) live in
 * {@code ExpressionStatisticsCalculatorTest}.
 */
public class ConvertTzStatisticUtilsTest {

    // ---------- convertTzDateTime ----------

    @Test
    public void testConvertTzDateTimeShiftsByZoneOffset() {
        final double input = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 0, 0));
        final OptionalDouble converted = ConvertTzStatisticUtils.convertTzDateTime(
                input, ConstantOperator.createVarchar("UTC"), ConstantOperator.createVarchar("Asia/Shanghai"));

        Assertions.assertTrue(converted.isPresent());
        // Asia/Shanghai is UTC+8 year-round, so the wall clock shifts forward 8 hours.
        Assertions.assertEquals(8 * 3600.0, converted.getAsDouble() - input, 0.001);
    }

    @Test
    public void testConvertTzDateTimeReturnsEmptyForInvalidZone() {
        final double input = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 0, 0));
        final OptionalDouble converted = ConvertTzStatisticUtils.convertTzDateTime(
                input, ConstantOperator.createVarchar("Not/AZone"), ConstantOperator.createVarchar("UTC"));

        Assertions.assertTrue(converted.isEmpty());
    }

    @Test
    public void testIsValidTimeZone() {
        Assertions.assertTrue(ConvertTzStatisticUtils.isValidTimeZone(ConstantOperator.createVarchar("UTC")));
        Assertions.assertTrue(ConvertTzStatisticUtils.isValidTimeZone(ConstantOperator.createVarchar("America/New_York")));
        Assertions.assertFalse(ConvertTzStatisticUtils.isValidTimeZone(ConstantOperator.createVarchar("Not/AZone")));
    }

    // ---------- hasTimezoneOffsetDrift ----------

    @Test
    public void testHasTimezoneOffsetDriftFalseForConstantOffsetZones() {
        final double min = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 20, 30));
        final double max = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 14, 45, 0));

        Assertions.assertFalse(ConvertTzStatisticUtils.hasTimezoneOffsetDrift(
                min, max, ConstantOperator.createVarchar("UTC"), ConstantOperator.createVarchar("Asia/Shanghai")));
    }

    @Test
    public void testHasTimezoneOffsetDriftTrueAcrossDstTransition() {
        // Europe/Berlin falls back on 2024-10-27 at 01:00 UTC; the interval straddles that transition.
        final double min = getLongFromDateTime(LocalDateTime.of(2024, 10, 27, 0, 30, 0));
        final double max = getLongFromDateTime(LocalDateTime.of(2024, 10, 27, 1, 30, 0));

        Assertions.assertTrue(ConvertTzStatisticUtils.hasTimezoneOffsetDrift(
                min, max, ConstantOperator.createVarchar("UTC"), ConstantOperator.createVarchar("Europe/Berlin")));
    }

    @Test
    public void testHasTimezoneOffsetDriftTrueWhenEndpointInSpringForwardGap() {
        // America/New_York springs forward 2024-03-10 02:00 -> 03:00. 02:30 is skipped; atZone
        // normalizes it to after the transition, so an instant-only check would miss the gap.
        final double min = getLongFromDateTime(LocalDateTime.of(2024, 3, 10, 2, 30, 0));
        final double max = getLongFromDateTime(LocalDateTime.of(2024, 3, 10, 3, 15, 0));

        Assertions.assertTrue(ConvertTzStatisticUtils.hasTimezoneOffsetDrift(
                min, max, ConstantOperator.createVarchar("America/New_York"),
                ConstantOperator.createVarchar("UTC")));
    }

    @Test
    public void testHasTimezoneOffsetDriftTrueWhenEndpointInFallBackOverlap() {
        // America/New_York falls back 2024-11-03 02:00 -> 01:00. 01:30 is ambiguous.
        final double min = getLongFromDateTime(LocalDateTime.of(2024, 11, 3, 1, 15, 0));
        final double max = getLongFromDateTime(LocalDateTime.of(2024, 11, 3, 1, 45, 0));

        Assertions.assertTrue(ConvertTzStatisticUtils.hasTimezoneOffsetDrift(
                min, max, ConstantOperator.createVarchar("America/New_York"),
                ConstantOperator.createVarchar("UTC")));
    }

    @Test
    public void testHasTimezoneOffsetDriftTrueForInvalidZone() {
        final double min = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 0, 0));
        final double max = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 12, 0, 0));

        Assertions.assertTrue(ConvertTzStatisticUtils.hasTimezoneOffsetDrift(
                min, max, ConstantOperator.createVarchar("Not/AZone"), ConstantOperator.createVarchar("UTC")));
    }

    // ---------- buildSingleBucketHistogram ----------

    @Test
    public void testBuildSingleBucketHistogramComputesNonMcvRows() {
        final Map<String, Long> mcv = Map.of("a", 100L, "b", 200L);
        final Histogram hist = ConvertTzStatisticUtils.buildSingleBucketHistogram(100.0, 200.0, 1000, mcv);

        Assertions.assertEquals(1, hist.getBuckets().size());
        final Bucket bucket = hist.getBuckets().get(0);
        Assertions.assertEquals(100.0, bucket.getLower(), 0.001);
        Assertions.assertEquals(200.0, bucket.getUpper(), 0.001);
        Assertions.assertEquals(700L, bucket.getCount()); // 1000 - (100 + 200)
        Assertions.assertEquals(0L, bucket.getUpperRepeats());
        Assertions.assertEquals(2, hist.getMCV().size());
    }

    @Test
    public void testBuildSingleBucketHistogramWithInfiniteBoundsHasNoBucket() {
        final Map<String, Long> mcv = Map.of("a", 100L);
        final Histogram hist = ConvertTzStatisticUtils.buildSingleBucketHistogram(
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1000, mcv);

        Assertions.assertTrue(hist.getBuckets().isEmpty());
        Assertions.assertEquals(1, hist.getMCV().size());
    }

    @Test
    public void testBuildSingleBucketHistogramClampsNonMcvRowsAtZero() {
        final Map<String, Long> mcv = Map.of("a", 800L, "b", 400L); // sum 1200 > total 1000
        final Histogram hist = ConvertTzStatisticUtils.buildSingleBucketHistogram(1.0, 2.0, 1000, mcv);

        Assertions.assertEquals(0L, hist.getBuckets().get(0).getCount());
    }

    // ---------- transformHistogram ----------

    @Test
    public void testTransformHistogramConvertsMcvForConstantZones() {
        final String fromTz = "UTC";
        final String toTz = "Asia/Shanghai";
        final String dt1 = "2024-01-15 10:20:30";
        final String dt2 = "2024-01-15 14:45:00";
        final ColumnStatistic childStats = ColumnStatistic.builder()
                .setDistinctValuesCount(2)
                .setHistogram(new Histogram(Collections.emptyList(), Map.of(dt1, 100L, dt2, 200L)))
                .build();
        final CallOperator call = convertTzCall(
                ConstantOperator.createVarchar(fromTz), ConstantOperator.createVarchar(toTz));

        final Optional<Histogram> result = ConvertTzStatisticUtils.transformHistogram(
                call, childStats, 1.0, 2.0, 1000,
                Optional.of(ConstantOperator.createVarchar(fromTz)),
                Optional.of(ConstantOperator.createVarchar(toTz)));

        Assertions.assertTrue(result.isPresent());
        final Map<String, Long> mcv = result.get().getMCV();
        Assertions.assertEquals(2, mcv.size());
        Assertions.assertEquals(100L, mcv.get(convertTzMcvKey(dt1, fromTz, toTz)));
        Assertions.assertEquals(200L, mcv.get(convertTzMcvKey(dt2, fromTz, toTz)));
        Assertions.assertEquals(1, result.get().getBuckets().size());
        final Bucket bucket = result.get().getBuckets().get(0);
        Assertions.assertEquals(1.0, bucket.getLower(), 0.001);
        Assertions.assertEquals(2.0, bucket.getUpper(), 0.001);
        Assertions.assertEquals(700L, bucket.getCount()); // 1000 - (100 + 200)
    }

    @Test
    public void testTransformHistogramEmptyWhenNoMcv() {
        final ColumnStatistic childStats = ColumnStatistic.builder().setDistinctValuesCount(2).build();
        final CallOperator call = convertTzCall(
                ConstantOperator.createVarchar("UTC"), ConstantOperator.createVarchar("Asia/Shanghai"));

        Assertions.assertTrue(ConvertTzStatisticUtils.transformHistogram(
                call, childStats, 1.0, 2.0, 1000,
                Optional.of(ConstantOperator.createVarchar("UTC")),
                Optional.of(ConstantOperator.createVarchar("Asia/Shanghai"))).isEmpty());
    }

    @Test
    public void testTransformHistogramEmptyWhenTimezonesNotConstant() {
        final ColumnRefOperator fromTzCol = new ColumnRefOperator(1, VarcharType.VARCHAR, "from_tz", true);
        final ColumnRefOperator toTzCol = new ColumnRefOperator(2, VarcharType.VARCHAR, "to_tz", true);
        final ColumnStatistic childStats = ColumnStatistic.builder()
                .setDistinctValuesCount(1)
                .setHistogram(new Histogram(Collections.emptyList(), Map.of("2024-01-15 10:20:30", 100L)))
                .build();
        final CallOperator call = convertTzCall(fromTzCol, toTzCol);

        Assertions.assertTrue(ConvertTzStatisticUtils.transformHistogram(
                call, childStats, 1.0, 2.0, 1000, Optional.empty(), Optional.empty()).isEmpty());
    }

    @Test
    public void testTransformHistogramEmptyWhenMcvKeyInvalid() {
        final ColumnStatistic childStats = ColumnStatistic.builder()
                .setDistinctValuesCount(1)
                .setHistogram(new Histogram(Collections.emptyList(), Map.of("not-a-datetime", 100L)))
                .build();
        final CallOperator call = convertTzCall(
                ConstantOperator.createVarchar("UTC"), ConstantOperator.createVarchar("Asia/Shanghai"));

        Assertions.assertTrue(ConvertTzStatisticUtils.transformHistogram(
                call, childStats, 1.0, 2.0, 1000,
                Optional.of(ConstantOperator.createVarchar("UTC")),
                Optional.of(ConstantOperator.createVarchar("Asia/Shanghai"))).isEmpty());
    }

    @Test
    public void testTransformHistogramEmptyWhenTimezoneInvalid() {
        final ColumnStatistic childStats = ColumnStatistic.builder()
                .setDistinctValuesCount(1)
                .setHistogram(new Histogram(Collections.emptyList(), Map.of("2024-01-15 10:20:30", 100L)))
                .build();
        final CallOperator call = convertTzCall(
                ConstantOperator.createVarchar("Not/AZone"), ConstantOperator.createVarchar("UTC"));

        Assertions.assertTrue(ConvertTzStatisticUtils.transformHistogram(
                call, childStats, 1.0, 2.0, 1000,
                Optional.of(ConstantOperator.createVarchar("Not/AZone")),
                Optional.of(ConstantOperator.createVarchar("UTC"))).isEmpty());
    }

    private static CallOperator convertTzCall(ScalarOperator fromTz, ScalarOperator toTz) {
        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        return new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(dtCol, fromTz, toTz));
    }

    private static String convertTzMcvKey(String datetime, String fromTz, String toTz) {
        final ConstantOperator converted = ScalarOperatorFunctions.convert_tz(
                ConstantOperator.createVarchar(datetime).castTo(DateType.DATETIME).get(),
                ConstantOperator.createVarchar(fromTz),
                ConstantOperator.createVarchar(toTz));
        return converted.castTo(VarcharType.VARCHAR).get().getVarchar();
    }
}
