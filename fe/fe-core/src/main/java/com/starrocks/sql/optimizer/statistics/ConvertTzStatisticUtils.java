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

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * Convert_tz-specific helpers used by {@link ExpressionStatisticCalculator}.
 * Orchestration (NDV, nulls, assembling {@link ColumnStatistic}) stays in the calculator.
 */
public final class ConvertTzStatisticUtils {
    // Timezone offsets differ by at most 26 hours
    // (see ExtractRangePredicateFromScalarApplyRule).
    public static final double MAX_TIMEZONE_OFFSET_SECONDS = 26.0 * 3600.0;

    private ConvertTzStatisticUtils() {
    }

    public static OptionalDouble convertTzDateTime(double dateTimeStat, ConstantOperator fromTz,
                                                   ConstantOperator toTz) {
        try {
            ConstantOperator input = ConstantOperator.createDatetime(Utils.getDatetimeFromLong((long) dateTimeStat));
            ConstantOperator converted = ScalarOperatorFunctions.convert_tz(input, fromTz, toTz);
            return OptionalDouble.of(Utils.getLongFromDateTime(converted.getDatetime()));
        } catch (DateTimeException | SemanticException e) {
            return OptionalDouble.empty();
        }
    }

    /**
     * convert_tz is only a constant wall-clock shift while both zones keep the same UTC offset over
     * the whole input range. Around a DST transition the mapping is not monotonic in wall-clock space,
     * e.g. UTC 00:30 and 01:30 both map to Europe/Berlin 02:30 on 2024-10-27 while 00:59 maps to 02:59,
     * so converting only the endpoints would under-range the result.
     *
     * {@code LocalDateTime.atZone} also skips spring-forward gaps and picks one offset in fall-back
     * overlaps, so an instant interval built from the endpoints can miss the transition entirely.
     * If either endpoint is skipped or ambiguous in {@code fromTz}, keep the widened range.
     */
    public static boolean hasTimezoneOffsetDrift(double minValue, double maxValue,
                                                 ConstantOperator fromTz, ConstantOperator toTz) {
        try {
            ZoneId from = ZoneId.of(fromTz.getVarchar());
            ZoneId to = ZoneId.of(toTz.getVarchar());
            LocalDateTime minLocal = Utils.getDatetimeFromLong((long) minValue);
            LocalDateTime maxLocal = Utils.getDatetimeFromLong((long) maxValue);
            if (isSkippedOrAmbiguous(from, minLocal) || isSkippedOrAmbiguous(from, maxLocal)) {
                return true;
            }
            Instant minInstant = minLocal.atZone(from).toInstant();
            Instant maxInstant = maxLocal.atZone(from).toInstant();
            Instant start = minInstant.isAfter(maxInstant) ? maxInstant : minInstant;
            Instant end = minInstant.isAfter(maxInstant) ? minInstant : maxInstant;
            return hasOffsetTransition(from, start, end) || hasOffsetTransition(to, start, end);
        } catch (DateTimeException e) {
            // Invalid zone id or out-of-range temporal value: keep the widened range.
            return true;
        }
    }

    public static boolean isValidTimeZone(ConstantOperator tz) {
        try {
            ZoneId.of(tz.getVarchar());
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }

    private static boolean isSkippedOrAmbiguous(ZoneId zone, LocalDateTime localDateTime) {
        return zone.getRules().getValidOffsets(localDateTime).size() != 1;
    }

    private static boolean hasOffsetTransition(ZoneId zone, Instant start, Instant end) {
        ZoneRules rules = zone.getRules();
        if (!rules.getOffset(start).equals(rules.getOffset(end))) {
            return true;
        }
        ZoneOffsetTransition next = rules.nextTransition(start);
        return next != null && !next.getInstant().isAfter(end);
    }

    public static Optional<Histogram> transformHistogram(CallOperator callOperator,
                                                         ColumnStatistic childStats,
                                                         double minValue, double maxValue,
                                                         double rowCount,
                                                         Optional<ConstantOperator> fromTz,
                                                         Optional<ConstantOperator> toTz) {
        Histogram hist = childStats == null ? null : childStats.getHistogram();
        if (hist == null || hist.getMCV().isEmpty()) {
            return Optional.empty();
        }

        if (fromTz.isEmpty() || toTz.isEmpty()) {
            return Optional.empty();
        }

        final Type resultType = callOperator.getType();
        Map<String, Long> newMcv = new HashMap<>();
        for (Map.Entry<String, Long> entry : hist.getMCV().entrySet()) {
            Optional<ConstantOperator> parsedKey =
                    ConstantOperator.createVarchar(entry.getKey()).castTo(resultType);
            if (parsedKey.isEmpty() || parsedKey.get().isNull()) {
                return Optional.empty();
            }

            ConstantOperator converted;
            try {
                converted = ScalarOperatorFunctions.convert_tz(parsedKey.get(), fromTz.get(), toTz.get());
            } catch (DateTimeException | SemanticException e) {
                return Optional.empty();
            }

            Optional<ConstantOperator> keyString = converted.castTo(VarcharType.VARCHAR);
            if (keyString.isEmpty()) {
                return Optional.empty();
            }
            newMcv.merge(keyString.get().getVarchar(), entry.getValue(), Long::sum);
        }

        // Exact per-key MCV transform; keep one covering bucket for the non-MCV mass
        // (same idea as HistogramStatisticsCollectJob.buildCollectSingleBucket in stats collection).
        return Optional.of(buildSingleBucketHistogram(minValue, maxValue, rowCount, newMcv));
    }

    /**
     * Build an MCV histogram with a single covering bucket for the remaining non-MCV rows.
     * Mirrors the stats-collection fallback that stores one bucket over [min, max] with
     * count = totalRows - sum(MCV) when a full multi-bucket histogram is unavailable.
     */
    public static Histogram buildSingleBucketHistogram(double minValue, double maxValue,
                                                       double totalRows, Map<String, Long> mcv) {
        if (Double.isInfinite(minValue) || Double.isInfinite(maxValue)
                || Double.isNaN(minValue) || Double.isNaN(maxValue)) {
            return new Histogram(Collections.emptyList(), mcv);
        }
        long mcvRows = mcv.values().stream().mapToLong(Long::longValue).sum();
        long nonMcvRows = Math.max(0L, Math.round(totalRows) - mcvRows);
        List<Bucket> buckets = List.of(new Bucket(minValue, maxValue, nonMcvRows, 0L));
        return new Histogram(buckets, mcv);
    }
}
