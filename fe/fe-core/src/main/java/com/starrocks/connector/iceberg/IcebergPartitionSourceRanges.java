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
import com.starrocks.sql.optimizer.Utils;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

// The value range each partition source column covers in a snapshot, read from the partition summaries of the
// manifest list only (no manifest file is opened). Values are in the optimizer's statistics domain (DATE/DATETIME
// as Utils.getLongFromDateTime of the local wall time, TIMESTAMPTZ in the session time zone), so they can widen
// ANALYZE min/max directly. A manifest or field without usable bounds is skipped: the result only ever widens
// existing statistics, so missing information under-corrects but never misleads.
public final class IcebergPartitionSourceRanges {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionSourceRanges.class);

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_HOUR = 3600L * MICROS_PER_SECOND;
    private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;
    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private IcebergPartitionSourceRanges() {
    }

    // Key: lower-cased source column name.
    public static Map<String, Pair<Double, Double>> compute(Schema schema, Map<Integer, PartitionSpec> specs,
                                                            List<ManifestFile> dataManifests) {
        Map<String, Pair<Double, Double>> ranges = new HashMap<>();
        for (ManifestFile manifest : dataManifests) {
            if (!manifest.hasAddedFiles() && !manifest.hasExistingFiles()) {
                continue;
            }
            List<ManifestFile.PartitionFieldSummary> summaries = manifest.partitions();
            PartitionSpec spec = specs.get(manifest.partitionSpecId());
            if (summaries == null || spec == null) {
                continue;
            }
            List<PartitionField> fields = spec.fields();
            List<Types.NestedField> partitionTypes = spec.partitionType().fields();
            for (int i = 0; i < fields.size() && i < summaries.size() && i < partitionTypes.size(); i++) {
                PartitionField field = fields.get(i);
                ManifestFile.PartitionFieldSummary summary = summaries.get(i);
                Types.NestedField source = schema.findField(field.sourceId());
                // A field whose source column is no longer in the current schema (evolution) is skipped.
                if (source == null || summary.lowerBound() == null || summary.upperBound() == null) {
                    continue;
                }
                String name = schema.findColumnName(field.sourceId());
                try {
                    Type partitionType = partitionTypes.get(i).type();
                    // A DATE column promoted to TIMESTAMP keeps day-count bounds in older manifests, which would
                    // decode as microseconds near 1970; skip them as the file-level stats do.
                    if (IcebergUtil.isPromotedDateBound(partitionType, summary.lowerBound())
                            || IcebergUtil.isPromotedDateBound(partitionType, summary.upperBound())) {
                        continue;
                    }
                    Object lower = Conversions.fromByteBuffer(partitionType, summary.lowerBound());
                    Object upper = Conversions.fromByteBuffer(partitionType, summary.upperBound());
                    Pair<Double, Double> range = toSourceRange(field.transform().toString(), source.type(), lower, upper);
                    if (range != null && range.first <= range.second) {
                        ranges.merge(name.toLowerCase(Locale.ROOT), range,
                                (a, b) -> Pair.create(Math.min(a.first, b.first), Math.max(a.second, b.second)));
                    }
                } catch (RuntimeException e) {
                    LOG.debug("skip partition bounds of {} in manifest {}", name, manifest.path(), e);
                }
            }
        }
        return ranges;
    }

    // The source column range [lo, hi] a partition value range [lower, upper] of the given transform covers, or
    // null when the transform does not bound the source column (bucket, truncate, void, ...) or the source type
    // has no numeric statistics domain (string, binary, boolean, ...).
    static Pair<Double, Double> toSourceRange(String transform, Type sourceType, Object lower, Object upper) {
        switch (transform) {
            case "identity":
                return identityRange(sourceType, lower, upper);
            case "year":
            case "month": {
                int lo = ((Number) lower).intValue();
                int hi = ((Number) upper).intValue() + 1;
                LocalDate start = "year".equals(transform) ? EPOCH.plusYears(lo) : EPOCH.plusMonths(lo);
                LocalDate end = "year".equals(transform) ? EPOCH.plusYears(hi) : EPOCH.plusMonths(hi);
                return dayRange(sourceType, start.toEpochDay(), end.toEpochDay());
            }
            case "day":
                return dayRange(sourceType, ((Number) lower).longValue(), ((Number) upper).longValue() + 1);
            case "hour":
                if (sourceType instanceof Types.TimestampType ts) {
                    return timestampRange(ts, ((Number) lower).longValue() * MICROS_PER_HOUR,
                            (((Number) upper).longValue() + 1) * MICROS_PER_HOUR - 1);
                }
                return null;
            default:
                return null;
        }
    }

    // Source range of the days [startDay, endDayExclusive).
    private static Pair<Double, Double> dayRange(Type sourceType, long startDay, long endDayExclusive) {
        if (sourceType instanceof Types.DateType) {
            return Pair.create(dateValue(startDay), dateValue(endDayExclusive - 1));
        } else if (sourceType instanceof Types.TimestampType ts) {
            return timestampRange(ts, startDay * MICROS_PER_DAY, endDayExclusive * MICROS_PER_DAY - 1);
        }
        return null;
    }

    private static Pair<Double, Double> identityRange(Type sourceType, Object lower, Object upper) {
        switch (sourceType.typeId()) {
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return Pair.create(((Number) lower).doubleValue(), ((Number) upper).doubleValue());
            case DECIMAL:
                return Pair.create(((BigDecimal) lower).doubleValue(), ((BigDecimal) upper).doubleValue());
            case DATE:
                return Pair.create(dateValue(((Number) lower).longValue()), dateValue(((Number) upper).longValue()));
            case TIMESTAMP:
                return timestampRange((Types.TimestampType) sourceType, ((Number) lower).longValue(),
                        ((Number) upper).longValue());
            default:
                return null;
        }
    }

    private static double dateValue(long epochDay) {
        return Utils.getLongFromDateTime(LocalDate.ofEpochDay(epochDay).atStartOfDay());
    }

    // A TIMESTAMP is read as its wall time, a TIMESTAMPTZ (a UTC instant) as the wall time in the session time zone.
    // UTC->local is not monotonic across a DST fall-back, so the TIMESTAMPTZ range is widened to cover every local
    // value inside it rather than converting the two endpoints.
    private static Pair<Double, Double> timestampRange(Types.TimestampType type, long loMicros, long hiMicros) {
        if (type.shouldAdjustToUTC()) {
            long[] local = IcebergUtil.widenedSessionTzBounds(loMicros, hiMicros);
            loMicros = local[0];
            hiMicros = local[1];
        }
        return Pair.create(wallTimeValue(loMicros), wallTimeValue(hiMicros));
    }

    private static double wallTimeValue(long wallMicros) {
        Instant instant = Instant.ofEpochSecond(Math.floorDiv(wallMicros, MICROS_PER_SECOND));
        return Utils.getLongFromDateTime(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    }
}
