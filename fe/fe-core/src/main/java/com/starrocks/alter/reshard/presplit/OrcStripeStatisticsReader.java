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
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Reads one ORC file's footer and emits per-stripe min/max/row-count statistics
 * projected onto a single StarRocks sort-key column. The ORC analogue of
 * {@link ParquetRowGroupStatisticsReader}: produced {@link RowGroupStatistics}
 * tuples already carry the StarRocks-side primitive type and feed the same
 * format-agnostic {@link ParquetMetadataSampler}.
 *
 * <p>Supported type window:
 * <ul>
 *   <li>ORC BYTE/SHORT/INT/LONG → StarRocks TINYINT/SMALLINT/INT/BIGINT</li>
 *   <li>ORC DATE → StarRocks DATE (day-of-epoch, timezone-free), gated to
 *       {@code [1970-01-01, 9999-12-31]} — values outside that window (year &le; 0
 *       rendering, pre-1582 proleptic/hybrid calendar ambiguity) fall back to data tier</li>
 *   <li>ORC DECIMAL → StarRocks DECIMAL of the SAME precision and scale (ORC orders decimal
 *       stats correctly, so footer min/max are usable). Non-matching precision/scale → data tier</li>
 *   <li>ORC TIMESTAMP (local, no timezone) → StarRocks DATETIME, using the tz-independent UTC stat,
 *       gated to {@code [1970-01-01, 9999-12-31]}. TIMESTAMP_INSTANT is deferred (the load applies a
 *       session-tz offset this reader cannot reproduce).</li>
 * </ul>
 * Everything else — STRING/CHAR/VARCHAR (data-tier fallback anyway), BOOLEAN,
 * FLOAT/DOUBLE, TIMESTAMP_INSTANT (load applies a session-tz offset this reader cannot reproduce),
 * non-matching-precision/scale DECIMAL, and any complex type — makes the reader throw
 * {@link MetaTierUnavailableException} so the pipeline falls back to data tier. That is
 * NOT a load failure. Pure I/O failures surface as {@link StarRocksException}.
 * Note: a legacy ORC file lacking modern UTC stats (pre-1.5-era footer) decodes through the JVM
 * default timezone, which may bias or reorder cut points — the load still routes every row by its
 * actual value, so this only mis-places the split, never loses or corrupts data.
 */
public final class OrcStripeStatisticsReader {

    private OrcStripeStatisticsReader() {
    }

    public static List<RowGroupStatistics> read(
            FileStatus fileStatus, Configuration hadoopConfig, Column sortKeyColumn) throws StarRocksException {
        Objects.requireNonNull(fileStatus, "fileStatus");
        Objects.requireNonNull(hadoopConfig, "hadoopConfig");
        Objects.requireNonNull(sortKeyColumn, "sortKeyColumn");

        // Pin the footer read to the load's snapshotted file length. ORC otherwise
        // re-stats the path at open time, which could read a different file
        // version/length than the one the load enumerated — breaking the
        // "sample exactly what the load reads" invariant (the Parquet reader pins
        // the same way via HadoopInputFile.fromStatus).
        try (Reader reader = OrcFile.createReader(
                fileStatus.getPath(), OrcFile.readerOptions(hadoopConfig).maxLength(fileStatus.getLen()))) {
            SortKeyLocation location = locateSortKeyColumn(reader.getSchema(), sortKeyColumn);
            List<StripeStatistics> stripeStatistics = reader.getStripeStatistics();
            List<StripeInformation> stripes = reader.getStripes();
            if (stripeStatistics.size() != stripes.size()) {
                throw new MetaTierUnavailableException(String.format(
                        "ORC stripe-statistics count %d does not match stripe count %d for %s",
                        stripeStatistics.size(), stripes.size(), fileStatus.getPath()));
            }
            List<RowGroupStatistics> result = new ArrayList<>(stripeStatistics.size());
            for (int stripeIndex = 0; stripeIndex < stripeStatistics.size(); stripeIndex++) {
                result.add(convertStripe(
                        stripeStatistics.get(stripeIndex), stripes.get(stripeIndex), location));
            }
            return result;
        } catch (IOException ioException) {
            throw new StarRocksException(
                    "failed to read ORC footer for " + fileStatus.getPath() + ": " + ioException.getMessage(),
                    ioException);
        }
    }

    private static SortKeyLocation locateSortKeyColumn(TypeDescription schema, Column sortKeyColumn)
            throws MetaTierUnavailableException {
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> fieldTypes = schema.getChildren();
        if (fieldNames == null || fieldTypes == null) {
            throw new MetaTierUnavailableException("ORC schema root is not a struct; meta tier supports flat schemas");
        }
        String columnName = sortKeyColumn.getName();
        int fieldIndex = -1;
        for (int i = 0; i < fieldNames.size(); i++) {
            if (!fieldNames.get(i).equalsIgnoreCase(columnName)) {
                continue;
            }
            // ORC field names are case-sensitive and may include case-distinct
            // siblings. StarRocks column names are case-insensitive, so we cannot
            // pick one silently — fall back to data tier rather than risk
            // projecting onto the wrong column.
            if (fieldIndex >= 0) {
                throw new MetaTierUnavailableException(String.format(
                        "ORC schema has multiple case-variant matches for sort-key column \"%s\" "
                                + "(at field indexes %d and %d)", columnName, fieldIndex, i));
            }
            fieldIndex = i;
        }
        if (fieldIndex < 0) {
            throw new MetaTierUnavailableException(
                    "ORC schema does not contain sort-key column \"" + columnName + "\"");
        }
        TypeDescription fieldType = fieldTypes.get(fieldIndex);
        rejectIncompatibleTypeMapping(fieldType, sortKeyColumn);
        // ORC assigns column ids in schema pre-order; getColumnStatistics() is
        // indexed by that id (index 0 is the struct root).
        return new SortKeyLocation(fieldType.getId(), sortKeyColumn);
    }

    /**
     * Reject ORC/StarRocks type pairings outside meta tier's supported window
     * eagerly, before iterating stripes. Anything outside the window means "fall
     * back to data tier", not "fail the load". The supported window is the unannotated
     * integer categories, ORC DATE → StarRocks DATE, ORC DECIMAL → a same-precision/scale
     * StarRocks DECIMAL, and ORC TIMESTAMP → StarRocks DATETIME. String, boolean,
     * floating-point, TIMESTAMP_INSTANT, non-matching DECIMAL, and complex types are deferred
     * (fall back to data tier).
     */
    private static void rejectIncompatibleTypeMapping(
            TypeDescription fieldType, Column sortKeyColumn) throws MetaTierUnavailableException {
        TypeDescription.Category orcCategory = fieldType.getCategory();
        PrimitiveType starRocksPrimitive = sortKeyColumn.getType().getPrimitiveType();
        boolean compatible = switch (orcCategory) {
            case BYTE, SHORT, INT, LONG -> starRocksPrimitive.isIntegerType();
            // ORC DATE is day-of-epoch (no timezone) → StarRocks DATE only.
            case DATE -> starRocksPrimitive == PrimitiveType.DATE;
            // ORC orders decimal stats correctly (no Parquet unsigned-byte trap), so footer
            // min/max are usable; require an exact precision/scale match with the StarRocks column.
            case DECIMAL -> decimalMatchesExactly(fieldType, sortKeyColumn);
            // Plain ORC TIMESTAMP is local (no timezone); the BE load stores its UTC wall clock
            // verbatim (no session-tz offset, unlike TIMESTAMP_INSTANT) → StarRocks DATETIME.
            case TIMESTAMP -> starRocksPrimitive == PrimitiveType.DATETIME;
            default -> false;
        };
        if (!compatible) {
            throw new MetaTierUnavailableException(String.format(
                    "ORC %s cannot map to StarRocks %s for sort-key column \"%s\"",
                    orcCategory, starRocksPrimitive, sortKeyColumn.getName()));
        }
    }

    /**
     * True only when the StarRocks sort-key column is a DECIMAL whose precision and scale
     * exactly equal the ORC decimal field's. The BE split validator requires an exact match
     * too; anything else falls back to data tier.
     */
    private static boolean decimalMatchesExactly(TypeDescription fieldType, Column sortKeyColumn) {
        Type starRocksType = sortKeyColumn.getType();
        if (!starRocksType.isDecimalOfAnyVersion()) {
            return false;
        }
        ScalarType scalarType = (ScalarType) starRocksType;
        return fieldType.getPrecision() == scalarType.getScalarPrecision()
                && fieldType.getScale() == scalarType.getScalarScale();
    }

    private static RowGroupStatistics convertStripe(
            StripeStatistics stripeStatistics, StripeInformation stripeInformation, SortKeyLocation location)
            throws MetaTierUnavailableException {
        long rowCount = stripeInformation.getNumberOfRows();
        ColumnStatistics[] columnStatistics = stripeStatistics.getColumnStatistics();
        ColumnStatistics sortKeyStatistics =
                location.columnId < columnStatistics.length ? columnStatistics[location.columnId] : null;
        if (sortKeyStatistics instanceof IntegerColumnStatistics integerStatistics
                && integerStatistics.getNumberOfValues() > 0) {
            return convertIntegerStripe(integerStatistics, rowCount, location);
        }
        if (sortKeyStatistics instanceof DateColumnStatistics dateStatistics
                && dateStatistics.getNumberOfValues() > 0) {
            return convertDateStripe(dateStatistics, rowCount, location);
        }
        if (sortKeyStatistics instanceof DecimalColumnStatistics decimalStatistics
                && decimalStatistics.getNumberOfValues() > 0) {
            return convertDecimalStripe(decimalStatistics, rowCount, location);
        }
        if (sortKeyStatistics instanceof TimestampColumnStatistics timestampStatistics
                && timestampStatistics.getNumberOfValues() > 0) {
            return convertTimestampStripe(timestampStatistics, rowCount, location);
        }
        // Absent / all-null stats (no presence flag on ORC numeric/date stats, so an
        // empty stripe is detected via getNumberOfValues() == 0) → missing min/max.
        return new RowGroupStatistics(null, null, rowCount, /*truncated=*/ false);
    }

    private static RowGroupStatistics convertIntegerStripe(
            IntegerColumnStatistics integerStatistics, long rowCount, SortKeyLocation location)
            throws MetaTierUnavailableException {
        Variant minVariant;
        Variant maxVariant;
        try {
            minVariant = Variant.of(location.starRocksColumn.getType(), Long.toString(integerStatistics.getMinimum()));
            maxVariant = Variant.of(location.starRocksColumn.getType(), Long.toString(integerStatistics.getMaximum()));
        } catch (RuntimeException conversionFailure) {
            // Variant.of throws when a stat value cannot be represented as the
            // StarRocks type (e.g. a BIGINT min outside TINYINT range). Translate
            // to a meta-tier fallback signal so the pipeline retries with data tier
            // rather than aborting the load.
            throw new MetaTierUnavailableException(String.format(
                    "ORC stats value not representable for sort-key column \"%s\": %s",
                    location.starRocksColumn.getName(), conversionFailure.getMessage()));
        }
        // ORC integer statistics are always exact (no truncation like Parquet binary).
        return new RowGroupStatistics(
                new Tuple(List.of(minVariant)), new Tuple(List.of(maxVariant)), rowCount, /*truncated=*/ false);
    }

    private static RowGroupStatistics convertDateStripe(
            DateColumnStatistics dateStatistics, long rowCount, SortKeyLocation location)
            throws MetaTierUnavailableException {
        Variant minVariant;
        Variant maxVariant;
        try {
            // getMinimumDayOfEpoch() is day-of-epoch (timezone-free).
            LocalDate minDate = LocalDate.ofEpochDay(dateStatistics.getMinimumDayOfEpoch());
            LocalDate maxDate = LocalDate.ofEpochDay(dateStatistics.getMaximumDayOfEpoch());
            // rejectDateOutsideWindow throws a checked MetaTierUnavailableException (not a
            // RuntimeException), so a window rejection propagates past the catch below keeping its
            // own message. ofEpochDay and Variant.of RuntimeExceptions are wrapped as a meta-tier
            // fallback signal — mirroring convertIntegerStripe — so any unrepresentable value falls
            // back to data tier instead of escaping read() (which only catches IOException) as an
            // uncaught exception that would skip pre-split entirely.
            MetaTierTemporalWindow.rejectDateOutsideWindow(minDate);
            MetaTierTemporalWindow.rejectDateOutsideWindow(maxDate);
            minVariant = Variant.of(location.starRocksColumn.getType(), minDate.format(DateUtils.DATE_FORMATTER));
            maxVariant = Variant.of(location.starRocksColumn.getType(), maxDate.format(DateUtils.DATE_FORMATTER));
        } catch (RuntimeException conversionFailure) {
            throw new MetaTierUnavailableException(String.format(
                    "ORC date stats value not representable for sort-key column \"%s\": %s",
                    location.starRocksColumn.getName(), conversionFailure.getMessage()));
        }
        return new RowGroupStatistics(
                new Tuple(List.of(minVariant)), new Tuple(List.of(maxVariant)), rowCount, /*truncated=*/ false);
    }

    private static RowGroupStatistics convertDecimalStripe(
            DecimalColumnStatistics decimalStatistics, long rowCount, SortKeyLocation location)
            throws MetaTierUnavailableException {
        Variant minVariant;
        Variant maxVariant;
        try {
            // HiveDecimal min/max carry the source scale; ORC orders decimal stats correctly
            // (no Parquet-style unsigned-byte trap), and the exact precision/scale gate
            // guarantees the source scale equals the StarRocks column scale. Variant.of
            // RuntimeExceptions are wrapped as a meta-tier fallback signal — mirroring
            // convertIntegerStripe/convertDateStripe — so an unrepresentable value falls back
            // to data tier instead of escaping read() (which only catches IOException).
            // Render via toPlainString() (not toString()) so large-exponent values never
            // surface in scientific notation, which the BE datum_from_string would reject.
            minVariant = Variant.of(location.starRocksColumn.getType(),
                    decimalStatistics.getMinimum().bigDecimalValue().toPlainString());
            maxVariant = Variant.of(location.starRocksColumn.getType(),
                    decimalStatistics.getMaximum().bigDecimalValue().toPlainString());
        } catch (RuntimeException conversionFailure) {
            throw new MetaTierUnavailableException(String.format(
                    "ORC decimal stats value not representable for sort-key column \"%s\": %s",
                    location.starRocksColumn.getName(), conversionFailure.getMessage()));
        }
        return new RowGroupStatistics(
                new Tuple(List.of(minVariant)), new Tuple(List.of(maxVariant)), rowCount, /*truncated=*/ false);
    }

    private static RowGroupStatistics convertTimestampStripe(
            TimestampColumnStatistics timestampStatistics, long rowCount, SortKeyLocation location)
            throws MetaTierUnavailableException {
        Variant minVariant;
        Variant maxVariant;
        try {
            // getMinimumUTC()/getMaximumUTC() return the tz-independent UTC wall clock (raw UTC
            // millis, NOT the TimeZone.getDefault()-shifted getMinimum()). This matches what the BE
            // load stores for a plain (non-instant) TIMESTAMP: the wall clock verbatim, no session-tz
            // offset. getNanos() carries the full sub-second nanos (millis fraction + sub-millisecond),
            // which BE also keeps for a plain TIMESTAMP; renderDateTime truncates to microseconds
            // (StarRocks DATETIME precision), mirroring the Parquet reader.
            // NOTE: a legacy ORC file carrying only pre-1.5-era stats (no minimumUtc) decodes through
            // TimeZone.getDefault(), so getMinimumUTC() may be tz-shifted (and under DST, shifted
            // non-uniformly so individual values can reorder); the high-level stats API exposes no flag
            // to detect this. BoundaryPlanner still emits a SORTED boundary set and BE routes every row
            // by its actual value, so the only effect is a possibly mis-placed / uneven split — never
            // wrong data or wrong results (and if a conversion ever yields min > max, the downstream
            // ParquetMetadataSampler treats the row group as fallback rather than emitting a boundary).
            // Modern writers (StarRocks unload, Spark, Hive, ORC >= 1.5) emit minimumUtc and are unaffected.
            // rejectDateOutsideWindow throws a CHECKED MetaTierUnavailableException that propagates past the
            // catch(RuntimeException) below (keeping its own message); getMinimumUTC/Variant.of
            // RuntimeExceptions are wrapped as the data-tier fallback signal — mirroring convertDateStripe.
            LocalDateTime minDateTime = utcTimestampToDateTime(timestampStatistics.getMinimumUTC());
            LocalDateTime maxDateTime = utcTimestampToDateTime(timestampStatistics.getMaximumUTC());
            MetaTierTemporalWindow.rejectDateOutsideWindow(minDateTime.toLocalDate());
            MetaTierTemporalWindow.rejectDateOutsideWindow(maxDateTime.toLocalDate());
            minVariant = Variant.of(location.starRocksColumn.getType(), renderDateTime(minDateTime));
            maxVariant = Variant.of(location.starRocksColumn.getType(), renderDateTime(maxDateTime));
        } catch (RuntimeException conversionFailure) {
            throw new MetaTierUnavailableException(String.format(
                    "ORC timestamp stats value not representable for sort-key column \"%s\": %s",
                    location.starRocksColumn.getName(), conversionFailure.getMessage()));
        }
        return new RowGroupStatistics(
                new Tuple(List.of(minVariant)), new Tuple(List.of(maxVariant)), rowCount, /*truncated=*/ false);
    }

    private static LocalDateTime utcTimestampToDateTime(Timestamp utcTimestamp) {
        // getTime() is UTC epoch millis (no TimeZone.getDefault() shift, unlike getMinimum());
        // getNanos() is the full sub-second nanos (0..999_999_999). floorDiv keeps the whole-second
        // part correct; ofEpochSecond combines them. The window gate rejects pre-1970 / post-9999.
        long epochSecond = Math.floorDiv(utcTimestamp.getTime(), 1000L);
        return LocalDateTime.ofEpochSecond(epochSecond, utcTimestamp.getNanos(), ZoneOffset.UTC);
    }

    private static String renderDateTime(LocalDateTime dateTime) {
        // Mirrors DateVariant.getStringValue / the Parquet reader: second precision unless there is a
        // sub-second part, then 6-digit microseconds. parseStrictDateTime round-trips both.
        if (dateTime.getNano() == 0) {
            return dateTime.format(DateUtils.DATE_TIME_FORMATTER);
        }
        return dateTime.format(DateUtils.DATE_TIME_FORMATTER)
                + "." + String.format("%06d", dateTime.getNano() / 1000);
    }

    private record SortKeyLocation(int columnId, Column starRocksColumn) {
    }
}
