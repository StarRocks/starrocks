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
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Reads one Parquet file's footer and emits per-row-group min/max/row-count
 * statistics projected onto a single StarRocks sort-key column. Produced
 * {@link RowGroupStatistics} tuples already carry the StarRocks-side
 * primitive type required by
 * {@link BoundaryPlanner#validateTupleAgainstSchema} — the caller (a
 * {@link RowGroupStatisticsProvider}) just aggregates results across files.
 *
 * <p>Supported type window:
 * <ul>
 *   <li>Unannotated Parquet INT32/INT64 → StarRocks TINYINT/SMALLINT/INT/BIGINT</li>
 *   <li>Unannotated Parquet BOOLEAN → StarRocks BOOLEAN</li>
 *   <li>Parquet INT32 with DATE annotation → StarRocks DATE</li>
 *   <li>Parquet INT64 with TIMESTAMP annotation, {@code isAdjustedToUTC=false}
 *       (MILLIS/MICROS/NANOS) → StarRocks DATETIME. UTC-adjusted timestamps are
 *       deferred (the load applies a session-tz offset this reader cannot reproduce).</li>
 *   <li>Parquet BINARY with UTF8 (string) annotation → StarRocks CHAR/VARCHAR
 *       (always marked truncated → forces data-tier fallback for string sort keys)</li>
 *   <li>Parquet INT32/INT64 with DECIMAL annotation → StarRocks DECIMAL of the SAME
 *       precision and scale (the unscaled integer's signed order equals the decimal
 *       order). FIXED_LEN_BYTE_ARRAY/BINARY-backed decimals are deferred (their footer
 *       min/max may use unsigned byte ordering, wrong for negatives) → data tier.</li>
 * </ul>
 * <p>DATE/DATETIME values are additionally gated to {@code [1970-01-01, 9999-12-31]}; values
 * outside that window (year &le; 0 mis-renders through the {@code yyyy} formatters, pre-1970
 * has unverified FE/BE timestamp-division parity, pre-1582 has calendar-parity questions)
 * fall back to data tier.
 * Anything else (UINT_*, UTC-adjusted/INT96 timestamps, JSON, BSON, UUID, FLOAT, DOUBLE,
 * FIXED_LEN_BYTE_ARRAY/BINARY-backed DECIMAL, raw BINARY for VARBINARY) makes the reader throw
 * {@link MetaTierUnavailableException} so the pipeline falls back to data tier — not a
 * load failure. Pure I/O failures surface as {@link StarRocksException}.
 */
public final class ParquetRowGroupStatisticsReader {

    // Parquet plaintext-footer trailing magic. "PARE" marks an encrypted footer (unreadable here).
    private static final byte[] PLAINTEXT_FOOTER_MAGIC = {'P', 'A', 'R', '1'};

    private ParquetRowGroupStatisticsReader() {
    }

    public static List<RowGroupStatistics> read(
            FileStatus fileStatus, Configuration hadoopConfig, Column sortKeyColumn) throws StarRocksException {
        Objects.requireNonNull(fileStatus, "fileStatus");
        Objects.requireNonNull(hadoopConfig, "hadoopConfig");
        Objects.requireNonNull(sortKeyColumn, "sortKeyColumn");

        try {
            HadoopInputFile inputFile = HadoopInputFile.fromStatus(fileStatus, hadoopConfig);
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = reader.getFooter();
                SortKeyLocation location = locateSortKeyColumn(
                        metadata.getFileMetaData().getSchema(), inputFile, sortKeyColumn);
                List<BlockMetaData> blocks = metadata.getBlocks();
                List<RowGroupStatistics> rowGroupStatistics = new ArrayList<>(blocks.size());
                for (BlockMetaData block : blocks) {
                    rowGroupStatistics.add(convertBlock(block, location));
                }
                return rowGroupStatistics;
            }
        } catch (IOException ioException) {
            throw new StarRocksException(
                    "failed to read Parquet footer for " + fileStatus.getPath() + ": " + ioException.getMessage(),
                    ioException);
        }
    }

    private static SortKeyLocation locateSortKeyColumn(MessageType schema, InputFile inputFile, Column sortKeyColumn)
            throws MetaTierUnavailableException {
        String columnName = sortKeyColumn.getName();
        int fieldIndex = -1;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (!schema.getFieldName(i).equalsIgnoreCase(columnName)) {
                continue;
            }
            // Parquet schemas allow case-distinct siblings (e.g. "id" and "ID"). StarRocks
            // column names are case-insensitive, so we cannot pick one silently — fall back
            // to data tier rather than risk projecting onto the wrong column.
            if (fieldIndex >= 0) {
                throw new MetaTierUnavailableException(String.format(
                        "Parquet schema has multiple case-variant matches for sort-key column \"%s\" "
                                + "(at indexes %d and %d)", columnName, fieldIndex, i));
            }
            fieldIndex = i;
        }
        if (fieldIndex < 0) {
            throw new MetaTierUnavailableException(
                    "Parquet schema does not contain sort-key column \"" + columnName + "\"");
        }
        org.apache.parquet.schema.Type fieldType = schema.getType(fieldIndex);
        if (!fieldType.isPrimitive()) {
            throw new MetaTierUnavailableException(
                    "sort-key column \"" + columnName + "\" is a group type; meta tier supports primitive scalars only");
        }
        PrimitiveTypeName parquetTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
        LogicalTypeAnnotation logicalAnnotation = fieldType.getLogicalTypeAnnotation();
        ColumnPath path = ColumnPath.get(schema.getFieldName(fieldIndex));
        // Footer min/max for a byte-array (FLBA/BINARY) decimal are only signed-ordered when the
        // file's raw column_orders entry for this leaf is TypeDefinedOrder. parquet-mr's converted
        // PrimitiveType.columnOrder() defaults a missing column_orders to TYPE_DEFINED_ORDER, so we
        // inspect the raw footer instead. Only a byte-array decimal pays this extra footer read.
        boolean signedByteArrayOrder = false;
        if ((parquetTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                || parquetTypeName == PrimitiveTypeName.BINARY)
                && logicalAnnotation instanceof DecimalLogicalTypeAnnotation) {
            signedByteArrayOrder = declaresSignedByteArrayOrder(
                    readColumnOrders(inputFile), leafColumnIndex(schema, path));
        }
        rejectIncompatibleTypeMapping(parquetTypeName, logicalAnnotation, signedByteArrayOrder, sortKeyColumn);
        return new SortKeyLocation(path, parquetTypeName, logicalAnnotation, sortKeyColumn);
    }

    /**
     * Reject Parquet/StarRocks type pairings outside meta tier's supported window
     * eagerly, before iterating row groups. Anything outside the window means
     * "fall back to data tier", not "fail the load". The supported window here is
     * the unannotated integer/boolean subset, the UTF8 string annotation for character
     * columns, INT32 with the DATE annotation → StarRocks DATE, INT64 with a non-UTC
     * TIMESTAMP annotation → StarRocks DATETIME, and INT32/INT64 with a DECIMAL annotation
     * → a same-precision/scale StarRocks DECIMAL. FIXED_LEN_BYTE_ARRAY/BINARY-backed DECIMAL
     * and other annotations (UINT_*, UTC-adjusted TIMESTAMP, JSON, BSON, UUID, ...) are
     * deferred (fall back to data tier).
     */
    private static void rejectIncompatibleTypeMapping(
            PrimitiveTypeName parquetTypeName,
            LogicalTypeAnnotation logicalAnnotation,
            boolean signedByteArrayOrder,
            Column sortKeyColumn) throws MetaTierUnavailableException {
        PrimitiveType starRocksPrimitive = sortKeyColumn.getType().getPrimitiveType();
        boolean compatible = switch (parquetTypeName) {
            case INT32 -> {
                if (logicalAnnotation == null) {
                    // isIntegerType() is exactly {TINYINT, SMALLINT, INT, BIGINT} — the
                    // unannotated integer window; LARGEINT is intentionally excluded.
                    yield starRocksPrimitive.isIntegerType();
                }
                if (logicalAnnotation instanceof DateLogicalTypeAnnotation) {
                    // INT32+DATE is days-since-epoch; only a StarRocks DATE sort key
                    // gives those day counts their intended calendar meaning.
                    yield starRocksPrimitive == PrimitiveType.DATE;
                }
                // INT32-backed DECIMAL: the signed-integer order of the unscaled stat equals the
                // decimal order, so footer min/max are safe. Require an exact precision/scale match
                // with the StarRocks column (the BE split validator requires the same).
                yield logicalAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                        && decimalMatchesExactly(decimalAnnotation, sortKeyColumn);
            }
            case INT64 -> {
                if (logicalAnnotation == null) {
                    yield starRocksPrimitive.isIntegerType();
                }
                if (logicalAnnotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation) {
                    // Only local (non-UTC-adjusted) timestamps: the load stores those ticks
                    // verbatim as the DATETIME wall clock, so the FE-rendered boundary matches.
                    // UTC-adjusted timestamps get a session-tz offset at load time → data tier.
                    yield !timestampAnnotation.isAdjustedToUTC()
                            && starRocksPrimitive == PrimitiveType.DATETIME;
                }
                // INT64-backed DECIMAL: same signed-order safety as INT32. Exact precision/scale.
                yield logicalAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                        && decimalMatchesExactly(decimalAnnotation, sortKeyColumn);
            }
            case BOOLEAN -> logicalAnnotation == null && starRocksPrimitive == PrimitiveType.BOOLEAN;
            case BINARY -> logicalAnnotation instanceof StringLogicalTypeAnnotation
                    && (starRocksPrimitive == PrimitiveType.CHAR || starRocksPrimitive == PrimitiveType.VARCHAR);
            case FIXED_LEN_BYTE_ARRAY -> logicalAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                    && decimalMatchesExactly(decimalAnnotation, sortKeyColumn)
                    && signedByteArrayOrder;
            default -> false;
        };
        if (!compatible) {
            throw new MetaTierUnavailableException(String.format(
                    "Parquet %s%s cannot map to StarRocks %s for sort-key column \"%s\"",
                    parquetTypeName,
                    logicalAnnotation == null ? "" : " (" + logicalAnnotation + ")",
                    starRocksPrimitive,
                    sortKeyColumn.getName()));
        }
    }

    private static RowGroupStatistics convertBlock(BlockMetaData block, SortKeyLocation location)
            throws StarRocksException {
        long rowCount = block.getRowCount();
        ColumnChunkMetaData chunk = findColumnChunk(block, location.path);
        if (chunk == null) {
            return new RowGroupStatistics(null, null, rowCount, /*truncated=*/ false);
        }
        Statistics<?> statistics = chunk.getStatistics();
        if (statistics == null || statistics.isEmpty() || !statistics.hasNonNullValue()) {
            return new RowGroupStatistics(null, null, rowCount, /*truncated=*/ false);
        }
        Variant minVariant;
        Variant maxVariant;
        try {
            minVariant = toVariant(statistics.genericGetMin(), location);
            maxVariant = toVariant(statistics.genericGetMax(), location);
        } catch (RuntimeException conversionFailure) {
            // Variant.of and its subclass constructors throw IllegalArgumentException
            // (e.g. unsupported type) or RuntimeException (e.g. BoolVariant on a
            // malformed literal) when a stat value cannot be represented. Translate
            // to a meta-tier fallback signal so the pipeline retries with data tier rather
            // than aborting the load. NOTE: toVariant's date/datetime path also throws the
            // checked MetaTierUnavailableException (out-of-window rejection); that is not a
            // RuntimeException and intentionally propagates past this catch with its own
            // message — do not widen this to catch (Exception).
            throw new MetaTierUnavailableException(String.format(
                    "Parquet stats value not representable for sort-key column \"%s\": %s",
                    location.starRocksColumn.getName(), conversionFailure.getMessage()));
        }
        // Parquet writers commonly truncate BINARY/string min/max (parquet-mr defaults
        // to 64 bytes). Truncation does not just inflate spurious overlap — it can also
        // hide real overlap when adjacent row groups share a long common prefix, which
        // the downstream overlap detector cannot recover. Mark binary stats as
        // truncated unconditionally so the pipeline falls back to data tier for string
        // sort keys until column-index exactness flags are read explicitly. Numeric
        // and boolean stats are always exact and stay false.
        boolean truncated = location.parquetTypeName == PrimitiveTypeName.BINARY;
        return new RowGroupStatistics(
                new Tuple(List.of(minVariant)),
                new Tuple(List.of(maxVariant)),
                rowCount,
                truncated);
    }

    private static Variant toVariant(Object parquetValue, SortKeyLocation location)
            throws MetaTierUnavailableException {
        if (location.logicalAnnotation instanceof DateLogicalTypeAnnotation) {
            // INT32 days since 1970-01-01 → canonical "yyyy-MM-dd".
            LocalDate date = LocalDate.ofEpochDay(((Number) parquetValue).longValue());
            MetaTierTemporalWindow.rejectDateOutsideWindow(date);
            return Variant.of(location.starRocksColumn.getType(), date.format(DateUtils.DATE_FORMATTER));
        }
        if (location.logicalAnnotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation) {
            long ticks = ((Number) parquetValue).longValue();
            LocalDateTime dateTime = epochTicksToUtcDateTime(ticks, timestampAnnotation.getUnit());
            // A negative (pre-1970) tick lands on a date before the window's lower bound and is
            // rejected here, which also keeps the floorDiv/floorMod conversion above in the range
            // where it is provably identical to BE's signed division.
            MetaTierTemporalWindow.rejectDateOutsideWindow(dateTime.toLocalDate());
            return Variant.of(location.starRocksColumn.getType(), renderDateTime(dateTime));
        }
        if (location.logicalAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation) {
            // Reconstruct the signed unscaled integer at the source scale; the exact precision/scale
            // gate guarantees it equals the StarRocks column scale.
            BigInteger unscaled;
            if (location.parquetTypeName == PrimitiveTypeName.INT32
                    || location.parquetTypeName == PrimitiveTypeName.INT64) {
                unscaled = BigInteger.valueOf(((Number) parquetValue).longValue());
            } else {
                // FIXED_LEN_BYTE_ARRAY / BINARY: big-endian two's-complement bytes. The gate accepted
                // these only when the footer declared TypeDefinedOrder, so the min/max are signed.
                unscaled = new BigInteger(((Binary) parquetValue).getBytes());
            }
            BigDecimal decoded = new BigDecimal(unscaled, decimalAnnotation.getScale());
            return Variant.of(location.starRocksColumn.getType(), decoded.toPlainString());
        }
        String rendered = location.parquetTypeName == PrimitiveTypeName.BINARY
                ? ((Binary) parquetValue).toStringUsingUTF8()
                : parquetValue.toString();
        return Variant.of(location.starRocksColumn.getType(), rendered);
    }

    private static LocalDateTime epochTicksToUtcDateTime(long ticks, LogicalTypeAnnotation.TimeUnit unit) {
        long ticksPerSecond;
        long nanosPerTick;
        switch (unit) {
            case MILLIS -> {
                ticksPerSecond = 1_000L;
                nanosPerTick = 1_000_000L;
            }
            case MICROS -> {
                ticksPerSecond = 1_000_000L;
                nanosPerTick = 1_000L;
            }
            case NANOS -> {
                ticksPerSecond = 1_000_000_000L;
                nanosPerTick = 1L;
            }
            default -> throw new IllegalArgumentException("unsupported Parquet timestamp unit " + unit);
        }
        // floorDiv/floorMod keep pre-1970 (negative) ticks correct.
        long epochSecond = Math.floorDiv(ticks, ticksPerSecond);
        long nanoOfSecond = Math.floorMod(ticks, ticksPerSecond) * nanosPerTick;
        return LocalDateTime.ofEpochSecond(epochSecond, (int) nanoOfSecond, ZoneOffset.UTC);
    }

    private static String renderDateTime(LocalDateTime dateTime) {
        // Mirrors DateVariant.getStringValue: second precision unless there is a
        // sub-second part, then 6-digit microseconds. parseStrictDateTime round-trips both.
        if (dateTime.getNano() == 0) {
            return dateTime.format(DateUtils.DATE_TIME_FORMATTER);
        }
        return dateTime.format(DateUtils.DATE_TIME_FORMATTER)
                + "." + String.format("%06d", dateTime.getNano() / 1000);
    }

    private static ColumnChunkMetaData findColumnChunk(BlockMetaData block, ColumnPath path) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
            if (chunk.getPath().equals(path)) {
                return chunk;
            }
        }
        return null;
    }

    /**
     * True only when the StarRocks sort-key column is a DECIMAL whose precision and scale
     * exactly equal the Parquet decimal annotation's. The BE split validator requires an
     * exact match too; anything else falls back to data tier rather than risk a boundary from
     * the wrong type domain.
     */
    private static boolean decimalMatchesExactly(
            DecimalLogicalTypeAnnotation annotation, Column sortKeyColumn) {
        Type starRocksType = sortKeyColumn.getType();
        if (!starRocksType.isDecimalOfAnyVersion()) {
            return false;
        }
        ScalarType scalarType = (ScalarType) starRocksType;
        return annotation.getPrecision() == scalarType.getScalarPrecision()
                && annotation.getScale() == scalarType.getScalarScale();
    }

    /**
     * True only when the file's raw footer positively declares a {@code TypeDefinedOrder} column
     * order for this leaf. For a DECIMAL logical type that guarantees parquet ordered the byte-array
     * min/max as signed two's-complement (parquet-mr's {@code BINARY_AS_SIGNED_INTEGER_COMPARATOR}).
     *
     * <p>A null/short {@code column_orders} list (legacy file that omitted it) or an unset entry
     * (UNDEFINED order) is treated as unknown → data tier. We must read the RAW footer for this:
     * parquet-mr's converted {@code PrimitiveType.columnOrder()} defaults a missing {@code column_orders}
     * to {@code TYPE_DEFINED_ORDER}, so it cannot distinguish a legacy file. Package-private so the
     * predicate is unit-testable (parquet-mr's high-level writer cannot produce a file without
     * {@code column_orders}).
     */
    static boolean declaresSignedByteArrayOrder(List<ColumnOrder> columnOrders, int leafIndex) {
        return columnOrders != null
                && leafIndex >= 0
                && leafIndex < columnOrders.size()
                && columnOrders.get(leafIndex).isSetTYPE_ORDER();
    }

    /**
     * Read the raw Thrift footer's {@code column_orders} list, or {@code null} if it cannot be
     * obtained — an encrypted footer ({@code PARE} magic), a too-short file, a corrupt footer
     * length, an absent list, or any IO/parse failure. Any failure is non-fatal: the caller
     * treats {@code null} as "column order unknown" and falls back to the data tier. We read the
     * raw footer (not the converted schema) because parquet-mr defaults a missing column order to
     * TYPE_DEFINED_ORDER, which would not distinguish a legacy file.
     */
    private static List<ColumnOrder> readColumnOrders(InputFile inputFile) {
        try (SeekableInputStream input = inputFile.newStream()) {
            long length = inputFile.getLength();
            int trailerLength = 8; // [footerLength:4 little-endian][magic:4]
            if (length < PLAINTEXT_FOOTER_MAGIC.length + trailerLength) {
                return null;
            }
            byte[] trailer = new byte[trailerLength];
            input.seek(length - trailerLength);
            input.readFully(trailer);
            for (int i = 0; i < PLAINTEXT_FOOTER_MAGIC.length; i++) {
                if (trailer[4 + i] != PLAINTEXT_FOOTER_MAGIC[i]) {
                    return null; // encrypted ("PARE") or non-parquet footer — cannot confirm order
                }
            }
            int footerLength = (trailer[0] & 0xff)
                    | ((trailer[1] & 0xff) << 8)
                    | ((trailer[2] & 0xff) << 16)
                    | ((trailer[3] & 0xff) << 24);
            long footerStart = length - trailerLength - footerLength;
            if (footerLength < 0 || footerStart < PLAINTEXT_FOOTER_MAGIC.length) {
                return null; // corrupt footer length
            }
            byte[] footerBytes = new byte[footerLength];
            input.seek(footerStart);
            input.readFully(footerBytes);
            org.apache.parquet.format.FileMetaData footer =
                    Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
            return footer.isSetColumn_orders() ? footer.getColumn_orders() : null;
        } catch (IOException | RuntimeException failure) {
            return null;
        }
    }

    /**
     * Leaf index of {@code path} among the schema's leaf columns. The footer's {@code column_orders}
     * list is parallel to {@link MessageType#getPaths()} (leaf order). Returns -1 if not found.
     */
    private static int leafColumnIndex(MessageType schema, ColumnPath path) {
        List<String[]> paths = schema.getPaths();
        for (int i = 0; i < paths.size(); i++) {
            if (ColumnPath.get(paths.get(i)).equals(path)) {
                return i;
            }
        }
        return -1;
    }

    private record SortKeyLocation(
            ColumnPath path, PrimitiveTypeName parquetTypeName,
            LogicalTypeAnnotation logicalAnnotation, Column starRocksColumn) {
    }
}
