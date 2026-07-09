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

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
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
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads one Parquet file's footer and emits per-row-group min/max/row-count
 * statistics projected onto ALL of the StarRocks sort-key columns, one
 * {@link com.starrocks.catalog.Variant} per column, composed into a per-column
 * bounding-box {@link com.starrocks.catalog.Tuple}. Absent nulls, the box is a valid loose
 * lexicographic bound of the row group: minTuple/maxTuple are composed from each column's
 * independent footer min/max, not from a single co-occurring row. A NULL sorts below every
 * value, so it can fall under minTuple, exactly as in the single-column meta tier -- but data
 * safety never depends on the bound: the BE routes every row (including nulls) by its true
 * value, so a null-leading row simply lands in the leftmost tablet. Produced
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
 *   <li>Parquet INT64 with a TIMESTAMP annotation (MILLIS/MICROS/NANOS) -> StarRocks DATETIME.
 *       A non-UTC-adjusted (local) timestamp is stored verbatim. A UTC-adjusted
 *       ({@code isAdjustedToUTC=true}) timestamp reaches the meta tier only when the load session
 *       timezone is a fixed offset: the reader then adds that constant offset (the same one the BE
 *       load applies) to the decoded UTC value. A DST / named / unknown load timezone -> data tier.</li>
 *   <li>Parquet BINARY with UTF8 (string) annotation -> StarRocks VARCHAR or CHAR
 *       (footer min/max are decoded as UTF-8 and used directly; FE compares them in the
 *       same unsigned-byte order the BE uses to route rows). CHAR is safe even though the BE
 *       right-pads a CHAR routing key with '\0' to its fixed width before routing: '\0' is the
 *       minimum byte, so fixed-width '\0'-padding is order-preserving under the BE unsigned
 *       memcmp + shorter-prefix-is-smaller tiebreak, and the BE stores the boundary itself
 *       stripped, so a NUL-free CHAR boundary separates rows exactly as VARCHAR does. A CHAR
 *       min/max that contains a '\0' is the one exception (the BE truncates a CHAR boundary at
 *       the first NUL while FE compares raw bytes) and is rejected -> data tier.</li>
 *   <li>Parquet INT32/INT64 with DECIMAL annotation → StarRocks DECIMAL of the SAME
 *       precision and scale (the unscaled integer's signed order equals the decimal order).</li>
 *   <li>Parquet FIXED_LEN_BYTE_ARRAY/BINARY with DECIMAL annotation → StarRocks DECIMAL of the
 *       SAME precision and scale, but ONLY when the file's raw footer declares a TypeDefinedOrder
 *       column order for that leaf (then the big-endian two's-complement footer min/max are
 *       signed-ordered). A file with no/UNDEFINED column order is deferred → data tier.</li>
 *   <li>Parquet INT32 with an UNSIGNED INT annotation (UINT_8/UINT_16/UINT_32) → any StarRocks
 *       integer sort key (decoded as the true unsigned magnitude and range-checked), but ONLY when
 *       the file's raw footer declares a TypeDefinedOrder column order for that leaf.</li>
 * </ul>
 * <p>DATE and DATETIME boundaries are gated to {@code [0001-01-01, 9999-12-31]}; a value outside
 * that window falls back to data tier. The window reaches year 1 because the BE day-of-epoch DATE
 * load is proleptic-Gregorian and the BE timestamp load reconstructs the same wall clock the FE
 * {@code floorDiv}/{@code floorMod} split computes (it borrows a whole second for a negative
 * sub-second remainder), so the boundary is FE/BE-identical down to year 1
 * (see {@link MetaTierTemporalWindow}).
 * Anything else (UINT_64, signed INT_8/16/32 annotations, INT96 timestamps, UTC-adjusted timestamps
 * under a non-fixed-offset load timezone, JSON,
 * BSON, UUID, FLOAT, DOUBLE, byte-array DECIMAL without a footer-declared TypeDefinedOrder,
 * raw BINARY for VARBINARY) makes the reader throw
 * {@link MetaTierUnavailableException} so the pipeline falls back to data tier — not a
 * load failure. Pure I/O failures surface as {@link StarRocksException}.
 */
public final class ParquetRowGroupStatisticsReader {

    // Parquet plaintext-footer trailing magic. "PARE" marks an encrypted footer (unreadable here).
    private static final byte[] PLAINTEXT_FOOTER_MAGIC = {'P', 'A', 'R', '1'};

    private ParquetRowGroupStatisticsReader() {
    }

    public static List<RowGroupStatistics> read(
            FileStatus fileStatus, Configuration hadoopConfig, List<Column> sortKeyColumns, String loadTimeZone)
            throws StarRocksException {
        Objects.requireNonNull(fileStatus, "fileStatus");
        Objects.requireNonNull(hadoopConfig, "hadoopConfig");
        Objects.requireNonNull(sortKeyColumns, "sortKeyColumns");
        Preconditions.checkArgument(!sortKeyColumns.isEmpty(), "sortKeyColumns must be non-empty");

        try {
            HadoopInputFile inputFile = HadoopInputFile.fromStatus(fileStatus, hadoopConfig);
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = reader.getFooter();
                MessageType schema = metadata.getFileMetaData().getSchema();
                Optional<ZoneOffset> loadOffset = MetaTierTemporalWindow.fixedLoadOffset(loadTimeZone);
                // Read the raw footer column_orders at most once per file (a leaf that needs the
                // check indexes into this list). null -> unknown order for every leaf -> those
                // leaves defer to data tier via declaresTypeDefinedColumnOrder.
                List<ColumnOrder> columnOrders = readColumnOrders(inputFile);
                List<SortKeyLocation> locations = new ArrayList<>(sortKeyColumns.size());
                for (Column sortKeyColumn : sortKeyColumns) {
                    locations.add(locateSortKeyColumn(schema, columnOrders, sortKeyColumn, loadOffset));
                }
                List<BlockMetaData> blocks = metadata.getBlocks();
                List<RowGroupStatistics> rowGroupStatistics = new ArrayList<>(blocks.size());
                for (BlockMetaData block : blocks) {
                    rowGroupStatistics.add(convertBlock(block, locations));
                }
                return rowGroupStatistics;
            }
        } catch (IOException ioException) {
            throw new StarRocksException(
                    "failed to read Parquet footer for " + fileStatus.getPath() + ": " + ioException.getMessage(),
                    ioException);
        }
    }

    private static SortKeyLocation locateSortKeyColumn(MessageType schema, List<ColumnOrder> columnOrders,
            Column sortKeyColumn, Optional<ZoneOffset> loadOffset) throws MetaTierUnavailableException {
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
        // inspect the raw footer instead. Byte-array DECIMAL and INT32-backed UNSIGNED ints both
        // need a footer-declared TypeDefinedOrder before their footer min/max can be trusted (see
        // declaresTypeDefinedColumnOrder). Only those leaves pay the extra raw-footer read.
        boolean typeDefinedColumnOrder = false;
        if (requiresColumnOrderCheck(parquetTypeName, logicalAnnotation)) {
            typeDefinedColumnOrder = declaresTypeDefinedColumnOrder(columnOrders, leafColumnIndex(schema, path));
        }
        rejectIncompatibleTypeMapping(parquetTypeName, logicalAnnotation, typeDefinedColumnOrder, sortKeyColumn,
                loadOffset);
        // A UTC-adjusted INT64 TIMESTAMP is stored by the BE as a local wall clock = UTC instant + the
        // fixed session-tz offset. The gate above accepted it only when loadOffset is present, so capture
        // that constant offset to apply per row group; every other mapping leaves it null (no shift).
        ZoneOffset utcAdjustedOffset = null;
        if (logicalAnnotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation
                && timestampAnnotation.isAdjustedToUTC()) {
            utcAdjustedOffset = loadOffset.orElse(null);
        }
        return new SortKeyLocation(path, parquetTypeName, logicalAnnotation, sortKeyColumn, utcAdjustedOffset);
    }

    /**
     * Reject Parquet/StarRocks type pairings outside meta tier's supported window
     * eagerly, before iterating row groups. Anything outside the window means
     * "fall back to data tier", not "fail the load". The supported window here is
     * the unannotated integer/boolean subset, the UTF8 string annotation for character
     * columns, INT32 with the DATE annotation → StarRocks DATE, INT64 with a non-UTC
     * TIMESTAMP annotation → StarRocks DATETIME, and INT32/INT64 with a DECIMAL annotation
     * → a same-precision/scale StarRocks DECIMAL. FIXED_LEN_BYTE_ARRAY/BINARY-backed DECIMAL is
     * accepted on the same exact-precision/scale match but only when the caller resolved a
     * footer-declared TypeDefinedOrder ({@code typeDefinedColumnOrder}); a file with no/UNDEFINED
     * column order and other annotations (UINT_*, UTC-adjusted TIMESTAMP, JSON, BSON, UUID, ...)
     * are deferred (fall back to data tier).
     */
    private static void rejectIncompatibleTypeMapping(
            PrimitiveTypeName parquetTypeName,
            LogicalTypeAnnotation logicalAnnotation,
            boolean typeDefinedColumnOrder,
            Column sortKeyColumn,
            Optional<ZoneOffset> loadOffset) throws MetaTierUnavailableException {
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
                if (logicalAnnotation instanceof IntLogicalTypeAnnotation intAnnotation
                        && !intAnnotation.isSigned() && intAnnotation.getBitWidth() <= 32) {
                    // UINT_8/16/32 are all INT32-backed (the bit-width guard rejects a malformed
                    // INT32 + UINT_64 annotation). Accept any StarRocks integer sort key (decode +
                    // range-check, like the unannotated integer path), but only when the file declares a
                    // TypeDefinedOrder for this leaf so the footer min/max are unsigned-ordered.
                    yield starRocksPrimitive.isIntegerType() && typeDefinedColumnOrder;
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
                    // Local (non-UTC-adjusted) timestamps store their ticks verbatim as the DATETIME
                    // wall clock. UTC-adjusted timestamps get a session-tz offset at load time; the meta
                    // tier can match that boundary only for a fixed-offset load timezone (loadOffset
                    // present) -- otherwise data tier.
                    yield starRocksPrimitive == PrimitiveType.DATETIME
                            && (!timestampAnnotation.isAdjustedToUTC() || loadOffset.isPresent());
                }
                // INT64-backed DECIMAL: same signed-order safety as INT32. Exact precision/scale.
                yield logicalAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                        && decimalMatchesExactly(decimalAnnotation, sortKeyColumn);
            }
            case BOOLEAN -> logicalAnnotation == null && starRocksPrimitive == PrimitiveType.BOOLEAN;
            case BINARY -> {
                if (logicalAnnotation instanceof StringLogicalTypeAnnotation) {
                    // VARCHAR and CHAR. The BE right-pads a CHAR routing key with '\0' to its fixed
                    // width before routing (_padding_char_column, be/src/exec/data_sinks/tablet_sink.cpp),
                    // but '\0' is the minimum byte, so fixed-width '\0'-padding is order-preserving under
                    // the BE unsigned memcmp + shorter-prefix tiebreak, and a CHAR StringVariant is
                    // canonicalized (truncated at the first '\0') to match the BE's strnlen view of a
                    // CHAR value, so a CHAR boundary separates rows exactly as a VARCHAR one does.
                    yield starRocksPrimitive.isCharFamily();
                }
                yield isSignedByteArrayDecimal(logicalAnnotation, sortKeyColumn, typeDefinedColumnOrder);
            }
            case FIXED_LEN_BYTE_ARRAY -> isSignedByteArrayDecimal(logicalAnnotation, sortKeyColumn, typeDefinedColumnOrder);
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

    private static RowGroupStatistics convertBlock(BlockMetaData block, List<SortKeyLocation> locations)
            throws StarRocksException {
        long rowCount = block.getRowCount();
        List<Variant> minValues = new ArrayList<>(locations.size());
        List<Variant> maxValues = new ArrayList<>(locations.size());
        for (SortKeyLocation location : locations) {
            ColumnChunkMetaData chunk = findColumnChunk(block, location.path);
            if (chunk == null) {
                // A column with no chunk in this row group: cannot bound the tuple -> whole
                // row group has no usable stats (data-tier fallback downstream).
                return new RowGroupStatistics(null, null, rowCount, /*truncated=*/ false);
            }
            Statistics<?> statistics = chunk.getStatistics();
            if (statistics == null || statistics.isEmpty() || !statistics.hasNonNullValue()) {
                return new RowGroupStatistics(null, null, rowCount, /*truncated=*/ false);
            }
            try {
                minValues.add(toVariant(statistics.genericGetMin(), location));
                maxValues.add(toVariant(statistics.genericGetMax(), location));
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
        }
        // Parquet chunk stats are never truncated (parquet-mr defaults truncate.length to
        // Integer.MAX_VALUE and parquet-cpp drops oversized stats, which the isEmpty() guard
        // above already routes to data tier). Absent nulls the box tuple is a valid loose
        // lexicographic bound of the row group; a null (which sorts below every value) can fall
        // under minTuple, but data safety does not rely on the bound -- the BE routes every row
        // by its true value, so a null-leading row lands in the leftmost tablet (as in the
        // single-column meta tier).
        return new RowGroupStatistics(new Tuple(minValues), new Tuple(maxValues), rowCount, /*truncated=*/ false);
    }

    private static Variant toVariant(Object parquetValue, SortKeyLocation location)
            throws MetaTierUnavailableException {
        if (location.logicalAnnotation instanceof DateLogicalTypeAnnotation) {
            // INT32 days since 1970-01-01 → canonical "yyyy-MM-dd".
            LocalDate date = LocalDate.ofEpochDay(((Number) parquetValue).longValue());
            MetaTierTemporalWindow.rejectOutsideWindow(date);
            return Variant.of(location.starRocksColumn.getType(), date.format(DateUtils.DATE_FORMATTER_UNIX));
        }
        if (location.logicalAnnotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation) {
            long ticks = ((Number) parquetValue).longValue();
            LocalDateTime dateTime = epochTicksToUtcDateTime(ticks, timestampAnnotation.getUnit());
            if (timestampAnnotation.isAdjustedToUTC()) {
                // UTC-adjusted ticks are a UTC instant; add the fixed session-tz offset (captured on the
                // location, guaranteed present for an accepted UTC-adjusted timestamp) to get the stored
                // local wall clock -- matching the BE load. A non-UTC-adjusted tick is already local.
                dateTime = dateTime.plusSeconds(location.utcAdjustedOffset.getTotalSeconds());
            }
            // A pre-1970 (negative) tick is in window too: epochTicksToUtcDateTime's floorDiv/floorMod
            // split matches the BE timestamp load, so the boundary equals the loaded value
            // (see MetaTierTemporalWindow).
            MetaTierTemporalWindow.rejectOutsideWindow(dateTime.toLocalDate());
            return Variant.of(location.starRocksColumn.getType(), MetaTierTemporalWindow.renderDateTime(dateTime));
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
        if (location.logicalAnnotation instanceof IntLogicalTypeAnnotation intAnnotation && !intAnnotation.isSigned()) {
            // The gate admits only INT32-backed UNSIGNED ints; decode the true magnitude. A plain
            // toString() would sign-extend a high-bit-set UINT_32 (stored as a negative int32).
            // IntVariant range-checks the magnitude against the target column (too-narrow → data tier).
            return Variant.of(location.starRocksColumn.getType(),
                    Long.toString(Integer.toUnsignedLong((Integer) parquetValue)));
        }
        String rendered = location.parquetTypeName == PrimitiveTypeName.BINARY
                ? ((Binary) parquetValue).toStringUsingUTF8()
                : parquetValue.toString();
        // A CHAR value is NUL-canonicalized in the StringVariant constructor; VARCHAR keeps raw bytes.
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
     * A byte-array (FIXED_LEN_BYTE_ARRAY/BINARY) DECIMAL is accepted only with an exact
     * precision/scale match AND a footer-declared TypeDefinedOrder — then its big-endian
     * two's-complement footer min/max are signed-ordered (see {@link #declaresTypeDefinedColumnOrder}).
     */
    private static boolean isSignedByteArrayDecimal(
            LogicalTypeAnnotation logicalAnnotation, Column sortKeyColumn, boolean typeDefinedColumnOrder) {
        return logicalAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                && decimalMatchesExactly(decimalAnnotation, sortKeyColumn)
                && typeDefinedColumnOrder;
    }

    /**
     * Leaves whose footer min/max are only trustworthy with a positively-declared TypeDefinedOrder:
     * byte-array (FIXED_LEN_BYTE_ARRAY/BINARY) DECIMAL (signed two's-complement order) and INT32-backed
     * UNSIGNED integers (unsigned order; a legacy file could otherwise surface signed-ordered min/max).
     */
    private static boolean requiresColumnOrderCheck(
            PrimitiveTypeName parquetTypeName, LogicalTypeAnnotation logicalAnnotation) {
        if ((parquetTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                || parquetTypeName == PrimitiveTypeName.BINARY)
                && logicalAnnotation instanceof DecimalLogicalTypeAnnotation) {
            return true;
        }
        return parquetTypeName == PrimitiveTypeName.INT32
                && logicalAnnotation instanceof IntLogicalTypeAnnotation intAnnotation
                && !intAnnotation.isSigned()
                && intAnnotation.getBitWidth() <= 32;
    }

    /**
     * True only when the file's raw footer positively declares a {@code TypeDefinedOrder} column
     * order for this leaf — required to trust the footer min/max for byte-array DECIMAL (signed
     * two's-complement order) and for UNSIGNED integers (unsigned order).
     *
     * <p>A null/short {@code column_orders} list (legacy file that omitted it) or an unset entry
     * (UNDEFINED order) is treated as unknown → data tier. We must read the RAW footer for this:
     * parquet-mr's converted {@code PrimitiveType.columnOrder()} defaults a missing {@code column_orders}
     * to {@code TYPE_DEFINED_ORDER}, so it cannot distinguish a legacy file. Package-private so the
     * predicate is unit-testable (parquet-mr's high-level writer cannot produce a file without
     * {@code column_orders}).
     */
    static boolean declaresTypeDefinedColumnOrder(List<ColumnOrder> columnOrders, int leafIndex) {
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
            input.seek(footerStart);
            // footerLength comes from the (untrusted) trailer, so do NOT materialize a
            // new byte[footerLength] — a corrupt/huge value could raise OutOfMemoryError, an Error
            // that escapes the IOException|RuntimeException fail-safe below. Cap Thrift to the
            // footer's own bytes with a length-bounded view instead.
            org.apache.parquet.format.FileMetaData footer =
                    Util.readFileMetaData(ByteStreams.limit(input, footerLength));
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
            LogicalTypeAnnotation logicalAnnotation, Column starRocksColumn, ZoneOffset utcAdjustedOffset) {
    }
}
