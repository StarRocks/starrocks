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
import com.starrocks.type.PrimitiveType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
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
 * <p>Supported type window (intentionally conservative for P1):
 * <ul>
 *   <li>Unannotated Parquet INT32/INT64 → StarRocks TINYINT/SMALLINT/INT/BIGINT</li>
 *   <li>Unannotated Parquet BOOLEAN → StarRocks BOOLEAN</li>
 *   <li>Parquet BINARY with UTF8 (string) logical annotation → StarRocks CHAR/VARCHAR
 *       (always marked truncated → forces Tier 2 fallback for string sort keys)</li>
 * </ul>
 * Anything else (DATE, DECIMAL, UINT_*, TIMESTAMP, JSON, BSON, UUID, FLOAT, DOUBLE,
 * INT96, FIXED_LEN_BYTE_ARRAY, raw BINARY for VARBINARY) makes the reader throw
 * {@link Tier1UnavailableException} so the pipeline falls back to Tier 2 — not a
 * load failure. Pure I/O failures surface as {@link StarRocksException}.
 */
public final class ParquetRowGroupStatisticsReader {

    private ParquetRowGroupStatisticsReader() {
    }

    public static List<RowGroupStatistics> read(
            FileStatus fileStatus, Configuration hadoopConfig, Column sortKeyColumn) throws StarRocksException {
        Objects.requireNonNull(fileStatus, "fileStatus");
        Objects.requireNonNull(hadoopConfig, "hadoopConfig");
        Objects.requireNonNull(sortKeyColumn, "sortKeyColumn");

        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromStatus(fileStatus, hadoopConfig))) {
            ParquetMetadata metadata = reader.getFooter();
            SortKeyLocation location = locateSortKeyColumn(
                    metadata.getFileMetaData().getSchema(), sortKeyColumn);
            List<BlockMetaData> blocks = metadata.getBlocks();
            List<RowGroupStatistics> rowGroupStatistics = new ArrayList<>(blocks.size());
            for (BlockMetaData block : blocks) {
                rowGroupStatistics.add(convertBlock(block, location));
            }
            return rowGroupStatistics;
        } catch (IOException ioException) {
            throw new StarRocksException(
                    "failed to read Parquet footer for " + fileStatus.getPath() + ": " + ioException.getMessage(),
                    ioException);
        }
    }

    private static SortKeyLocation locateSortKeyColumn(MessageType schema, Column sortKeyColumn)
            throws Tier1UnavailableException {
        String columnName = sortKeyColumn.getName();
        int fieldIndex = -1;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (!schema.getFieldName(i).equalsIgnoreCase(columnName)) {
                continue;
            }
            // Parquet schemas allow case-distinct siblings (e.g. "id" and "ID"). StarRocks
            // column names are case-insensitive, so we cannot pick one silently — fall back
            // to Tier 2 rather than risk projecting onto the wrong column.
            if (fieldIndex >= 0) {
                throw new Tier1UnavailableException(String.format(
                        "Parquet schema has multiple case-variant matches for sort-key column \"%s\" "
                                + "(at indexes %d and %d)", columnName, fieldIndex, i));
            }
            fieldIndex = i;
        }
        if (fieldIndex < 0) {
            throw new Tier1UnavailableException(
                    "Parquet schema does not contain sort-key column \"" + columnName + "\"");
        }
        org.apache.parquet.schema.Type fieldType = schema.getType(fieldIndex);
        if (!fieldType.isPrimitive()) {
            throw new Tier1UnavailableException(
                    "sort-key column \"" + columnName + "\" is a group type; Tier 1 supports primitive scalars only");
        }
        PrimitiveTypeName parquetTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
        LogicalTypeAnnotation logicalAnnotation = fieldType.getLogicalTypeAnnotation();
        rejectIncompatibleTypeMapping(parquetTypeName, logicalAnnotation, sortKeyColumn);
        return new SortKeyLocation(
                ColumnPath.get(schema.getFieldName(fieldIndex)), parquetTypeName, sortKeyColumn);
    }

    /**
     * Reject Parquet/StarRocks type pairings outside Tier 1's supported window
     * eagerly, before iterating row groups. Anything outside the window means
     * "fall back to Tier 2", not "fail the load". Logical annotations
     * (DATE, DECIMAL, UINT_*, TIMESTAMP, JSON, BSON, UUID, ...) change the
     * value's meaning and ordering and are deferred to a future commit; the
     * supported window here is the unannotated subset plus the UTF8 string
     * annotation for character columns.
     */
    private static void rejectIncompatibleTypeMapping(
            PrimitiveTypeName parquetTypeName,
            LogicalTypeAnnotation logicalAnnotation,
            Column sortKeyColumn) throws Tier1UnavailableException {
        PrimitiveType starRocksPrimitive = sortKeyColumn.getType().getPrimitiveType();
        boolean compatible = switch (parquetTypeName) {
            case INT32, INT64 -> logicalAnnotation == null
                    && (starRocksPrimitive == PrimitiveType.TINYINT
                        || starRocksPrimitive == PrimitiveType.SMALLINT
                        || starRocksPrimitive == PrimitiveType.INT
                        || starRocksPrimitive == PrimitiveType.BIGINT);
            case BOOLEAN -> logicalAnnotation == null && starRocksPrimitive == PrimitiveType.BOOLEAN;
            case BINARY -> logicalAnnotation instanceof StringLogicalTypeAnnotation
                    && (starRocksPrimitive == PrimitiveType.CHAR || starRocksPrimitive == PrimitiveType.VARCHAR);
            default -> false;
        };
        if (!compatible) {
            throw new Tier1UnavailableException(String.format(
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
            // to a Tier-1 fallback signal so the pipeline retries with Tier 2 rather
            // than aborting the load.
            throw new Tier1UnavailableException(String.format(
                    "Parquet stats value not representable for sort-key column \"%s\": %s",
                    location.starRocksColumn.getName(), conversionFailure.getMessage()));
        }
        // Parquet writers commonly truncate BINARY/string min/max (parquet-mr defaults
        // to 64 bytes). Truncation does not just inflate spurious overlap — it can also
        // hide real overlap when adjacent row groups share a long common prefix, which
        // the downstream overlap detector cannot recover. Mark binary stats as
        // truncated unconditionally so the pipeline falls back to Tier 2 for string
        // sort keys until column-index exactness flags are read explicitly. Numeric
        // and boolean stats are always exact and stay false.
        boolean truncated = location.parquetTypeName == PrimitiveTypeName.BINARY;
        return new RowGroupStatistics(
                new Tuple(List.of(minVariant)),
                new Tuple(List.of(maxVariant)),
                rowCount,
                truncated);
    }

    private static Variant toVariant(Object parquetValue, SortKeyLocation location) {
        String rendered = location.parquetTypeName == PrimitiveTypeName.BINARY
                ? ((Binary) parquetValue).toStringUsingUTF8()
                : parquetValue.toString();
        return Variant.of(location.starRocksColumn.getType(), rendered);
    }

    private static ColumnChunkMetaData findColumnChunk(BlockMetaData block, ColumnPath path) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
            if (chunk.getPath().equals(path)) {
                return chunk;
            }
        }
        return null;
    }

    private record SortKeyLocation(ColumnPath path, PrimitiveTypeName parquetTypeName, Column starRocksColumn) {
    }
}
