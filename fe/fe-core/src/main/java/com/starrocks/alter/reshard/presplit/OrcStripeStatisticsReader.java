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
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;

import java.io.IOException;
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
 * <p>Supported type window (intentionally conservative, matching the Parquet
 * reader's integer subset):
 * <ul>
 *   <li>ORC BYTE/SHORT/INT/LONG → StarRocks TINYINT/SMALLINT/INT/BIGINT</li>
 * </ul>
 * Everything else — STRING/CHAR/VARCHAR (whose stats would always need data-tier
 * fallback anyway), BOOLEAN, FLOAT/DOUBLE, DATE, TIMESTAMP, DECIMAL, and any
 * complex type — makes the reader throw {@link MetaTierUnavailableException} so
 * the pipeline falls back to data tier. That is NOT a load failure. Pure I/O
 * failures surface as {@link StarRocksException}.
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
        rejectIncompatibleTypeMapping(fieldType.getCategory(), sortKeyColumn);
        // ORC assigns column ids in schema pre-order; getColumnStatistics() is
        // indexed by that id (index 0 is the struct root).
        return new SortKeyLocation(fieldType.getId(), sortKeyColumn);
    }

    /**
     * Reject ORC/StarRocks type pairings outside meta tier's supported window
     * eagerly, before iterating stripes. Anything outside the window means "fall
     * back to data tier", not "fail the load". Only the unannotated integer
     * categories are admitted in v1; string, boolean, floating-point, and the
     * logical date/decimal/timestamp categories are deferred.
     */
    private static void rejectIncompatibleTypeMapping(
            TypeDescription.Category orcCategory, Column sortKeyColumn) throws MetaTierUnavailableException {
        PrimitiveType starRocksPrimitive = sortKeyColumn.getType().getPrimitiveType();
        boolean compatible = switch (orcCategory) {
            case BYTE, SHORT, INT, LONG -> starRocksPrimitive.isIntegerType();
            default -> false;
        };
        if (!compatible) {
            throw new MetaTierUnavailableException(String.format(
                    "ORC %s cannot map to StarRocks %s for sort-key column \"%s\"",
                    orcCategory, starRocksPrimitive, sortKeyColumn.getName()));
        }
    }

    private static RowGroupStatistics convertStripe(
            StripeStatistics stripeStatistics, StripeInformation stripeInfo, SortKeyLocation location)
            throws MetaTierUnavailableException {
        long rowCount = stripeInfo.getNumberOfRows();
        ColumnStatistics[] columnStatistics = stripeStatistics.getColumnStatistics();
        ColumnStatistics sortKeyStatistics =
                location.columnId < columnStatistics.length ? columnStatistics[location.columnId] : null;
        // ORC integer stats expose no presence flag (getMinimum() returns a default
        // when the stripe has no values), so we must treat absent / all-null stats
        // as missing explicitly rather than emit a bogus min/max. The instanceof
        // pattern also absorbs the out-of-bounds (null) case above.
        if (!(sortKeyStatistics instanceof IntegerColumnStatistics integerStatistics)
                || integerStatistics.getNumberOfValues() == 0) {
            return new RowGroupStatistics(null, null, rowCount, /*truncated=*/ false);
        }
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
                new Tuple(List.of(minVariant)),
                new Tuple(List.of(maxVariant)),
                rowCount,
                /*truncated=*/ false);
    }

    private record SortKeyLocation(int columnId, Column starRocksColumn) {
    }
}
