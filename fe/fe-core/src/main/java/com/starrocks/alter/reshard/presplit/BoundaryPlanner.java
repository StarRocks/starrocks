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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Pure-function planner: validates the sample, sorts it, and picks
 * {@code requestedTabletCount - 1} row-quantile cuts. Validation mirrors the
 * BE-side check in {@code compute_split_ranges_from_external_boundaries} so
 * malformed input fails here instead of at publish time. The result class
 * collapses adjacent duplicates.
 */
public final class BoundaryPlanner {
    private BoundaryPlanner() {
    }

    /**
     * @throws StarRocksException when any sampled tuple violates the schema.
     */
    public static BoundaryPlannerResult planRowQuantileBoundaries(
            SampleSet sampleSet, int requestedTabletCount, List<Column> sortKey) throws StarRocksException {
        Objects.requireNonNull(sampleSet, "sampleSet");
        Objects.requireNonNull(sortKey, "sortKey");
        Preconditions.checkArgument(!sortKey.isEmpty(), "sortKey must be non-empty");
        Preconditions.checkArgument(requestedTabletCount >= 2,
                "requestedTabletCount must be >= 2, was %s", requestedTabletCount);

        List<Tuple> sampledTuples = sampleSet.getTuples();
        validateAgainstSchema(sampledTuples, sortKey);

        if (sampledTuples.size() < 2) {
            return BoundaryPlannerResult.NO_SPLIT;
        }

        List<Tuple> sortedTuples = new ArrayList<>(sampledTuples);
        sortedTuples.sort(Tuple::compareTo);

        // Walk quantile positions but enforce strictly-increasing cuts above the observed
        // minimum. A boundary at or below the running lower bound would produce an empty
        // tablet; instead, scan forward to the first tuple strictly greater than the
        // lower bound. When no such tuple exists we stop emitting (all-equal samples
        // therefore degrade to NO_SPLIT, while heavy-minimum prefixes still produce
        // useful cuts at their distinct higher keys).
        Tuple lowerBound = sortedTuples.get(0);
        List<Tuple> cutTuples = new ArrayList<>(requestedTabletCount - 1);
        for (int cutOrdinal = 1; cutOrdinal < requestedTabletCount; cutOrdinal++) {
            int position = (int) rowQuantilePosition(cutOrdinal, requestedTabletCount, sortedTuples.size());
            Tuple cut = sortedTuples.get(position);
            if (cut.compareTo(lowerBound) <= 0) {
                int advanced = position + 1;
                while (advanced < sortedTuples.size()
                        && sortedTuples.get(advanced).compareTo(lowerBound) <= 0) {
                    advanced++;
                }
                if (advanced >= sortedTuples.size()) {
                    break;
                }
                cut = sortedTuples.get(advanced);
            }
            cutTuples.add(cut);
            lowerBound = cut;
        }
        return new BoundaryPlannerResult(cutTuples);
    }

    /**
     * Position of the {@code cutOrdinal}-th cut (1-based) among
     * {@code totalCuts} cuts, taken over a sorted list of {@code size} items.
     * The {@code (long)} cast avoids int overflow when {@code size * cutOrdinal}
     * exceeds {@link Integer#MAX_VALUE}.
     */
    static long rowQuantilePosition(int cutOrdinal, int totalCuts, long size) {
        return (long) cutOrdinal * size / totalCuts;
    }

    private static void validateAgainstSchema(List<Tuple> tuples, List<Column> sortKey) throws StarRocksException {
        for (int rowIndex = 0; rowIndex < tuples.size(); rowIndex++) {
            validateTupleAgainstSchema(tuples.get(rowIndex), sortKey, "Sample row " + rowIndex);
        }
    }

    /**
     * Validate a single tuple against the sort-key schema. Used by both the
     * Tier-2 planner (sample tuples) and Tier 1 (row-group min/max tuples).
     * The {@code tupleLabel} is prepended to every error message.
     */
    static void validateTupleAgainstSchema(Tuple tuple, List<Column> sortKey, String tupleLabel)
            throws StarRocksException {
        if (tuple == null) {
            throw new StarRocksException(tupleLabel + ": tuple is null");
        }
        List<Variant> values = tuple.getValues();
        if (values == null) {
            throw new StarRocksException(tupleLabel + ": tuple values is null");
        }
        if (values.size() != sortKey.size()) {
            throw new StarRocksException(String.format(
                    "%s: tuple arity %d != sort-key arity %d",
                    tupleLabel, values.size(), sortKey.size()));
        }
        for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
            validateValueAgainstColumn(values.get(columnIndex), sortKey.get(columnIndex), tupleLabel, columnIndex);
        }
    }

    private static void validateValueAgainstColumn(Variant value, Column column, String tupleLabel, int columnIndex)
            throws StarRocksException {
        if (value == null) {
            throw new StarRocksException(String.format(
                    "%s col[%d]: value is null", tupleLabel, columnIndex));
        }
        Type valueType = value.getType();
        Type columnType = column.getType();
        if (!columnType.isScalarType()) {
            throw new StarRocksException(String.format(
                    "%s col[%d]: only scalar sort-key columns are supported, schema type is %s",
                    tupleLabel, columnIndex, columnType));
        }
        if (valueType.getPrimitiveType() != columnType.getPrimitiveType()) {
            throw new StarRocksException(String.format(
                    "%s col[%d]: variant primitive type %s != schema %s",
                    tupleLabel, columnIndex, valueType.getPrimitiveType(), columnType.getPrimitiveType()));
        }
        // The BE-side comparator works on raw unscaled values; precision and scale must
        // match exactly or rows would mis-order across tablets.
        if (columnType.isDecimalOfAnyVersion()) {
            ScalarType variantScalarType = (ScalarType) valueType;
            ScalarType columnScalarType = (ScalarType) columnType;
            if (variantScalarType.getScalarPrecision() != columnScalarType.getScalarPrecision()
                    || variantScalarType.getScalarScale() != columnScalarType.getScalarScale()) {
                throw new StarRocksException(String.format(
                        "%s col[%d]: decimal precision/scale (%d, %d) != schema (%d, %d)",
                        tupleLabel, columnIndex,
                        variantScalarType.getScalarPrecision(), variantScalarType.getScalarScale(),
                        columnScalarType.getScalarPrecision(), columnScalarType.getScalarScale()));
            }
        }
    }
}
