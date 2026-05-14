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
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Tier 1 boundary planner. Turns per-row-group {@code (minTuple, rowCount)}
 * statistics into {@code requestedTabletCount - 1} row-quantile boundaries
 * directly, without materializing a row sample.
 *
 * <p>Restricted to single-column sort keys — per-column Parquet min/max
 * doesn't imply real composite lex-tuples. Throws
 * {@link Tier1UnavailableException} when statistics are
 * missing/all-null/truncated or row-group ranges overlap above
 * {@code overlapThreshold}; the caller then retries with Tier 2.
 */
public final class ParquetMetadataSampler {

    public static final double DEFAULT_OVERLAP_THRESHOLD = 0.3;

    private final RowGroupStatisticsProvider statisticsProvider;
    private final double overlapThreshold;

    public ParquetMetadataSampler(RowGroupStatisticsProvider statisticsProvider) {
        this(statisticsProvider, DEFAULT_OVERLAP_THRESHOLD);
    }

    public ParquetMetadataSampler(RowGroupStatisticsProvider statisticsProvider, double overlapThreshold) {
        this.statisticsProvider = Objects.requireNonNull(statisticsProvider, "statisticsProvider");
        Preconditions.checkArgument(overlapThreshold >= 0.0 && overlapThreshold <= 1.0,
                "overlapThreshold must be in [0, 1], was %s", overlapThreshold);
        this.overlapThreshold = overlapThreshold;
    }

    /**
     * @throws Tier1UnavailableException when Tier 1 cannot produce boundaries
     *         with sufficient precision and the caller should fall back to Tier 2.
     * @throws StarRocksException any other sampling failure (provider RPC error,
     *         provider contract violation, etc.) — coordinator treats as
     *         "skip pre-split".
     */
    public BoundaryPlannerResult tryPlan(SampleRequest request, int requestedTabletCount) throws StarRocksException {
        Objects.requireNonNull(request, "request");
        Preconditions.checkArgument(requestedTabletCount >= 2,
                "requestedTabletCount must be >= 2, was %s", requestedTabletCount);
        rejectCompositeSortKey(request);

        List<RowGroupStatistics> fetched = statisticsProvider.fetch(request);
        if (fetched == null) {
            throw new StarRocksException("RowGroupStatisticsProvider returned null");
        }
        List<RowGroupStatistics> usable = filterUsableRowGroups(fetched, request.getSortKey());
        if (usable.isEmpty()) {
            return BoundaryPlannerResult.NO_SPLIT;
        }
        usable.sort(Comparator.comparing(RowGroupStatistics::getMinTuple));

        long totalRowCount = checkOverlapAndSumRowCount(usable);
        if (totalRowCount == 0L) {
            return BoundaryPlannerResult.NO_SPLIT;
        }

        return classifyResult(placeQuantileBoundaries(usable, totalRowCount, requestedTabletCount), usable);
    }

    /**
     * Decide the final outcome from the placed cuts. Empty cuts plus a
     * non-degenerate stats envelope signals "Tier 1 was too coarse" — Tier 2
     * (row sampling) may still find an interior boundary, so signal fallback.
     * Genuinely all-equal data returns {@link BoundaryPlannerResult#NO_SPLIT}.
     */
    private static BoundaryPlannerResult classifyResult(List<Tuple> cuts, List<RowGroupStatistics> sortedRowGroups)
            throws Tier1UnavailableException {
        if (!cuts.isEmpty()) {
            return new BoundaryPlannerResult(cuts);
        }
        Tuple globalMin = sortedRowGroups.get(0).getMinTuple();
        boolean hasRange = sortedRowGroups.stream()
                .anyMatch(rowGroup -> rowGroup.getMaxTuple().compareTo(globalMin) > 0);
        if (hasRange) {
            throw new Tier1UnavailableException(
                    "row-group metadata too coarse to place row-quantile boundaries");
        }
        return BoundaryPlannerResult.NO_SPLIT;
    }

    private static void rejectCompositeSortKey(SampleRequest request) throws Tier1UnavailableException {
        int arity = request.getSortKey().size();
        if (arity != 1) {
            throw new Tier1UnavailableException(
                    "composite sort key (arity " + arity + ") — Tier 1 supports single-column only");
        }
    }

    private static List<RowGroupStatistics> filterUsableRowGroups(
            List<RowGroupStatistics> allStatistics, List<Column> sortKey) throws StarRocksException {
        if (allStatistics.isEmpty()) {
            return new ArrayList<>(0);
        }
        List<RowGroupStatistics> usable = new ArrayList<>(allStatistics.size());
        for (int index = 0; index < allStatistics.size(); index++) {
            RowGroupStatistics rowGroup = allStatistics.get(index);
            if (rowGroup == null) {
                throw new StarRocksException("RowGroupStatisticsProvider returned a null entry at index " + index);
            }
            if (rowGroup.getRowCount() == 0) {
                continue;
            }
            if (rowGroup.getMinTuple() == null || rowGroup.getMaxTuple() == null) {
                throw new Tier1UnavailableException(
                        "row group missing min/max statistics; rowCount=" + rowGroup.getRowCount());
            }
            if (rowGroup.isTruncated()) {
                throw new Tier1UnavailableException(
                        "row group has truncated min/max statistics (Parquet string truncation); "
                                + "ordering on truncated bounds is unreliable");
            }
            if (isAllNullTuple(rowGroup.getMinTuple()) || isAllNullTuple(rowGroup.getMaxTuple())) {
                throw new Tier1UnavailableException("row group has all-null min/max statistics");
            }
            BoundaryPlanner.validateTupleAgainstSchema(
                    rowGroup.getMinTuple(), sortKey, "Row group [" + index + "] minTuple");
            BoundaryPlanner.validateTupleAgainstSchema(
                    rowGroup.getMaxTuple(), sortKey, "Row group [" + index + "] maxTuple");
            if (rowGroup.getMinTuple().compareTo(rowGroup.getMaxTuple()) > 0) {
                // Treat unreliable bound ordering like other "footer stats are bad"
                // signals — Tier 2 can still sample real rows.
                throw new Tier1UnavailableException(
                        "Row group [" + index + "]: minTuple > maxTuple (unreliable footer statistics)");
            }
            usable.add(rowGroup);
        }
        return usable;
    }

    /**
     * Walks the row groups in sorted-by-min order. Counts each row group whose
     * range overlaps any previously-seen group ({@code current.minTuple <
     * maxSeen}), so a wide group covering many narrower groups after it is
     * detected correctly (a pure pairwise check would miss those nested
     * overlaps).
     */
    private long checkOverlapAndSumRowCount(List<RowGroupStatistics> sortedRowGroups)
            throws Tier1UnavailableException {
        long totalRowCount = sortedRowGroups.get(0).getRowCount();
        Tuple maxSeen = sortedRowGroups.get(0).getMaxTuple();
        int overlappingPairCount = 0;
        for (int i = 1; i < sortedRowGroups.size(); i++) {
            RowGroupStatistics current = sortedRowGroups.get(i);
            totalRowCount += current.getRowCount();
            if (current.getMinTuple().compareTo(maxSeen) < 0) {
                overlappingPairCount++;
            }
            if (current.getMaxTuple().compareTo(maxSeen) > 0) {
                maxSeen = current.getMaxTuple();
            }
        }
        if (sortedRowGroups.size() > 1) {
            double overlapFraction = (double) overlappingPairCount / (sortedRowGroups.size() - 1);
            if (overlapFraction > overlapThreshold) {
                throw new Tier1UnavailableException(String.format(
                        "row-group overlap fraction %.3f > threshold %.3f", overlapFraction, overlapThreshold));
            }
        }
        return totalRowCount;
    }

    /**
     * Walk the sorted row groups once with a shared cursor and emit a boundary
     * for each of the {@code requestedTabletCount - 1} cumulative-row thresholds.
     * Using {@code <=} (not {@code <}) at the cursor advances assigns a threshold
     * landing on a row-group boundary to the row group that starts at it.
     * Cuts equal to the overall minimum are filtered out — they would create an
     * empty leading tablet.
     */
    private static List<Tuple> placeQuantileBoundaries(
            List<RowGroupStatistics> sortedRowGroups, long totalRowCount, int requestedTabletCount) {
        Tuple minimum = sortedRowGroups.get(0).getMinTuple();
        List<Tuple> cuts = new ArrayList<>(requestedTabletCount - 1);
        long accumRows = 0L;
        int cursor = 0;
        for (int cutOrdinal = 1; cutOrdinal < requestedTabletCount; cutOrdinal++) {
            long threshold = BoundaryPlanner.rowQuantilePosition(cutOrdinal, requestedTabletCount, totalRowCount);
            while (cursor < sortedRowGroups.size()) {
                long cursorRowCount = sortedRowGroups.get(cursor).getRowCount();
                if (accumRows + cursorRowCount > threshold) {
                    break;
                }
                accumRows += cursorRowCount;
                cursor++;
            }
            if (cursor >= sortedRowGroups.size()) {
                break;
            }
            Tuple cut = sortedRowGroups.get(cursor).getMinTuple();
            if (cut.compareTo(minimum) > 0) {
                cuts.add(cut);
            }
        }
        return cuts;
    }

    private static boolean isAllNullTuple(Tuple tuple) {
        List<Variant> values = tuple.getValues();
        return values == null || values.stream().allMatch(value -> value instanceof NullVariant);
    }
}
