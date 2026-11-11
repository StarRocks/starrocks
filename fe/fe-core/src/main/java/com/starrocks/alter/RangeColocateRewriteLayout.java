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

package com.starrocks.alter;

import com.starrocks.alter.reshard.presplit.BoundaryPlanner;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;

import java.util.ArrayList;
import java.util.List;

/**
 * Plans the shadow tablet boundaries of a range rewrite of a table in a range colocate group, so that the new index
 * is aligned with the group: every tablet lies inside one ColocateRange, and every ColocateRange has a tablet.
 *
 * <p>The boundaries are the lower bounds of the ColocateRanges, expanded into the new sort key, plus row-quantile
 * cuts of the sample inside each ColocateRange. The expansion appends a NULL for each column after the colocate
 * columns, and NULL sorts below every value, so a full sort-key tuple is at or above an expanded bound exactly when
 * its colocate columns are at or above the ColocateRange's bound: the order of the sort key and the ColocateRange
 * a tuple's colocate columns fall in agree, and no tablet between two boundaries crosses a ColocateRange bound.
 */
final class RangeColocateRewriteLayout {
    private final List<ColocateRange> colocateRanges;
    private final List<Column> sortKeyColumns;
    private final int colocateColumnCount;

    /**
     * @param colocateRanges the ColocateRanges of the group
     * @param sortKeyColumns the new sort key, which starts with the colocate columns
     * @param colocateColumnCount the number of colocate columns of the group
     */
    RangeColocateRewriteLayout(List<ColocateRange> colocateRanges, List<Column> sortKeyColumns,
                               int colocateColumnCount) {
        this.colocateRanges = colocateRanges;
        this.sortKeyColumns = sortKeyColumns;
        this.colocateColumnCount = colocateColumnCount;
    }

    /**
     * Whether a sampled cut can split a ColocateRange. When the colocate columns are the whole sort key, every cut
     * has the shape of a ColocateRange bound and is dropped (see {@link #planBoundaries}).
     */
    boolean usesSampledCuts() {
        return colocateColumnCount < sortKeyColumns.size();
    }

    /** The lower bounds of the ColocateRanges, expanded into the sort key: one tablet per ColocateRange. */
    List<Tuple> colocateBoundaries() {
        List<Tuple> boundaries = new ArrayList<>(colocateRanges.size());
        for (ColocateRange colocateRange : colocateRanges) {
            Range<Tuple> expanded = ColocateRangeUtils.expandToFullSortKey(
                    colocateRange.getRange(), sortKeyColumns, colocateColumnCount);
            if (!expanded.isMinimum()) {
                boundaries.add(expanded.getLowerBound());
            }
        }
        return boundaries;
    }

    /** Splits the sample of the new sort key by the ColocateRange its colocate columns fall in. */
    List<List<Tuple>> bucketByColocateRange(List<Tuple> samples) {
        List<List<Tuple>> samplesByRange = new ArrayList<>(colocateRanges.size());
        for (int i = 0; i < colocateRanges.size(); i++) {
            samplesByRange.add(new ArrayList<>());
        }
        for (Tuple sample : samples) {
            samplesByRange.get(colocateRangeIndexOf(sample)).add(sample);
        }
        return samplesByRange;
    }

    /**
     * Shares {@code requestedTabletCount} tablets among the ColocateRanges by their share of the sample, with at
     * least one tablet per ColocateRange.
     */
    int[] tabletCountsBySampleShare(List<List<Tuple>> samplesByRange, int requestedTabletCount) {
        long sampleCount = samplesByRange.stream().mapToLong(List::size).sum();
        int[] tabletCounts = new int[colocateRanges.size()];
        for (int i = 0; i < tabletCounts.length; i++) {
            long share = sampleCount == 0 ? 0
                    : Math.round((double) requestedTabletCount * samplesByRange.get(i).size() / sampleCount);
            tabletCounts[i] = (int) Math.max(1, share);
        }
        return tabletCounts;
    }

    /**
     * The number of base tablets of each ColocateRange, at least one: the tablets whose lower bound's colocate
     * columns fall in it. A base tablet that crosses a ColocateRange bound, while the group is being aligned, counts
     * for the ColocateRange of its lower bound.
     */
    int[] tabletCountsOfBaseTablets(List<Tablet> baseTablets) {
        int[] tabletCounts = new int[colocateRanges.size()];
        for (Tablet tablet : baseTablets) {
            Tuple prefix = ColocateRangeUtils.extractColocatePrefix(tablet.getRange().getRange(), colocateColumnCount);
            tabletCounts[ColocateRangeMgr.indexOf(colocateRanges, prefix)]++;
        }
        for (int i = 0; i < tabletCounts.length; i++) {
            tabletCounts[i] = Math.max(1, tabletCounts[i]);
        }
        return tabletCounts;
    }

    /**
     * The shadow tablet boundaries: the ColocateRange bounds plus, in each ColocateRange that gets k >= 2 tablets,
     * the k - 1 row-quantile cuts of its sample. A cut whose values after the colocate columns are all NULL is
     * dropped: it has the shape of a ColocateRange bound, so the first split of the tablet it starts would register
     * it as a new ColocateRange and realign every table of the group.
     *
     * @throws StarRocksException when a sampled tuple does not match the sort key
     */
    List<Tuple> planBoundaries(List<List<Tuple>> samplesByRange, int[] tabletCounts) throws StarRocksException {
        List<Tuple> cuts = new ArrayList<>();
        for (int i = 0; i < tabletCounts.length; i++) {
            if (tabletCounts[i] < 2) {
                continue;
            }
            SampleSet rangeSamples = new SampleSet(samplesByRange.get(i), Estimates.ZERO);
            for (Tuple cut : BoundaryPlanner.planRowQuantileBoundaries(rangeSamples, tabletCounts[i], sortKeyColumns)
                    .getBoundaries()) {
                if (!ColocateRangeUtils.hasCanonicalLowerBound(Range.ge(cut), sortKeyColumns, colocateColumnCount)) {
                    cuts.add(cut);
                }
            }
        }
        return mergeBoundaries(colocateBoundaries(), cuts);
    }

    /** The number of tablets in each ColocateRange of the layout the boundaries describe. */
    int[] tabletCountsOf(List<Tuple> boundaries) {
        int[] tabletCounts = new int[colocateRanges.size()];
        // The tablet that is unbounded below.
        tabletCounts[0]++;
        for (Tuple boundary : boundaries) {
            tabletCounts[colocateRangeIndexOf(boundary)]++;
        }
        return tabletCounts;
    }

    /**
     * Sorts the boundaries and keeps one of each run that compares equal. Equal means {@code compareTo == 0}, not
     * {@code equals}: a NULL equals only a NULL of the same type, VARCHAR length included, while every NULL compares
     * equal to every other.
     */
    static List<Tuple> mergeBoundaries(List<Tuple> first, List<Tuple> second) {
        List<Tuple> sorted = new ArrayList<>(first.size() + second.size());
        sorted.addAll(first);
        sorted.addAll(second);
        sorted.sort(Tuple::compareTo);
        List<Tuple> merged = new ArrayList<>(sorted.size());
        for (Tuple boundary : sorted) {
            if (merged.isEmpty() || merged.get(merged.size() - 1).compareTo(boundary) != 0) {
                merged.add(boundary);
            }
        }
        return merged;
    }

    private int colocateRangeIndexOf(Tuple tuple) {
        return ColocateRangeMgr.indexOf(colocateRanges,
                ColocateRangeUtils.extractColocatePrefix(Range.ge(tuple), colocateColumnCount));
    }
}
