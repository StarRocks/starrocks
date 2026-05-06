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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.common.Range;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for range distribution colocate operations.
 */
public class ColocateRangeUtils {

    /**
     * Expands a colocate range (on colocate column prefix) to a full sort key range
     * by appending NULL variant values for the remaining sort key columns.
     *
     * <p>Colocate ranges are always in [lower, upper) form (inclusive lower, exclusive upper),
     * which is guaranteed by ColocateRangeMgr.splitColocateRange(). For this form, NULL
     * variant (which sorts before all normal values) is the correct sentinel for both bounds.
     *
     * <p>For example, with sort key (k1, k2, k3), colocate columns (k1):
     * <ul>
     *   <li>[100, 200) -> [(100, NULL, NULL), (200, NULL, NULL))</li>
     *   <li>[100, +inf) -> [(100, NULL, NULL), +inf)</li>
     *   <li>(-inf, 200) -> (-inf, (200, NULL, NULL))</li>
     * </ul>
     *
     * <p>For ALL range (initial state), returns ALL directly without expansion.
     *
     * @param colocateRange the colocate range to expand (must be [lower, upper) form)
     * @param sortKeyColumns the full sort key columns
     * @param colocateColumnCount the number of colocate columns (prefix of sort key)
     * @return the expanded range covering the full sort key
     */
    public static Range<Tuple> expandToFullSortKey(Range<Tuple> colocateRange,
                                                    List<Column> sortKeyColumns,
                                                    int colocateColumnCount) {
        Preconditions.checkArgument(colocateColumnCount >= 0
                        && colocateColumnCount <= sortKeyColumns.size(),
                "colocateColumnCount %s out of range [0, %s]",
                colocateColumnCount, sortKeyColumns.size());
        if (colocateRange.isAll()) {
            return Range.all();
        }
        // Colocate ranges are always [lower, upper) form
        Preconditions.checkArgument(colocateRange.isMinimum() || colocateRange.isLowerBoundIncluded(),
                "Colocate range lower bound must be inclusive or infinite");
        Preconditions.checkArgument(colocateRange.isMaximum() || !colocateRange.isUpperBoundIncluded(),
                "Colocate range upper bound must be exclusive or infinite");

        int remainingColumns = sortKeyColumns.size() - colocateColumnCount;
        Tuple lowerBound = colocateRange.isMinimum() ? null
                : extendTupleWithNull(colocateRange.getLowerBound(), sortKeyColumns,
                        colocateColumnCount, remainingColumns);
        Tuple upperBound = colocateRange.isMaximum() ? null
                : extendTupleWithNull(colocateRange.getUpperBound(), sortKeyColumns,
                        colocateColumnCount, remainingColumns);
        return Range.of(lowerBound, upperBound,
                colocateRange.isLowerBoundIncluded(),
                colocateRange.isUpperBoundIncluded());
    }

    private static Tuple extendTupleWithNull(Tuple tuple, List<Column> sortKeyColumns,
                                              int colocateColumnCount, int remainingColumns) {
        List<Variant> values = new ArrayList<>(tuple.getValues());
        for (int i = 0; i < remainingColumns; i++) {
            values.add(Variant.nullVariant(sortKeyColumns.get(colocateColumnCount + i).getType()));
        }
        return new Tuple(values);
    }

    /**
     * Extracts the colocate column prefix from a tablet's full sort-key range.
     *
     * <p>Inverse of {@link #expandToFullSortKey}: a tablet whose range was produced by
     * expansion stores a full sort-key tuple in its lower bound, but the colocate-range
     * lookup keys on the colocate prefix only.
     *
     * <p>If the tablet range is unbounded below (lower bound is -inf), this returns
     * {@code null}, signaling the caller to fall back to the first colocate range
     * (which always begins at -inf by the {@link ColocateRangeMgr} invariant).
     *
     * <p>Unlike {@link #expandToFullSortKey}, this method requires
     * {@code colocateColumnCount > 0}: a colocate group with zero colocate columns
     * is not a meaningful concept on the lookup side (every value would map to the
     * same range, which is just the no-colocate case).
     *
     * @param tabletRange the tablet's full sort-key range
     * @param colocateColumnCount the number of colocate columns (sort key prefix length),
     *                            must be positive
     * @return the colocate prefix Tuple, or {@code null} if the range is unbounded below
     */
    public static Tuple extractColocatePrefix(Range<Tuple> tabletRange, int colocateColumnCount) {
        Preconditions.checkArgument(colocateColumnCount > 0,
                "colocateColumnCount must be positive, got %s", colocateColumnCount);
        if (tabletRange.isMinimum()) {
            return null;
        }
        List<Variant> values = tabletRange.getLowerBound().getValues();
        Preconditions.checkArgument(values.size() >= colocateColumnCount,
                "tablet lower bound has %s values, fewer than colocateColumnCount %s",
                values.size(), colocateColumnCount);
        if (values.size() == colocateColumnCount) {
            return tabletRange.getLowerBound();
        }
        // subList returns a view backed by the original list; copy so that the
        // returned Tuple does not retain a reference to the tablet's bound.
        return new Tuple(new ArrayList<>(values.subList(0, colocateColumnCount)));
    }
}
