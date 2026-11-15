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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Utility methods for range distribution colocate operations.
 */
public class ColocateRangeUtils {
    private static final Logger LOG = LogManager.getLogger(ColocateRangeUtils.class);

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

    /**
     * Returns true iff {@code range}'s lower bound is a canonical {@code (k, NULL...)} tuple
     * (i.e. the shape {@link #expandToFullSortKey} produces from a colocate-range bound). The
     * upper bound is intentionally NOT required to be canonical — in a multi-way split a mid-way
     * child can have a canonical lower at the colocate boundary and a within-prefix non-canonical
     * upper. The caller pairs this with old-tablet containment to decide whether the boundary
     * is genuinely new.
     */
    public static boolean hasCanonicalLowerBound(Range<Tuple> range, List<Column> sortKeyColumns,
                                                  int colocateColumnCount) {
        Preconditions.checkArgument(colocateColumnCount >= 0
                        && colocateColumnCount <= sortKeyColumns.size(),
                "colocateColumnCount %s out of range [0, %s]",
                colocateColumnCount, sortKeyColumns.size());
        return !range.isMinimum()
                && isCanonicalTuple(range.getLowerBound(), sortKeyColumns, colocateColumnCount);
    }

    private static boolean isCanonicalTuple(Tuple tuple, List<Column> sortKeyColumns,
                                            int colocateColumnCount) {
        if (tuple == null) {
            return false;
        }
        List<Variant> values = tuple.getValues();
        if (values.size() != sortKeyColumns.size()) {
            return false;
        }
        for (int i = colocateColumnCount; i < values.size(); i++) {
            if (!(values.get(i) instanceof NullVariant)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true iff {@code tabletRange} is fully contained within the {@link ColocateRange}
     * that owns the colocate prefix of its lower bound. Used by both the scan-time alignment
     * guard ({@link com.starrocks.planner.RangeColocateScanDispatch}) and the post-publish
     * split classifier — both must agree on what "stays inside the colocate range" means so
     * the post-split classification cannot accept a tablet that the scan-time guard would
     * reject.
     *
     * <p>Returns {@code false} when no {@link ColocateRange} owns the prefix (caller should
     * treat this as the "missing coverage" defensive case rather than as crossing).
     */
    public static boolean isContainedInOwningColocateRange(Range<Tuple> tabletRange,
                                                          List<ColocateRange> ranges,
                                                          List<Column> sortKeyColumns,
                                                          int colocateColumnCount) {
        Tuple lowerPrefix = colocateColumnCount > 0
                ? extractColocatePrefix(tabletRange, colocateColumnCount)
                : null;
        int idx = ColocateRangeMgr.indexOf(ranges, lowerPrefix);
        if (idx < 0) {
            return false;
        }
        Range<Tuple> expanded = expandToFullSortKey(
                ranges.get(idx).getRange(), sortKeyColumns, colocateColumnCount);
        return expanded.contains(tabletRange);
    }

    /**
     * Binds a colocate group's ranges to ONE materialized index's sort key, so a tablet of that index
     * can be classified without the caller having to keep the pieces consistent by hand.
     *
     * <p>Exists because the classification needs three things that must agree — the ranges, those same
     * ranges expanded to the full sort key, and the colocate column count — and only the caller knows
     * which index's sort key is the right one. A rollup or MV can have a shorter sort key than the base
     * index, and expanding against the wrong one silently misclassifies every tablet of that index
     * rather than failing. Holding the three together makes that mistake unrepresentable, and expanding
     * once per index keeps it off the per-tablet path.
     */
    public static final class Classifier {
        private final List<ColocateRange> ranges;
        private final List<Range<Tuple>> expandedRanges;
        private final int colocateColumnCount;

        private Classifier(List<ColocateRange> ranges, List<Range<Tuple>> expandedRanges,
                           int colocateColumnCount) {
            this.ranges = ranges;
            this.expandedRanges = expandedRanges;
            this.colocateColumnCount = colocateColumnCount;
        }

        /**
         * @param ranges the colocate group's ranges; {@code null} or empty means the table has no
         *               usable range topology, and this returns {@code null} so the caller skips
         *               classification entirely
         * @param sortKeyColumns the sort key of the index whose tablets will be classified
         */
        @Nullable
        public static Classifier of(@Nullable List<ColocateRange> ranges, List<Column> sortKeyColumns,
                                    int colocateColumnCount) {
            // Empty is not the same as "covers everything": a registered group whose range record has
            // not been replayed yet reports an empty list, and classifying against it would call every
            // tablet uncontained. Treat it as not-colocate, exactly like a table with no group.
            if (ranges == null || ranges.isEmpty()) {
                return null;
            }
            // Expand once per index, so the per-tablet path below only compares.
            List<Range<Tuple>> expandedRanges = new ArrayList<>(ranges.size());
            for (ColocateRange colocateRange : ranges) {
                expandedRanges.add(expandToFullSortKey(
                        colocateRange.getRange(), sortKeyColumns, colocateColumnCount));
            }
            return new Classifier(ranges, expandedRanges, colocateColumnCount);
        }

        /**
         * Index of the {@link ColocateRange} that FULLY contains {@code tabletRange}, or {@code -1}
         * when none does: a null range, a range too short to carry the colocate prefix, a prefix no
         * range covers, or a range that already spans a boundary. Never throws, so a caller that has
         * already crossed a point of no return can act on the answer instead of unwinding.
         *
         * <p>Same containment rule as {@link #isContainedInOwningColocateRange} and the scan-time guard
         * in {@code RangeColocateScanDispatch}, so a caller deciding what may be merged agrees with the
         * guard deciding what may be dispatched. The two are kept in step by an equivalence case in
         * {@code ColocateRangeUtilsTest} rather than by expressing one in terms of the other, which
         * would make {@code isContainedInOwningColocateRange} expand every range where it now expands
         * only the matched one -- a per-tablet cost at its callers in {@code SplitTabletJob}.
         */
        public int indexOf(@Nullable Range<Tuple> tabletRange) {
            if (tabletRange == null) {
                return -1;
            }
            Tuple lowerPrefix;
            try {
                // colocateColumnCount == 0 must keep the prefix null (indexOf maps null to the first
                // range); extractColocatePrefix requires a positive count.
                lowerPrefix = colocateColumnCount > 0
                        ? extractColocatePrefix(tabletRange, colocateColumnCount)
                        : null;
            } catch (RuntimeException e) {
                // A mixed-version or faulty BE can publish a range whose lower tuple is shorter than
                // the colocate prefix, which trips extractColocatePrefix's precondition. Such a range
                // IS misaligned, so report it as uncontained rather than throwing at callers whose
                // only sensible response would be to report it as uncontained anyway.
                LOG.warn("Cannot extract the {}-column colocate prefix of range {}; "
                        + "treating it as uncontained.", colocateColumnCount, tabletRange, e);
                return -1;
            }
            int idx = ColocateRangeMgr.indexOf(ranges, lowerPrefix);
            if (idx < 0) {
                return -1;
            }
            return expandedRanges.get(idx).contains(tabletRange) ? idx : -1;
        }

        /** As {@link #indexOf(Range)}; a tablet with no {@link TabletRange} classifies as {@code -1}. */
        public int indexOf(Tablet tablet) {
            TabletRange tabletRange = tablet.getRange();
            return indexOf(tabletRange == null ? null : tabletRange.getRange());
        }
    }

    /**
     * Returns the PACK shard-group id that owns {@code tabletRange}, by mapping the tablet's
     * colocate prefix to the {@link ColocateRange} that contains it. Returns
     * {@link PhysicalPartition#INVALID_SHARD_GROUP_ID} when no range covers the prefix.
     *
     * <p>Shared by the post-publish split classifier ({@code SplitTabletJob}) and the placement
     * backstop ({@code ColocateChecker}): both must agree on which PACK group a tablet range maps
     * to. Callers apply their own out-of-range policy — the classifier treats it as an invariant
     * violation, the backstop skips the tablet.
     */
    public static long lookupPackShardGroupId(Range<Tuple> tabletRange, List<ColocateRange> ranges,
                                              int colocateColumnCount) {
        Tuple colocatePrefix = extractColocatePrefix(tabletRange, colocateColumnCount);
        int rangeIndex = ColocateRangeMgr.indexOf(ranges, colocatePrefix);
        if (rangeIndex < 0) {
            return PhysicalPartition.INVALID_SHARD_GROUP_ID;
        }
        return ranges.get(rangeIndex).getShardGroupId();
    }
}
