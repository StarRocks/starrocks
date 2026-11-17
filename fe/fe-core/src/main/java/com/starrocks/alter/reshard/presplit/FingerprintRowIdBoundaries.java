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
import com.starrocks.common.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Derives range-split boundaries for a table whose sort key is a single hidden row-id column holding
 * {@code FROM_BINARY(<SHA-256 digest>, 'encode64')}: a 44-character standard base64 string of 32
 * uniformly distributed bytes. No data is read; the estimated output size alone decides how many
 * equal shares the key space is cut into.
 *
 * <p>Every base64 character of such an id is uniform over the 64 symbols, so the leading characters
 * behave as digits of a uniform number in base 64, and a cut is a short prefix of those digits. The
 * symbol table below is in byte order ({@code '+' < '/' < digits < upper case < lower case}) because
 * the BE compares VARCHAR keys with memcmp; ordering the table any other way would make a prefix
 * that is numerically larger sort lower.
 *
 * <p>The prefix is made long enough that the space holds at least 256 positions per tablet, so
 * rounding a cut to a whole position moves a tablet's share by less than 1/256 of the mean. The
 * tablet count is clamped by {@code tablet_reshard_min_split_size} on the unrounded share; the
 * rounding is far below the error of the byte estimate itself.
 */
final class FingerprintRowIdBoundaries {

    /** The base64 alphabet in byte order, indexed by digit value. */
    private static final String SYMBOLS = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private static final int RADIX = 64;

    /** Minimum number of prefix positions available to each tablet. */
    private static final long POSITIONS_PER_TABLET = 256;

    private FingerprintRowIdBoundaries() {
    }

    /**
     * @param totalBytes           estimated number of bytes the load will write
     * @param requestedTabletCount tablet count the caller's byte-based sizing asked for
     * @param sortKeyColumn        the row-id column the cuts are expressed in
     */
    static DerivedBoundarySource.Result plan(long totalBytes, int requestedTabletCount, Column sortKeyColumn) {
        Objects.requireNonNull(sortKeyColumn, "sortKeyColumn");

        if (totalBytes <= 0) {
            return DerivedBoundarySource.Result.skipped(SkipReason.ESTIMATE_UNAVAILABLE);
        }
        long minSplitSize = Config.tablet_reshard_min_split_size;
        if (minSplitSize <= 0) {
            return DerivedBoundarySource.Result.skipped(SkipReason.DERIVATION_FAILED);
        }
        int effectiveTabletCount = (int) Math.min(requestedTabletCount, totalBytes / minSplitSize);
        if (effectiveTabletCount < 2) {
            return DerivedBoundarySource.Result.skipped(SkipReason.ROW_ID_SPAN_TOO_SMALL);
        }

        int length = 1;
        long space = RADIX;
        while (space < POSITIONS_PER_TABLET * effectiveTabletCount) {
            length++;
            space *= RADIX;
        }

        List<Tuple> cuts = new ArrayList<>(effectiveTabletCount - 1);
        for (int i = 1; i < effectiveTabletCount; i++) {
            // Strictly increasing because space > effectiveTabletCount. The exact multiply cannot
            // overflow for any realistic count; an absurd one throws rather than yielding wrong cuts.
            long position = Math.multiplyExact((long) i, space) / effectiveTabletCount;
            // The variant carries the column's own type: the FE and BE boundary validators reject a
            // variant whose type differs from the column's.
            cuts.add(new Tuple(List.of(Variant.of(sortKeyColumn.getType(), toSymbols(position, length)))));
        }
        return DerivedBoundarySource.Result.of(new BoundaryPlannerResult(cuts));
    }

    /** Writes {@code position} as {@code length} base-64 digits, most significant first. */
    private static String toSymbols(long position, int length) {
        char[] digits = new char[length];
        for (int i = length - 1; i >= 0; i--) {
            digits[i] = SYMBOLS.charAt((int) (position % RADIX));
            position /= RADIX;
        }
        return new String(digits);
    }
}
