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

import com.starrocks.common.util.DateUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * The value windows the meta tier accepts for temporal sort-key boundaries. DATE accepts
 * {@code [0001-01-01, 9999-12-31]}; DATETIME accepts the narrower {@code [1970-01-01, 9999-12-31]}.
 * A value outside its window falls back to data tier (it is not a load failure). The upper bound
 * is the StarRocks DATE/DATETIME domain ceiling for both.
 *
 * <p>Why DATE reaches year 1 but DATETIME stops at the epoch — the FE-computed boundary must equal
 * the value the BE load stores:
 * <ul>
 *   <li>DATE carries no sub-second part, and BE decodes a day-of-epoch by adding a fixed Julian
 *       offset and converting with the same proleptic-Gregorian algorithm {@link LocalDate} uses
 *       (no 1582 cutover), so a DATE boundary is FE/BE-identical for every representable value down
 *       to the start of the unambiguous AD range.</li>
 *   <li>DATETIME below 1970-01-01 is not yet safe: the BE Parquet INT64-timestamp load splits the
 *       signed tick with truncating division and does not borrow a whole second for a negative
 *       sub-second remainder, so a pre-1970 fractional-second value decodes to a value that does not
 *       match the FE {@code Math.floorDiv}/{@code floorMod} boundary (and the BE ORC pre-epoch path
 *       drops the sub-second). Until the BE load handles a negative sub-second, a sub-1970 DATETIME
 *       stays on the data tier.</li>
 * </ul>
 *
 * <p>This lives in one place — rather than being duplicated per reader like the integer-stat
 * conversion — because both responsibilities are format-agnostic: the window check operates on an
 * already-decoded {@link LocalDate}, and the microsecond datetime renderer on an already-decoded
 * {@link LocalDateTime}, identical whether the value came from a Parquet INT32/INT64 stat or an ORC
 * day-of-epoch / timestamp stat.
 */
final class MetaTierTemporalWindow {

    private static final LocalDate MIN_SUPPORTED_DATE = LocalDate.of(1, 1, 1);
    private static final LocalDate MIN_SUPPORTED_DATETIME_DATE = LocalDate.of(1970, 1, 1);
    private static final LocalDate MAX_SUPPORTED_DATE = LocalDate.of(9999, 12, 31);

    private MetaTierTemporalWindow() {
    }

    /**
     * Throw {@link MetaTierUnavailableException} (the meta-tier-to-data-tier fallback signal)
     * if a DATE sort-key boundary is outside {@code [0001-01-01, 9999-12-31]}.
     */
    static void rejectDateOutsideWindow(LocalDate date) throws MetaTierUnavailableException {
        if (date.isBefore(MIN_SUPPORTED_DATE) || date.isAfter(MAX_SUPPORTED_DATE)) {
            throw new MetaTierUnavailableException(
                    "DATE meta tier supports [0001-01-01, 9999-12-31] only; value "
                            + date + " is outside that window");
        }
    }

    /**
     * Throw {@link MetaTierUnavailableException} if the calendar date of a DATETIME sort-key boundary
     * is outside {@code [1970-01-01, 9999-12-31]}. The lower bound is the epoch (not year 1 as for
     * DATE) because the BE timestamp load does not yet decode a pre-1970 sub-second value to a
     * boundary-matching wall clock — see the class comment.
     */
    static void rejectDateTimeOutsideWindow(LocalDate date) throws MetaTierUnavailableException {
        if (date.isBefore(MIN_SUPPORTED_DATETIME_DATE) || date.isAfter(MAX_SUPPORTED_DATE)) {
            throw new MetaTierUnavailableException(
                    "DATETIME meta tier supports [1970-01-01, 9999-12-31] only; value "
                            + date + " is outside that window");
        }
    }

    /**
     * Render a decoded UTC {@link LocalDateTime} as StarRocks canonical datetime text
     * ("yyyy-MM-dd HH:mm:ss[.ffffff]"): second precision unless there is a sub-second part,
     * then a fixed 6-digit microsecond fraction. {@link DateUtils#parseStrictDateTime} and the
     * BE {@code datum_from_string} parser round-trip both. Shared by the Parquet and ORC footer
     * readers (mirrors {@code DateVariant.getStringValue}).
     *
     * <p>Both branches use the {@code %Y}-based {@code *_UNIX} formatters (the same family
     * {@code parseStrictDateTime} uses), not the deprecated {@code yyyy} ones: a
     * {@link java.time.format.DateTimeFormatter} renders numeric fields as ASCII via
     * DecimalStyle.STANDARD, so the output is locale-independent — unlike {@code String.format("%06d", …)},
     * whose digits would follow the FE JVM's default locale and emit non-ASCII digits
     * (Arabic/Persian), breaking {@code parseStrictDateTime} / BE boundary parsing.
     */
    static String renderDateTime(LocalDateTime dateTime) {
        if (dateTime.getNano() == 0) {
            return dateTime.format(DateUtils.DATE_TIME_FORMATTER_UNIX);
        }
        return dateTime.format(DateUtils.DATE_TIME_MS_FORMATTER_UNIX);
    }
}
