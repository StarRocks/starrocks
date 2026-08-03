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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Optional;

/**
 * The value window the meta tier accepts for DATE and DATETIME sort-key boundaries:
 * {@code [0001-01-01, 9999-12-31]}. A value outside it falls back to data tier (it is not a load
 * failure). The upper bound is the StarRocks DATE/DATETIME domain ceiling.
 *
 * <p>Why the window reaches year 1 — the FE-computed boundary must equal the value the BE load
 * stores, and that holds for every representable value down to the start of the unambiguous AD range:
 * <ul>
 *   <li>DATE carries no sub-second part, and BE decodes a day-of-epoch by adding a fixed Julian
 *       offset and converting with the same proleptic-Gregorian algorithm {@link LocalDate} uses
 *       (no 1582 cutover), so a DATE boundary is FE/BE-identical.</li>
 *   <li>DATETIME below 1970-01-01 is safe too: the BE timestamp load reconstructs the same wall
 *       clock the FE computes with {@code Math.floorDiv}/{@code floorMod} — keeping the sub-second of
 *       a pre-1970 value rather than dropping or corrupting it — and both the load and the
 *       boundary-string parse pack the calendar date through the same proleptic conversion, so a
 *       pre-1970 (and pre-1582) DATETIME boundary matches the loaded value down to year 1.</li>
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
    private static final LocalDate MAX_SUPPORTED_DATE = LocalDate.of(9999, 12, 31);

    private MetaTierTemporalWindow() {
    }

    /**
     * Throw {@link MetaTierUnavailableException} (the meta-tier-to-data-tier fallback signal) if the
     * calendar date of a DATE or DATETIME sort-key boundary is outside {@code [0001-01-01, 9999-12-31]}.
     */
    static void rejectOutsideWindow(LocalDate date) throws MetaTierUnavailableException {
        if (date.isBefore(MIN_SUPPORTED_DATE) || date.isAfter(MAX_SUPPORTED_DATE)) {
            throw new MetaTierUnavailableException(
                    "DATE/DATETIME meta tier supports [0001-01-01, 9999-12-31] only; value "
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

    /**
     * Resolve the load session timezone to the FIXED UTC offset the BE load applies when shifting a
     * UTC-adjusted Parquet timestamp / ORC TIMESTAMP_INSTANT to a local DATETIME wall clock, or empty
     * when the meta tier cannot safely reproduce that shift.
     *
     * <p>The BE adds the session-tz offset to the UTC instant (Parquet Int64ToDateTimeConverter when
     * isAdjustedToUTC=true; ORC orc_ts_to_native_ts when is_instant=true). We can match that boundary
     * only for a FIXED-OFFSET zone, where every instant (incl. pre-1970) gets the same constant offset,
     * so adding it to the decoded UTC min/max is order-preserving. A DST / named zone applies a
     * per-instant offset (the BE row load uses per-instant cctz), which one scalar cannot reproduce and
     * which can even reorder wall clocks near a fall-back transition -- return empty so the reader defers
     * to the data tier (never a wrong split). Named-zone aliases (CST/PRC) resolve to Asia/Shanghai, also
     * non-fixed, so they defer too.
     *
     * <p>FE reproduces the exact BE zone: the FE stamps TQueryGlobals.time_zone from the session variable
     * (CoordinatorPreprocessor.genQueryGlobals) and offset forms are canonicalized to +-HH:MM at SET time,
     * so ZoneId.of parses them. Any unresolvable / non-fixed zone -> empty (fail-safe).
     */
    static Optional<ZoneOffset> fixedLoadOffset(String loadTimeZone) {
        if (loadTimeZone == null || loadTimeZone.isEmpty()) {
            return Optional.empty();
        }
        try {
            ZoneRules rules = ZoneId.of(loadTimeZone).getRules();
            if (!rules.isFixedOffset()) {
                return Optional.empty();
            }
            return Optional.of(rules.getOffset(Instant.EPOCH));
        } catch (DateTimeException unresolvable) {
            return Optional.empty();
        }
    }
}
