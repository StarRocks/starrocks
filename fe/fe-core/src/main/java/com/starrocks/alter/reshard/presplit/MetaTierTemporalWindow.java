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

import java.time.LocalDate;

/**
 * The value window the meta tier accepts for DATE/DATETIME sort-key boundaries:
 * {@code [1970-01-01, 9999-12-31]}. A value outside it falls back to data tier
 * (it is not a load failure).
 *
 * <p>Why these bounds — render must be unambiguous and the FE-computed boundary must
 * equal the value the BE load stores:
 * <ul>
 *   <li>Below 1970-01-01 the BE timestamp conversion uses signed C++ division whose
 *       (seconds, nanos) split differs from {@code Math.floorDiv}/{@code floorMod},
 *       and pre-1582 dates raise proleptic-vs-hybrid calendar parity questions.</li>
 *   <li>Year 0 / BCE mis-renders through the {@code yyyy} (year-of-era) formatters.</li>
 *   <li>Above year 9999 leaves the StarRocks DATE/DATETIME domain.</li>
 * </ul>
 *
 * <p>This lives in one place — rather than being duplicated per reader like the
 * integer-stat conversion — because the constraint is format-agnostic: it operates on
 * an already-decoded {@link LocalDate}, identical whether the value came from a Parquet
 * INT32/INT64 stat or an ORC day-of-epoch stat.
 */
final class MetaTierTemporalWindow {

    private static final LocalDate MIN_SUPPORTED_DATE = LocalDate.of(1970, 1, 1);
    private static final LocalDate MAX_SUPPORTED_DATE = LocalDate.of(9999, 12, 31);

    private MetaTierTemporalWindow() {
    }

    /**
     * Throw {@link MetaTierUnavailableException} (the meta-tier-to-data-tier fallback signal)
     * if {@code date} is outside {@code [1970-01-01, 9999-12-31]}.
     */
    static void rejectDateOutsideWindow(LocalDate date) throws MetaTierUnavailableException {
        if (date.isBefore(MIN_SUPPORTED_DATE) || date.isAfter(MAX_SUPPORTED_DATE)) {
            throw new MetaTierUnavailableException(
                    "DATE/DATETIME meta tier supports [1970-01-01, 9999-12-31] only; value "
                            + date + " is outside that window");
        }
    }
}
