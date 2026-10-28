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

#pragma once

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>

#include <cstdint>

namespace starrocks {

// Caches the UTC offset currently in force for a cctz::time_zone, so that repeated conversions
// against nearby times can skip cctz's transition-table search.
//
// cctz::time_zone itself already keeps such a hint (TimeZoneInfo::local_time_hint_ /
// time_local_hint_ in time_zone_info.cc), but it is a single mutable slot shared by *every*
// caller of that zone process-wide: under any real concurrency it becomes a cross-core
// cache-line ping-pong, and one thread's hint constantly evicts another's. This class holds the
// same kind of window privately per caller instead.
//
// Not thread-safe and not meant to be shared: each execution thread (or, in exprs/, each
// FunctionContext worker via FunctionContext::get_or_create_thread_state) should own its own
// instance. A single instance is reused across an unbounded number of calls and automatically
// adapts as the input strays outside its current window -- there is no reset needed between
// unrelated calls to the *same* (offset semantics never change for a fixed IANA zone), but an
// instance must not be reused across two different cctz::time_zone values without expecting a
// cache miss on the first call after the switch (which is always handled correctly, just not
// from the fast path).
//
// Real-world timestamp columns are almost always time-clustered within one scan batch, so most
// consecutive calls land in the same window; for a fixed-offset zone (no DST, e.g. "+08:00") the
// window degenerates to unbounded after the very first call, so every later call is a hit
// regardless of the time range covered.
class TzOffsetCache {
public:
    // Returns the UTC offset (seconds east) in force for `tz` at absolute unix time `unix_sec`.
    int64_t offset_for_unix(int64_t unix_sec, const cctz::time_zone& tz);

    // Interprets the wall-clock fields (year, month, day, hour, minute, second) as being in `tz`
    // and returns the absolute unix-second instant, matching cctz::convert(civil_second, tz)'s
    // tie-break exactly: a SKIPPED (nonexistent) civil time resolves to the transition instant, a
    // REPEATED (ambiguous) one resolves to the earlier ("pre") instant. Ambiguous inputs are
    // always re-resolved through the authoritative cctz lookup and are never cached (they are
    // rare -- at most ~1 hour per DST transition).
    //
    // `civil_as_utc_sec` is the same wall-clock fields reinterpreted as literal UTC seconds since
    // the epoch (i.e. what they'd mean with no timezone applied at all) -- the caller computes
    // this via a cheap, non-cctz calendar routine (e.g. TimestampValue::to_unix_second(), which
    // this class -- living in base/, below types/ -- cannot call itself) so the common
    // cache-hit path never has to touch cctz::civil_second at all. Passing a value inconsistent
    // with the other fields is a caller bug; it is only used verbatim, never re-derived.
    int64_t unix_for_civil(int64_t civil_as_utc_sec, int year, int month, int day, int hour, int minute, int second,
                           const cctz::time_zone& tz);

private:
    // `zone` records which cctz::time_zone the window was computed for. An instance's (from, to)
    // pair is normally fixed for its whole lifetime, but this guards against a window silently
    // being reused after the caller switches to a different zone.
    struct AbsWindow {
        bool has_value = false;
        cctz::time_zone zone;
        int64_t offset = 0;
        int64_t lo = 0; // inclusive, unix seconds
        int64_t hi = 0; // exclusive, unix seconds
    } _abs_window;

    // Bounds are in the same "civil fields reinterpreted as literal UTC seconds" domain as
    // `unix_for_civil`'s civil_as_utc_sec parameter, not cctz::civil_second -- comparing plain
    // int64s on the hot path avoids cctz::civil_second arithmetic (specifically
    // cctz::detail::impl::n_min and friends, the normalization routine backing civil_second's
    // arithmetic operators) entirely once the window is warm.
    struct CivilWindow {
        bool has_value = false;
        cctz::time_zone zone;
        int64_t offset = 0; // seconds east of UTC
        int64_t lo = 0;     // inclusive, civil-as-utc seconds
        int64_t hi = 0;     // exclusive, civil-as-utc seconds
    } _civil_window;
};

} // namespace starrocks
