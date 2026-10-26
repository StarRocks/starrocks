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
#include <vector>

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

    // Batch forms for a reader converting a column of instants in one zone. Both compare the zone once per call
    // instead of once per value (cctz::time_zone's operator== is out of line, ~5 ns per hit in offset_for_unix), and
    // share a table of the zone's offset changes over a range of instants, kept across calls and extended to the
    // transitions around it, so later batches in the same range reuse it.
    //
    // uniform_offset_for_unix: true, with *offset, when one offset is in force over all of [lo, hi]. A reader passes
    // the min and max of a batch and on true adds one constant to every value -- always for a zone without daylight
    // saving time, and for time-clustered data.
    bool uniform_offset_for_unix(int64_t lo, int64_t hi, const cctz::time_zone& tz, int64_t* offset);

    // The least and greatest offsets in force over [lo, hi], e.g. to turn the UTC min/max of a column chunk into local
    // bounds that hold every value between them, though local times are not monotonic across a daylight saving
    // change. False when the range cannot be tabled (see offsets_for_unix); a caller then has no safe bounds.
    bool offset_range_for_unix(int64_t lo, int64_t hi, const cctz::time_zone& tz, int64_t* min_offset,
                               int64_t* max_offset);

    // offsets[i] is the offset at unix_secs[i]; rows with is_null[i] != 0 (is_null may be nullptr) get 0. Each value
    // is found in the table of offset changes over the batch's range (a few compares even for decades of daylight
    // saving time); only a range with more than kMaxTableTransitions changes falls back to a cctz lookup per value.
    void offsets_for_unix(const int64_t* unix_secs, const uint8_t* is_null, size_t n, const cctz::time_zone& tz,
                          int64_t* offsets);

    // Per-value form of the batch forms, for a caller converting values one at a time in one zone: bind(tz) once
    // before a run of values (it compares the zone), then offset_in_bound_zone(unix_sec) per value, without a zone
    // comparison. A value outside the table rebuilds it over the values seen so far, widened by their span, so the
    // table is rebuilt a logarithmic number of times for any data; once that needs more than kMaxTableTransitions
    // transitions, values outside the table are looked up in cctz one by one (as without the cache).
    void bind(const cctz::time_zone& tz);
    int64_t offset_in_bound_zone(int64_t unix_sec) {
        if (unix_sec >= _segment_lo && unix_sec <= _segment_hi) {
            return _segment_offset;
        }
        return _offset_in_bound_zone_slow(unix_sec);
    }

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
    // Offsets of one zone: offsets[j] is in force from trans[j - 1] (from lo for j = 0) up to trans[j]; trans holds
    // the transition instants in (lo, hi], ascending. Built for [requested_lo, requested_hi] and widened to the
    // transitions around it, [lo, hi] (inclusive). Only used by the batch and bound forms.
    static constexpr size_t kMaxTableTransitions = 512;
    static constexpr int64_t kMaxTableSeconds = int64_t(1) << 40; // ~34,800 years around 1970
    static constexpr int64_t kMinGrowSeconds = 366LL * 86400;
    struct OffsetTable {
        bool has_value = false;
        cctz::time_zone zone;
        int64_t requested_lo = 0;
        int64_t requested_hi = 0;
        int64_t lo = 0;
        int64_t hi = 0;
        std::vector<int64_t> trans;
        std::vector<int64_t> offsets;
        // buckets[b]: the number of transitions at or before trans[0] + (b << kBucketShift), so an instant between
        // trans[0] and trans.back() starts its search one bucket (~24 days, shorter than daylight saving phases)
        // before its index instead of binary searching. Empty when the range needs more than kMaxBuckets.
        std::vector<uint32_t> buckets;
    } _table, _new_table;
    static constexpr int kBucketShift = 21;
    static constexpr size_t kMaxBuckets = 1 << 16;
    // The last range _cover could not build a table for.
    struct {
        bool has_value = false;
        cctz::time_zone zone;
        int64_t lo = 0;
        int64_t hi = 0;
    } _failed;
    // The zone of bind(), and whether a table for the values seen in it could not be built.
    bool _bound = false;
    cctz::time_zone _bound_zone;
    bool _grow_failed = false;
    // _table holds offsets of the bound zone; kept by bind() and _cover() so offset_in_bound_zone compares no zone.
    bool _table_in_bound_zone = false;
    // The stretch of the bound zone without a transition that the last value outside it fell in, with its offset:
    // the inline check of offset_in_bound_zone (empty: lo > hi).
    int64_t _segment_lo = 1;
    int64_t _segment_hi = 0;
    int64_t _segment_offset = 0;
    int64_t _offset_in_bound_zone_slow(int64_t unix_sec);
    // Makes _table cover [lo, hi] in `tz`; false, leaving _table as it was, when that needs more than
    // kMaxTableTransitions transitions or the zone's transitions cannot be enumerated up to hi.
    bool _cover(int64_t lo, int64_t hi, const cctz::time_zone& tz);
    static bool _build_table(int64_t lo, int64_t hi, const cctz::time_zone& tz, OffsetTable* table);
    // Appends the transitions after table->hi up to `hi`; false, leaving the table as it was, past
    // kMaxTableTransitions or the zone's transition table.
    static bool _extend_table(int64_t hi, const cctz::time_zone& tz, OffsetTable* table);
    static bool _finish_extension(OffsetTable* table, size_t old_size);
    // Index into _table.offsets for an instant the table covers: the number of transitions at or before it.
    size_t _table_index(int64_t unix_sec) const;

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
