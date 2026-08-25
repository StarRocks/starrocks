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

#include "base/time/tz_offset_cache.h"

#include <algorithm>
#include <chrono>
#include <limits>

namespace starrocks {

namespace {
// civil_second interpreted as if it were UTC, in whole seconds since the Unix epoch. Used only
// as an offset-free anchor for the additive identity civil = epoch + (unix_seconds + tz_offset);
// not itself a valid timezone conversion.
int64_t civil_as_utc_seconds(const cctz::civil_second& cs) {
    return cs - cctz::civil_second(1970, 1, 1, 0, 0, 0);
}

// Past the last transition in a zone's explicit table, cctz's next_transition() returns false
// unconditionally ("ignoring future_spec_", per its own doc comment) -- it cannot tell us whether
// that's because the zone will hold `offset` forever (e.g. Asia/Shanghai and Asia/Kolkata, which
// abolished DST decades ago, or America/Phoenix, which has been fixed MST since it was defined --
// all of them reach this point on ordinary present-day lookups, not just far-future ones) or
// because it's a still-cycling DST zone we've simply run past the enumerable table for (only
// reachable past ~year 2437 for this build's tzdata).
//
// There is no zone-specific logic here -- this is a generic probe applied to whatever zone was
// passed in, and it works by exploiting a property of DST rules in general rather than anything
// about a particular zone: every real-world annual DST cycle has exactly two transitions, and
// each phase (DST / standard time) lasts several months, never mere days or weeks. So sampling
// the offset at a few points spread roughly a quarter-year apart -- +91d, +182d, +273d, i.e.
// ~3/6/9 months out, alongside `offset` itself already sampled at `tp` -- puts one sample in
// each of the four seasons. If the zone is still cycling, at least one of those samples is
// guaranteed to land in a different phase than `tp` and show a different offset; if it's
// permanently fixed, all of them trivially match. The exact day counts aren't calibrated to any
// zone's specific transition dates (that would be pointless -- the whole point is this has to
// work for a zone we can no longer enumerate transitions for); they just need to be spaced closer
// together than the shortest real-world DST phase, which they are by a wide margin.
bool offset_is_constant_beyond(const cctz::time_zone& tz, const cctz::time_point<cctz::seconds>& tp, int64_t offset) {
    for (int64_t days : {91, 182, 273}) {
        if (tz.lookup_offset(tp + cctz::seconds(days * 86400)).offset != offset) {
            return false;
        }
    }
    return true;
}
} // namespace

int64_t TzOffsetCache::offset_for_unix(int64_t unix_sec, const cctz::time_zone& tz) {
    if (_abs_window.has_value && tz == _abs_window.zone && unix_sec >= _abs_window.lo && unix_sec < _abs_window.hi) {
        return _abs_window.offset;
    }

    static const cctz::time_point<cctz::seconds> epoch =
            std::chrono::time_point_cast<cctz::seconds>(std::chrono::system_clock::from_time_t(0));
    const cctz::time_point<cctz::seconds> tp = epoch + cctz::seconds(unix_sec);
    _abs_window.offset = tz.lookup_offset(tp).offset;

    int64_t lo = std::numeric_limits<int64_t>::min();
    int64_t hi = std::numeric_limits<int64_t>::max();
    cctz::time_zone::civil_transition ct;
    // next_transition(tp)/prev_transition(tp) both use a *strict* inequality against tp, so
    // probing prev_transition at tp+1s (rather than tp) is what makes it return "the largest
    // transition <= tp" instead of skipping over a transition that lands exactly on it.
    const bool has_prev = tz.prev_transition(tp + cctz::seconds(1), &ct);
    if (has_prev) {
        lo = tz.lookup(ct.to).trans.time_since_epoch().count();
    }
    const bool has_next = tz.next_transition(tp, &ct);
    if (has_next) {
        hi = tz.lookup(ct.to).trans.time_since_epoch().count();
    }
    if (has_prev && !has_next && !offset_is_constant_beyond(tz, tp, _abs_window.offset)) {
        // has_prev true but has_next false, and the offset actually varies later on: this is a
        // zone with an ongoing DST cycle that we've simply run past cctz's enumerable transition
        // table for (see offset_is_constant_beyond's comment). Caching an unbounded window here
        // would silently reuse this offset for arbitrarily-far future instants that could be in
        // the opposite DST season -- so don't cache; every such row gets a fresh, authoritative
        // lookup instead. When the offset turns out to be constant beyond this point (e.g.
        // Asia/Shanghai, Asia/Kolkata, America/Phoenix -- the common case this branch actually
        // hits on ordinary present-day lookups), fall through and cache with hi left at +inf.
        _abs_window.has_value = false;
        return _abs_window.offset;
    }
    _abs_window.zone = tz;
    _abs_window.lo = lo;
    _abs_window.hi = hi;
    _abs_window.has_value = true;
    return _abs_window.offset;
}

int64_t TzOffsetCache::unix_for_civil(int64_t civil_as_utc_sec, int year, int month, int day, int hour, int minute,
                                      int second, const cctz::time_zone& tz) {
    if (_civil_window.has_value && tz == _civil_window.zone && civil_as_utc_sec >= _civil_window.lo &&
        civil_as_utc_sec < _civil_window.hi) {
        return civil_as_utc_sec - _civil_window.offset;
    }

    // Cold path: only reached ~twice per DST transition (or once per call for a wildly
    // out-of-order stream), so it's fine to pay cctz::civil_second's construction/arithmetic
    // cost here -- unlike the hot path above, which never touches it.
    const cctz::civil_second cs(year, month, day, hour, minute, second);
    const cctz::time_zone::civil_lookup cl = tz.lookup(cs);
    const cctz::time_point<cctz::seconds> answer =
            (cl.kind == cctz::time_zone::civil_lookup::SKIPPED) ? cl.trans : cl.pre;
    const int64_t answer_unix = answer.time_since_epoch().count();

    if (cl.kind != cctz::time_zone::civil_lookup::UNIQUE) {
        // Ambiguous wall-clock value (the skipped/repeated hour around a DST change, ~1h/year).
        // Too rare to be worth caching a window for; always re-resolve authoritatively.
        _civil_window.has_value = false;
        return answer_unix;
    }

    cctz::time_zone::civil_transition prev_trans, next_trans;
    const bool has_prev = tz.prev_transition(answer + cctz::seconds(1), &prev_trans);
    const bool has_next = tz.next_transition(answer, &next_trans);

    int64_t lo = std::numeric_limits<int64_t>::min();
    int64_t hi = std::numeric_limits<int64_t>::max();
    if (has_prev) {
        // For a spring-forward (gap) transition, prev_trans.to is later and starts the segment;
        // for a fall-back (repeat) transition, prev_trans.from is later, and cctz's "prefer pre"
        // tie-break means the whole repeated hour still belongs to the *earlier* segment, not
        // this one -- either way max(from, to) is where the current segment actually starts.
        lo = civil_as_utc_seconds(std::max(prev_trans.from, prev_trans.to));
    }
    if (has_next) {
        hi = civil_as_utc_seconds(std::min(next_trans.from, next_trans.to));
    }
    if (has_prev && !has_next && !offset_is_constant_beyond(tz, answer, civil_as_utc_sec - answer_unix)) {
        // Same "cctz gives up enumerating past the explicit table" hazard as in offset_for_unix
        // above, and the same disambiguation: only skip caching when the offset actually varies
        // beyond this point (a still-cycling DST zone run past the enumerable table), not when
        // it's simply a zone permanently fixed since its last historical transition (the common
        // case this branch hits on ordinary present-day lookups for zones like Asia/Shanghai).
        _civil_window.has_value = false;
        return answer_unix;
    }
    if (!(lo < hi)) {
        // Pre-existing degenerate/adjacent-transitions guard.
        _civil_window.has_value = false;
        return answer_unix;
    }

    _civil_window.zone = tz;
    // civil_as_utc_sec is the caller's non-cctz computation of the same quantity
    // civil_as_utc_seconds(cs) would give; using it here (instead of recomputing via cctz) keeps
    // this assignment consistent with what the hot path above will compare against later.
    _civil_window.offset = civil_as_utc_sec - answer_unix;
    _civil_window.lo = lo;
    _civil_window.hi = hi;
    _civil_window.has_value = true;
    return answer_unix;
}

} // namespace starrocks
