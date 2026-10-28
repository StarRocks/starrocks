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

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <random>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "base/time/timezone_utils.h"

namespace starrocks {

namespace {

cctz::time_point<cctz::seconds> unix_to_tp(int64_t unix_sec) {
    static const cctz::time_point<cctz::seconds> epoch =
            std::chrono::time_point_cast<cctz::seconds>(std::chrono::system_clock::from_time_t(0));
    return epoch + cctz::seconds(unix_sec);
}

int64_t ground_truth_offset(const cctz::time_zone& tz, int64_t unix_sec) {
    return tz.lookup(unix_to_tp(unix_sec)).offset;
}

cctz::time_point<cctz::seconds> ground_truth_unix_for_civil(const cctz::civil_second& cs, const cctz::time_zone& tz) {
    return cctz::convert(cs, tz);
}

// Stands in for what a real caller (TimestampValue::to_unix_second(), which base/ cannot call)
// computes: the civil fields reinterpreted as literal UTC seconds. This test only needs *a*
// correct implementation of that quantity, not the production one -- cross-checking against
// TimestampValue itself is done at the exprs/ level (time_functions_test.cpp), which can see
// both types.
int64_t civil_as_utc_seconds(const cctz::civil_second& cs) {
    return cs - cctz::civil_second(1970, 1, 1, 0, 0, 0);
}

int64_t call_unix_for_civil(TzOffsetCache& cache, const cctz::civil_second& cs, const cctz::time_zone& tz) {
    return cache.unix_for_civil(civil_as_utc_seconds(cs), cs.year(), cs.month(), cs.day(), cs.hour(), cs.minute(),
                                cs.second(), tz);
}

} // namespace

class TzOffsetCacheTest : public testing::Test {};

// Densely probes every recorded transition (+/- a handful of seconds/minutes/hours/day) plus a
// dense random sample, checking TzOffsetCache::offset_for_unix against cctz's direct lookup.
// Exercises both a DST zone (America/Los_Angeles) and a fixed-offset zone (no transitions at
// all, so the cache should degenerate to "always a hit after the first call").
PARALLEL_TEST(TzOffsetCacheTest, offset_for_unix_matches_ground_truth) {
    std::vector<std::string> zones = {
            "America/Los_Angeles", "Europe/London", "Australia/Lord_Howe", "UTC", "+08:00", "-08:00"};
    for (const auto& name : zones) {
        // find_cctz_time_zone (not cctz::load_time_zone, which only understands IANA names) is
        // what production code actually resolves timezone strings with, and is what makes
        // "+08:00"/"-08:00" -- fixed-offset, no-DST-table zones -- work in this list.
        cctz::time_zone tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(name, tz)) << name;

        TzOffsetCache cache;
        std::mt19937_64 rng(12345);

        auto tp = cctz::time_point<cctz::seconds>::min();
        cctz::time_zone::civil_transition trans;
        int count = 0;
        while (tz.next_transition(tp, &trans) && count < 500) {
            int64_t instant = tz.lookup(trans.to).trans.time_since_epoch().count();
            for (int64_t d : {-90000LL, -3601LL, -3600LL, -1LL, 0LL, 1LL, 3600LL, 3601LL, 90000LL}) {
                int64_t probe = instant + d;
                EXPECT_EQ(cache.offset_for_unix(probe, tz), ground_truth_offset(tz, probe)) << name << " abs=" << probe;
            }
            tp = tz.lookup(trans.to).trans;
            ++count;
        }

        std::vector<int64_t> samples;
        std::uniform_int_distribution<int64_t> d(-1000000000LL, 3000000000LL);
        for (int i = 0; i < 20000; ++i) samples.push_back(d(rng));
        std::sort(samples.begin(), samples.end());
        for (int64_t probe : samples) {
            EXPECT_EQ(cache.offset_for_unix(probe, tz), ground_truth_offset(tz, probe)) << name << " abs=" << probe;
        }

        // Adversarial (non-monotonic) order must still be correct with a fresh cache.
        std::shuffle(samples.begin(), samples.end(), rng);
        TzOffsetCache shuffled_cache;
        for (int64_t probe : samples) {
            EXPECT_EQ(shuffled_cache.offset_for_unix(probe, tz), ground_truth_offset(tz, probe))
                    << name << " (shuffled) abs=" << probe;
        }
    }
}

// Same idea for unix_for_civil, additionally probing exactly the SKIPPED (nonexistent) and
// REPEATED (ambiguous) civil ranges around each transition, where cctz::convert()'s tie-break
// (SKIPPED -> transition instant, REPEATED -> earlier/"pre" instant) must be reproduced exactly.
PARALLEL_TEST(TzOffsetCacheTest, unix_for_civil_matches_ground_truth) {
    std::vector<std::string> zones = {
            "America/Los_Angeles", "Europe/London", "Australia/Lord_Howe", "UTC", "+08:00", "-08:00"};
    for (const auto& name : zones) {
        // find_cctz_time_zone (not cctz::load_time_zone, which only understands IANA names) is
        // what production code actually resolves timezone strings with, and is what makes
        // "+08:00"/"-08:00" -- fixed-offset, no-DST-table zones -- work in this list.
        cctz::time_zone tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(name, tz)) << name;

        TzOffsetCache cache;
        std::mt19937_64 rng(999);

        auto tp = cctz::time_point<cctz::seconds>::min();
        cctz::time_zone::civil_transition trans;
        int count = 0;
        while (tz.next_transition(tp, &trans) && count < 500) {
            for (cctz::civil_second base : {trans.from, trans.to}) {
                for (int64_t d : {-3660LL, -3601LL, -1LL, 0LL, 1LL, 3600LL, 3660LL, 86400LL, -86400LL}) {
                    cctz::civil_second cs = base + d;
                    int64_t got = call_unix_for_civil(cache, cs, tz);
                    int64_t want = ground_truth_unix_for_civil(cs, tz).time_since_epoch().count();
                    EXPECT_EQ(got, want) << name << " cs=" << cs;
                }
            }
            tp = tz.lookup(trans.to).trans;
            ++count;
        }

        std::vector<cctz::civil_second> samples;
        std::uniform_int_distribution<int> yd(1970, 2060), md(1, 12), dd(1, 28), hd(0, 23), mnd(0, 59), sd(0, 59);
        for (int i = 0; i < 20000; ++i) {
            samples.emplace_back(yd(rng), md(rng), dd(rng), hd(rng), mnd(rng), sd(rng));
        }
        std::sort(samples.begin(), samples.end());
        for (auto& cs : samples) {
            int64_t got = call_unix_for_civil(cache, cs, tz);
            int64_t want = ground_truth_unix_for_civil(cs, tz).time_since_epoch().count();
            EXPECT_EQ(got, want) << name << " cs=" << cs;
        }

        std::shuffle(samples.begin(), samples.end(), rng);
        TzOffsetCache shuffled_cache;
        for (auto& cs : samples) {
            int64_t got = call_unix_for_civil(shuffled_cache, cs, tz);
            int64_t want = ground_truth_unix_for_civil(cs, tz).time_since_epoch().count();
            EXPECT_EQ(got, want) << name << " (shuffled) cs=" << cs;
        }
    }
}

// Regression test for a real bug: next_transition()/prev_transition() give up ("ignoring
// future_spec_", per cctz's own doc comment on NextTransition) once past a zone's explicit
// zoneinfo table, even for a zone with an ongoing POSIX future DST rule -- has_prev is still
// true (a real transition exists somewhere) but has_next becomes false. Before the fix, that
// combination was (wrongly) treated the same as "no transitions anywhere" (a genuinely
// fixed-offset zone, where both are false) and cached an unbounded window, so whichever
// season's offset got computed first for a date past the table could get silently reused for
// any later date -- including one in the opposite season. Both orderings are checked since the
// bug depended on which query happened to populate the cache first.
PARALLEL_TEST(TzOffsetCacheTest, offset_for_unix_past_explicit_table_does_not_poison_cache) {
    cctz::time_zone tz;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("America/Los_Angeles", tz));

    // Both instants are far beyond this build's explicit zoneinfo table (verified to end around
    // year 2437) and fall in opposite DST seasons.
    cctz::civil_second summer(5468, 8, 10, 8, 0, 0);
    cctz::civil_second winter(5469, 1, 15, 8, 0, 0);
    int64_t summer_unix = cctz::convert(summer, tz).time_since_epoch().count();
    int64_t winter_unix = cctz::convert(winter, tz).time_since_epoch().count();
    ASSERT_NE(ground_truth_offset(tz, summer_unix), ground_truth_offset(tz, winter_unix))
            << "test fixture assumption broken: these two instants must be in different DST seasons";

    TzOffsetCache summer_first;
    EXPECT_EQ(summer_first.offset_for_unix(summer_unix, tz), ground_truth_offset(tz, summer_unix));
    EXPECT_EQ(summer_first.offset_for_unix(winter_unix, tz), ground_truth_offset(tz, winter_unix));

    TzOffsetCache winter_first;
    EXPECT_EQ(winter_first.offset_for_unix(winter_unix, tz), ground_truth_offset(tz, winter_unix));
    EXPECT_EQ(winter_first.offset_for_unix(summer_unix, tz), ground_truth_offset(tz, summer_unix));
}

// Same bug class, civil direction.
PARALLEL_TEST(TzOffsetCacheTest, unix_for_civil_past_explicit_table_does_not_poison_cache) {
    cctz::time_zone tz;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("America/Los_Angeles", tz));

    cctz::civil_second summer(5468, 8, 10, 8, 0, 0);
    cctz::civil_second winter(5469, 1, 15, 8, 0, 0);
    int64_t want_summer = ground_truth_unix_for_civil(summer, tz).time_since_epoch().count();
    int64_t want_winter = ground_truth_unix_for_civil(winter, tz).time_since_epoch().count();

    TzOffsetCache summer_first;
    EXPECT_EQ(call_unix_for_civil(summer_first, summer, tz), want_summer);
    EXPECT_EQ(call_unix_for_civil(summer_first, winter, tz), want_winter);

    TzOffsetCache winter_first;
    EXPECT_EQ(call_unix_for_civil(winter_first, winter, tz), want_winter);
    EXPECT_EQ(call_unix_for_civil(winter_first, summer, tz), want_summer);
}

// The fix must not regress the fixed-offset fast path this whole cache exists for: a zone with
// literally no transitions (has_prev == has_next == false everywhere) still needs to degenerate
// to an unbounded, cached window so consecutive calls stay O(1) regardless of how far apart the
// queried instants are.
PARALLEL_TEST(TzOffsetCacheTest, fixed_offset_zone_still_gets_unbounded_cached_window) {
    cctz::time_zone tz;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", tz));

    TzOffsetCache cache;
    EXPECT_EQ(cache.offset_for_unix(0, tz), 28800);
    // A wildly different instant must still be a cache *hit* (same offset either way, but this
    // is what proves the window is actually unbounded rather than accidentally narrow).
    EXPECT_EQ(cache.offset_for_unix(4000000000000LL, tz), 28800);
    EXPECT_EQ(cache.offset_for_unix(-4000000000000LL, tz), 28800);
}

// Regression test for a bug found in review of the fix above: the same has_prev-&&-!has_next
// condition that correctly flags "still cycling past the enumerable table" also fires -- for an
// entirely different reason -- on ordinary present-day lookups into a zone that had exactly one
// (or a handful of) historical transitions and has been permanently fixed ever since: e.g.
// Asia/Shanghai stopped observing DST in 1991, Asia/Kolkata's last offset change was in 1945,
// and America/Phoenix has had no DST since the 1940s. For those zones has_next is false for
// *every* query from that point onward, not just far-future ones, so unconditionally disabling
// the cache there would defeat this class's whole purpose for some of the most common timezones
// in practice. The fix must tell the two cases apart and still cache an unbounded window for the
// permanently-fixed case.
PARALLEL_TEST(TzOffsetCacheTest, permanently_fixed_zone_after_last_transition_still_caches_unbounded) {
    std::vector<std::string> zones = {"Asia/Shanghai", "Asia/Kolkata", "America/Phoenix"};
    for (const auto& name : zones) {
        cctz::time_zone tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(name, tz)) << name;

        // Sanity-check the fixture assumption: an arbitrary present-day instant really does land
        // in the has_prev && !has_next branch for these zones (i.e. this test would be vacuous
        // otherwise).
        cctz::civil_second now(2026, 8, 25, 12, 0, 0);
        int64_t now_unix = cctz::convert(now, tz).time_since_epoch().count();
        cctz::time_zone::civil_transition ct;
        ASSERT_TRUE(tz.prev_transition(unix_to_tp(now_unix) + cctz::seconds(1), &ct)) << name;
        ASSERT_FALSE(tz.next_transition(unix_to_tp(now_unix), &ct)) << name;

        TzOffsetCache cache;
        EXPECT_EQ(cache.offset_for_unix(now_unix, tz), ground_truth_offset(tz, now_unix)) << name;
        EXPECT_TRUE(cache._abs_window.has_value) << name << ": should still cache, not disable";
        EXPECT_EQ(cache._abs_window.hi, std::numeric_limits<int64_t>::max())
                << name << ": should cache an unbounded window, not a bounded or absent one";

        // A wildly different (but still post-last-transition) future instant must be a hit
        // against that same unbounded window, and still correct.
        int64_t far_future_unix = now_unix + 100LL * 365 * 86400;
        EXPECT_EQ(cache.offset_for_unix(far_future_unix, tz), ground_truth_offset(tz, far_future_unix)) << name;
        EXPECT_EQ(cache._abs_window.hi, std::numeric_limits<int64_t>::max())
                << name << ": window should not have been narrowed/invalidated by the second call";
    }
}

// Same idea, civil direction.
PARALLEL_TEST(TzOffsetCacheTest, permanently_fixed_zone_after_last_transition_still_caches_unbounded_civil) {
    std::vector<std::string> zones = {"Asia/Shanghai", "Asia/Kolkata", "America/Phoenix"};
    for (const auto& name : zones) {
        cctz::time_zone tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(name, tz)) << name;

        cctz::civil_second now(2026, 8, 25, 12, 0, 0);
        cctz::time_point<cctz::seconds> now_answer = cctz::convert(now, tz);
        cctz::time_zone::civil_transition ct;
        ASSERT_TRUE(tz.prev_transition(now_answer + cctz::seconds(1), &ct)) << name;
        ASSERT_FALSE(tz.next_transition(now_answer, &ct)) << name;

        TzOffsetCache cache;
        int64_t got = call_unix_for_civil(cache, now, tz);
        EXPECT_EQ(got, now_answer.time_since_epoch().count()) << name;
        EXPECT_TRUE(cache._civil_window.has_value) << name << ": should still cache, not disable";
        EXPECT_EQ(cache._civil_window.hi, std::numeric_limits<int64_t>::max())
                << name << ": should cache an unbounded window, not a bounded or absent one";

        cctz::civil_second far_future = now + int64_t{100LL * 365 * 86400};
        int64_t want = ground_truth_unix_for_civil(far_future, tz).time_since_epoch().count();
        EXPECT_EQ(call_unix_for_civil(cache, far_future, tz), want) << name;
        EXPECT_EQ(cache._civil_window.hi, std::numeric_limits<int64_t>::max())
                << name << ": window should not have been narrowed/invalidated by the second call";
    }
}

// A single TzOffsetCache instance is used exactly as convert_tz_const uses it: unix_for_civil()
// against `from`, then offset_for_unix() against `to`, on the same instance. Verifies the two
// directions don't interfere with each other's window, across DST and fixed-offset zone pairs.
PARALLEL_TEST(TzOffsetCacheTest, combined_round_trip_matches_reference_convert_tz) {
    struct Pair {
        std::string from, to;
    };
    std::vector<Pair> pairs = {
            {"UTC", "America/Los_Angeles"},
            {"America/Los_Angeles", "UTC"},
            {"+00:00", "-08:00"},
            {"-08:00", "+00:00"},
    };
    for (auto& p : pairs) {
        cctz::time_zone from_tz, to_tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(p.from, from_tz));
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(p.to, to_tz));

        TzOffsetCache cache;
        std::mt19937_64 rng(42);
        std::vector<cctz::civil_second> samples;
        std::uniform_int_distribution<int> yd(1970, 2060), md(1, 12), dd(1, 28), hd(0, 23), mnd(0, 59), sd(0, 59);
        for (int i = 0; i < 20000; ++i) {
            samples.emplace_back(yd(rng), md(rng), dd(rng), hd(rng), mnd(rng), sd(rng));
        }
        // Deliberately unsorted/interleaved to stress both windows at once.
        std::shuffle(samples.begin(), samples.end(), rng);

        for (auto& cs : samples) {
            int64_t unix_sec = call_unix_for_civil(cache, cs, from_tz);
            int64_t offset = cache.offset_for_unix(unix_sec, to_tz);
            cctz::civil_second got = cctz::civil_second(1970, 1, 1, 0, 0, 0) + (unix_sec + offset);

            // Reference: exactly what convert_tz_const computed before this optimization
            // (civil -> absolute in `from`, then absolute -> civil in `to`).
            cctz::time_point<cctz::seconds> ref_abs = cctz::convert(cs, from_tz);
            cctz::civil_second want = to_tz.lookup(ref_abs).cs;

            EXPECT_EQ(got, want) << p.from << "->" << p.to << " cs=" << cs;
        }
    }
}

} // namespace starrocks
