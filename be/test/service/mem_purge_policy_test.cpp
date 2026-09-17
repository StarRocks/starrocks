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

#include "service/mem_purge_policy.h"

#include "gtest/gtest.h"

namespace starrocks {

// A round number so the percentages below are exact and the expectations can be read as GiB.
static constexpr int64_t kLimit = 100L * 1024 * 1024 * 1024;
static constexpr int64_t kGiB = 1024L * 1024 * 1024;

TEST(MemPurgePolicyTest, enter_thresholds) {
    EXPECT_EQ(0, decay_level_to_enter(0, kLimit));
    EXPECT_EQ(0, decay_level_to_enter(69 * kGiB, kLimit));
    EXPECT_EQ(1, decay_level_to_enter(70 * kGiB, kLimit)) << "the threshold itself enters the level";
    EXPECT_EQ(1, decay_level_to_enter(84 * kGiB, kLimit));
    EXPECT_EQ(2, decay_level_to_enter(85 * kGiB, kLimit));
    EXPECT_EQ(2, decay_level_to_enter(99 * kGiB, kLimit));
    EXPECT_EQ(3, decay_level_to_enter(kLimit, kLimit));
    EXPECT_EQ(3, decay_level_to_enter(200 * kGiB, kLimit)) << "past the top rung there is nowhere higher to go";
}

// A resident size that crosses several thresholds between two scans has to enter the highest one
// it reached. Climbing a rung per scan would spend the runway the climb exists to protect: at the
// rates this was built for, the whole 70%-to-100% span is covered in seconds.
TEST(MemPurgePolicyTest, levels_can_be_skipped) {
    EXPECT_EQ(3, decay_level_to_enter(kLimit, kLimit));
    EXPECT_EQ(2, decay_level_to_enter(90 * kGiB, kLimit));
}

// Entering and leaving deliberately disagree, so a resident size hovering on a threshold does not
// walk every arena twice per scan. Nothing at all happens inside the band.
TEST(MemPurgePolicyTest, hysteresis_band_holds_the_level) {
    for (int64_t rss : {80 * kGiB, 82 * kGiB, 84 * kGiB}) {
        EXPECT_LT(decay_level_to_enter(rss, kLimit), 2) << "rss " << rss << " must not enter level 2";
        EXPECT_GE(decay_level_to_hold(rss, kLimit), 2) << "rss " << rss << " must not leave level 2 either";
    }
    // Below the band the level is finally given up.
    EXPECT_EQ(1, decay_level_to_hold(79 * kGiB, kLimit));
    // Above it the level is entered.
    EXPECT_EQ(2, decay_level_to_enter(85 * kGiB, kLimit));
}

TEST(MemPurgePolicyTest, hold_is_never_below_enter) {
    for (int64_t gib = 0; gib <= 110; gib++) {
        const int64_t rss = gib * kGiB;
        EXPECT_GE(decay_level_to_hold(rss, kLimit), decay_level_to_enter(rss, kLimit))
                << "at " << gib << "GiB: a level that can be entered must not be one that has to be left";
    }
}

// The rungs are fractions of the configured decay, not absolute durations. The numbers they were
// tuned as are what 5000ms produces.
TEST(MemPurgePolicyTest, ladder_is_relative_to_the_configured_decay) {
    EXPECT_EQ(3000, decay_ms_at_level(1, 5000));
    EXPECT_EQ(1000, decay_ms_at_level(2, 5000));
    EXPECT_EQ(0, decay_ms_at_level(3, 5000));

    EXPECT_EQ(600, decay_ms_at_level(1, 1000));
    EXPECT_EQ(200, decay_ms_at_level(2, 1000));
    EXPECT_EQ(0, decay_ms_at_level(3, 1000));
}

// The property the fractions exist for: a BE configured tighter than the default must never be
// handed a rung that is looser than what it already runs with, or "tightening" would slow reclaim
// down at the moment it matters most.
TEST(MemPurgePolicyTest, no_rung_is_looser_than_the_baseline) {
    for (ssize_t baseline : {100, 250, 500, 1000, 3000, 5000, 10000}) {
        ssize_t previous = baseline;
        for (int level = 1; level <= kNumDecayLevels; level++) {
            const ssize_t ms = decay_ms_at_level(level, baseline);
            EXPECT_LT(ms, baseline) << "baseline " << baseline << " level " << level << " is not tighter";
            EXPECT_LE(ms, previous) << "baseline " << baseline << " level " << level << " is looser than level "
                                    << level - 1;
            previous = ms;
        }
    }
}

// -1 is a legal decay meaning "never purge". It has to be handed back on restore, but it gives
// the ladder nothing to scale from, so the ladder falls back to the default rather than refusing
// to engage on the configuration most likely to need it.
TEST(MemPurgePolicyTest, ladder_reference_falls_back_for_unusable_baselines) {
    EXPECT_EQ(5000, decay_ladder_reference_ms(5000, 5000));
    EXPECT_EQ(1000, decay_ladder_reference_ms(1000, 5000));
    EXPECT_EQ(5000, decay_ladder_reference_ms(-1, 5000)) << "never-purge is no reference to scale from";
    EXPECT_EQ(5000, decay_ladder_reference_ms(0, 5000)) << "neither is a decay of zero";
}

TEST(MemPurgePolicyTest, config_range_passes_acceptable_values) {
    int32_t warned = 10;
    EXPECT_EQ(10, config_ms_in_range("x", 10, 10, 1000, &warned)) << "the floor itself is acceptable";
    EXPECT_EQ(1000, config_ms_in_range("x", 1000, 10, 1000, &warned)) << "so is the ceiling";
    EXPECT_EQ(100, config_ms_in_range("x", 100, 10, 1000, &warned));
    EXPECT_EQ(10, warned) << "an accepted value resets the warn-once state";
}

TEST(MemPurgePolicyTest, config_range_clamps_to_the_nearer_bound) {
    int32_t warned = 10;
    EXPECT_EQ(10, config_ms_in_range("x", 0, 10, 1000, &warned));
    EXPECT_EQ(10, config_ms_in_range("x", -1, 10, 1000, &warned));
    EXPECT_EQ(10, config_ms_in_range("x", 9, 10, 1000, &warned));
    EXPECT_EQ(1000, config_ms_in_range("x", 1001, 10, 1000, &warned));
    EXPECT_EQ(1000, config_ms_in_range("x", 3600000, 10, 1000, &warned)) << "an hour is refused, not honoured";
}

// A floor of 0 accepts 0: for the step-down hold that means "relax as soon as the resident size
// is under the rung", leaving the ratio band as the only damping, which is a real choice rather
// than a misconfiguration.
TEST(MemPurgePolicyTest, a_floor_of_zero_accepts_zero) {
    int32_t warned = 0;
    EXPECT_EQ(0, config_ms_in_range("x", 0, 0, 300000, &warned));
    EXPECT_EQ(0, warned);
    EXPECT_EQ(0, config_ms_in_range("x", -5, 0, 300000, &warned)) << "but not a negative hold";
    EXPECT_EQ(-5, warned);
}

// The warn-once state is what keeps a misconfiguration from writing a line on every pass of a
// 10Hz loop. It starts at the floor rather than at 0 because 0 is the value a misconfigured
// interval most often has: with 0 as the sentinel, a configured 0 compares equal on the first
// pass and is clamped silently, which is the bug this pins.
TEST(MemPurgePolicyTest, a_refused_zero_is_still_reported_once) {
    int32_t warned = 10;
    config_ms_in_range("x", 0, 10, 1000, &warned);
    EXPECT_EQ(0, warned) << "the first refusal of 0 has to be recorded, not skipped";

    config_ms_in_range("x", 0, 10, 1000, &warned);
    EXPECT_EQ(0, warned) << "and repeats of the same value must not re-report";

    config_ms_in_range("x", -1, 10, 1000, &warned);
    EXPECT_EQ(-1, warned) << "a different refused value is reported again";

    config_ms_in_range("x", 50, 10, 1000, &warned);
    EXPECT_EQ(10, warned) << "recovering resets the state, so a later 0 is reported once more";
    config_ms_in_range("x", 0, 10, 1000, &warned);
    EXPECT_EQ(0, warned);
}

} // namespace starrocks
