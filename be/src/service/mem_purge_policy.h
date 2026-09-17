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

#include <sys/types.h>

#include <cstdint>

// The decisions the memory purge daemon makes, separated from the loop that makes them so they
// can be tested without a running BE, a real allocator or wall-clock time. The daemon itself is
// then only the parts that cannot be: reading the resident size, applying a decay, and waiting.

namespace starrocks {

// One rung of the ladder that tightens jemalloc's decay as the resident size climbs.
//
// `enter_ratio` and `leave_ratio` are fractions of the process memory limit, and they differ so
// that a resident size hovering on a threshold does not toggle the rung: entering rung N needs
// rss >= limit * enter_ratio, leaving it needs rss < limit * leave_ratio, and in between nothing
// happens. This is hysteresis in the resident size; the hold before a step down is hysteresis in
// time, and the two are independent.
//
// `decay_fraction` is of the configured decay, not an absolute duration: a BE configured tighter
// than the default must not have a rung that is looser than what it already runs with, or
// "tightening" would slow reclaim down at the moment it matters most.
struct DecayLevel {
    double enter_ratio;
    double leave_ratio;
    double decay_fraction;
};

// The two cheap rungs exist to keep the backlog small, because how long the last rung takes is
// decided by how much is dirty when it runs: the same walk measured 7.5s once and 3.6s another
// time. Giving the background threads a wider window before the expensive step is therefore
// worth more than moving the expensive step itself.
//
// The fractions are what the rungs were tuned as at the 5000ms the BE ships with: 3000ms and
// 1000ms. Expressing them as fractions keeps that tuning and makes a BE configured tighter --
// 1000ms, say -- get 600ms and 200ms rather than two rungs looser than its own baseline.
inline constexpr DecayLevel kDecayLevels[] = {
        {0.70, 0.65, 0.60},
        {0.85, 0.80, 0.20},
        {1.00, 0.95, 0.00},
};
inline constexpr int kNumDecayLevels = sizeof(kDecayLevels) / sizeof(kDecayLevels[0]);

// The rung `rss` calls for on its own: the highest whose enter_ratio it has reached, or 0 for
// none. Rungs may be skipped -- a resident size that crosses two thresholds between scans enters
// the higher one directly, since waiting to climb one rung per scan would spend the runway the
// climb exists to protect.
int decay_level_to_enter(int64_t rss, int64_t limit);

// The lowest rung `rss` still justifies: the highest whose leave_ratio it has reached, or 0 for
// none. Always at or above what decay_level_to_enter() returns for the same rss.
int decay_level_to_hold(int64_t rss, int64_t limit);

// The decay to apply at `level`, as a fraction of `ladder_reference_ms`. Level 0 is not a rung --
// it means "whatever the process was configured with" -- and is rejected here so that a caller
// cannot silently get 0ms, which is the most expensive rung rather than the cheapest.
ssize_t decay_ms_at_level(int level, ssize_t ladder_reference_ms);

// The reference the ladder scales from, given what the process was started with.
//
// Restoring and scaling are not the same value. Restoring has to hand back exactly what was
// configured, -1 ("never purge") included, but -1 gives the ladder nothing to scale from -- nor
// does a decay that could not be read -- so the ladder falls back to `default_ms` rather than
// refusing to engage on the configuration most likely to need it.
ssize_t decay_ladder_reference_ms(ssize_t restore_ms, ssize_t default_ms);

// A mutable config read and clamped into [floor, ceiling], returning the value to use.
//
// `warned` carries across calls the value that was last refused, so that a misconfiguration is
// reported once rather than on every pass of a loop that runs at 10Hz. It starts at `floor`,
// which is inside the range and so can never be a refused value -- unlike a fixed 0, which is
// the value a misconfigured interval most often has.
int32_t config_ms_in_range(const char* name, int32_t configured, int32_t floor, int32_t ceiling, int32_t* warned);

} // namespace starrocks
