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

#include "common/logging.h"

namespace starrocks {

int decay_level_to_enter(int64_t rss, int64_t limit) {
    int level = 0;
    for (int i = 0; i < kNumDecayLevels; i++) {
        if (rss >= static_cast<int64_t>(limit * kDecayLevels[i].enter_ratio)) {
            level = i + 1;
        }
    }
    return level;
}

int decay_level_to_hold(int64_t rss, int64_t limit) {
    int level = 0;
    for (int i = 0; i < kNumDecayLevels; i++) {
        if (rss >= static_cast<int64_t>(limit * kDecayLevels[i].leave_ratio)) {
            level = i + 1;
        }
    }
    return level;
}

ssize_t decay_ms_at_level(int level, ssize_t ladder_reference_ms) {
    DCHECK_GE(level, 1);
    DCHECK_LE(level, kNumDecayLevels);
    return static_cast<ssize_t>(ladder_reference_ms * kDecayLevels[level - 1].decay_fraction);
}

ssize_t decay_ladder_reference_ms(ssize_t restore_ms, ssize_t default_ms) {
    return restore_ms > 0 ? restore_ms : default_ms;
}

int32_t config_ms_in_range(const char* name, int32_t configured, int32_t floor, int32_t ceiling, int32_t* warned) {
    if (configured >= floor && configured <= ceiling) {
        *warned = floor;
        return configured;
    }
    const int32_t used = configured < floor ? floor : ceiling;
    if (configured != *warned) {
        LOG(WARNING) << name << " is " << configured << ", outside [" << floor << ", " << ceiling << "]ms; using "
                     << used << "ms instead";
        *warned = configured;
    }
    return used;
}

} // namespace starrocks
