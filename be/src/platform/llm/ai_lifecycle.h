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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>

namespace starrocks {

struct AIQueryLifecycleSnapshot {
    bool cancelled = false;
    int64_t monotonic_deadline_ns = 0;
};

using AIQueryLifecycleProbe = std::function<AIQueryLifecycleSnapshot()>;

enum class AILifecycleState : uint8_t { ACTIVE, CANCELLED, DEADLINE_EXCEEDED };

struct AILifecycleObservation {
    AILifecycleState state = AILifecycleState::CANCELLED;
    int64_t effective_deadline_ns = 0;
};

// Combines an immutable logical-request deadline with the Query's current
// cancellation/deadline snapshot. Query lifecycle is deliberately injected at
// the Platform boundary so this module never depends on Runtime or Exec.
inline AILifecycleObservation observe_ai_lifecycle(const AIQueryLifecycleProbe& probe, int64_t request_deadline_ns,
                                                   int64_t monotonic_now_ns) noexcept {
    if (!probe) {
        return {.state = AILifecycleState::CANCELLED};
    }

    AIQueryLifecycleSnapshot snapshot;
    try {
        snapshot = probe();
    } catch (...) {
        return {.state = AILifecycleState::CANCELLED};
    }
    if (snapshot.cancelled) {
        return {.state = AILifecycleState::CANCELLED};
    }

    // QueryRuntimeState always supplies a finite Query deadline. Missing,
    // negative, or saturated values cannot safely authorize external work.
    if (snapshot.monotonic_deadline_ns <= 0 || snapshot.monotonic_deadline_ns == std::numeric_limits<int64_t>::max() ||
        request_deadline_ns < 0) {
        return {.state = AILifecycleState::DEADLINE_EXCEEDED};
    }

    const int64_t effective_deadline_ns = request_deadline_ns == 0
                                                  ? snapshot.monotonic_deadline_ns
                                                  : std::min(request_deadline_ns, snapshot.monotonic_deadline_ns);
    return {.state = monotonic_now_ns >= effective_deadline_ns ? AILifecycleState::DEADLINE_EXCEEDED
                                                               : AILifecycleState::ACTIVE,
            .effective_deadline_ns = effective_deadline_ns};
}

} // namespace starrocks
