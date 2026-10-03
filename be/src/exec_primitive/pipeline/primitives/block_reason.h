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

#include <cstdint>

namespace starrocks::pipeline {

// Typed reason why a wakeable operator is blocked when its driver parks. A plain bool cannot tell the
// machine "this branch has no wakeup"; a named reason can. The operator declares why it blocked, and its
// subscriptions declare which reasons they cover, so a mismatch is caught when the driver parks. Each enum
// value is the index of a one-hot bit in the covered_wakeups() mask.
enum class BlockReason : uint8_t {
    NONE = 0,     // not blocked / a non-spill reason
    WAIT_FLUSH,   // writer full / mem-tables in flight        (woken by flush completion)
    WAIT_RESTORE, // reader waiting on restore IO              (woken by restore completion)
    WAIT_CHANNEL, // spill-process channel holding tasks       (woken by channel enqueue/drain)
    WAIT_LATCH,   // own load tasks (join build-side load)
    WAIT_BUILD,   // a neighbour's phase handoff (join build-done)
};

// One-hot bit for a reason, used to build and test covered_wakeups() masks. NONE has no bit: a
// not-blocked operator covers nothing and never fails the check.
constexpr uint32_t block_reason_bit(BlockReason reason) {
    return reason == BlockReason::NONE ? 0u : (1u << static_cast<uint32_t>(reason));
}

// Compile-time guard for a block_reason() return value. It accepts a reason only if that reason is in the
// operator's coverage mask (or is NONE, which is always allowed because it means "not parked"). So
// `return named<R, Mask>()` with an R that covered_wakeups() does not cover does not compile: an operator
// cannot return a reason its subscriptions never wake. The runtime check at park time
// (verify_block_reason_covered) is then a second line of defense, not the only one. The function returns R
// unchanged, so the value is the same as the plain enum; it only adds the static_assert.
template <BlockReason R, uint32_t Mask>
constexpr BlockReason named() {
    static_assert(R == BlockReason::NONE || (block_reason_bit(R) & Mask) != 0u,
                  "block_reason() may only return a reason inside this operator's covered_wakeups() mask");
    return R;
}

inline const char* block_reason_name(BlockReason reason) {
    switch (reason) {
    case BlockReason::NONE:
        return "NONE";
    case BlockReason::WAIT_FLUSH:
        return "WAIT_FLUSH";
    case BlockReason::WAIT_RESTORE:
        return "WAIT_RESTORE";
    case BlockReason::WAIT_CHANNEL:
        return "WAIT_CHANNEL";
    case BlockReason::WAIT_LATCH:
        return "WAIT_LATCH";
    case BlockReason::WAIT_BUILD:
        return "WAIT_BUILD";
    }
    return "UNKNOWN";
}

} // namespace starrocks::pipeline
