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
#include <optional>

#include "exec/pipeline/primitives/block_reason.h"

namespace starrocks::pipeline {

// CRTP mixin for the simple single-axis restore source edge shared by the agg-blocking and
// distinct-blocking sources. Each parks INPUT_EMPTY on one reason (WAIT_RESTORE, restore IO in flight) and
// waits on one drain at teardown (the spiller's restore IO), reached through its own spiller.
//
// Derived supplies two private members:
//   const std::shared_ptr<spill::Spiller>& _spiller(); // the operator's spill::Spiller
//   std::optional<BlockReason> _blocked_on();           // WAIT_RESTORE when parked on restore IO, else nullopt
// and inherits the shared predicates below: the value_or(NONE) block_reason and the one-reason coverage
// mask.
//
// has_output() and _blocked_on() stay on the derived class, because they are not identical across the two
// sources: one computes has_output() straight from _blocked_on(), the other leads with a base-class output
// check and carries a cancel branch. Each operator also keeps its own prepare() source-list subscription,
// like the flat sink mixin.
template <class Derived>
class SpillableSimpleRestoreSourceMixin {
protected:
    // Read only when parked (has_output() == false). _blocked_on() returns WAIT_RESTORE on exactly that
    // branch and nullopt on every runnable / not-spill-blocked branch, so value_or(NONE) names the restore
    // park and NONE everywhere else. value_or works on a runtime BlockReason produced on the derived class,
    // so named<>() (a compile-time literal R guard) cannot wrap this; that guard lives on each derived
    // _blocked_on()'s literal WAIT_RESTORE return, passed through the operator's own kCoveredWakeups mask.
    BlockReason spill_source_block_reason() const {
        return static_cast<const Derived*>(this)->_blocked_on().value_or(BlockReason::NONE);
    }

    // Compile-time copy of the wakeup table shared by every simple restore source: the prepare()
    // source-list subscription covers exactly WAIT_RESTORE (restore IO completion).
    static constexpr uint32_t kSpillSourceRestoreCoveredWakeups = block_reason_bit(BlockReason::WAIT_RESTORE);
};

} // namespace starrocks::pipeline
