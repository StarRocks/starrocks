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

#include <atomic>

namespace starrocks::pipeline {

// Slot accounting for metadata warm tasks (connector footer prefetch) on a scan operator.
// Warm tasks run on this counter, separate from the data io-task counter, so they fill spare
// io-task slots without perturbing the adaptive governor or the data re-submit gate (which
// read data-only counts); pending_finish() waits on running().
//
// The teardown contract: disable() is called from set_finishing(), while try_reserve() can be
// called from a data io-task completion on an executor thread AFTER the driver already observed
// running() == 0 via pending_finish(). try_reserve() increments the counter FIRST and only then
// checks the disabled flag, so a late reservation either is seen by the driver (count > 0 ->
// PENDING_FINISH waits) or rolls back -- never both zero-and-proceeding. Without this ordering
// the operator could re-arm a warm task after teardown began and be destroyed under it.
class WarmSlotReservation {
public:
    // Reserve one warm slot. Fails (and leaves the count unchanged) when reservations are
    // disabled, when max_warm_inflight slots are already running, or when data + warm would
    // exceed io_task_cap. fetch_add then validate-or-rollback, so concurrent reservers
    // (driver, io-finish, task self-retrigger) cannot overshoot either bound.
    bool try_reserve(int max_warm_inflight, int data_reserved, int io_task_cap) {
        int prev = _running.fetch_add(1);
        if (_disabled.load()) {
            _running.fetch_sub(1);
            return false;
        }
        if (prev + 1 > max_warm_inflight || data_reserved + prev + 1 > io_task_cap) {
            _running.fetch_sub(1);
            return false;
        }
        return true;
    }

    // Release a slot taken by try_reserve, either when the warm task finishes or when the
    // caller fails to submit it.
    void release() { _running.fetch_sub(1); }

    // Permanently reject new reservations; already-reserved slots drain via release().
    void disable() { _disabled.store(true); }

    bool disabled() const { return _disabled.load(); }

    // May transiently exceed the try_reserve bounds while concurrent reservations validate and
    // roll back; the number of successfully held slots never does. Readers tolerate the
    // overshoot: pending_finish() only waits longer, and the peak counter is informational.
    int running() const { return _running.load(); }

private:
    std::atomic<int> _running{0};
    std::atomic<bool> _disabled{false};
};

} // namespace starrocks::pipeline
