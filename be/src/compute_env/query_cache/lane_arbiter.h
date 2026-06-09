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
#include <gutil/macros.h>

#include <atomic>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
namespace starrocks::query_cache {
class LaneArbiter;
using LaneArbiterRawPtr = LaneArbiter*;
using LaneArbiterPtr = std::shared_ptr<LaneArbiter>;
using LaneOwnerType = int64_t;

enum AcquireResult {
    // AR_BUSY:  the new lane owner fails to grasping the lane, so wait for an available lane.
    AR_BUSY = 0,
    // AR_PROBE: the new lane owner succeeds in grasping the lane for the first time, so the cache must be probed at first.
    AR_PROBE = 1,
    // AR_SKIP:  cache probing hits the total result, so mark the lane_owner is processed, no io tasks needs to launch.
    AR_SKIP = 2,
    // AR_IO: the lane owner holds the lane or lane arbiter is in passthrough mode, so the lane owner can submit io tasks.
    AR_IO = 3,
};

// the lane arbiter is used to dictate the assignment of lanes to lane owner.
// the lane arbiter is the bookkeeper of lanes' assignment
class LaneArbiter {
public:
    struct LaneAssignment {
        LaneOwnerType lane_owner;
        // assign_seqno is used records the sequence number when the lane attaches to the lane.
        // when we pull chunk from the multilane operators, we always choose the earliest-assigned lane, to
        // reduce the retention time of the data inside multilane operators in order to reduce summit memory usage
        int64_t assign_seqno;
        bool operator==(const LaneAssignment& rhs) const {
            return lane_owner == rhs.lane_owner && assign_seqno == rhs.assign_seqno;
        }
        bool operator!=(const LaneAssignment& rhs) const { return !(*this == rhs); }
    };

    static constexpr LaneAssignment LANE_UNASSIGNED = {-1, -1};
    static constexpr int32_t NO_FREE_LANE = -1L;
    static constexpr int32_t NEW_LANE_BIT = static_cast<int32_t>(1) << 31;

    LaneArbiter(int num_lanes);
    ~LaneArbiter() = default;
    void enable_passthrough_mode();
    bool in_passthrough_mode() const;
    AcquireResult try_acquire_lane(LaneOwnerType lane_owner);
    size_t must_acquire_lane(LaneOwnerType lane_owner);
    void release_lane(LaneOwnerType lane_owner);
    std::optional<size_t> preferred_lane() const;
    int num_lanes() const;
    void mark_processed(LaneOwnerType lane_owner);

private:
    int32_t _acquire_lane(LaneOwnerType lane_owner);
    DISALLOW_COPY_AND_MOVE(LaneArbiter);
    std::atomic<bool> _passthrough_mode;
    const size_t _num_lanes;
    std::vector<LaneAssignment> _assignments;
    std::unordered_set<LaneOwnerType> _processed;
    std::atomic<int64_t> _assign_sequencer{0};
};

} // namespace starrocks::query_cache
