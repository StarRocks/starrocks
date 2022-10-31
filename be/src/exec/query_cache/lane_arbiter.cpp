// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/query_cache/lane_arbiter.h"

#include <glog/logging.h>

#include <algorithm>
#include <limits>

namespace starrocks {
namespace query_cache {

LaneArbiter::LaneArbiter(int num_lanes)
        : _passthrough_mode(false), _num_lanes(num_lanes), _assignments(num_lanes, LANE_UNASSIGNED) {}

void LaneArbiter::enable_passthrough_mode() {
    _passthrough_mode = true;
}

bool LaneArbiter::in_passthrough_mode() const {
    return _passthrough_mode;
}

int LaneArbiter::num_lanes() const {
    return _num_lanes;
}

std::optional<size_t> LaneArbiter::preferred_lane() const {
    int lane = NO_FREE_LANE;
    int min_seqno = std::numeric_limits<int>::max();
    for (auto i = 0; i < _assignments.size(); ++i) {
        if (_assignments[i] != LANE_UNASSIGNED && _assignments[i].assign_seqno < min_seqno) {
            min_seqno = _assignments[i].assign_seqno;
            lane = i;
        }
    }
    return lane == NO_FREE_LANE ? std::optional<size_t>{} : std::optional<size_t>{lane};
}

int32_t LaneArbiter::_acquire_lane(LaneOwnerType lane_owner) {
    auto unassigned_lane = NO_FREE_LANE;
    for (auto i = 0; i < _assignments.size(); ++i) {
        if (_assignments[i] == LANE_UNASSIGNED) {
            unassigned_lane = i;
            continue;
        }
        if (_assignments[i].lane_owner == lane_owner) {
            return i;
        }
    }
    if (unassigned_lane == NO_FREE_LANE) {
        return unassigned_lane;
    }
    _assignments[unassigned_lane] =
            LaneAssignment{.lane_owner = lane_owner, .assign_seqno = _assign_sequencer.fetch_add(1)};
    unassigned_lane = unassigned_lane | NEW_LANE_BIT;
    return unassigned_lane;
}

AcquireResult LaneArbiter::try_acquire_lane(LaneOwnerType lane_owner) {
    if (in_passthrough_mode()) {
        return AcquireResult::AR_IO;
    }
    if (_processed.count(lane_owner)) {
        return AcquireResult::AR_SKIP;
    }
    auto lane = _acquire_lane(lane_owner);
    if (lane == NO_FREE_LANE) {
        return AcquireResult::AR_BUSY;
    } else if ((lane & NEW_LANE_BIT) == NEW_LANE_BIT) {
        return AcquireResult::AR_PROBE;
    } else {
        return AcquireResult::AR_IO;
    }
}

size_t LaneArbiter::must_acquire_lane(LaneOwnerType lane_owner) {
    DCHECK(!in_passthrough_mode());
    int lane = _acquire_lane(lane_owner);
    DCHECK(lane != NO_FREE_LANE);
    lane = lane & ~NEW_LANE_BIT;
    return lane;
}

void LaneArbiter::release_lane(LaneOwnerType lane_owner) {
    _processed.insert(lane_owner);
    for (auto& _assignment : _assignments) {
        if (_assignment.lane_owner == lane_owner) {
            _assignment = LANE_UNASSIGNED;
        }
    }
}

void LaneArbiter::mark_processed(LaneOwnerType lane_owner) {
    _processed.insert(lane_owner);
}

} // namespace query_cache
} // namespace starrocks
