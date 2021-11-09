// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/operator.h"

#include <algorithm>

#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
Operator::Operator(int32_t id, const std::string& name, int32_t plan_node_id)
        : _id(id), _name(name), _plan_node_id(plan_node_id) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    _runtime_profile = std::make_shared<RuntimeProfile>(strings::Substitute("$0 (id=$1)", upper_name, _id));
    _runtime_profile->set_metadata(_id);
    _mem_tracker = std::make_unique<MemTracker>(_runtime_profile.get(), -1, _runtime_profile->name(), nullptr);
}

Status Operator::prepare(RuntimeState* state) {
    _push_timer = ADD_TIMER(_runtime_profile, "PushTotalTime");
    _pull_timer = ADD_TIMER(_runtime_profile, "PullTotalTime");

    _push_chunk_num_counter = ADD_COUNTER(_runtime_profile, "PushChunkNum", TUnit::UNIT);
    _push_row_num_counter = ADD_COUNTER(_runtime_profile, "PushRowNum", TUnit::UNIT);
    _pull_chunk_num_counter = ADD_COUNTER(_runtime_profile, "PullChunkNum", TUnit::UNIT);
    _pull_row_num_counter = ADD_COUNTER(_runtime_profile, "PullRowNum", TUnit::UNIT);
    return Status::OK();
}

Status Operator::close(RuntimeState* state) {
    return Status::OK();
}

} // namespace starrocks::pipeline
