// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/operator.h"

#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
Operator::Operator(int32_t id, const std::string& name, int32_t plan_node_id)
        : _id(id), _name(name), _plan_node_id(plan_node_id) {
    std::stringstream ss;
    ss << name << " (id=" << _id << ")";
    _runtime_profile = std::make_shared<RuntimeProfile>(ss.str());
    _runtime_profile->set_metadata(_id);
}

Status Operator::prepare(RuntimeState* state) {
    _mem_tracker = std::make_shared<MemTracker>(_runtime_profile.get(), -1, _runtime_profile->name(),
                                                state->instance_mem_tracker());
    return Status::OK();
}

Status Operator::close(RuntimeState* state) {
    if (_mem_tracker != nullptr) {
        _mem_tracker->close();
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
