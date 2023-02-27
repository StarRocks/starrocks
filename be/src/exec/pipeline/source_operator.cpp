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

#include "exec/pipeline/source_operator.h"

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {

void SourceOperatorFactory::add_group_dependent_pipeline(const Pipeline* dependent_op) {
    _group_leader->_group_dependent_pipelines.emplace_back(dependent_op);
}
const std::vector<const Pipeline*>& SourceOperatorFactory::group_dependent_pipelines() const {
    return _group_leader->_group_dependent_pipelines;
}

void SourceOperatorFactory::set_group_leader(SourceOperatorFactory* parent) {
    if (this == parent) {
        return;
    }
    _group_leader = parent->group_leader();
}
SourceOperatorFactory* SourceOperatorFactory::group_leader() {
    return _group_leader;
}

bool SourceOperatorFactory::is_adaptive_group_active() const {
    if (_group_leader != this) {
        return _group_leader->is_adaptive_group_active();
    }

    if (adaptive_state() != AdaptiveState::ACTIVE) {
        return false;
    }

    const auto& pipelines = _group_dependent_pipelines;
    if (!_group_dependent_pipelines_ready) {
        _group_dependent_pipelines_ready = std::all_of(pipelines.begin(), pipelines.end(), [](const auto& pipeline) {
            return pipeline->source_operator_factory()->is_adaptive_group_active();
        });
        if (!_group_dependent_pipelines_ready) {
            return false;
        }
    }

    _group_dependent_pipelines_finished = std::all_of(pipelines.begin(), pipelines.end(), [](const auto& pipeline) {
        const auto& drivers = pipeline->drivers();
        if (drivers.empty()) {
            return false;
        }
        return std::all_of(drivers.begin(), drivers.end(),
                           [](const auto& driver) { return driver->sink_operator()->is_finished(); });
    });
    return _group_dependent_pipelines_finished;
}

} // namespace starrocks::pipeline