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

void SourceOperatorFactory::adjust_dop() {
    const size_t max_parent_dop = std::accumulate(
            _upstream_sources.begin(), _upstream_sources.end(), static_cast<size_t>(0),
            [](size_t max_dop, const auto* parent) { return std::max(max_dop, parent->degree_of_parallelism()); });

    if (max_parent_dop > 0 && max_parent_dop < _degree_of_parallelism) {
        _degree_of_parallelism = max_parent_dop;
    }
}

void SourceOperatorFactory::add_group_dependent_pipeline(const Pipeline* dependent_op) {
    group_leader()->_group_dependent_pipelines.emplace_back(dependent_op);
}
const std::vector<const Pipeline*>& SourceOperatorFactory::group_dependent_pipelines() const {
    return group_leader()->_group_dependent_pipelines;
}

void SourceOperatorFactory::add_upstream_source(SourceOperatorFactory* parent) {
    _upstream_sources.emplace_back(parent);
    if (_group_parent == this) { // Set the group parent once when adding the first upstream source.
        _group_parent = parent->group_leader();
    }
}

SourceOperatorFactory* SourceOperatorFactory::group_leader() const {
    if (_group_parent != this) {
        _group_parent = _group_parent->group_leader();
    }
    return _group_parent;
}

void SourceOperatorFactory::union_group(SourceOperatorFactory* other_group) {
    auto* group_leader = this->group_leader();
    auto* other_group_leader = other_group->group_leader();
    if (group_leader != other_group_leader) {
        other_group_leader->_group_parent = group_leader;
    }
}

bool SourceOperatorFactory::is_adaptive_group_initial_active() const {
    return group_leader()->adaptive_initial_state() == AdaptiveState::ACTIVE;
}

} // namespace starrocks::pipeline