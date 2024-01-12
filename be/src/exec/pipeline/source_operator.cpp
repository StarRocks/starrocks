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
    group_leader()->_group_dependent_pipelines.emplace_back(dependent_op);
}
const std::vector<const Pipeline*>& SourceOperatorFactory::group_dependent_pipelines() const {
    return group_leader()->_group_dependent_pipelines;
}

void SourceOperatorFactory::set_group_leader(SourceOperatorFactory* parent) {
    if (this == parent) {
        return;
    }
    _group_leader = parent->group_leader();
}

SourceOperatorFactory* SourceOperatorFactory::group_leader() const {
    return _group_leader;
}

bool SourceOperatorFactory::is_adaptive_group_initial_active() const {
    return group_leader()->adaptive_initial_state() == AdaptiveState::ACTIVE;
}

} // namespace starrocks::pipeline