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

namespace starrocks::pipeline {

// Narrow contract a Pipeline needs from its owning execution group, so the
// Pipeline depends on this abstraction instead of the concrete ExecutionGroup
// (which already owns its pipelines). Keep this operation-focused.
class PipelineGroup {
public:
    virtual ~PipelineGroup() = default;

    // Called when all of a pipeline's drivers have finished.
    virtual void count_down_pipeline() = 0;

    // Whether this group runs as a group execution; drives the IsGroupExecution profile flag.
    virtual bool is_group_execution() const = 0;
};

using PipelineGroupRawPtr = PipelineGroup*;

} // namespace starrocks::pipeline
