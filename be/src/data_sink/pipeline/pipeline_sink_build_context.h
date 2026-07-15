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

#include <utility>

#include "data_sink/pipeline/pipeline_sink_graph.h"
#include "exec_primitive/pipeline/pipeline_fwd.h"

namespace starrocks {

class RowDescriptor;

namespace pipeline {

class UnifiedExecPlanFragmentParams;

struct PipelineSinkBuildContext {
    PipelineSinkBuildContext(PipelineSinkGraph& graph, OpFactories upstream,
                             const UnifiedExecPlanFragmentParams& request, const RowDescriptor& row_desc)
            : graph(graph), upstream(std::move(upstream)), request(request), row_desc(row_desc) {}

    PipelineSinkBuildContext(const PipelineSinkBuildContext&) = delete;
    PipelineSinkBuildContext& operator=(const PipelineSinkBuildContext&) = delete;
    PipelineSinkBuildContext(PipelineSinkBuildContext&&) = default;
    PipelineSinkBuildContext& operator=(PipelineSinkBuildContext&&) = delete;

    PipelineSinkGraph& graph;
    OpFactories upstream;
    const UnifiedExecPlanFragmentParams& request;
    const RowDescriptor& row_desc;
};

} // namespace pipeline
} // namespace starrocks
