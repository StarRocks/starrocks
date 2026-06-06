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

#include <memory>

#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks {

class ExecNode;

namespace pipeline {

class RefCountedRuntimeFilterProbeCollector;

void init_runtime_filter_for_operator(
        const ExecNode& exec_node, OperatorFactory* op, PipelineBuilderContext* context,
        const std::shared_ptr<RefCountedRuntimeFilterProbeCollector>& rc_rf_probe_collector);

void may_add_chunk_accumulate_operator(OpFactories& ops, PipelineBuilderContext* context, int id);

} // namespace pipeline
} // namespace starrocks
