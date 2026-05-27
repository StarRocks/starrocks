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

#include "exec/pipeline/exec_node_pipeline_adapter.h"

#include "exec/exec_node.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks::pipeline {

void init_runtime_filter_for_operator(
        const ExecNode& exec_node, OperatorFactory* op, PipelineBuilderContext* context,
        const std::shared_ptr<RefCountedRuntimeFilterProbeCollector>& rc_rf_probe_collector) {
    op->init_runtime_filter(context->fragment_context()->runtime_filter_hub(), exec_node.get_tuple_ids(),
                            exec_node.local_rf_waiting_set(), exec_node.row_desc(), rc_rf_probe_collector,
                            exec_node.filter_null_value_columns(), exec_node.tuple_slot_mappings());
}

void may_add_chunk_accumulate_operator(OpFactories& ops, PipelineBuilderContext* context, int id) {
    ops.emplace_back(std::make_shared<ChunkAccumulateOperatorFactory>(context->next_operator_id(), id));
}

} // namespace starrocks::pipeline
