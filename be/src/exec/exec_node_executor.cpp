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

#include "exec/exec_node.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

void ExecNode::init_runtime_filter_for_operator(OperatorFactory* op, pipeline::PipelineBuilderContext* context,
                                                const RcRfProbeCollectorPtr& rc_rf_probe_collector) {
    op->init_runtime_filter(context->fragment_context()->runtime_filter_hub(), this->get_tuple_ids(),
                            this->local_rf_waiting_set(), this->row_desc(), rc_rf_probe_collector,
                            _filter_null_value_columns, _tuple_slot_mappings);
}

void ExecNode::may_add_chunk_accumulate_operator(OpFactories& ops, pipeline::PipelineBuilderContext* context, int id) {
    // TODO(later): Need to rewrite ChunkAccumulateOperator to support StreamPipelines,
    // for now just disable it in stream pipelines:
    // - make sure UPDATE_BEFORE/UPDATE_AFTER are in the same chunk.
    if (!context->is_stream_pipeline()) {
        ops.emplace_back(std::make_shared<pipeline::ChunkAccumulateOperatorFactory>(context->next_operator_id(), id));
    }
}

} // namespace starrocks
