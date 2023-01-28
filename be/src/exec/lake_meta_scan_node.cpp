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

#include "exec/lake_meta_scan_node.h"

#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/lake_meta_scan_prepare_operator.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/scan/meta_scan_operator.h"

namespace starrocks {

LakeMetaScanNode::LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : MetaScanNode(pool, tnode, descs) {
    _name = "lake_meta_scan";
}

Status LakeMetaScanNode::open(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }
    for (auto& scan_range : _scan_ranges) {
        MetaScannerParams scanner_params;
        scanner_params.scan_range = scan_range.get();
        LakeMetaScanner* scanner = _obj_pool.add(new LakeMetaScanner(this));
        RETURN_IF_ERROR(scanner->init(state, scanner_params));
        _scanners.push_back(scanner);
    }

    if (_scanners.size() <= 0) {
        return Status::InternalError("Invalid ScanRange.");
    }

    DCHECK_GT(_scanners.size(), 0);
    RETURN_IF_ERROR(_scanners[_cursor_idx]->open(state));

    _cursor_idx = 0;

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    return Status::OK();
}

Status LakeMetaScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);
    RETURN_IF_CANCELLED(state);

    if (!_scanners[_cursor_idx]->has_more()) {
        _scanners[_cursor_idx]->close(state);
        _cursor_idx++;
    }
    if (_cursor_idx >= _scanners.size()) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_scanners[_cursor_idx]->open(state));
    RETURN_IF_ERROR(_scanners[_cursor_idx]->get_chunk(state, chunk));
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> LakeMetaScanNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    auto* morsel_queue_factory = context->morsel_queue_factory_of_source_operator(id());
    size_t dop = morsel_queue_factory->size();
    bool shared_morsel_queue = morsel_queue_factory->is_shared();

    size_t buffer_capacity = pipeline::ScanOperator::max_buffer_capacity() * dop;
    int64_t mem_limit = runtime_state()->query_mem_tracker_ptr()->limit() * config::scan_use_query_mem_ratio;
    pipeline::ChunkBufferLimiterPtr buffer_limiter = std::make_unique<pipeline::DynamicChunkBufferLimiter>(
            buffer_capacity, buffer_capacity, mem_limit, runtime_state()->chunk_size());

    auto scan_ctx_factory = std::make_shared<pipeline::MetaScanContextFactory>(this, dop, shared_morsel_queue,
                                                                               std::move(buffer_limiter));

    auto scan_prepare_op = std::make_shared<pipeline::LakeMetaScanPrepareOperatorFactory>(context->next_operator_id(),
                                                                                          id(), this, scan_ctx_factory);
    scan_prepare_op->set_degree_of_parallelism(shared_morsel_queue ? 1 : dop);

    auto scan_prepare_pipeline = pipeline::OpFactories{
            std::move(scan_prepare_op),
            std::make_shared<pipeline::NoopSinkOperatorFactory>(context->next_operator_id(), id()),
    };
    context->add_pipeline(scan_prepare_pipeline);

    auto scan_op = std::make_shared<pipeline::MetaScanOperatorFactory>(context->next_operator_id(), this, dop,
                                                                       scan_ctx_factory);
    return pipeline::decompose_scan_node_to_pipeline(scan_op, this, context);
}

} // namespace starrocks
