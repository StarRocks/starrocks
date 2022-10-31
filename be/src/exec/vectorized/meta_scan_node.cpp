// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/meta_scan_node.h"

#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/olap_meta_scan_context.h"
#include "exec/pipeline/scan/olap_meta_scan_operator.h"
#include "exec/pipeline/scan/olap_meta_scan_prepare_operator.h"


namespace starrocks {
namespace vectorized {

MetaScanNode::MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _desc_tbl(descs),
          _meta_scan_node(tnode.meta_scan_node),
          _tuple_id(tnode.olap_scan_node.tuple_id){}

MetaScanNode::~MetaScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status MetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

void MetaScanNode::_init_counter(RuntimeState* state) {
    _scan_timer = ADD_TIMER(_runtime_profile, "ScanTime");
    _meta_scan_profile = _runtime_profile->create_child("META_SCAN", true, false);

    _io_timer = ADD_TIMER(_meta_scan_profile, "IOTime");
}

Status MetaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        _scan_ranges.emplace_back(std::make_unique<TInternalScanRange>(scan_range.scan_range.internal_scan_range));
        COUNTER_UPDATE(_tablet_counter, 1);
    }
    return Status::OK();
}

Status MetaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    _tablet_counter = ADD_COUNTER(runtime_profile(), "TabletCount ", TUnit::UNIT);

    // get desc tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _is_init = true;
    return Status::OK();
}

Status MetaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return ScanNode::close(state);
}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> MetaScanNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    auto* morsel_queue_factory = context->morsel_queue_factory_of_source_operator(id());
    size_t dop = morsel_queue_factory->size();
    bool shared_morsel_queue = morsel_queue_factory->is_shared();

    size_t buffer_capacity = pipeline::ScanOperator::max_buffer_capacity() * dop;
    int64_t mem_limit = runtime_state()->query_mem_tracker_ptr()->limit() * config::scan_use_query_mem_ratio;
    pipeline::ChunkBufferLimiterPtr buffer_limiter = std::make_unique<pipeline::DynamicChunkBufferLimiter>(
            buffer_capacity, buffer_capacity, mem_limit, runtime_state()->chunk_size());

    auto scan_ctx_factory = std::make_shared<pipeline::OlapMetaScanContextFactory>(this, dop, shared_morsel_queue,
                                                                                   std::move(buffer_limiter));

    auto scan_prepare_op = std::make_shared<pipeline::OlapMetaScanPrepareOperatorFactory>(context->next_operator_id(),
                                                                                          id(), this, scan_ctx_factory);
    scan_prepare_op->set_degree_of_parallelism(shared_morsel_queue ? 1 : dop);

    auto scan_prepare_pipeline = pipeline::OpFactories{
            std::move(scan_prepare_op),
            std::make_shared<pipeline::NoopSinkOperatorFactory>(context->next_operator_id(), id()),
    };
    context->add_pipeline(scan_prepare_pipeline);

    auto scan_op = std::make_shared<pipeline::OlapMetaScanOperatorFactory>(context->next_operator_id(), this, dop,
                                                                           scan_ctx_factory);
    return pipeline::decompose_scan_node_to_pipeline(scan_op, this, context);
}

} // namespace vectorized

} // namespace starrocks
