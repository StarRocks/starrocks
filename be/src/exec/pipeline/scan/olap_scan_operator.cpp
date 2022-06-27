// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/olap_scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/scan/olap_chunk_source.h"
#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/vectorized/olap_scan_node.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"

namespace starrocks::pipeline {

// ==================== OlapScanOperatorFactory ====================

OlapScanOperatorFactory::OlapScanOperatorFactory(int32_t id, ScanNode* scan_node, OlapScanContextPtr ctx)
        : ScanOperatorFactory(id, scan_node), _ctx(std::move(ctx)) {}

Status OlapScanOperatorFactory::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapScanOperatorFactory::do_close(RuntimeState*) {}

OperatorPtr OlapScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapScanOperator>(this, _id, driver_sequence, _scan_node, _num_committed_scan_tasks, _ctx);
}

// ==================== OlapScanOperator ====================

OlapScanOperator::OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node,
                                   std::atomic<int>& num_committed_scan_tasks, OlapScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, scan_node, num_committed_scan_tasks),
          _ctx(std::move(ctx)),
          _default_max_scan_concurrency(scan_node->max_scan_concurrency()) {
    _ctx->ref();
}

OlapScanOperator::~OlapScanOperator() {
    auto* state = runtime_state();
    if (state == nullptr) {
        return;
    }

    _ctx->unref(state);
}

bool OlapScanOperator::has_output() const {
    if (!_ctx->is_prepare_finished() || _ctx->is_finished()) {
        return false;
    }

    return ScanOperator::has_output();
}

bool OlapScanOperator::is_finished() const {
    if (_ctx->is_finished() || _is_finished) {
        return true;
    }

    // ScanOperator::is_finished() will check whether the morsel queue has more morsels,
    // and some kinds of morsel queue will be ready after the scan context prepares ready.
    // Therefore, return false when the context is not ready.
    if (!_ctx->is_prepare_finished()) {
        return false;
    }

    return ScanOperator::is_finished();
}

Status OlapScanOperator::do_prepare(RuntimeState*) {
    auto* max_scan_concurrency_counter = ADD_COUNTER(_unique_metrics, "DefaultMaxScanConcurrency", TUnit::UNIT);
    COUNTER_SET(max_scan_concurrency_counter, static_cast<int64_t>(_default_max_scan_concurrency));
    bool shared_scan = _ctx->is_shared_scan();
    _unique_metrics->add_info_string("SharedScan", shared_scan ? "True" : "False");

    return Status::OK();
}

void OlapScanOperator::do_close(RuntimeState* state) {
    auto* max_scan_concurrency_counter = ADD_COUNTER(_unique_metrics, "AvgMaxScanConcurrency", TUnit::UNIT);
    COUNTER_SET(max_scan_concurrency_counter, static_cast<int64_t>(_avg_max_scan_concurrency()));
}

ChunkSourcePtr OlapScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    return std::make_shared<OlapChunkSource>(_driver_sequence, _chunk_source_profiles[chunk_source_index].get(),
                                             std::move(morsel), olap_scan_node, _ctx.get());
}

void OlapScanOperator::attach_chunk_source(int32_t source_index) {
    _ctx->attach_shared_input(_driver_sequence, source_index);
}

void OlapScanOperator::detach_chunk_source(int32_t source_index) {
    _ctx->detach_shared_input(_driver_sequence, source_index);
}

bool OlapScanOperator::has_shared_chunk_source() const {
    return _ctx->has_active_input();
}

bool OlapScanOperator::has_buffer_output() const {
    return !_ctx->get_chunk_buffer().empty(_driver_sequence);
}

ChunkPtr OlapScanOperator::get_chunk_from_buffer() {
    vectorized::ChunkPtr chunk = nullptr;
    if (_ctx->get_chunk_buffer().try_get(_driver_sequence, &chunk)) {
        return chunk;
    }
    return nullptr;
}

bool OlapScanOperator::has_available_buffer() const {
    // TODO: consider the global buffer
    return _ctx->get_chunk_buffer().size(_driver_sequence) <= _buffer_size;
}

size_t OlapScanOperator::max_scan_concurrency() const {
    int64_t query_limit = runtime_state()->query_mem_tracker_ptr()->limit();

    size_t avg_row_bytes = _ctx->avg_row_bytes();
    if (avg_row_bytes == 0) {
        return _default_max_scan_concurrency;
    }

    // Assume that the memory tried in the storage layer is the same as the output chunk.
    size_t row_mem_usage = avg_row_bytes * 2;
    size_t chunk_mem_usage = row_mem_usage * runtime_state()->chunk_size();
    DCHECK_GT(chunk_mem_usage, 0);

    // limit scan memory usage not greater than 1/4 query limit.
    size_t concurrency = std::max<size_t>(query_limit * config::scan_use_query_mem_ratio / chunk_mem_usage, 1);

    if (_prev_max_scan_concurrency != concurrency) {
        _sum_max_scan_concurrency += concurrency;
        ++_num_max_scan_concurrency;
        _prev_max_scan_concurrency = concurrency;
    }

    return concurrency;
}

size_t OlapScanOperator::_avg_max_scan_concurrency() const {
    if (_num_max_scan_concurrency > 0) {
        return _sum_max_scan_concurrency / _num_max_scan_concurrency;
    }

    return 0;
}

} // namespace starrocks::pipeline
