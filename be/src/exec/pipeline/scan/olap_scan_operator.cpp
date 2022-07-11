// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/olap_scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
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

OlapScanOperatorFactory::OlapScanOperatorFactory(int32_t id, ScanNode* scan_node, ChunkBufferLimiterPtr buffer_limiter,
                                                 OlapScanContextPtr ctx)
        : ScanOperatorFactory(id, scan_node, std::move(buffer_limiter)), _ctx(std::move(ctx)) {}

Status OlapScanOperatorFactory::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapScanOperatorFactory::do_close(RuntimeState*) {}

OperatorPtr OlapScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapScanOperator>(this, _id, driver_sequence, _scan_node, _buffer_limiter.get(), _ctx);
}

// ==================== OlapScanOperator ====================

OlapScanOperator::OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node,
                                   ChunkBufferLimiter* buffer_limiter, OlapScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, scan_node, buffer_limiter), _ctx(std::move(ctx)) {
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
    return Status::OK();
}

void OlapScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr OlapScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    return std::make_shared<OlapChunkSource>(_chunk_source_profiles[chunk_source_index].get(), std::move(morsel),
                                             olap_scan_node, _ctx.get(), _buffer_limiter);
}

} // namespace starrocks::pipeline
