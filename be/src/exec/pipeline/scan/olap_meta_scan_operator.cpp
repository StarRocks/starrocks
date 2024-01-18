// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/olap_meta_scan_operator.h"

#include <utility>

#include "exec/pipeline/scan/olap_meta_chunk_source.h"
#include "exec/pipeline/scan/olap_meta_scan_context.h"
#include "exec/vectorized/olap_meta_scanner.h"

namespace starrocks::pipeline {

OlapMetaScanOperatorFactory::OlapMetaScanOperatorFactory(int32_t id, ScanNode* meta_scan_node, size_t dop,
                                                         std::shared_ptr<OlapMetaScanContextFactory> ctx_factory)
        : ScanOperatorFactory(id, meta_scan_node), _ctx_factory(std::move(ctx_factory)) {}

Status OlapMetaScanOperatorFactory::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapMetaScanOperatorFactory::do_close(RuntimeState* state) {}

OperatorPtr OlapMetaScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapMetaScanOperator>(this, _id, driver_sequence, dop, _scan_node,
                                                  _ctx_factory->get_or_create(driver_sequence));
}

OlapMetaScanOperator::OlapMetaScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                           ScanNode* meta_scan_node, OlapMetaScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, dop, meta_scan_node), _ctx(std::move(ctx)) {}

OlapMetaScanOperator::~OlapMetaScanOperator() = default;

bool OlapMetaScanOperator::has_output() const {
    if (!_ctx->is_prepare_finished()) {
        return false;
    }
    return ScanOperator::has_output();
}

bool OlapMetaScanOperator::is_finished() const {
    if (!_ctx->is_prepare_finished()) {
        return false;
    }
    return ScanOperator::is_finished();
}

Status OlapMetaScanOperator::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapMetaScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr OlapMetaScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    return std::make_shared<OlapMetaChunkSource>(this, _runtime_profile.get(), std::move(morsel), _ctx);
}

ChunkPtr OlapMetaScanOperator::get_chunk_from_buffer() {
    ChunkPtr chunk = nullptr;
    if (_ctx->get_chunk_buffer().try_get(_driver_sequence, &chunk)) {
        return chunk;
    }
    return nullptr;
}

size_t OlapMetaScanOperator::num_buffered_chunks() const {
    return _ctx->get_chunk_buffer().size(_driver_sequence);
}

size_t OlapMetaScanOperator::buffer_size() const {
    return _ctx->get_chunk_buffer().limiter()->size();
}

size_t OlapMetaScanOperator::buffer_capacity() const {
    return _ctx->get_chunk_buffer().limiter()->capacity();
}

size_t OlapMetaScanOperator::default_buffer_capacity() const {
    return _ctx->get_chunk_buffer().limiter()->default_capacity();
}

ChunkBufferTokenPtr OlapMetaScanOperator::pin_chunk(int num_chunks) {
    return _ctx->get_chunk_buffer().limiter()->pin(num_chunks);
}

bool OlapMetaScanOperator::is_buffer_full() const {
    return _ctx->get_chunk_buffer().limiter()->is_full();
}

void OlapMetaScanOperator::set_buffer_finished() {
    _ctx->get_chunk_buffer().set_finished(_driver_sequence);
}

} // namespace starrocks::pipeline
