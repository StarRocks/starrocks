// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/meta_scan_operator.h"

#include "exec/pipeline/scan/meta_chunk_source.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/vectorized/meta_scanner.h"

namespace starrocks::pipeline {
MetaScanOperatorFactory::MetaScanOperatorFactory(int32_t id, ScanNode* meta_scan_node, size_t dop,
                                                 std::shared_ptr<MetaScanContextFactory> ctx_factory)
        : ScanOperatorFactory(id, meta_scan_node), _ctx_factory(ctx_factory) {}

Status MetaScanOperatorFactory::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void MetaScanOperatorFactory::do_close(RuntimeState* state) {}

OperatorPtr MetaScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<MetaScanOperator>(this, _id, driver_sequence, dop, _scan_node,
                                              _ctx_factory->get_or_create(driver_sequence));
}

MetaScanOperator::MetaScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                   ScanNode* meta_scan_node, MetaScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, dop, meta_scan_node), _ctx(ctx) {}

bool MetaScanOperator::has_output() const {
    if (!_ctx->is_prepare_finished()) {
        return false;
    }
    return ScanOperator::has_output();
}

bool MetaScanOperator::is_finished() const {
    if (!_ctx->is_prepare_finished()) {
        return false;
    }
    return ScanOperator::is_finished();
}

Status MetaScanOperator::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void MetaScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr MetaScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    return std::make_shared<MetaChunkSource>(_driver_sequence, _runtime_profile.get(), std::move(morsel), _ctx);
}

ChunkPtr MetaScanOperator::get_chunk_from_buffer() {
    ChunkPtr chunk = nullptr;
    if (_ctx->get_chunk_buffer().try_get(_driver_sequence, &chunk)) {
        return chunk;
    }
    return nullptr;
}

size_t MetaScanOperator::num_buffered_chunks() const {
    return _ctx->get_chunk_buffer().size(_driver_sequence);
}

size_t MetaScanOperator::buffer_size() const {
    return _ctx->get_chunk_buffer().limiter()->size();
}

size_t MetaScanOperator::buffer_capacity() const {
    return _ctx->get_chunk_buffer().limiter()->capacity();
}

size_t MetaScanOperator::default_buffer_capacity() const {
    return _ctx->get_chunk_buffer().limiter()->default_capacity();
}

ChunkBufferTokenPtr MetaScanOperator::pin_chunk(int num_chunks) {
    return _ctx->get_chunk_buffer().limiter()->pin(num_chunks);
}

bool MetaScanOperator::is_buffer_full() const {
    return _ctx->get_chunk_buffer().limiter()->is_full();
}

void MetaScanOperator::set_buffer_finished() {
    _ctx->get_chunk_buffer().set_finished(_driver_sequence);
}

} // namespace starrocks::pipeline