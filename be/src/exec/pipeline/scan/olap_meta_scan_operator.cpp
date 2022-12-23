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

#include "exec/pipeline/scan/olap_meta_scan_operator.h"

#include <utility>

#include "exec/olap_meta_scanner.h"
#include "exec/pipeline/scan/olap_meta_chunk_source.h"
#include "exec/pipeline/scan/olap_meta_scan_context.h"

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
    return std::make_shared<OlapMetaChunkSource>(_driver_sequence, _runtime_profile.get(), std::move(morsel), _ctx);
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
