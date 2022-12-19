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

#include "exec/pipeline/scan/olap_schema_scan_operator.h"

#include <utility>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/olap_schema_chunk_source.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::pipeline {

OlapSchemaScanOperatorFactory::OlapSchemaScanOperatorFactory(int32_t id, ScanNode* schema_scan_node, size_t dop,
                                                             const TPlanNode& t_node,
                                                             ChunkBufferLimiterPtr buffer_limiter)
        : ScanOperatorFactory(id, schema_scan_node),
          _chunk_buffer(BalanceStrategy::kDirect, dop, std::move(buffer_limiter)) {
    _ctx = std::make_shared<OlapSchemaScanContext>(t_node, _chunk_buffer);
}

Status OlapSchemaScanOperatorFactory::do_prepare(RuntimeState* state) {
    return _ctx->prepare(state);
}

void OlapSchemaScanOperatorFactory::do_close(RuntimeState* state) {}

OperatorPtr OlapSchemaScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapSchemaScanOperator>(this, _id, driver_sequence, dop, _scan_node, _ctx);
}

OlapSchemaScanOperator::OlapSchemaScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence,
                                               int32_t dop, ScanNode* scan_node, OlapSchemaScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, dop, scan_node), _ctx(std::move(ctx)) {}

OlapSchemaScanOperator::~OlapSchemaScanOperator() = default;

Status OlapSchemaScanOperator::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapSchemaScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr OlapSchemaScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    return std::make_shared<OlapSchemaChunkSource>(_driver_sequence, _chunk_source_profiles[chunk_source_index].get(),
                                                   std::move(morsel), _ctx);
}

ChunkPtr OlapSchemaScanOperator::get_chunk_from_buffer() {
    ChunkPtr chunk = nullptr;
    if (_ctx->get_chunk_buffer().try_get(_driver_sequence, &chunk)) {
        return chunk;
    }
    return nullptr;
}

size_t OlapSchemaScanOperator::num_buffered_chunks() const {
    return _ctx->get_chunk_buffer().size(_driver_sequence);
}

size_t OlapSchemaScanOperator::buffer_size() const {
    return _ctx->get_chunk_buffer().limiter()->size();
}

size_t OlapSchemaScanOperator::buffer_capacity() const {
    return _ctx->get_chunk_buffer().limiter()->capacity();
}

size_t OlapSchemaScanOperator::default_buffer_capacity() const {
    return _ctx->get_chunk_buffer().limiter()->default_capacity();
}

ChunkBufferTokenPtr OlapSchemaScanOperator::pin_chunk(int num_chunks) {
    return _ctx->get_chunk_buffer().limiter()->pin(num_chunks);
}

bool OlapSchemaScanOperator::is_buffer_full() const {
    return _ctx->get_chunk_buffer().limiter()->is_full();
}

void OlapSchemaScanOperator::set_buffer_finished() {
    _ctx->get_chunk_buffer().set_finished(_driver_sequence);
}

} // namespace starrocks::pipeline
