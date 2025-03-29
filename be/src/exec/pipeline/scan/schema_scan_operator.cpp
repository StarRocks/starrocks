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

#include "exec/pipeline/scan/schema_scan_operator.h"

#include <utility>

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/pipeline/scan/schema_chunk_source.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::pipeline {

SchemaScanOperatorFactory::SchemaScanOperatorFactory(int32_t id, ScanNode* schema_scan_node, size_t dop,
                                                     const TPlanNode& t_node, ChunkBufferLimiterPtr buffer_limiter)
        : ScanOperatorFactory(id, schema_scan_node),
          _chunk_buffer(BalanceStrategy::kDirect, dop, std::move(buffer_limiter)) {
    _ctx = std::make_shared<SchemaScanContext>(t_node, _chunk_buffer);
}

Status SchemaScanOperatorFactory::do_prepare(RuntimeState* state) {
    return _ctx->prepare(state);
}

void SchemaScanOperatorFactory::do_close(RuntimeState* state) {}

OperatorPtr SchemaScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<SchemaScanOperator>(this, _id, driver_sequence, dop, _scan_node, _ctx);
}

SchemaScanOperator::SchemaScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                       ScanNode* scan_node, SchemaScanContextPtr ctx)
        : ScanOperator(factory, id, driver_sequence, dop, scan_node), _ctx(std::move(ctx)) {}

SchemaScanOperator::~SchemaScanOperator() = default;

Status SchemaScanOperator::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void SchemaScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr SchemaScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    return std::make_shared<SchemaChunkSource>(this, _chunk_source_profiles[chunk_source_index].get(),
                                               std::move(morsel), _ctx);
}

} // namespace starrocks::pipeline
