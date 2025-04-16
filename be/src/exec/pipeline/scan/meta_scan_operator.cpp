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

#include "exec/pipeline/scan/meta_scan_operator.h"

#include <utility>

#include "exec/meta_scanner.h"
#include "exec/pipeline/scan/meta_chunk_source.h"
#include "exec/pipeline/scan/meta_scan_context.h"

namespace starrocks::pipeline {
MetaScanOperatorFactory::MetaScanOperatorFactory(int32_t id, ScanNode* meta_scan_node, size_t dop,
                                                 std::shared_ptr<MetaScanContextFactory> ctx_factory)
        : ScanOperatorFactory(id, meta_scan_node), _ctx_factory(std::move(ctx_factory)) {}

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
        : ScanOperator(factory, id, driver_sequence, dop, meta_scan_node), _ctx(std::move(ctx)) {}

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
    return std::make_shared<MetaChunkSource>(this, _runtime_profile.get(), std::move(morsel), _ctx);
}

} // namespace starrocks::pipeline