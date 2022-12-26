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

#include "exec/pipeline/scan/meta_scan_prepare_operator.h"

#include "exec/meta_scanner.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/scan/meta_scan_operator.h"
#include "gen_cpp/Types_types.h"
#include "storage/olap_common.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks::pipeline {

MetaScanPrepareOperator::MetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                 int32_t driver_sequence, const std::string& operator_name,
                                                 MetaScanContextPtr scan_ctx)
        : SourceOperator(factory, id, operator_name, plan_node_id, driver_sequence), _scan_ctx(scan_ctx) {}

Status MetaScanPrepareOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_prepare_scan_context(state));
    _scan_ctx->set_prepare_finished();
    return Status::OK();
}

void MetaScanPrepareOperator::close(RuntimeState* state) {
    SourceOperator::close(state);
}

bool MetaScanPrepareOperator::has_output() const {
    return false;
}

bool MetaScanPrepareOperator::is_finished() const {
    return true;
}

StatusOr<ChunkPtr> MetaScanPrepareOperator::pull_chunk(RuntimeState* state) {
    return nullptr;
}

MetaScanPrepareOperatorFactory::MetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id,
                                                               const std::string& operator_name,
                                                               std::shared_ptr<MetaScanContextFactory> scan_ctx_factory)
        : SourceOperatorFactory(id, operator_name, plan_node_id), _scan_ctx_factory(scan_ctx_factory) {}

Status MetaScanPrepareOperatorFactory::prepare(RuntimeState* state) {
    return Status::OK();
}

void MetaScanPrepareOperatorFactory::close(RuntimeState* state) {}

} // namespace starrocks::pipeline