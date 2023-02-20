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

#include "exec/pipeline/scan/lake_meta_scan_prepare_operator.h"

#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/scan/meta_scan_operator.h"
#include "gen_cpp/Types_types.h"
#include "storage/olap_common.h"

namespace starrocks::pipeline {

LakeMetaScanPrepareOperator::LakeMetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                         int32_t driver_sequence, LakeMetaScanNode* const scan_node,
                                                         MetaScanContextPtr scan_ctx)
        : MetaScanPrepareOperator(factory, id, plan_node_id, driver_sequence, std::string("lake_meta_scan_prepare"),
                                  scan_ctx),
          _scan_node(scan_node) {}

Status LakeMetaScanPrepareOperator::_prepare_scan_context(RuntimeState* state) {
    auto meta_scan_ranges = _morsel_queue->olap_scan_ranges();
    for (auto& scan_range : meta_scan_ranges) {
        MetaScannerParams params;
        params.scan_range = scan_range;
        auto scanner = std::make_shared<LakeMetaScanner>(_scan_node);
        RETURN_IF_ERROR(scanner->init(state, params));
        TTabletId tablet_id = scan_range->tablet_id;
        _scan_ctx->add_scanner(tablet_id, scanner);
    }
    return Status::OK();
}

LakeMetaScanPrepareOperatorFactory::LakeMetaScanPrepareOperatorFactory(
        int32_t id, int32_t plan_node_id, LakeMetaScanNode* const scan_node,
        std::shared_ptr<MetaScanContextFactory> scan_ctx_factory)
        : MetaScanPrepareOperatorFactory(id, plan_node_id, "lake_meta_scan_prepare", scan_ctx_factory),
          _scan_node(scan_node) {}

OperatorPtr LakeMetaScanPrepareOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<LakeMetaScanPrepareOperator>(this, _id, _plan_node_id, driver_sequence, _scan_node,
                                                         _scan_ctx_factory->get_or_create(driver_sequence));
}

} // namespace starrocks::pipeline
