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

#pragma once

#include "exec/lake_meta_scan_node.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/scan/meta_scan_prepare_operator.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

class LakeMetaScanPrepareOperator final : public MetaScanPrepareOperator {
public:
    LakeMetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                LakeMetaScanNode* const scan_node, MetaScanContextPtr scan_ctx);
    ~LakeMetaScanPrepareOperator() override = default;

private:
    Status _prepare_scan_context(RuntimeState* state) override;

    LakeMetaScanNode* const _scan_node;
};

class LakeMetaScanPrepareOperatorFactory final : public MetaScanPrepareOperatorFactory {
public:
    LakeMetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, LakeMetaScanNode* const scan_node,
                                       std::shared_ptr<MetaScanContextFactory> scan_ctx_factory);

    ~LakeMetaScanPrepareOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    LakeMetaScanNode* const _scan_node;
};

} // namespace starrocks::pipeline