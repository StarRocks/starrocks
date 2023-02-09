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

#include "exec/meta_scan_node.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {
class MetaScanPrepareOperator : public SourceOperator {
public:
    MetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            const std::string& operator_name, MetaScanContextPtr scan_ctx);
    ~MetaScanPrepareOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

protected:
    virtual Status _prepare_scan_context(RuntimeState* state) = 0;
    MetaScanContextPtr _scan_ctx;
};

class MetaScanPrepareOperatorFactory : public SourceOperatorFactory {
public:
    MetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, const std::string& operator_name,
                                   std::shared_ptr<MetaScanContextFactory> scan_ctx_factory);

    ~MetaScanPrepareOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

protected:
    std::shared_ptr<MetaScanContextFactory> _scan_ctx_factory;
};

} // namespace starrocks::pipeline