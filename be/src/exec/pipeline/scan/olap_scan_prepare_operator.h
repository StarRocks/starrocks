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

#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {

class OlapScanNode;

namespace pipeline {

// It does some common preparation works for OlapScan, after its local waiting set is ready
// and before OlapScanOperator::pull_chunk. That is, OlapScanOperator depends on
// it and waits until it is finished.
class OlapScanPrepareOperator final : public SourceOperator {
public:
    OlapScanPrepareOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                            int32_t driver_sequence, OlapScanContextPtr ctx);
    ~OlapScanPrepareOperator() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    OlapScanContextPtr _ctx;
};

class OlapScanPrepareOperatorFactory final : public SourceOperatorFactory {
public:
    OlapScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, OlapScanNode* const scan_node,
                                   OlapScanContextFactoryPtr ctx_factory);
    ~OlapScanPrepareOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

private:
    OlapScanNode* const _scan_node;
    OlapScanContextFactoryPtr _ctx_factory;
};

} // namespace pipeline
} // namespace starrocks
