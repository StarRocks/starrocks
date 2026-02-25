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

#include "exec/pipeline/fetch_processor.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {
class FetchProcessor;
class FetchSourceOperator final : public SourceOperator {
public:
    FetchSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                        std::shared_ptr<FetchProcessor> processor);

    ~FetchSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    bool pending_finish() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        return Status::NotSupported("FetchSourceOperator does not support push_chunk");
    }

private:
    std::shared_ptr<FetchProcessor> _processor;
};

class FetchSourceOperatorFactory final : public SourceOperatorFactory {
public:
    FetchSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                               std::shared_ptr<FetchProcessorFactory> processor_factory)
            : SourceOperatorFactory(id, "fetch_source", plan_node_id),
              _processor_factory(std::move(processor_factory)) {}

    ~FetchSourceOperatorFactory() override = default;

    void close(RuntimeState* state) override {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }
    bool support_event_scheduler() const override { return true; }

private:
    std::shared_ptr<FetchProcessorFactory> _processor_factory;
};

} // namespace starrocks::pipeline