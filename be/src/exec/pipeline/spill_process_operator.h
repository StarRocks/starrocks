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

#include <memory>

#include "exec/exec_node.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/spill/spiller.h"

namespace starrocks::pipeline {
// operator for process spill task
// This operator fetches the task from the process channel and executes this chunk task,
// and then takes the chunk fetched from the task and tries to execute Spiller::spill().
// But this thread will never execute any IO task.

class SpillProcessOperator final : public SourceOperator {
public:
    SpillProcessOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                         int32_t driver_sequence, SpillProcessChannelPtr channel)
            : SourceOperator(factory, id, name, plan_node_id, driver_sequence), _channel(std::move(channel)) {}

    ~SpillProcessOperator() override = default;

    Status prepare(RuntimeState* state) override { return SourceOperator::prepare(state); }

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    SpillProcessChannelPtr _channel;
};

//
class SpillProcessOperatorFactory final : public SourceOperatorFactory {
public:
    SpillProcessOperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id,
                                SpillProcessChannelFactoryPtr process_ctx)
            : SourceOperatorFactory(id, name, plan_node_id), _process_ctx(std::move(process_ctx)) {}
    ~SpillProcessOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override { return SourceOperatorFactory::prepare(state); }

    void close(RuntimeState* state) override { SourceOperatorFactory::close(state); }

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    SpillProcessChannelFactoryPtr _process_ctx;
};
} // namespace starrocks::pipeline