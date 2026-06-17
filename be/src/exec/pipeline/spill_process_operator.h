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
#include <optional>

#include "compute_env/spill/spiller.h"
#include "exec/exec_node.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/spill_process_channel.h"

namespace starrocks::pipeline {
// operator for process spill task
// This operator fetches the task from the process channel and executes this chunk task,
// and then takes the chunk fetched from the task and tries to execute Spiller::spill().
// But this thread will never execute any IO task.

class SpillProcessOperator final : public SourceOperator {
public:
    SpillProcessOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                         int32_t driver_sequence, SpillProcessChannelPtr channel)
            : SourceOperator(factory, id, name, plan_node_id, true, driver_sequence), _channel(std::move(channel)) {}

    ~SpillProcessOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    // The pump sleeps INPUT_EMPTY on has_output() = has_task() && !is_full(). An empty channel is waiting
    // on the writer's enqueue (WAIT_CHANNEL); a full spiller is waiting on flush/restore IO to drain
    // (WAIT_RESTORE). Flush completion notifies both lists, and the channel handshake wakes the source
    // side, so the prepare() source-list subscription covers both reasons.
    BlockReason block_reason() const override;
    static constexpr uint32_t kCoveredWakeups =
            block_reason_bit(BlockReason::WAIT_CHANNEL) | block_reason_bit(BlockReason::WAIT_RESTORE);
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    Status set_finished(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    // The single branching predicate that block_reason() reads. It returns the reason the pump is parked on
    // while has_output() is false: WAIT_CHANNEL when the channel holds no task (waiting on the writer's
    // enqueue), WAIT_RESTORE when a task is queued but the spiller writer is full (waiting on flush/restore
    // IO). It returns null on a finished channel or a reset spiller -- those are not a spill-covered block.
    // has_output() still delegates to the channel (the channel owns its own has-task/not-full predicate and
    // the null-spiller case); block_reason() is just value_or(NONE) over this.
    std::optional<BlockReason> _blocked_on() const;

    bool _is_finished = false;
    SpillProcessChannelPtr _channel;
};

static_assert(SpillProcessOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_CHANNEL));
static_assert(SpillProcessOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_RESTORE));

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

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

    // Always event-ready. This factory serves both the agg and the hash-join spill pipelines, so reading
    // one family's config here would let that family's kill switch wrongly demote the other family's
    // fragment. This source's own subscriptions are unconditional and do not need a config. Each family's
    // kill switch lives on its own factories (agg sink/source, join build/probe), which the fragment gate
    // ANDs together.
    bool support_event_scheduler() const override { return true; }

private:
    SpillProcessChannelFactoryPtr _process_ctx;
};
} // namespace starrocks::pipeline
