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

#include <cstdint>

#include "common/config_exec_flow_fwd.h"
#include "compute_env/spill/spiller.h"
#include "exec/pipeline/primitives/block_reason.h"
#include "exec/pipeline/primitives/spillable_flat_sink_mixin.h"
#include "exec/pipeline/sort/partition_sort_sink_operator.h"
#include "exec/pipeline/spill_process_channel.h"

namespace starrocks::pipeline {
class SpillablePartitionSortSinkOperator final : public PartitionSortSinkOperator,
                                                 public SpillableFlatSinkMixin<SpillablePartitionSortSinkOperator> {
    friend class SpillableFlatSinkMixin<SpillablePartitionSortSinkOperator>;

public:
    template <class... Args>
    SpillablePartitionSortSinkOperator(Args&&... args)
            : PartitionSortSinkOperator(std::forward<Args>(args)..., "spillable_local_sort_sink") {}

    ~SpillablePartitionSortSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;
    Status prepare_local_state(RuntimeState* state) override { return Status::OK(); }

    void close(RuntimeState* state) override;

    // The sink sleeps OUTPUT_FULL on need_input(): a full writer (spiller is_full(), woken by the
    // flush-completion defer on the sink list) or the spill-process channel still holding a task (woken by the
    // channel handshake). The two reasons are read separately -- ChunksSorter::is_full() collapses them into a
    // single bool, which the SpillableFlatSinkMixin predicate keeps distinct for block_reason().
    bool need_input() const override { return spill_sink_need_input(); }
    BlockReason block_reason() const override { return spill_sink_block_reason(); }
    static constexpr uint32_t kCoveredWakeups = kSpillSinkCoveredWakeups;
    uint32_t covered_wakeups() const override { return kCoveredWakeups; }

    bool is_finished() const override { return _is_finished || _sort_context->is_finished(); }

    void set_execute_mode(int performance_level) override {
        if (_chunks_sorter) {
            _chunks_sorter->set_spill_stragety(spill::SpillStrategy::SPILL_ALL);
        }
    }

    bool spillable() const override { return true; }

    size_t estimated_memory_reserved(const ChunkPtr& chunk) override;
    size_t estimated_memory_reserved() override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;

    Status set_finished(RuntimeState* state) override;

private:
    // Raw spiller/spill-channel pair that the SpillableFlatSinkMixin reads to compute
    // need_input()/block_reason(). These return the underlying spiller and channel directly
    // (not the collapsed ChunksSorter::is_full(), which ORs spiller-full with channel-has-task into one bool)
    // so the mixin reads WAIT_FLUSH (spiller->is_full()) and WAIT_CHANNEL (channel->has_task()) as distinct
    // named reasons.
    const std::shared_ptr<spill::Spiller>& _spiller() const { return _chunks_sorter->spiller(); }
    SpillProcessChannelPtr _spill_channel() const { return _chunks_sorter->spill_channel(); }

    DECLARE_ONCE_DETECTOR(_set_finishing_once);
};

static_assert(SpillablePartitionSortSinkOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_FLUSH));
static_assert(SpillablePartitionSortSinkOperator::kCoveredWakeups & block_reason_bit(BlockReason::WAIT_CHANNEL));

class SpillablePartitionSortSinkOperatorFactory final : public PartitionSortSinkOperatorFactory {
public:
    template <class... Args>
    SpillablePartitionSortSinkOperatorFactory(Args&&... args)
            : PartitionSortSinkOperatorFactory(std::forward<Args>(args)..., "spillable_local_sort_sink"){};

    ~SpillablePartitionSortSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // Event-scheduler opt-in, gated on enable_spill_sort_events (default false). This is the only false edge
    // among the sort fragment's factories (the source and spill-process factories already report true), so
    // the kill-switch demotes the whole sort fragment back to the poller without a rebuild.
    bool support_event_scheduler() const override { return config::enable_spill_sort_events; }

private:
    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
};

} // namespace starrocks::pipeline