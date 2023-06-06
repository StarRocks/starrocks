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

#include "exec/pipeline/sort/partition_sort_sink_operator.h"
#include "exec/spill/spiller.h"

namespace starrocks::pipeline {
class SpillablePartitionSortSinkOperator final : public PartitionSortSinkOperator {
public:
    template <class... Args>
    SpillablePartitionSortSinkOperator(Args&&... args)
            : PartitionSortSinkOperator(std::forward<Args>(args)..., "spillable_local_sort_sink") {}

    ~SpillablePartitionSortSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool need_input() const override { return !is_finished() && !_chunks_sorter->is_full(); }

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
};

class SpillablePartitionSortSinkOperatorFactory final : public PartitionSortSinkOperatorFactory {
public:
    template <class... Args>
    SpillablePartitionSortSinkOperatorFactory(Args&&... args)
            : PartitionSortSinkOperatorFactory(std::forward<Args>(args)..., "spillable_local_sort_sink"){};

    ~SpillablePartitionSortSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
};

} // namespace starrocks::pipeline