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

#include <mutex>
#include <queue>
#include <utility>

#include "exec/pipeline/exchange/local_exchange_memory_manager.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {
class LocalExchangeSourceOperator final : public SourceOperator {
    class PartitionChunk {
    public:
        PartitionChunk(ChunkPtr chunk, std::shared_ptr<std::vector<uint32_t>> indexes, const uint32_t from,
                       const uint32_t size, const size_t memory_usage)
                : chunk(std::move(chunk)),
                  indexes(std::move(indexes)),
                  from(from),
                  size(size),
                  memory_usage(memory_usage) {}

        PartitionChunk(const PartitionChunk&) = delete;

        PartitionChunk(PartitionChunk&&) = default;

        ChunkPtr chunk;
        std::shared_ptr<std::vector<uint32_t>> indexes;
        const uint32_t from;
        const uint32_t size;
        const size_t memory_usage;
    };

public:
    LocalExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager)
            : SourceOperator(factory, id, "local_exchange_source", plan_node_id, driver_sequence),
              _memory_manager(memory_manager) {}

    Status add_chunk(ChunkPtr chunk);

    Status add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes, uint32_t from,
                     uint32_t size, size_t memory_bytes);

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;
    Status set_finishing(RuntimeState* state) override {
        std::lock_guard<std::mutex> l(_chunk_lock);
        _is_finished = true;
        return Status::OK();
    }

    bool is_epoch_finished() const override {
        std::lock_guard<std::mutex> l(_chunk_lock);
        return _is_epoch_finished && _full_chunk_queue.empty() && !_partition_rows_num;
    }
    Status set_epoch_finishing(RuntimeState* state) override {
        std::lock_guard<std::mutex> l(_chunk_lock);
        _is_epoch_finished = true;
        return Status::OK();
    }
    Status reset_epoch(RuntimeState* state) override {
        _is_epoch_finished = false;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    ChunkPtr _pull_passthrough_chunk(RuntimeState* state);

    ChunkPtr _pull_shuffle_chunk(RuntimeState* state);

    bool _local_buffer_almost_full() const {
        return _local_memory_usage >= _memory_manager->get_memory_limit_per_driver() * 0.8;
    }

    bool _is_finished = false;
    std::queue<ChunkPtr> _full_chunk_queue;
    std::queue<PartitionChunk> _partition_chunk_queue;
    size_t _partition_rows_num = 0;
    size_t _local_memory_usage = 0;

    // TODO(KKS): make it lock free
    mutable std::mutex _chunk_lock;
    const std::shared_ptr<LocalExchangeMemoryManager>& _memory_manager;

    // STREAM MV
    bool _is_epoch_finished = false;
};

class LocalExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                       std::shared_ptr<LocalExchangeMemoryManager> memory_manager)
            : SourceOperatorFactory(id, "local_exchange_source", plan_node_id),
              _memory_manager(std::move(memory_manager)) {}

    ~LocalExchangeSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        std::shared_ptr<LocalExchangeSourceOperator> source = std::make_shared<LocalExchangeSourceOperator>(
                this, _id, _plan_node_id, driver_sequence, _memory_manager);
        _sources.emplace_back(source.get());
        return source;
    }

    std::vector<LocalExchangeSourceOperator*>& get_sources() { return _sources; }

private:
    std::shared_ptr<LocalExchangeMemoryManager> _memory_manager;
    std::vector<LocalExchangeSourceOperator*> _sources;
};

} // namespace starrocks::pipeline
