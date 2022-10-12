// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
        PartitionChunk(vectorized::ChunkPtr chunk, std::shared_ptr<std::vector<uint32_t>> indexes, const uint32_t from,
                       const uint32_t size)
                : chunk(std::move(chunk)), indexes(std::move(indexes)), from(from), size(size) {}

        PartitionChunk(const PartitionChunk&) = delete;

        PartitionChunk(PartitionChunk&&) = default;

        vectorized::ChunkPtr chunk;
        std::shared_ptr<std::vector<uint32_t>> indexes;
        const uint32_t from;
        const uint32_t size;
    };

public:
    LocalExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager)
            : SourceOperator(factory, id, "local_exchange_source", plan_node_id, driver_sequence),
              _memory_manager(memory_manager) {}

    Status add_chunk(vectorized::ChunkPtr chunk);

    Status add_chunk(vectorized::ChunkPtr chunk, std::shared_ptr<std::vector<uint32_t>> indexes, uint32_t from,
                     uint32_t size);

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;
    Status set_finishing(RuntimeState* state) override {
        std::lock_guard<std::mutex> l(_chunk_lock);
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    vectorized::ChunkPtr _pull_passthrough_chunk(RuntimeState* state);

    vectorized::ChunkPtr _pull_shuffle_chunk(RuntimeState* state);

    bool _is_finished = false;
    std::queue<vectorized::ChunkPtr> _full_chunk_queue;
    std::queue<PartitionChunk> _partition_chunk_queue;
    size_t _partition_rows_num = 0;

    // TODO(KKS): make it lock free
    mutable std::mutex _chunk_lock;
    const std::shared_ptr<LocalExchangeMemoryManager>& _memory_manager;
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
