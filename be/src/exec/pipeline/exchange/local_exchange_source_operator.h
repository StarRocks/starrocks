// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <mutex>
#include <utility>

#include "exec/pipeline/exchange/local_exchange_memory_manager.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {
class LocalExchangeSourceOperator final : public SourceOperator {
public:
    LocalExchangeSourceOperator(int32_t id, const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager)
            : SourceOperator(id, "local_exchange_source", -1), _memory_manager(memory_manager) {}

    Status add_chunk(vectorized::ChunkPtr chunk);

    Status add_chunk(vectorized::Chunk* chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    bool has_output() const override;

    bool is_finished() const override;

    void finish(RuntimeState* state) override { _is_finished = true; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    std::atomic<bool> _is_finished{false};
    vectorized::ChunkPtr _full_chunk = nullptr;
    vectorized::ChunkUniquePtr _partial_chunk = nullptr;
    // TODO(KKS): make it lock free
    mutable std::mutex _chunk_lock;
    const std::shared_ptr<LocalExchangeMemoryManager>& _memory_manager;
};

class LocalExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalExchangeSourceOperatorFactory(int32_t id, std::shared_ptr<LocalExchangeMemoryManager> memory_manager)
            : SourceOperatorFactory(id, "local_exchange_source", -1), _memory_manager(std::move(memory_manager)) {}

    ~LocalExchangeSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        std::shared_ptr<LocalExchangeSourceOperator> source =
                std::make_shared<LocalExchangeSourceOperator>(_id, _memory_manager);
        _sources.emplace_back(source.get());
        return source;
    }

    std::vector<LocalExchangeSourceOperator*>& get_sources() { return _sources; }

private:
    std::shared_ptr<LocalExchangeMemoryManager> _memory_manager;
    std::vector<LocalExchangeSourceOperator*> _sources;
};

} // namespace starrocks::pipeline
