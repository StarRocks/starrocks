// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exprs/expr_context.h"

namespace starrocks {
class ExprContext;
class RuntimeState;

namespace pipeline {
// Inspire from com.facebook.presto.operator.exchange.LocalExchanger
// Exchange the local data from local sink operator to local source operator
class LocalExchanger {
public:
    explicit LocalExchanger(std::shared_ptr<LocalExchangeMemoryManager> memory_manager)
            : _memory_manager(std::move(memory_manager)) {}

    virtual Status accept(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence) = 0;

    virtual void finish(RuntimeState* state) = 0;

    bool need_input() const;

    void increment_sink_number() { _sink_number++; }

    int32_t decrement_sink_number() { return _sink_number--; }

protected:
    std::shared_ptr<LocalExchangeMemoryManager> _memory_manager;
    std::atomic<int32_t> _sink_number{0};
};

// Exchange the local data for shuffle
class PartitionExchanger final : public LocalExchanger {
    class Partitioner {
    public:
        Partitioner(LocalExchangeSourceOperatorFactory* source, const bool is_shuffle,
                    const std::vector<ExprContext*>& partition_expr_ctxs)
                : _source(source), _is_shuffle(is_shuffle), _partition_expr_ctxs(partition_expr_ctxs) {
            _partitions_columns.resize(partition_expr_ctxs.size());
            _hash_values.reserve(config::vector_chunk_size);
        }

        // Divide chunk into shuffle partitions.
        // partition_row_indexes records the row indexes for the current shuffle index.
        // Sender will arrange the row indexes according to partitions.
        // For example, if there are 3 channels, it will put partition 0's row first,
        // then partition 1's row indexes, then put partition 2's row indexes in the last.
        Status partition_chunk(const vectorized::ChunkPtr& chunk, std::vector<uint32_t>& partition_row_indexes);

        size_t partition_begin_offset(size_t partition_id) { return _partition_row_indexes_start_points[partition_id]; }

        size_t partition_end_offset(size_t partition_id) {
            return _partition_row_indexes_start_points[partition_id + 1];
        }

    private:
        LocalExchangeSourceOperatorFactory* _source;
        const bool _is_shuffle;
        // Compute per-row partition values.
        const std::vector<ExprContext*> _partition_expr_ctxs;

        vectorized::Columns _partitions_columns;
        std::vector<uint32_t> _hash_values;
        // This array record the channel start point in _row_indexes
        // And the last item is the number of rows of the current shuffle chunk.
        // It will easy to get number of rows belong to one channel by doing
        // _partition_row_indexes_start_points[i + 1] - _partition_row_indexes_start_points[i]
        std::vector<size_t> _partition_row_indexes_start_points;
    };

public:
    PartitionExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                       LocalExchangeSourceOperatorFactory* source, bool is_shuffle,
                       const std::vector<ExprContext*>& _partition_expr_ctxs, size_t num_sinks);

    Status accept(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence) override;

    void finish(RuntimeState* state) override {
        if (decrement_sink_number() == 1) {
            for (auto* source : _source->get_sources()) {
                source->finish(state);
            }
        }
    }

private:
    LocalExchangeSourceOperatorFactory* _source;
    // Used for local shuffle exchanger.
    // The sink_driver_sequence-th local sink operator exclusively uses the sink_driver_sequence-th partitioner.
    // TODO(lzh): limit the size of _partitioners, because it will cost too much memory when dop is high.
    std::vector<Partitioner> _partitioners;
};

// Exchange the local data for broadcast
class BroadcastExchanger final : public LocalExchanger {
public:
    BroadcastExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                       LocalExchangeSourceOperatorFactory* source)
            : LocalExchanger(memory_manager), _source(source) {}

    Status accept(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence) override;

    void finish(RuntimeState* state) override {
        if (decrement_sink_number() == 1) {
            for (auto* source : _source->get_sources()) {
                source->finish(state);
            }
        }
    }

private:
    LocalExchangeSourceOperatorFactory* _source;
};

// Exchange the local data for one local source operation
class PassthroughExchanger final : public LocalExchanger {
public:
    PassthroughExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                         LocalExchangeSourceOperatorFactory* source)
            : LocalExchanger(memory_manager), _source(source) {}

    Status accept(const vectorized::ChunkPtr& chunk, int32_t sink_driver_sequence) override;

    void finish(RuntimeState* state) override {
        if (decrement_sink_number() == 1) {
            for (auto* source : _source->get_sources()) {
                source->finish(state);
            }
        }
    }

private:
    LocalExchangeSourceOperatorFactory* _source;
    std::atomic<size_t> _next_accept_source = 0;
};

} // namespace pipeline
} // namespace starrocks
