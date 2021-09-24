// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"

namespace starrocks {
class ExprContext;
class RuntimeState;
namespace pipeline {
// Inspire from com.facebook.presto.operator.exchange.LocalExchanger
// Exchange the local data from local sink operator to local source operator
class LocalExchanger {
public:
    LocalExchanger(std::shared_ptr<LocalExchangeMemoryManager> memory_manager)
            : _memory_manager(std::move(memory_manager)) {}

    virtual Status accept(const vectorized::ChunkPtr& chunk) = 0;

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
public:
    PartitionExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                       LocalExchangeSourceOperatorFactory* source, bool is_shuffle,
                       const std::vector<ExprContext*>& _partition_expr_ctxs);

    Status accept(const vectorized::ChunkPtr& chunk) override;

    void finish(RuntimeState* state) override {
        if (decrement_sink_number() == 1) {
            for (auto* source : _source->get_sources()) {
                source->finish(state);
            }
        }
    }

private:
    LocalExchangeSourceOperatorFactory* _source;
    bool _is_shuffle = true;
    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values

    vectorized::Columns _partitions_columns;
    std::vector<uint32_t> _hash_values;
    // This array record the channel start point in _row_indexes
    // And the last item is the number of rows of the current shuffle chunk.
    // It will easy to get number of rows belong to one channel by doing
    // _channel_row_idx_start_points[i + 1] - _channel_row_idx_start_points[i]
    std::vector<uint16_t> _channel_row_idx_start_points;
    // Record the row indexes for the current shuffle index. Sender will arrange the row indexes
    // according to channels. For example, if there are 3 channels, this _row_indexes will put
    // channel 0's row first, then channel 1's row indexes, then put channel 2's row indexes in
    // the last.
    std::vector<uint32_t> _row_indexes;
};

// Exchange the local data for broadcast
class BroadcastExchanger final : public LocalExchanger {
public:
    BroadcastExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                       LocalExchangeSourceOperatorFactory* source)
            : LocalExchanger(memory_manager), _source(source) {}

    Status accept(const vectorized::ChunkPtr& chunk) override;

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
    Status accept(const vectorized::ChunkPtr& chunk) override;

    void finish(RuntimeState* state) override {
        if (decrement_sink_number() == 1) {
            _source->get_sources()[0]->finish(state);
        }
    }

private:
    LocalExchangeSourceOperatorFactory* _source;
};
} // namespace pipeline
} // namespace starrocks
