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
#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/exchange/shuffler.h"
#include "exprs/expr_context.h"

namespace starrocks {
class ExprContext;
class RuntimeState;

namespace pipeline {
// Inspire from com.facebook.presto.operator.exchange.LocalExchanger
// Exchange the local data from local sink operator to local source operator
class LocalExchanger {
public:
    explicit LocalExchanger(std::string name, std::shared_ptr<LocalExchangeMemoryManager> memory_manager,
                            LocalExchangeSourceOperatorFactory* source)
            : _name(std::move(name)), _memory_manager(std::move(memory_manager)), _source(source) {}

    virtual Status prepare(RuntimeState* state) { return Status::OK(); }
    virtual void close(RuntimeState* state) {}

    virtual Status accept(const ChunkPtr& chunk, int32_t sink_driver_sequence) = 0;

    virtual void finish(RuntimeState* state) {
        if (decr_sinker() == 1) {
            for (auto* source : _source->get_sources()) {
                source->set_finishing(state);
            }
        }
    }

    // All LocalExchangeSourceOperators have finished.
    virtual bool is_all_sources_finished() const {
        for (const auto& source_op : _source->get_sources()) {
            if (!source_op->is_finished()) {
                return false;
            }
        }
        return true;
    }

    void epoch_finish(RuntimeState* state) {
        if (incr_epoch_finished_sinker() == _sink_number) {
            for (auto* source : _source->get_sources()) {
                source->set_epoch_finishing(state);
            }
            // reset the number to be reused in the next epoch.
            _epoch_finished_sinker = 0;
        }
    }

    const std::string& name() const { return _name; }

    bool need_input() const;

    virtual void incr_sinker() { _sink_number++; }
    int32_t decr_sinker() { return _sink_number--; }

    int32_t source_dop() const { return _source->get_sources().size(); }

    int32_t incr_epoch_finished_sinker() { return ++_epoch_finished_sinker; }

    size_t get_memory_usage() const { return _memory_manager->get_memory_usage(); }

protected:
    const std::string _name;
    std::shared_ptr<LocalExchangeMemoryManager> _memory_manager;
    std::atomic<int32_t> _sink_number = 0;
    LocalExchangeSourceOperatorFactory* _source;

    // Stream MV
    std::atomic<int32_t> _epoch_finished_sinker = 0;
};

// Exchange the local data for shuffle
class PartitionExchanger final : public LocalExchanger {
    class Partitioner {
    public:
        Partitioner(LocalExchangeSourceOperatorFactory* source, const TPartitionType::type part_type,
                    const std::vector<ExprContext*>& partition_expr_ctxs)
                : _source(source), _part_type(part_type), _partition_expr_ctxs(partition_expr_ctxs) {
            _partitions_columns.resize(partition_expr_ctxs.size());
            _hash_values.reserve(source->runtime_state()->chunk_size());
        }

        // Divide chunk into shuffle partitions.
        // partition_row_indexes records the row indexes for the current shuffle index.
        // Sender will arrange the row indexes according to partitions.
        // For example, if there are 3 channels, it will put partition 0's row first,
        // then partition 1's row indexes, then put partition 2's row indexes in the last.
        Status partition_chunk(const ChunkPtr& chunk, std::vector<uint32_t>& partition_row_indexes);

        size_t partition_begin_offset(size_t partition_id) { return _partition_row_indexes_start_points[partition_id]; }

        size_t partition_end_offset(size_t partition_id) {
            return _partition_row_indexes_start_points[partition_id + 1];
        }

        size_t partition_memory_usage(size_t partition_id) {
            if (partition_id >= _partition_memory_usage.size() || partition_id < 0) {
                throw std::runtime_error(fmt::format("invalid index {} to get partition memory usage, whose size = {}.",
                                                     partition_id, _partition_memory_usage.size()));
            } else {
                return _partition_memory_usage[partition_id];
            }
        }

    private:
        LocalExchangeSourceOperatorFactory* _source;
        const TPartitionType::type _part_type;
        // Compute per-row partition values.
        const std::vector<ExprContext*>& _partition_expr_ctxs;

        Columns _partitions_columns;
        std::vector<uint32_t> _hash_values;
        std::vector<uint32_t> _shuffle_channel_id;
        // This array record the channel start point in _row_indexes
        // And the last item is the number of rows of the current shuffle chunk.
        // It will easy to get number of rows belong to one channel by doing
        // _partition_row_indexes_start_points[i + 1] - _partition_row_indexes_start_points[i]
        std::vector<size_t> _partition_row_indexes_start_points;
        std::vector<size_t> _partition_memory_usage;
        std::unique_ptr<Shuffler> _shuffler;
    };

public:
    PartitionExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                       LocalExchangeSourceOperatorFactory* source, const TPartitionType::type part_type,
                       const std::vector<ExprContext*>& _partition_expr_ctxs);

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    Status accept(const ChunkPtr& chunk, int32_t sink_driver_sequence) override;

    void incr_sinker() override;

private:
    // Used for local shuffle exchanger.
    // The sink_driver_sequence-th local sink operator exclusively uses the sink_driver_sequence-th partitioner.
    // TODO(lzh): limit the size of _partitioners, because it will cost too much memory when dop is high.
    TPartitionType::type _part_type;
    std::vector<ExprContext*> _partition_exprs;

    std::vector<Partitioner> _partitioners;
};

// Exchange the local data for broadcast
class BroadcastExchanger final : public LocalExchanger {
public:
    BroadcastExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                       LocalExchangeSourceOperatorFactory* source)
            : LocalExchanger("Broadcast", memory_manager, source) {}

    Status accept(const ChunkPtr& chunk, int32_t sink_driver_sequence) override;
};

// Exchange the local data for one local source operation
class PassthroughExchanger final : public LocalExchanger {
public:
    PassthroughExchanger(const std::shared_ptr<LocalExchangeMemoryManager>& memory_manager,
                         LocalExchangeSourceOperatorFactory* source)
            : LocalExchanger("Passthrough", memory_manager, source) {}

    Status accept(const ChunkPtr& chunk, int32_t sink_driver_sequence) override;

private:
    std::atomic<size_t> _next_accept_source = 0;
};

} // namespace pipeline
} // namespace starrocks
