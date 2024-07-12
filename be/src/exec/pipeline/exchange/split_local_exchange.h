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
#include <queue>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/source_operator.h"
#include "exprs/expr_context.h"
namespace starrocks::pipeline {
class SplitLocalExchangeSinkOperator;
// ===== exchanger =====
class SplitLocalExchanger {
public:
    SplitLocalExchanger(int num_consumers, std::vector<ExprContext*>& split_expr_ctxs, size_t chunk_size)
            : _split_expr_ctxs(std::move(split_expr_ctxs)),
              _buffer(num_consumers),
              _num_consumers(num_consumers),
              _opened_source_opcount(num_consumers, 0),
              _chunk_size(chunk_size) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);
    Status push_chunk(const ChunkPtr& chunk, SplitLocalExchangeSinkOperator* sink_op);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t consuemr_index);
    bool can_pull_chunk(int32_t consumer_index) const;
    bool can_push_chunk() const;
    void open_source_operator(int32_t consumer_index);
    void close_source_operator(int32_t consumer_index);
    void open_sink_operator();
    void close_sink_operator();

private:
    // one input chunk will be split into _num_consumers chunks by _split_expr_ctxs and saved in _buffer
    std::vector<ExprContext*> _split_expr_ctxs;
    std::vector<std::queue<ChunkPtr>> _buffer;
    int _num_consumers;

    size_t _current_accumulated_row_size = 0;
    size_t _current_memory_usage = 0;
    // the max row size of one chunk, usally 4096
    size_t _chunk_size;

    int32_t _opened_sink_number = 0;
    int32_t _opened_source_number = 0;
    // every source can have dop operators
    std::vector<int32_t> _opened_source_opcount;

    size_t kBufferedRowSizeScaleFactor = config::split_exchanger_buffer_chunk_num;

    mutable std::mutex _mutex;
};

// ===== source op =====
class SplitLocalExchangeSourceOperator final : public SourceOperator {
public:
    SplitLocalExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     int32_t driver_sequence, int32_t _consumer_index,
                                     std::shared_ptr<SplitLocalExchanger> exchanger)
            : SourceOperator(factory, id, "split_local_exchange_source", plan_node_id, true, driver_sequence),
              _consumer_index(_consumer_index),
              _exchanger(std::move(std::move(exchanger))) {}

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
    int32_t _consumer_index;
    std::shared_ptr<SplitLocalExchanger> _exchanger;
};

class SplitLocalExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SplitLocalExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id, int32_t mcast_consumer_index,
                                            std::shared_ptr<SplitLocalExchanger> exchanger)
            : SourceOperatorFactory(id, "split_local_exchange_source", plan_node_id),
              _mcast_consumer_index(mcast_consumer_index),
              _exchanger(std::move(std::move(exchanger))) {}
    ~SplitLocalExchangeSourceOperatorFactory() override = default;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SplitLocalExchangeSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                  _mcast_consumer_index, _exchanger);
    }

private:
    int32_t _mcast_consumer_index;
    std::shared_ptr<SplitLocalExchanger> _exchanger;
};

// ===== sink op =====

class SplitLocalExchangeSinkOperator final : public Operator {
public:
    SplitLocalExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                   const int32_t driver_sequence, std::shared_ptr<SplitLocalExchanger> exchanger)
            : Operator(factory, id, "split_local_exchange_sink", plan_node_id, true, driver_sequence),
              _exchanger(std::move(std::move(exchanger))) {}

    ~SplitLocalExchangeSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    void update_counter(size_t memory_usage, size_t buffer_row_size);

private:
    bool _is_finished = false;
    const std::shared_ptr<SplitLocalExchanger> _exchanger;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_row_size_counter = nullptr;
};

class SplitLocalExchangeSinkOperatorFactory final : public OperatorFactory {
public:
    SplitLocalExchangeSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                          std::shared_ptr<SplitLocalExchanger> exchanger)
            : OperatorFactory(id, "split_local_exchange_sink", plan_node_id), _exchanger(std::move(exchanger)) {}

    ~SplitLocalExchangeSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SplitLocalExchangeSinkOperator>(this, _id, _plan_node_id, driver_sequence, _exchanger);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<SplitLocalExchanger> _exchanger;
};

} // namespace starrocks::pipeline
