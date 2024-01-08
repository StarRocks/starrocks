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

#include <atomic>

#include "exec/pipeline/source_operator.h"

namespace starrocks {
class DataStreamRecvr;
class RowDescriptor;
namespace pipeline {
class ExchangeSourceOperator : public SourceOperator {
public:
    ExchangeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "exchange_source", plan_node_id, false, driver_sequence) {}

    virtual ~ExchangeSourceOperator() = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    std::shared_ptr<DataStreamRecvr> _stream_recvr = nullptr;
    std::atomic<bool> _is_finishing = false;
};

class ExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    ExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id, const TExchangeNode& texchange_node,
                                  int32_t num_sender, const RowDescriptor& row_desc, bool enable_pipeline_level_shuffle)
            : SourceOperatorFactory(id, "exchange_source", plan_node_id),
              _texchange_node(texchange_node),
              _num_sender(num_sender),
              _row_desc(row_desc),
              _enable_pipeline_level_shuffle(enable_pipeline_level_shuffle) {}

    virtual ~ExchangeSourceOperatorFactory();

    const TExchangeNode& texchange_node() { return _texchange_node; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        ++_stream_recvr_cnt;
        // FIXME: it is unsafe to pass the raw `this` pointer to construct a shared_ptr object.
        // The shared_ptr object may live longer than the object `this` pointed to.
        return std::make_shared<ExchangeSourceOperator>(this, _id, _plan_node_id, driver_sequence);
    }

    bool could_local_shuffle() const override;
    TPartitionType::type partition_type() const override;

    std::shared_ptr<DataStreamRecvr> create_stream_recvr(RuntimeState* state);
    void close_stream_recvr();

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

private:
    const TExchangeNode& _texchange_node;
    const int32_t _num_sender;
    const RowDescriptor& _row_desc;
    const bool _enable_pipeline_level_shuffle;
    std::shared_ptr<DataStreamRecvr> _stream_recvr = nullptr;
    std::atomic<int64_t> _stream_recvr_cnt = 0;
};

} // namespace pipeline
} // namespace starrocks
