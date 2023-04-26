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
class SortExecExprs;
namespace pipeline {
class ExchangeMergeSortSourceOperator : public SourceOperator {
public:
    ExchangeMergeSortSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                    int32_t num_sender, const RowDescriptor& row_desc, SortExecExprs* sort_exec_exprs,
                                    const std::vector<bool>& is_asc_order, const std::vector<bool>& nulls_first,
                                    int64_t offset, int64_t limit)
            : SourceOperator(factory, id, "global_merge_source", plan_node_id, driver_sequence),
              _num_sender(num_sender),
              _row_desc(row_desc),
              _sort_exec_exprs(sort_exec_exprs),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first),
              _offset(offset),
              _limit(limit) {}

    ~ExchangeMergeSortSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    Status get_next_merging(RuntimeState* state, ChunkPtr* chunk);

    int32_t _num_sender;
    const RowDescriptor& _row_desc;

    SortExecExprs* _sort_exec_exprs;
    const std::vector<bool>& _is_asc_order;
    const std::vector<bool>& _nulls_first;

    std::shared_ptr<DataStreamRecvr> _stream_recvr;
    std::atomic<bool> _is_finished{false};

    int64_t _num_rows_returned = 0;
    int64_t _num_rows_input = 0;
    int64_t _offset;
    int64_t _limit;
};

class ExchangeMergeSortSourceOperatorFactory final : public SourceOperatorFactory {
public:
    ExchangeMergeSortSourceOperatorFactory(int32_t id, int32_t plan_node_id, int32_t num_sender,
                                           const RowDescriptor& row_desc, SortExecExprs* sort_exec_exprs,
                                           const std::vector<bool>& is_asc_order, const std::vector<bool>& nulls_first,
                                           int64_t offset, int64_t limit)
            : SourceOperatorFactory(id, "global_merge_source", plan_node_id),
              _num_sender(num_sender),
              _row_desc(row_desc),
              _sort_exec_exprs(sort_exec_exprs),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first),
              _offset(offset),
              _limit(limit) {}

    ~ExchangeMergeSortSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<ExchangeMergeSortSourceOperator>(this, _id, _plan_node_id, driver_sequence, _num_sender,
                                                                 _row_desc, _sort_exec_exprs, _is_asc_order,
                                                                 _nulls_first, _offset, _limit);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    int32_t _num_sender;
    const RowDescriptor& _row_desc;
    SortExecExprs* _sort_exec_exprs;
    const std::vector<bool>& _is_asc_order;
    const std::vector<bool>& _nulls_first;
    int64_t _offset;
    int64_t _limit;
};

} // namespace pipeline
} // namespace starrocks
