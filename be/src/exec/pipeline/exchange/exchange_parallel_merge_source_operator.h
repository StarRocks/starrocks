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

#include "exec/pipeline/source_operator.h"
#include "exec/sorting/merge_path.h"

namespace starrocks {
class SortExecExprs;
namespace pipeline {

// The number of sender may not equal to the degree of parallelism at the receiver side.
//
//        Sender                                                 Receiver
//
// +----------------------+       Network        +-------------------------------------+
// | ExchangeSinkOperator | -------------------> | ExchangeParallelMergeSourceOperator | ------------> output streams
// +----------------------+                      +-------------------------------------+
// +----------------------+       Network        +-------------------------------------+
// | ExchangeSinkOperator | -------------------> | ExchangeParallelMergeSourceOperator | no output stream
// +----------------------+                      +-------------------------------------+
// +----------------------+       Network        +-------------------------------------+
// | ExchangeSinkOperator | -------------------> | ExchangeParallelMergeSourceOperator | no output stream
// +----------------------+                ----->+-------------------------------------+
// +----------------------+      ---------/
// | ExchangeSinkOperator |-----/
// +----------------------+
//
// There will be as many ExchangeParallelMergeSourceOperator as degree of parallelism, and
// all the ExchangeParallelMergeSourceOperators will be involved in the parallel merge processing,
// and only one ExchangeParallelMergeSourceOperator, the operator with driver_sequence = 0 for simplicity,
// will output data.
//
// Besides, the number of sender may be smaller or greater than the degree of parallelism, the former normally
// equals to the number backend in the cluster.
//
// All the parallel merge processing is organized in the component named MergePathCascadeMerger, which
// can be easily integrated into the pipeline engine. The only thing for this operator need to to is to call
// the method MergePathCascadeMerger::try_get_next.
//
class ExchangeParallelMergeSourceOperator final : public SourceOperator {
public:
    ExchangeParallelMergeSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                        int32_t driver_sequence)
            : SourceOperator(factory, id, "global_parallel_merge_source", plan_node_id, false, driver_sequence) {}

    ~ExchangeParallelMergeSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_mutable() const override { return true; }

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    std::atomic<bool> _is_finished{false};

    DataStreamRecvr* _stream_recvr;
    merge_path::MergePathCascadeMerger* _merger;
};

class ExchangeParallelMergeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    ExchangeParallelMergeSourceOperatorFactory(int32_t id, int32_t plan_node_id, int32_t num_sender,
                                               const RowDescriptor& row_desc, SortExecExprs* sort_exec_exprs,
                                               const std::vector<bool>& is_asc_order,
                                               const std::vector<bool>& nulls_first, int64_t offset, int64_t limit)
            : SourceOperatorFactory(id, "global_parallel_merge_source", plan_node_id),
              _num_sender(num_sender),
              _row_desc(row_desc),
              _sort_exec_exprs(sort_exec_exprs),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first),
              _offset(offset),
              _limit(limit) {}

    ~ExchangeParallelMergeSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    DataStreamRecvr* get_stream_recvr(RuntimeState* state);
    merge_path::MergePathCascadeMerger* get_merge_path_merger(RuntimeState* state);
    void close_stream_recvr();

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    const int32_t _num_sender;
    const RowDescriptor& _row_desc;
    SortExecExprs* _sort_exec_exprs;
    const std::vector<bool>& _is_asc_order;
    const std::vector<bool>& _nulls_first;
    const int64_t _offset;
    const int64_t _limit;

    std::shared_ptr<DataStreamRecvr> _stream_recvr;
    std::atomic<int64_t> _stream_recvr_cnt = 0;
    std::unique_ptr<merge_path::MergePathCascadeMerger> _merger;
};
} // namespace pipeline
} // namespace starrocks
