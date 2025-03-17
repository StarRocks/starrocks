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
#include "multi_cast_local_exchange.h"
namespace starrocks::pipeline {
class SplitLocalExchangeSinkOperator;
// ===== exchanger =====
class SplitLocalExchanger final : public MultiCastLocalExchanger {
public:
    SplitLocalExchanger(int num_consumers, std::vector<ExprContext*>& split_expr_ctxs, size_t chunk_size)
            : _split_expr_ctxs(std::move(split_expr_ctxs)),
              _buffer(num_consumers),
              _opened_source_opcount(num_consumers, 0),
              _chunk_size(chunk_size) {}

    bool support_event_scheduler() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) override;
    Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t consuemr_index) override;
    bool can_pull_chunk(int32_t consumer_index) const override;
    bool can_push_chunk() const override;
    void open_source_operator(int32_t consumer_index) override;
    void close_source_operator(int32_t consumer_index) override;
    void open_sink_operator() override;
    void close_sink_operator() override;

private:
    // one input chunk will be split into _num_consumers chunks by _split_expr_ctxs and saved in _buffer
    std::vector<ExprContext*> _split_expr_ctxs;
    std::vector<std::queue<ChunkPtr>> _buffer;

    size_t _current_accumulated_row_size = 0;
    size_t _current_memory_usage = 0;

    int32_t _opened_sink_number = 0;
    int32_t _opened_source_number = 0;
    // every source can have dop operators
    std::vector<int32_t> _opened_source_opcount;

    size_t kBufferedRowSizeScaleFactor = config::split_exchanger_buffer_chunk_num;

    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_row_size_counter = nullptr;

    // the max row size of one chunk, usally 4096
    size_t _chunk_size;

    mutable std::mutex _mutex;
};

} // namespace starrocks::pipeline
