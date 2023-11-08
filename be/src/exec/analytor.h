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
#include <string>

#include "column/chunk.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"

namespace starrocks {

class ManagedFunctionStates;
using ManagedFunctionStatesPtr = std::unique_ptr<ManagedFunctionStates>;

struct FunctionTypes {
    TypeDescriptor result_type;
    bool has_nullable_child;
    bool is_nullable; // window function result whether is nullable
};

class Analytor;
using AnalytorPtr = std::shared_ptr<Analytor>;
using Analytors = std::vector<AnalytorPtr>;

// Component used to do analytic processing
// it contains common data struct and algorithm of analysis
class Analytor final : public pipeline::ContextWithDependency {
    friend class ManagedFunctionStates;

    // [start, end)
    struct FrameRange {
        int64_t start;
        int64_t end;
    };

    struct Segment {
        // Start position of current partition/peer group.
        int64_t start = 0;
        bool is_real = false;
        // If is_real = true, end represents the first position of next partition/peer_group.
        // If is_real = false, end represents the first position of next upcoming chunk.
        int64_t end = 0;

        void remove_first_n(int64_t cnt) {
            start -= cnt;
            end -= cnt;
        }
    };

    class SegmentStatistics {
    private:
        // We will not perform loop search until processing enough segments
        // segment canbe partition or peer group
        static constexpr int64_t MIN_SEGMENT_NUM = 16;

        // Overhead of binary search is O(N/S logN), where S denote the average size of segment
        // Overhead of loop search is O(N)
        // The default chunk_size is 4096, then logN turns out to be log(4096) = 12
        // Considering the error of estimation, we set the threshold to 8
        static constexpr int64_t AVERAGE_SIZE_THRESHOLD = 8;

    public:
        void update(int64_t segment_size) {
            _count++;
            _cumulative_size += segment_size;
            _average_size = _cumulative_size / _count;
        }

        void reset() {
            _count = 0;
            _cumulative_size = 0;
            _average_size = 0;
        }

        bool is_high_cardinality() { return _count > MIN_SEGMENT_NUM && _average_size < AVERAGE_SIZE_THRESHOLD; }

        int64_t _count = 0;
        int64_t _cumulative_size = 0;
        int64_t _average_size = 0;
    };

public:
    ~Analytor() override {
        if (_state != nullptr) {
            close(_state);
        }
    }
    Analytor(const TPlanNode& tnode, const RowDescriptor& child_row_desc, const TupleDescriptor* result_tuple_desc,
             bool use_hash_based_partition);

    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile);
    Status open(RuntimeState* state);
    void close(RuntimeState* state) override;

    Status process(RuntimeState* state, const ChunkPtr& chunk);
    Status finish_process(RuntimeState* state);

    bool is_sink_complete() { return _is_sink_complete.load(std::memory_order_acquire); }
    bool is_chunk_buffer_empty() {
        std::lock_guard<std::mutex> l(_buffer_mutex);
        return _buffer.empty();
    }
    bool is_chunk_buffer_full() { return _buffer.size() >= config::pipeline_analytic_max_buffer_size; }
    bool reached_limit() const { return _limit != -1 && _num_rows_returned >= _limit; }
    ChunkPtr poll_chunk_buffer() {
        std::lock_guard<std::mutex> l(_buffer_mutex);
        if (_buffer.empty()) {
            return nullptr;
        }
        ChunkPtr chunk = _buffer.front();
        _buffer.pop();
        return chunk;
    }
    void offer_chunk_to_buffer(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_buffer_mutex);
        _buffer.push(chunk);
    }

    std::string debug_string() const;

private:
    Status _prepare_processing_mode(RuntimeState* state, RuntimeProfile* runtime_profile);
    Status _evaluate_const_columns(int i);

    Status _check_has_error();
    // All input chunk will first evaluate and then append to these big columns
    // (_agg_intput_columns, _partition_columns, _order_columns), and these big columns may cause significant memory usage,
    // so parts of first rows will be removed as long as it is not necessary for window evaluation.
    void _remove_unused_rows(RuntimeState* state);
    Status _add_chunk(const ChunkPtr& chunk);
    // If src_column is const, but dst is not, unpack src_column then append. Otherwise just append
    void _append_column(size_t chunk_size, Column* dst_column, ColumnPtr& src_column);

    using ProcessByPartitionIfNecessaryFunc = Status (Analytor::*)(RuntimeState* state);
    using ProcessByPartitionFunc = void (Analytor::*)(RuntimeState* state);

    // Process partition when all the data of current partition is reached
    Status _materializing_process(RuntimeState* state);
    // Process partition as the boundary of current partition is not reached.
    // For window frame like:
    // 1. `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // 2. `ROWS BETWEEN UNBOUNDED PRECEDING AND N PRECEDING`
    // 3. `ROWS BETWEEN UNBOUNDED PRECEDING AND N FOLLOWING`
    Status _streaming_process_for_half_unbounded_rows_frame(RuntimeState* state);
    // Process partition as the boundary of current partition is not reached.
    // For window frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    Status _streaming_process_for_half_bounded_range_frame(RuntimeState* state);
    // Process partition as the boundary of current partition is not reached.
    // This approach is suitable for all types of window frame, because it process by definition of window function.
    // But in most cases, it is used for window frame like:
    // 1. `ROWS BETWEEN N PRECEDING AND M PRECEDING` or
    // 2. `ROWS BETWEEN N FOLLOWING AND M FOLLOWING` or
    // 3. `ROWS BETWEEN N PRECEDING AND M FOLLOWING` or
    // 4. `ROWS BETWEEN N PRECEDING AND CURRENT ROW` or
    // 5. `ROWS BETWEEN CURRENT ROW AND M FOLLOWING`
    Status _streaming_process_for_sliding_frame(RuntimeState* state);
    ProcessByPartitionIfNecessaryFunc _process_impl = nullptr;

    // For window frame `ROWS|RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    void _materializing_process_for_unbounded_frame(RuntimeState* state);
    // For window frame `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // materializing means that although the frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, we
    // cannot evaluate window function until all the data of current partition is reached
    // For example, `ntile` need all the data to calculate the bucket step
    void _materializing_process_for_half_unbounded_rows_frame(RuntimeState* state);
    // For window frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // materializing means that although the frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, we
    // cannot evaluate window function until all the data of current partition is reached
    // For example, `cume_dist` need all the data to calculate
    void _materializing_process_for_half_unbounded_range_frame(RuntimeState* state);
    // For window frame `ROWS BETWEEN N PRECEDING AND CURRENT ROW`
    void _materializing_process_for_sliding_frame(RuntimeState* state);
    ProcessByPartitionFunc _materializing_process_impl = nullptr;

    void _update_window_batch(int64_t partition_start, int64_t partition_end, int64_t frame_start, int64_t frame_end);
    void _update_window_batch_removable_cumulatively();

    Status _output_result_chunk(ChunkPtr* chunk);

    void _reset_state_for_next_partition();
    void _reset_window_state();
    void _init_window_result_columns();

    void _find_partition_end();
    void _find_peer_group_end();
    int64_t _find_first_not_equal(Column* column, int64_t target, int64_t start, int64_t end);
    int64_t _find_first_not_equal_for_hash_based_partition(int64_t target, int64_t start, int64_t end);
    void _find_candidate_partition_ends();
    void _find_candidate_peer_group_ends();

    bool _has_output() const { return _output_chunk_index < _input_chunks.size(); }
    int64_t _first_global_position_of_current_chunk() const {
        return _input_chunk_first_row_positions[_output_chunk_index];
    }
    bool _is_current_chunk_finished_eval() const { return _window_result_position() >= _current_chunk_size(); }
    size_t _current_chunk_size() const { return _input_chunks[_output_chunk_index]->num_rows(); }
    int64_t _get_global_position(int64_t local_position) const { return _removed_from_buffer_rows + local_position; }
    int64_t _window_result_position() const {
        return _get_global_position(_current_row_position) - _first_global_position_of_current_chunk();
    }
    FrameRange _get_frame_range() const {
        if (_is_unbounded_preceding) {
            return {_partition.start, _current_row_position + _rows_end_offset + 1};
        } else {
            return {_current_row_position + _rows_start_offset, _current_row_position + _rows_end_offset + 1};
        }
    }

    // This method will be used frequently, so it is better to get chunk_size through "current_chunk_size"
    // outside the method, because "current_chunk_size" contains a virtual function call which cannot be optimized out
    void _update_current_row_position(int64_t increment) { _current_row_position += increment; }

    void _get_window_function_result(size_t frame_start, size_t frame_end);

    // When calculating window functions such as CUME_DIST and PERCENT_RANK,
    // it's necessary to specify the size of the partition.
    void _set_partition_size_for_function();
    bool _require_partition_size(const std::string& function_name) {
        return function_name == "cume_dist" || function_name == "percent_rank";
    }

    RuntimeState* _state = nullptr;
    bool _is_closed = false;
    // TPlanNode is only valid in the PREPARE and INIT phase
    const TPlanNode& _tnode;
    const RowDescriptor& _child_row_desc;
    const TupleDescriptor* _result_tuple_desc;
    const bool _use_hash_based_partition;

    ObjectPool* _pool;
    std::unique_ptr<MemPool> _mem_pool;
    // The open phase still relies on the TFunction object for some initialization operations
    std::vector<TFunction> _fns;

    // Offset from the current row for ROWS windows with start or end bounds specified
    // with offsets. Is positive if the offset is FOLLOWING, negative if PRECEDING, and 0
    // if type is CURRENT ROW or UNBOUNDED PRECEDING/FOLLOWING.
    int64_t _rows_start_offset = 0;
    int64_t _rows_end_offset = 0;

    bool _is_unbounded_preceding = false;

    // The offset of the n-th window function in a row of window functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the window function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all window aggregate state
    size_t _max_agg_state_align_size = 1;
    std::vector<bool> _is_lead_lag_functions;
    std::vector<FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    std::vector<ManagedFunctionStatesPtr> _managed_fn_states;
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<std::vector<ColumnPtr>> _agg_intput_columns;
    std::vector<FunctionTypes> _agg_fn_types;

    std::vector<ExprContext*> _partition_ctxs;
    Columns _partition_columns;

    std::vector<ExprContext*> _order_ctxs;
    Columns _order_columns;

    // Tuple id of the buffered tuple (identical to the input child tuple, which is
    // assumed to come from a single SortNode). NULL if both partition_exprs and
    // order_by_exprs are empty.
    TTupleId _buffered_tuple_id = 0;

    bool _has_udaf = false;
    // Some window functions, eg. NTILE, need the boundary of partition to calculate its value.
    // For these functions, we must wait util the partition finished.
    bool _need_partition_materializing = false;
    bool _support_cumulative_algo = false;
    // When calculating window functions such as CUME_DIST and PERCENT_RANK,
    // it's necessary to specify the size of the partition.
    bool _should_set_partition_size = false;
    std::vector<int64_t> _partition_size_required_function_index;

    RuntimeProfile* _runtime_profile;
    RuntimeProfile::Counter* _remove_unused_rows_cnt = nullptr;
    RuntimeProfile::Counter* _remove_unused_total_rows = nullptr;
    RuntimeProfile::Counter* _column_resize_timer = nullptr;
    RuntimeProfile::Counter* _partition_search_timer = nullptr;
    RuntimeProfile::Counter* _peer_group_search_timer = nullptr;

    int64_t _num_rows_returned = 0;
    int64_t _limit; // -1: no limit

    // Output buffer
    std::atomic<bool> _is_sink_complete = false;
    std::queue<ChunkPtr> _buffer;
    std::mutex _buffer_mutex;

    // Input related related structures
    std::vector<ChunkPtr> _input_chunks;
    int64_t _output_chunk_index = 0;
    std::vector<int64_t> _input_chunk_first_row_positions;
    int64_t _input_rows = 0;
    bool _input_eos = false;

    // Temporary output related structures
    Columns _result_window_columns;

    // Assistant structures for removeing unused buffered input chunks
    int64_t _removed_from_buffer_rows = 0;
    int64_t _removed_chunk_index = 0;

    // Refer to the position of current row.
    int64_t _current_row_position = 0;

    Segment _partition;
    SegmentStatistics _partition_statistics;
    std::queue<int64_t> _candidate_partition_ends;

    Segment _peer_group;
    SegmentStatistics _peer_group_statistics;
    std::queue<int64_t> _candidate_peer_group_ends;
};

// Helper class that properly invokes destructor when state goes out of scope.
class ManagedFunctionStates {
public:
    ManagedFunctionStates(std::vector<FunctionContext*>* ctxs, AggDataPtr __restrict agg_states, Analytor* agg_node)
            : _ctxs(ctxs), _agg_states(agg_states), _agg_node(agg_node) {
        for (int i = 0; i < _agg_node->_agg_functions.size(); i++) {
            _agg_node->_agg_functions[i]->create((*_ctxs)[i], _agg_states + _agg_node->_agg_states_offsets[i]);
        }
    }

    ~ManagedFunctionStates() {
        for (int i = 0; i < _agg_node->_agg_functions.size(); i++) {
            _agg_node->_agg_functions[i]->destroy((*_ctxs)[i], _agg_states + _agg_node->_agg_states_offsets[i]);
        }
    }

    uint8_t* mutable_data() { return _agg_states; }
    const uint8_t* data() const { return _agg_states; }

private:
    std::vector<FunctionContext*>* _ctxs;
    AggDataPtr _agg_states;
    Analytor* _agg_node;
};

class AnalytorFactory;
using AnalytorFactoryPtr = std::shared_ptr<AnalytorFactory>;
class AnalytorFactory {
public:
    AnalytorFactory(size_t dop, const TPlanNode& tnode, const RowDescriptor& child_row_desc,
                    const TupleDescriptor* result_tuple_desc, const bool use_hash_based_partition)
            : _analytors(dop),
              _tnode(tnode),
              _child_row_desc(child_row_desc),
              _result_tuple_desc(result_tuple_desc),
              _use_hash_based_partition(use_hash_based_partition) {}
    AnalytorPtr create(int i);

private:
    Analytors _analytors;
    const TPlanNode& _tnode;
    const RowDescriptor& _child_row_desc;
    const TupleDescriptor* _result_tuple_desc;
    const bool _use_hash_based_partition;
};
} // namespace starrocks
