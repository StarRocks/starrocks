// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/exec_node.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"

namespace starrocks {
namespace vectorized {

class ManagedFunctionStates;
using ManagedFunctionStatesPtr = std::unique_ptr<ManagedFunctionStates>;

struct FunctionTypes {
    TypeDescriptor result_type;
    bool has_nullable_child;
    bool is_nullable; // window function result whether is nullable
};

struct FrameRange {
    int64_t start;
    int64_t end;
};

class AnalyticNode : public ExecNode {
public:
    ~AnalyticNode() override {}
    AnalyticNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status close(RuntimeState* state) override;

private:
    friend class ManagedFunctionStates;

    enum FrameType {
        Unbounded,               // BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        UnboundedPrecedingRange, // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        UnboundedPrecedingRows,  // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        Sliding                  // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    };

    Status _get_next_for_unbounded_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status _get_next_for_unbounded_preceding_range_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status _get_next_for_unbounded_preceding_rows_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status _get_next_for_sliding_frame(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status (AnalyticNode::*_get_next)(RuntimeState* state, ChunkPtr* chunk, bool* eos) = nullptr;

    void _update_window_batch_normal(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                     int64_t frame_end);

    // lead and lag function is special, the frame_start and frame_end
    // maybe less than zero.
    void _update_window_batch_lead_lag(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                       int64_t frame_end);

    void (AnalyticNode::*_update_window_batch)(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                               int64_t frame_end) = nullptr;

    void _reset_window_state();

    bool _need_fetch_next_chunk(int64_t found_partition_end);

    Status _fetch_next_chunk(RuntimeState* state);

    // Try fetch next partition data if necessary
    // Return value is current partition end position
    Status _try_fetch_next_partition_data(RuntimeState* state, int64_t* partition_end);

    void _get_window_function_result(int32_t start, int32_t end);

    Status _output_result_chunk(ChunkPtr* chunk);

    int64_t _get_total_position(int64_t local_position);

    bool _is_new_partition(int64_t found_partition_end);

    void _reset_state_for_new_partition(int64_t found_partition_end);

    int64_t _find_partition_end();

    void _find_peer_group_end();

    FrameRange _get_sliding_frame_range_no_start();

    FrameRange _get_sliding_frame_range_with_start();

    FrameRange (AnalyticNode::*_get_sliding_frame_range)() = nullptr;

    void _remove_unused_buffer_values();

    // Create new aggregate function result column by type
    void _create_agg_result_columns(int64_t chunk_size);

    int64_t _find_first_not_equal(Column* column, int64_t start, int64_t end);

    size_t _compute_memory_usage();

    void _append_column(size_t chunk_size, Column* dst_column, ColumnPtr& src_column);

    Columns _result_window_columns;
    std::vector<ChunkPtr> _input_chunks;
    std::vector<int64_t> input_chunk_first_row_positions;
    int64_t _input_rows = 0;
    int64_t _removed_from_buffer_rows = 0;
    int64_t _removed_chunk_index = 0;
    int64_t _output_chunk_index = 0;
    int64_t _window_result_position = 0;
    bool _input_eos = false;

#ifdef NDEBUG
    static constexpr int32_t BUFFER_CHUNK_NUMBER = 1000;
#else
    static constexpr int32_t BUFFER_CHUNK_NUMBER = 1;
#endif

#ifdef NDEBUG
    static constexpr size_t memory_check_batch_size = 65535;
#else
    static constexpr size_t memory_check_batch_size = 1;
#endif

    int64_t _current_row_position = 0;
    int64_t _partition_start = 0;
    int64_t _partition_end = 0;
    // A peer group is all of the rows that are peers within the specified ordering.
    // Rows are peers if they compare equal to each other using the specified ordering expression.
    int64_t _peer_group_start = 0;
    int64_t _peer_group_end = 0;

    // Offset from the current row for ROWS windows with start or end bounds specified
    // with offsets. Is positive if the offset is FOLLOWING, negative if PRECEDING, and 0
    // if type is CURRENT ROW or UNBOUNDED PRECEDING/FOLLOWING.
    int64_t _rows_start_offset = 0;
    int64_t _rows_end_offset = 0;

    int64_t _last_memory_usage = 0;

    std::unique_ptr<MemPool> _mem_pool;

    // The offset of the n-th window function in a row of window functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the window function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all window aggregate state
    size_t _max_agg_state_align_size = 1;
    std::vector<starrocks_udf::FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    std::vector<ManagedFunctionStatesPtr> _managed_fn_states;
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<std::vector<ColumnPtr>> _agg_intput_columns;
    std::vector<FunctionTypes> _agg_fn_types;

    std::vector<ExprContext*> _partition_ctxs;
    Columns _partition_columns;

    std::vector<ExprContext*> _order_ctxs;
    Columns _order_columns;

    // Tuple descriptor for storing results of analytic fn evaluation.
    const TupleDescriptor* _result_tuple_desc;
    // Tuple id of the buffered tuple (identical to the input child tuple, which is
    // assumed to come from a single SortNode). NULL if both partition_exprs and
    // order_by_exprs are empty.
    TTupleId _buffered_tuple_id = 0;

    // Time spent processing the child rows.
    RuntimeProfile::Counter* _compute_timer{};
};

// Helper class that properly invokes destructor when state goes out of scope.
class ManagedFunctionStates {
public:
    ManagedFunctionStates(AggDataPtr agg_states, AnalyticNode* agg_node)
            : _agg_states(agg_states), _agg_node(agg_node) {
        for (int i = 0; i < _agg_node->_agg_functions.size(); i++) {
            _agg_node->_agg_functions[i]->create(_agg_states + _agg_node->_agg_states_offsets[i]);
        }
    }

    ~ManagedFunctionStates() {
        for (int i = 0; i < _agg_node->_agg_functions.size(); i++) {
            _agg_node->_agg_functions[i]->destroy(_agg_states + _agg_node->_agg_states_offsets[i]);
        }
    }

    uint8_t* mutable_data() { return _agg_states; }
    const uint8_t* data() const { return _agg_states; }

private:
    AggDataPtr _agg_states;
    AnalyticNode* _agg_node;
};

} // namespace vectorized
} // namespace starrocks
