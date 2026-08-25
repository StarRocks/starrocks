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

#include <algorithm>
#include <atomic>
#include <deque>
#include <mutex>
#include <queue>
#include <string>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "common/global_types.h"
#include "common/memory/mem_hook_allocator.h"
#include "common/runtime_profile.h"
#include "common/statusor.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec_primitive/pipeline/primitives/pipeline_observer.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "types/type_descriptor.h"

namespace starrocks::pipeline {
class MemLimitedChunkQueue;
} // namespace starrocks::pipeline

namespace starrocks {

struct FunctionTypes {
    TypeDescriptor result_type;
    bool has_nullable_child;
    bool is_nullable; // window function result whether is nullable
};

class Analytor;
using AnalytorPtr = std::shared_ptr<Analytor>;
using Analytors = std::vector<AnalytorPtr>;

// In-memory FIFO carrying sealed-partition descriptor batches from the sink
// driver to the source driver. Purely resident by design: it only ever holds
// records the source can drain, so a full queue is a backpressure condition,
// never a deadlock risk — capacity enforcement therefore lives in the
// analytor's ready-backlog limits and this queue only provides ordering,
// cross-thread hand-off and end-of-stream semantics. (The input run, which
// buffers not-yet-consumable open-partition data, is the spillable one.)
class AnalyticDescriptorQueue {
public:
    void push(ChunkPtr batch) {
        std::lock_guard<std::mutex> l(_mutex);
        if (_consumer_closed) {
            return; // early LIMIT: the consumer will never read it
        }
        _batches.emplace_back(std::move(batch));
    }
    // Data available, or drained with the producer closed (pop -> EndOfFile).
    bool can_pop() {
        std::lock_guard<std::mutex> l(_mutex);
        return !_batches.empty() || _producer_closed;
    }
    StatusOr<ChunkPtr> pop() {
        std::lock_guard<std::mutex> l(_mutex);
        if (!_batches.empty()) {
            ChunkPtr batch = std::move(_batches.front());
            _batches.pop_front();
            return batch;
        }
        if (_producer_closed) {
            return Status::EndOfFile("no more descriptors");
        }
        return Status::InternalError("descriptor queue pop without can_pop");
    }
    void close_producer() {
        std::lock_guard<std::mutex> l(_mutex);
        _producer_closed = true;
    }
    void close_consumer() {
        std::lock_guard<std::mutex> l(_mutex);
        _consumer_closed = true;
        _batches.clear();
    }

private:
    std::mutex _mutex;
    std::deque<ChunkPtr> _batches;
    bool _producer_closed = false;
    bool _consumer_closed = false;
};

template <typename T>
class ManagedFunctionStates;

template <typename T>
using ManagedFunctionStatesPtr = std::unique_ptr<ManagedFunctionStates<T>>;

// Component used to do analytic processing
// it contains common data struct and algorithm of analysis
class Analytor final : public pipeline::ContextWithDependency {
    friend class ManagedFunctionStates<Analytor>;

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

    enum class RangeBoundaryType { UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, CURRENT_ROW, PRECEDING, FOLLOWING };

    struct RangeBoundarySpec {
        RangeBoundaryType type = RangeBoundaryType::CURRENT_ROW;
        ExprContext* expr_ctx = nullptr;
        MutableColumnPtr column;
        bool has_offset = false;
    };

public:
    ~Analytor() override;
    Analytor(const TPlanNode& tnode, const TupleDescriptor* result_tuple_desc, bool use_hash_based_partition);

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
    bool is_chunk_buffer_full();
    bool reached_limit() const { return _limit != -1 && _num_rows_returned >= _limit; }

    void attach_sink_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _pip_observable.attach_sink_observer(state, observer);
    }

    void attach_source_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _pip_observable.attach_source_observer(state, observer);
    }

    auto defer_notify_source() { return _pip_observable.defer_notify_source(); }
    auto defer_notify_sink() { return _pip_observable.defer_notify_sink(); }

    ChunkPtr poll_chunk_buffer() {
        auto notify = defer_notify_sink();
        std::lock_guard<std::mutex> l(_buffer_mutex);
        if (_buffer.empty()) {
            return nullptr;
        }
        ChunkPtr chunk = _buffer.front();
        _buffer.pop();
        return chunk;
    }
    void offer_chunk_to_buffer(const ChunkPtr& chunk) {
        auto notify = defer_notify_source();
        std::lock_guard<std::mutex> l(_buffer_mutex);
        _buffer.push(chunk);
    }

    std::string debug_string() const;

    // ==== Analytic spill (incremental partition-stream edition) ====
    // Spill support covers materializing whole-partition frames, i.e.
    // `... OVER (PARTITION BY k)` without a window clause, or with
    // `ROWS|RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
    // Every window value within a partition is one scalar per function, so
    // processing becomes two cooperating sequential streams (both
    // MemLimitedChunkQueue) shared by the sink and source drivers:
    // - InputRun holds the raw input chunks in arrival order, resident below
    //   its memory limit, flushing cold blocks beyond it;
    // - the descriptor queue publishes one record per sealed partition: its
    //   row count plus one result row per window function. It is a dedicated
    //   in-memory queue, never spilled: it only holds sealed records, which
    //   the source can always drain, so a full queue is a backpressure
    //   condition rather than a deadlock risk and the ready-backlog limits
    //   below are its capacity (see AnalyticDescriptorQueue).
    // A descriptor is the read permit for its rows. The source consumes the
    // sealed prefix of InputRun as soon as the covering descriptors arrive,
    // attaches the per-partition constants as result columns, and lets the
    // queues release consumed blocks immediately — it does not wait for the
    // sink to finish. A sealed-but-unconsumed backlog limit feeds back into
    // the sink's need_input(), so retained memory and disk stay bounded by
    // the open partition plus that backlog instead of the whole query.
    //
    // There is no activation event and no processing-mode conversion: the
    // memory limit IS the policy (spill_mode=force -> 0, auto -> threshold),
    // and eligible queries always execute on this single path.
    //
    // Known limitations, by design:
    // - A partition emits nothing before it seals: the first-row latency of
    //   one giant partition is SQL semantics, not a protocol artifact. The
    //   per-session escape hatch is the ANALYTIC bit of
    //   spillable_operator_mask.
    // - The resident threshold derives from spill_mem_table_size instead of
    //   the operator memory-reservation mechanism (follow-up).
    static bool tnode_supports_spill(const TPlanNode& tnode);
    bool spill_supported() const { return _spill_supported; }
    bool partition_streams_enabled() const { return _input_run != nullptr; }
    void set_input_run(std::shared_ptr<pipeline::MemLimitedChunkQueue> input_run) { _input_run = std::move(input_run); }
    const std::shared_ptr<pipeline::MemLimitedChunkQueue>& input_run() const { return _input_run; }
    AnalyticDescriptorQueue& descriptor_queue() { return _descriptor_queue; }
    // Sink side. False while a stream flush is in flight, while the
    // sealed-but-unconsumed backlog exceeds its limit, or once the consumer
    // side closed early (LIMIT).
    bool store_can_push();
    Status store_process_chunk(RuntimeState* state, const ChunkPtr& chunk);
    // Seals the open partition, publishes the last descriptors and closes
    // both producers. Bounded work with no IO barrier.
    Status store_finish_input(RuntimeState* state);
    // Source side. True when a descriptor permit and the covering input rows
    // can both make progress (a block on disk triggers an async load and
    // reads as false until loaded). store_pull_chunk() returns nullptr
    // transiently (flush race, or permit not yet published) and sets
    // store_eos() at end of stream after verifying the replay accounting.
    //
    // allow_share_columns lets a fully authorized chunk go out sharing the
    // input run's data columns instead of copying them. The caller must only
    // pass true when nothing will rewrite the returned chunk in place: the
    // queue hands out the ChunkPtr it stored and an in-flight flush task
    // serializes those columns outside the queue lock, so any filter or resize
    // races that serializer (see the comment at the output-building site).
    bool store_has_output();
    bool store_eos() const { return _store_eos; }
    StatusOr<ChunkPtr> store_pull_chunk(RuntimeState* state, bool allow_share_columns);

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
    // For generic RANGE frames materialized and processed by definition.
    void _materializing_process_for_range_frame(RuntimeState* state);
    // For RANGE frames whose start is UNBOUNDED PRECEDING and whose finite end bound only grows.
    void _materializing_process_for_growing_range_frame(RuntimeState* state);
    // For ROWS frames with finite bounds.
    void _materializing_process_for_sliding_frame(RuntimeState* state);
    ProcessByPartitionFunc _materializing_process_impl = nullptr;

    // Update all window aggregate states from the frame range [frame_start, frame_end) within the current
    // buffered partition [partition_start, partition_end). Positions are local to the analytor's buffered columns.
    void _update_window_batch(int64_t partition_start, int64_t partition_end, int64_t frame_start, int64_t frame_end);
    void _update_window_batch_removable_cumulatively();

    Status _output_result_chunk(ChunkPtr* chunk);

    void _reset_state_for_next_partition();
    void _reset_window_state();
    void _init_window_result_columns();

    void _find_partition_end();
    void _find_peer_group_end();
    int64_t _find_first_not_equal_for_hash_based_partition(int64_t target, int64_t start, int64_t end);
    void _find_candidate_partition_ends();
    void _find_candidate_peer_group_ends();
    void _compute_range_nonnull_segment();
    FrameRange _get_frame_for_range();
    bool _is_growing_range_frame() const;
    int64_t _resolve_range_offset_boundary(const RangeBoundarySpec& boundary, bool is_start, bool current_row_is_null);
    int64_t _seek_range_frame_boundary_with_offset(const RangeBoundarySpec& boundary, bool is_start);
    void _reset_range_frame_cursors();

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
    FrameRange _get_frame_for_rows() const {
        DCHECK(!_is_range_window);
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

    // ==== Analytic spill — see the comment on tnode_supports_spill ====
    // Sink-side fields below are owned by the sink driver, source-side fields
    // by the source driver; neither touches the other's. Everything crossing
    // the two pipelines flows through the queues (internally synchronized) or
    // through the atomic counters.
    struct AnalyticSpillContext {
        // -- Sink side: open-partition tracking. The open partition's key is
        // kept as one-row column copies so it survives chunk boundaries.
        bool has_open_partition = false;
        int64_t open_partition_rows = 0;
        MutableColumns last_partition_key;
        // Sealed-but-unpublished partitions: column 0 holds the row counts,
        // column 1 + i the one-row results of function i. Flushed into the
        // descriptor stream before store_process_chunk returns, so a sealed
        // record is never privately held across scheduling periods (the
        // source could otherwise starve with no way to trigger a flush).
        MutableColumns descriptor_builder;
        size_t builder_records = 0;
        int64_t builder_rows = 0;

        // -- Source side: replay cursor over the two streams.
        ChunkPtr descriptor_batch; // current descriptor chunk
        size_t descriptor_idx = 0; // next record within the batch
        int64_t out_rows_remaining = 0;
        bool descriptor_eos = false;
        bool input_eos = false;
        ChunkPtr pending_input; // popped but not fully covered by permits
        size_t pending_offset = 0;
        int64_t out_rows_total = 0;
    };

    static constexpr int32_t kOutputConsumerIndex = 0;
    // Sealed-but-unconsumed backlog limits: bound retained memory/disk when
    // the source lags; the open partition itself is never throttled. The
    // rows/partitions limits bound the sealed input payload and the record
    // count; the bytes limit bounds the descriptor stream's resident memory
    // (variable-size results make a pure count limit insufficient).
    static constexpr int64_t kStoreReadyRowsLimit = 1LL << 20;
    static constexpr int64_t kStoreReadyPartitionsLimit = 1LL << 16;
    static constexpr int64_t kStoreReadyDescriptorBytesLimit = 16LL << 20;

    Status _store_update_states(const std::vector<Columns>& fn_cols, int64_t frame_start, int64_t frame_end);
    Status _store_seal_open_partition();
    Status _store_flush_descriptors();
    void _store_open_partition(const Columns& part_cols, size_t row);
    bool _store_row_equals_last_key(const Columns& part_cols, size_t row) const;

    bool _spill_supported = false;
    bool _is_whole_partition_frame = false;
    std::shared_ptr<pipeline::MemLimitedChunkQueue> _input_run;
    AnalyticDescriptorQueue _descriptor_queue;
    bool _store_eos = false;
    AnalyticSpillContext _spill_ctx;
    // Cross-pipeline accounting. The sink publishes, the source consumes;
    // backpressure reads are relaxed (approximate is fine), the source's EOF
    // validation reads happen after both queues reported end-of-stream, which
    // orders them behind the sink's final updates.
    std::atomic<int64_t> _store_rows_pushed{0};
    std::atomic<int64_t> _store_rows_published{0};
    std::atomic<int64_t> _store_partitions_published{0};
    std::atomic<int64_t> _store_rows_consumed{0};
    std::atomic<int64_t> _store_partitions_consumed{0};
    std::atomic<int64_t> _store_descriptor_bytes_published{0};
    std::atomic<int64_t> _store_descriptor_bytes_consumed{0};

    RuntimeState* _state = nullptr;
    bool _is_closed = false;
    // TPlanNode is only valid in the PREPARE and INIT phase
    const TPlanNode& _tnode;
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
    bool _is_range_window = false;
    bool _is_range_offset_window = false;
    bool _range_order_is_asc = true;
    TypeDescriptor _range_order_type;
    RangeBoundarySpec _range_start_boundary;
    RangeBoundarySpec _range_end_boundary;
    bool _range_nonnull_segment_valid = false;
    int64_t _range_nonnull_start = 0;
    int64_t _range_nonnull_end = 0;
    int64_t _range_start_frame_cursor = 0;
    int64_t _range_end_frame_cursor = 0;
    int64_t _range_cumulative_frame_end = 0;

    // The offset of the n-th window function in a row of window functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the window function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all window aggregate state
    size_t _max_agg_state_align_size = 1;
    std::vector<bool> _is_lead_lag_functions;
    std::vector<FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    std::vector<ManagedFunctionStatesPtr<Analytor>> _managed_fn_states;
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<Columns> _agg_intput_columns;
    std::vector<FunctionTypes> _agg_fn_types;

    std::vector<ExprContext*> _partition_ctxs;
    MutableColumns _partition_columns;

    std::vector<ExprContext*> _order_ctxs;
    MutableColumns _order_columns;

    bool _has_udaf = false;
    // There are many reasons requiring the materializing processing.
    // 1. Unbounded window clause, like `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    // 2. Some certian window functions, eg. `NTILE`, need the boundary of partition to calculate its value.
    // Any of these conditions is satisfied, the materializing processing is required.
    bool _need_partition_materializing = false;
    bool _use_removable_cumulative_process = false;
    // When calculating window functions such as CUME_DIST and PERCENT_RANK,
    // it's necessary to specify the size of the partition.
    bool _should_set_partition_size = false;
    std::vector<int64_t> _partition_size_required_function_index;

    RuntimeProfile* _runtime_profile;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffered_rows = nullptr;
    RuntimeProfile::Counter* _remove_unused_rows_cnt = nullptr;
    RuntimeProfile::Counter* _remove_unused_total_rows = nullptr;
    RuntimeProfile::Counter* _column_resize_timer = nullptr;
    RuntimeProfile::Counter* _partition_search_timer = nullptr;
    RuntimeProfile::Counter* _peer_group_search_timer = nullptr;
    RuntimeProfile::Counter* _udaf_load_timer = nullptr;
    RuntimeProfile::Counter* _udaf_cache_hit_count = nullptr;
    RuntimeProfile::Counter* _udaf_cache_populate_count = nullptr;

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
    MutableColumns _result_window_columns;

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
    std::unique_ptr<Allocator> _allocator = std::make_unique<MemHookAllocator>();

    bool _is_merge_funcs;

    pipeline::PipeObservable _pip_observable;
};

// Helper class that properly invokes destructor when state goes out of scope.
template <typename T>
class ManagedFunctionStates {
public:
    ManagedFunctionStates(std::vector<FunctionContext*>* ctxs, AggDataPtr __restrict agg_states, T* context)
            : _ctxs(ctxs), _agg_states(agg_states), _context(context) {
        for (int i = 0; i < _context->_agg_functions.size(); i++) {
            _context->_agg_functions[i]->create((*_ctxs)[i], _agg_states + _context->_agg_states_offsets[i]);
        }
    }

    ~ManagedFunctionStates() {
        for (int i = 0; i < _context->_agg_functions.size(); i++) {
            _context->_agg_functions[i]->destroy((*_ctxs)[i], _agg_states + _context->_agg_states_offsets[i]);
        }
    }

    uint8_t* mutable_data() { return _agg_states; }
    const uint8_t* data() const { return _agg_states; }

private:
    std::vector<FunctionContext*>* _ctxs;
    AggDataPtr _agg_states;
    T* _context;
};

class AnalytorFactory;
using AnalytorFactoryPtr = std::shared_ptr<AnalytorFactory>;
class AnalytorFactory {
public:
    AnalytorFactory(size_t dop, const TPlanNode& tnode, const TupleDescriptor* result_tuple_desc,
                    const bool use_hash_based_partition)
            : _analytors(dop),
              _tnode(tnode),
              _result_tuple_desc(result_tuple_desc),
              _use_hash_based_partition(use_hash_based_partition) {}
    AnalytorPtr create(int i);

private:
    Analytors _analytors;
    const TPlanNode& _tnode;
    const TupleDescriptor* _result_tuple_desc;
    const bool _use_hash_based_partition;
};
} // namespace starrocks
