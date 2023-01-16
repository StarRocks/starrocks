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

#include <any>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gen_cpp/QueryPlanExtra_constants.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks {

struct HashTableKeyAllocator;

struct RawHashTableIterator {
    RawHashTableIterator(HashTableKeyAllocator* alloc_, size_t x_, int y_) : alloc(alloc_), x(x_), y(y_) {}
    bool operator==(const RawHashTableIterator& other) { return x == other.x && y == other.y; }
    bool operator!=(const RawHashTableIterator& other) { return !this->operator==(other); }
    inline void next();
    // return alloc[x]->states[y]
    inline uint8_t* value();
    HashTableKeyAllocator* alloc;
    size_t x;
    int y;
};

struct HashTableKeyAllocator {
    // number of states allocated consecutively in a single alloc
    static auto constexpr alloc_batch_size = 1024;
    // memory aligned when allocate
    static size_t constexpr aligned = 16;

    int aggregate_key_size = 0;
    std::vector<std::pair<void*, int>> vecs;
    MemPool* pool = nullptr;

    RawHashTableIterator begin() { return {this, 0, 0}; }

    RawHashTableIterator end() { return {this, vecs.size(), 0}; }

    AggDataPtr allocate() {
        if (vecs.empty() || vecs.back().second == alloc_batch_size) {
            uint8_t* mem = pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned);
            vecs.emplace_back(mem, 0);
        }
        return static_cast<AggDataPtr>(vecs.back().first) + aggregate_key_size * vecs.back().second++;
    }

    uint8_t* allocate_null_key_data() { return pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned); }

    void reset() { vecs.clear(); }
};

inline void RawHashTableIterator::next() {
    y++;
    if (y == alloc->vecs[x].second) {
        y = 0;
        x++;
    }
}

inline uint8_t* RawHashTableIterator::value() {
    return static_cast<uint8_t*>(alloc->vecs[x].first) + alloc->aggregate_key_size * y;
}

class Aggregator;
class SortedStreamingAggregator;

template <class HashMapWithKey>
struct AllocateState {
    AllocateState(Aggregator* aggregator_) : aggregator(aggregator_) {}
    inline AggDataPtr operator()(const typename HashMapWithKey::KeyType& key);
    inline AggDataPtr operator()(std::nullptr_t);

private:
    Aggregator* aggregator;
};

struct AggFunctionTypes {
    TypeDescriptor result_type;
    TypeDescriptor serde_type; // for serialize
    std::vector<FunctionContext::TypeDesc> arg_typedescs;
    bool has_nullable_child;
    bool is_nullable; // agg function result whether is nullable
};

struct ColumnType {
    TypeDescriptor result_type;
    bool is_nullable;
};

enum AggrPhase { AggrPhase1, AggrPhase2 };
enum AggrMode {
    AM_DEFAULT, // normal mode(cache feature turn off)
    // A blocking operator is split into a pair {blocking operator(before cache), blocking operator(after cache)]
    // process non-passthrough chunks: (pre-cache: input-->intermediate) => (post-cache: intermediate->output)
    // process passthrough chunks: (pre-cache: input-->input) => (post-cache: input--> output)
    AM_BLOCKING_PRE_CACHE,
    AM_BLOCKING_POST_CACHE,
    // A streaming operator is split into a pair {streaming operator(before cache), streaming operator(after cache)]
    // process non-passthrough chunks: (pre-cache: input-->intermediate) => (post-cache: intermediate->intermediate)
    // process passthrough chunks: (pre-cache: input-->input) = > (post-cache: input-->intermediate)
    AM_STREAMING_PRE_CACHE,
    AM_STREAMING_POST_CACHE
};

enum AggrAutoState { INIT_PREAGG = 0, ADJUST, PASS_THROUGH, FORCE_PREAGG, PREAGG, SELECTIVE_PREAGG };

struct AggrAutoContext {
    static constexpr size_t ContinuousUpperLimit = 10000;
    static constexpr int ForcePreaggLimit = 3;
    static constexpr int PreaggLimit = 100;
    static constexpr int AdjustLimit = 100;
    static constexpr double LowReduction = 0.2;
    static constexpr double HighReduction = 0.9;
    static constexpr size_t MaxHtSize = 64 * 1024 * 1024; // 64 MB
    static constexpr int StableLimit = 5;
    std::string get_auto_state_string(const AggrAutoState& state);
    size_t get_continuous_limit();
    void update_continuous_limit();
    bool is_high_reduction(const size_t agg_count, const size_t chunk_size);
    bool is_low_reduction(const size_t agg_count, const size_t chunk_size);
    size_t init_preagg_count = 0;
    size_t adjust_count = 0;
    size_t pass_through_count = 0;
    size_t force_preagg_count = 0;
    size_t preagg_count = 0;
    size_t selective_preagg_count = 0;
    size_t continuous_limit = 100;
};

struct StreamingHtMinReductionEntry {
    int min_ht_mem;
    double streaming_ht_min_reduction;
};

static const StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
        {0, 0.0},
        {256 * 1024, 1.1},
        {2 * 1024 * 1024, 2.0},
};

static const int STREAMING_HT_MIN_REDUCTION_SIZE =
        sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

using AggregatorPtr = std::shared_ptr<Aggregator>;

struct AggregatorParams {
    bool needs_finalize;
    bool has_outer_join_child;
    int64_t limit;
    TStreamingPreaggregationMode::type streaming_preaggregation_mode;
    TupleId intermediate_tuple_id;
    TupleId output_tuple_id;
    std::string sql_grouping_keys;
    std::string sql_aggregate_functions;
    std::vector<TExpr> conjuncts;
    std::vector<TExpr> grouping_exprs;
    std::vector<TExpr> aggregate_functions;
    std::vector<TExpr> intermediate_aggr_exprs;

    // Incremental MV
    // Whether it's testing, use MemStateTable in testing, instead use IMTStateTable.
    bool is_testing;
    // Whether input is only append-only or with retract messages.
    bool is_append_only;
    // Whether output is generated with retract or without retract messages.
    bool is_generate_retract;
    // The agg index of count agg function.
    int32_t count_agg_idx;
};
using AggregatorParamsPtr = std::shared_ptr<AggregatorParams>;
AggregatorParamsPtr convert_to_aggregator_params(const TPlanNode& tnode);

// it contains common data struct and algorithm of aggregation
class Aggregator : public pipeline::ContextWithDependency {
public:
    Aggregator(AggregatorParamsPtr&& params);

    virtual ~Aggregator() noexcept override {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state);
    virtual Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile,
                           MemTracker* mem_tracker);
    void close(RuntimeState* state) override;

    const MemPool* mem_pool() const { return _mem_pool.get(); }
    bool is_none_group_by_exprs() { return _group_by_expr_ctxs.empty(); }
    const std::vector<ExprContext*>& conjunct_ctxs() { return _conjunct_ctxs; }
    const std::vector<ExprContext*>& group_by_expr_ctxs() { return _group_by_expr_ctxs; }
    const std::vector<FunctionContext*>& agg_fn_ctxs() { return _agg_fn_ctxs; }
    const std::vector<std::vector<ExprContext*>>& agg_expr_ctxs() { return _agg_expr_ctxs; }
    int64_t limit() { return _limit; }
    bool needs_finalize() { return _needs_finalize; }
    bool is_ht_eos() { return _is_ht_eos; }
    void set_ht_eos() { _is_ht_eos = true; }
    bool is_sink_complete() { return _is_sink_complete.load(std::memory_order_acquire); }
    int64_t num_input_rows() { return _num_input_rows; }
    int64_t num_rows_returned() { return _num_rows_returned; }
    void update_num_rows_returned(int64_t increment) { _num_rows_returned += increment; };
    void update_num_input_rows(int64_t increment) { _num_input_rows += increment; }
    int64_t num_pass_through_rows() { return _num_pass_through_rows; }
    void set_aggr_phase(AggrPhase aggr_phase) { _aggr_phase = aggr_phase; }
    AggrPhase get_aggr_phase() { return _aggr_phase; }

    TStreamingPreaggregationMode::type streaming_preaggregation_mode() { return _streaming_preaggregation_mode; }
    const AggHashMapVariant& hash_map_variant() { return _hash_map_variant; }
    const AggHashSetVariant& hash_set_variant() { return _hash_set_variant; }
    std::any& it_hash() { return _it_hash; }
    const std::vector<uint8_t>& streaming_selection() { return _streaming_selection; }
    RuntimeProfile::Counter* agg_compute_timer() { return _agg_stat->agg_compute_timer; }
    RuntimeProfile::Counter* agg_expr_timer() { return _agg_stat->agg_function_compute_timer; }
    RuntimeProfile::Counter* streaming_timer() { return _agg_stat->streaming_timer; }
    RuntimeProfile::Counter* input_row_count() { return _agg_stat->input_row_count; }
    RuntimeProfile::Counter* rows_returned_counter() { return _agg_stat->rows_returned_counter; }
    RuntimeProfile::Counter* hash_table_size() { return _agg_stat->hash_table_size; }
    RuntimeProfile::Counter* pass_through_row_count() { return _agg_stat->pass_through_row_count; }

    void sink_complete() { _is_sink_complete.store(true, std::memory_order_release); }

    bool is_chunk_buffer_empty();
    ChunkPtr poll_chunk_buffer();
    void offer_chunk_to_buffer(const ChunkPtr& chunk);

    bool should_expand_preagg_hash_tables(size_t prev_row_returned, size_t input_chunk_size, int64_t ht_mem,
                                          int64_t ht_rows) const;

    // For aggregate without group by
    [[nodiscard]] Status compute_single_agg_state(Chunk* chunk, size_t chunk_size);
    // For aggregate with group by
    [[nodiscard]] Status compute_batch_agg_states(Chunk* chunk, size_t chunk_size);
    [[nodiscard]] Status compute_batch_agg_states_with_selection(Chunk* chunk, size_t chunk_size);

    // Convert one row agg states to chunk
    Status convert_to_chunk_no_groupby(ChunkPtr* chunk);

    void process_limit(ChunkPtr* chunk);

    Status evaluate_groupby_exprs(Chunk* chunk);
    Status evaluate_agg_fn_exprs(Chunk* chunk);
    Status evaluate_agg_input_column(Chunk* chunk, std::vector<ExprContext*>& agg_expr_ctxs, int i);

    [[nodiscard]] Status output_chunk_by_streaming(Chunk* input_chunk, ChunkPtr* chunk);

    // Elements queried in HashTable will be added to HashTable,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    // selection[i] = 0: found in hash table
    // selection[1] = 1: not found in hash table
    [[nodiscard]] Status output_chunk_by_streaming_with_selection(Chunk* input_chunk, ChunkPtr* chunk);

    // At first, we use single hash map, if hash map is too big,
    // we convert the single hash map to two level hash map.
    // two level hash map is better in large data set.
    void try_convert_to_two_level_map();
    void try_convert_to_two_level_set();

    Status check_has_error();

    void set_aggr_mode(AggrMode aggr_mode) { _aggr_mode = aggr_mode; }
    // reset_state is used to clear the internal state of the Aggregator, then it can process new tablet, in
    // multi-version cache, we should refill the chunks (i.e.partial-hit result) from the stale cache back to
    // the pre-cache agg, after that, the incremental rowsets are read out and merged with these partial state
    // to produce the final result that will be populated into the cache.
    // refill_chunk: partial-hit result of stale version.
    // refill_op: pre-cache agg operator, Aggregator's holder.
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks, pipeline::Operator* refill_op);

#ifdef NDEBUG
    static constexpr size_t two_level_memory_threshold = 33554432; // 32M, L3 Cache
    static constexpr size_t streaming_hash_table_size_threshold = 10000000;
#else
    static constexpr size_t two_level_memory_threshold = 64;
    static constexpr size_t streaming_hash_table_size_threshold = 4;
#endif
    HashTableKeyAllocator _state_allocator;

protected:
    AggregatorParamsPtr _params;

    bool _is_closed = false;
    RuntimeState* _state = nullptr;

    MemTracker* _mem_tracker = nullptr;

    ObjectPool* _pool;
    std::unique_ptr<MemPool> _mem_pool;
    // The open phase still relies on the TFunction object for some initialization operations
    std::vector<TFunction> _fns;

    RuntimeProfile* _runtime_profile;

    int64_t _limit = -1;
    int64_t _num_rows_returned = 0;
    int64_t _num_rows_processed = 0;

    // only used in pipeline engine
    std::atomic<bool> _is_sink_complete = false;
    // only used in pipeline engine
    std::queue<ChunkPtr> _buffer;
    std::mutex _buffer_mutex;

    // Certain aggregates require a finalize step, which is the final step of the
    // aggregate after consuming all input rows. The finalize step converts the aggregate
    // value into its final form. This is true if this node contains aggregate that requires
    // a finalize step.
    bool _needs_finalize;
    // Indicate whether data of the hash table has been taken out or reach limit
    bool _is_ht_eos = false;
    bool _is_only_group_by_columns = false;
    // At least one group by column is nullable
    bool _has_nullable_key = false;
    int64_t _num_input_rows = 0;
    int64_t _num_pass_through_rows = 0;

    TStreamingPreaggregationMode::type _streaming_preaggregation_mode;

    // The key is all group by column, the value is all agg function column
    AggHashMapVariant _hash_map_variant;
    AggHashSetVariant _hash_set_variant;
    std::any _it_hash;

    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the aggregate function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all aggregate state
    size_t _max_agg_state_align_size = 1;
    // The followings are aggregate function information:
    std::vector<FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    // agg state when no group by columns
    AggDataPtr _single_agg_state = nullptr;
    // The expr used to evaluate agg input columns
    // one agg function could have multi input exprs
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<std::vector<ColumnPtr>> _agg_input_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const Column*>> _agg_input_raw_columns;
    // The expr used to evaluate agg intermediate columns.
    std::vector<std::vector<ExprContext*>> _intermediate_agg_expr_ctxs;

    // Indicates we should use update or merge method to process aggregate column data
    std::vector<bool> _is_merge_funcs;
    // In order batch update agg states
    Buffer<AggDataPtr> _tmp_agg_states;
    std::vector<AggFunctionTypes> _agg_fn_types;

    // Exprs used to evaluate conjunct
    std::vector<ExprContext*> _conjunct_ctxs;

    // Exprs used to evaluate group by column
    std::vector<ExprContext*> _group_by_expr_ctxs;
    Columns _group_by_columns;
    std::vector<ColumnType> _group_by_types;

    // Tuple into which Update()/Merge()/Serialize() results are stored.
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    // Tuple into which Finalize() results are stored. Possibly the same as
    // the intermediate tuple.
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;

    // used for blocking aggregate
    AggrPhase _aggr_phase = AggrPhase1;
    AggrMode _aggr_mode = AM_DEFAULT;
    bool _is_passthrough = false;
    bool _is_pending_reset_state = false;
    std::vector<uint8_t> _streaming_selection;

    bool _has_udaf = false;

    AggStatistics* _agg_stat;

public:
    void build_hash_map(size_t chunk_size, bool agg_group_by_with_limit = false);
    void build_hash_map_with_selection(size_t chunk_size);
    void build_hash_map_with_selection_and_allocation(size_t chunk_size, bool agg_group_by_with_limit = false);
    Status convert_hash_map_to_chunk(int32_t chunk_size, ChunkPtr* chunk);

    void build_hash_set(size_t chunk_size);
    void build_hash_set_with_selection(size_t chunk_size);
    void convert_hash_set_to_chunk(int32_t chunk_size, ChunkPtr* chunk);

protected:
    bool _reached_limit() { return _limit != -1 && _num_rows_returned >= _limit; }

    bool _use_intermediate_as_input() {
        if (is_pending_reset_state()) {
            DCHECK(_aggr_mode == AM_BLOCKING_PRE_CACHE || _aggr_mode == AM_STREAMING_PRE_CACHE);
            return true;
        } else {
            return ((_aggr_mode == AM_BLOCKING_POST_CACHE) || (_aggr_mode == AM_STREAMING_POST_CACHE)) &&
                   !_is_passthrough;
        }
    }

    bool _use_intermediate_as_output() {
        return _aggr_mode == AM_STREAMING_PRE_CACHE || _aggr_mode == AM_BLOCKING_PRE_CACHE || !_needs_finalize;
    }

    Status _reset_state(RuntimeState* state);

    // initial const columns for i'th FunctionContext.
    Status _evaluate_const_columns(int i);

    // Create new aggregate function result column by type
    Columns _create_agg_result_columns(size_t num_rows, bool use_intermediate);
    Columns _create_group_by_columns(size_t num_rows);

    void _serialize_to_chunk(ConstAggDataPtr __restrict state, const Columns& agg_result_columns);
    void _finalize_to_chunk(ConstAggDataPtr __restrict state, const Columns& agg_result_columns);
    void _destroy_state(AggDataPtr __restrict state);

    ChunkPtr _build_output_chunk(const Columns& group_by_columns, const Columns& agg_result_columns,
                                 bool use_intermediate);

    void _set_passthrough(bool flag) { _is_passthrough = flag; }
    bool is_passthrough() const { return _is_passthrough; }

    void begin_pending_reset_state() { _is_pending_reset_state = true; }
    void end_pending_reset_state() { _is_pending_reset_state = false; }
    bool is_pending_reset_state() { return _is_pending_reset_state; }

    void _reset_groupby_exprs();
    Status _evaluate_group_by_exprs(Chunk* chunk);

    // Choose different agg hash map/set by different group by column's count, type, nullable
    template <typename HashVariantType>
    void _init_agg_hash_variant(HashVariantType& hash_variant);

    void _release_agg_memory();

    template <class HashMapWithKey>
    friend struct AllocateState;
};

template <class HashMapWithKey>
inline AggDataPtr AllocateState<HashMapWithKey>::operator()(const typename HashMapWithKey::KeyType& key) {
    AggDataPtr agg_state = aggregator->_state_allocator.allocate();
    *reinterpret_cast<typename HashMapWithKey::KeyType*>(agg_state) = key;
    for (int i = 0; i < aggregator->_agg_fn_ctxs.size(); i++) {
        aggregator->_agg_functions[i]->create(aggregator->_agg_fn_ctxs[i],
                                              agg_state + aggregator->_agg_states_offsets[i]);
    }
    return agg_state;
}

template <class HashMapWithKey>
inline AggDataPtr AllocateState<HashMapWithKey>::operator()(std::nullptr_t) {
    AggDataPtr agg_state = aggregator->_state_allocator.allocate_null_key_data();
    for (int i = 0; i < aggregator->_agg_fn_ctxs.size(); i++) {
        aggregator->_agg_functions[i]->create(aggregator->_agg_fn_ctxs[i],
                                              agg_state + aggregator->_agg_states_offsets[i]);
    }
    return agg_state;
}

template <class T>
class AggregatorFactoryBase {
public:
    using Ptr = std::shared_ptr<T>;
    AggregatorFactoryBase(const TPlanNode& tnode) : _tnode(tnode) {}

    Ptr get_or_create(size_t id) {
        auto it = _aggregators.find(id);
        if (it != _aggregators.end()) {
            return it->second;
        }
        auto aggregator = std::make_shared<T>(convert_to_aggregator_params(_tnode));
        aggregator->set_aggr_mode(_aggr_mode);
        _aggregators[id] = aggregator;
        return aggregator;
    }

    void set_aggr_mode(AggrMode aggr_mode) { _aggr_mode = aggr_mode; }

private:
    const TPlanNode& _tnode;
    std::unordered_map<size_t, Ptr> _aggregators;
    AggrMode _aggr_mode = AggrMode::AM_DEFAULT;
};

using AggregatorFactory = AggregatorFactoryBase<Aggregator>;
using AggregatorFactoryPtr = std::shared_ptr<AggregatorFactory>;

using StreamingAggregatorFactory = AggregatorFactoryBase<SortedStreamingAggregator>;
using StreamingAggregatorFactoryPtr = std::shared_ptr<StreamingAggregatorFactory>;

} // namespace starrocks
