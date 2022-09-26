// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <algorithm>
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
#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/aggregate/agg_hash_variant.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gen_cpp/QueryPlanExtra_constants.h"
#include "gutil/strings/substitute.h"
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

    vectorized::AggDataPtr allocate() {
        if (vecs.empty() || vecs.back().second == alloc_batch_size) {
            uint8_t* mem = pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned);
            vecs.emplace_back(mem, 0);
        }
        return static_cast<vectorized::AggDataPtr>(vecs.back().first) + aggregate_key_size * vecs.back().second++;
    }

    uint8_t* allocate_null_key_data() { return pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned); }
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

template <class HashMapWithKey>
struct AllocateState {
    AllocateState(Aggregator* aggregator_) : aggregator(aggregator_) {}
    inline vectorized::AggDataPtr operator()(const typename HashMapWithKey::KeyType& key);
    inline vectorized::AggDataPtr operator()(std::nullptr_t);

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

struct GroupByColumnTypes {
    TypeDescriptor result_type;
    bool is_nullable;
};

enum AggrPhase { AggrPhase1, AggrPhase2 };

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
};

static std::shared_ptr<AggregatorParams> convert_to_aggregator_params(const TPlanNode& tnode) {
    auto params = std::make_shared<AggregatorParams>();
    params->conjuncts = tnode.conjuncts;
    params->limit = tnode.limit;

    switch (tnode.node_type) {
    case TPlanNodeType::AGGREGATION_NODE: {
        params->needs_finalize = tnode.agg_node.need_finalize;
        params->streaming_preaggregation_mode = tnode.agg_node.streaming_preaggregation_mode;
        params->intermediate_tuple_id = tnode.agg_node.intermediate_tuple_id;
        params->output_tuple_id = tnode.agg_node.output_tuple_id;
        params->sql_grouping_keys = tnode.agg_node.__isset.sql_grouping_keys ? tnode.agg_node.sql_grouping_keys : "";
        params->sql_aggregate_functions =
                tnode.agg_node.__isset.sql_aggregate_functions ? tnode.agg_node.sql_grouping_keys : "";
        params->has_outer_join_child =
                tnode.agg_node.__isset.has_outer_join_child && tnode.agg_node.has_outer_join_child;
        params->grouping_exprs = tnode.agg_node.grouping_exprs;
        params->aggregate_functions = tnode.agg_node.aggregate_functions;
        break;
    }
    case TPlanNodeType::STREAM_AGG_NODE: {
        params->intermediate_tuple_id = tnode.stream_agg_node.intermediate_tuple_id;
        params->output_tuple_id = tnode.stream_agg_node.output_tuple_id;
        params->sql_grouping_keys =
                tnode.stream_agg_node.__isset.sql_grouping_keys ? tnode.stream_agg_node.sql_grouping_keys : "";
        params->sql_aggregate_functions = tnode.stream_agg_node.__isset.sql_aggregate_functions
                                          ? tnode.stream_agg_node.sql_aggregate_functions
                                          : "";
        params->has_outer_join_child =
                tnode.stream_agg_node.__isset.has_outer_join_child && tnode.stream_agg_node.has_outer_join_child;
        params->grouping_exprs = tnode.stream_agg_node.grouping_exprs;
        params->aggregate_functions = tnode.stream_agg_node.aggregate_functions;
        break;
    }
    default:
        VLOG(1) << "it's impossible type:" << tnode.node_type;
        __builtin_unreachable();
    }
    return params;
}

// Component used to process aggregation including bloking aggregate and streaming aggregate
// it contains common data struct and algorithm of aggregation
class Aggregator final : public pipeline::ContextWithDependency {
public:
//    Aggregator(const TPlanNode& tnode);
    Aggregator(std::shared_ptr<AggregatorParams>&& params);

    ~Aggregator() {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state);
    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile, MemTracker* mem_tracker);

    void close(RuntimeState* state) override;

    const MemPool* mem_pool() const { return _mem_pool.get(); }
    bool is_none_group_by_exprs() { return _group_by_expr_ctxs.empty(); }
    const std::vector<ExprContext*>& conjunct_ctxs() { return _conjunct_ctxs; }
    const std::vector<ExprContext*>& group_by_expr_ctxs() { return _group_by_expr_ctxs; }
    const std::vector<starrocks_udf::FunctionContext*>& agg_fn_ctxs() { return _agg_fn_ctxs; }
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
    const vectorized::AggHashMapVariant& hash_map_variant() { return _hash_map_variant; }
    const vectorized::AggHashSetVariant& hash_set_variant() { return _hash_set_variant; }
    std::any& it_hash() { return _it_hash; }
    const std::vector<uint8_t>& streaming_selection() { return _streaming_selection; }
    RuntimeProfile::Counter* get_results_timer() { return _get_results_timer; }
    RuntimeProfile::Counter* agg_compute_timer() { return _agg_compute_timer; }
    RuntimeProfile::Counter* streaming_timer() { return _streaming_timer; }
    RuntimeProfile::Counter* input_row_count() { return _input_row_count; }
    RuntimeProfile::Counter* rows_returned_counter() { return _rows_returned_counter; }
    RuntimeProfile::Counter* hash_table_size() { return _hash_table_size; }
    RuntimeProfile::Counter* pass_through_row_count() { return _pass_through_row_count; }

    void sink_complete() { _is_sink_complete.store(true, std::memory_order_release); }

    bool is_chunk_buffer_empty();
    vectorized::ChunkPtr poll_chunk_buffer();
    void offer_chunk_to_buffer(const vectorized::ChunkPtr& chunk);

    bool should_expand_preagg_hash_tables(size_t prev_row_returned, size_t input_chunk_size, int64_t ht_mem,
                                          int64_t ht_rows) const;

    // For aggregate without group by
    void compute_single_agg_state(size_t chunk_size);
    // For aggregate with group by
    void compute_batch_agg_states(size_t chunk_size);
    void compute_batch_agg_states_with_selection(size_t chunk_size);

    // Convert one row agg states to chunk
    void convert_to_chunk_no_groupby(vectorized::ChunkPtr* chunk);

    void process_limit(vectorized::ChunkPtr* chunk);

    Status evaluate_exprs(vectorized::Chunk* chunk);

    void output_chunk_by_streaming(vectorized::ChunkPtr* chunk);

    // Elements queried in HashTable will be added to HashTable,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    // selection[i] = 0: found in hash table
    // selection[1] = 1: not found in hash table
    void output_chunk_by_streaming_with_selection(vectorized::ChunkPtr* chunk);

    // At first, we use single hash map, if hash map is too big,
    // we convert the single hash map to two level hash map.
    // two level hash map is better in large data set.
    void try_convert_to_two_level_map();
    void try_convert_to_two_level_set();

    Status check_has_error();

#ifdef NDEBUG
    static constexpr size_t two_level_memory_threshold = 33554432; // 32M, L3 Cache
    static constexpr size_t streaming_hash_table_size_threshold = 10000000;
#else
    static constexpr size_t two_level_memory_threshold = 64;
    static constexpr size_t streaming_hash_table_size_threshold = 4;
#endif
    HashTableKeyAllocator _state_allocator;

private:
    std::shared_ptr<AggregatorParams> _params;

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

    // only used in pipeline engine
    std::atomic<bool> _is_sink_complete = false;
    // only used in pipeline engine
    std::queue<vectorized::ChunkPtr> _buffer;
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
    vectorized::AggHashMapVariant _hash_map_variant;
    vectorized::AggHashSetVariant _hash_set_variant;
    std::any _it_hash;

    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the aggregate function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all aggregate state
    size_t _max_agg_state_align_size = 1;
    // The followings are aggregate function information:
    std::vector<starrocks_udf::FunctionContext*> _agg_fn_ctxs;
    std::vector<const vectorized::AggregateFunction*> _agg_functions;
    // agg state when no group by columns
    vectorized::AggDataPtr _single_agg_state = nullptr;
    // The expr used to evaluate agg input columns
    // one agg function could have multi input exprs
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<std::vector<vectorized::ColumnPtr>> _agg_intput_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const vectorized::Column*>> _agg_input_raw_columns;
    // Indicates we should use update or merge method to process aggregate column data
    std::vector<bool> _is_merge_funcs;
    // In order batch update agg states
    vectorized::Buffer<vectorized::AggDataPtr> _tmp_agg_states;
    std::vector<AggFunctionTypes> _agg_fn_types;

    // Exprs used to evaluate conjunct
    std::vector<ExprContext*> _conjunct_ctxs;

    // Exprs used to evaluate group by column
    std::vector<ExprContext*> _group_by_expr_ctxs;
    vectorized::Columns _group_by_columns;
    std::vector<GroupByColumnTypes> _group_by_types;

    // Tuple into which Update()/Merge()/Serialize() results are stored.
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    // Tuple into which Finalize() results are stored. Possibly the same as
    // the intermediate tuple.
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;

    // used for blocking aggregate
    AggrPhase _aggr_phase = AggrPhase1;

    std::vector<uint8_t> _streaming_selection;

    bool _has_udaf = false;

    RuntimeProfile::Counter* _get_results_timer{};
    RuntimeProfile::Counter* _agg_compute_timer{};
    RuntimeProfile::Counter* _streaming_timer{};
    RuntimeProfile::Counter* _input_row_count{};
    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _hash_table_size{};
    RuntimeProfile::Counter* _iter_timer{};
    RuntimeProfile::Counter* _agg_append_timer{};
    RuntimeProfile::Counter* _group_by_append_timer{};
    RuntimeProfile::Counter* _pass_through_row_count{};
    RuntimeProfile::Counter* _expr_compute_timer{};
    RuntimeProfile::Counter* _expr_release_timer{};

public:
    template <typename HashMapWithKey>
    void build_hash_map(HashMapWithKey& hash_map_with_key, size_t chunk_size, bool agg_group_by_with_limit = false) {
        if (agg_group_by_with_limit) {
            if (hash_map_with_key.hash_map.size() >= _limit) {
                build_hash_map_with_selection(hash_map_with_key, chunk_size);
                return;
            } else {
                _streaming_selection.assign(chunk_size, 0);
            }
        }
        hash_map_with_key.compute_agg_states(chunk_size, _group_by_columns, _mem_pool.get(),
                                             AllocateState<HashMapWithKey>(this), &_tmp_agg_states);
    }

    template <typename HashMapWithKey>
    void build_hash_map_with_selection(HashMapWithKey& hash_map_with_key, size_t chunk_size) {
        hash_map_with_key.compute_agg_states(chunk_size, _group_by_columns, AllocateState<HashMapWithKey>(this),
                                             &_tmp_agg_states, &_streaming_selection);
    }

    template <typename HashSetWithKey>
    void build_hash_set(HashSetWithKey& hash_set, size_t chunk_size) {
        hash_set.build_set(chunk_size, _group_by_columns, _mem_pool.get());
    }

    template <typename HashSetWithKey>
    void build_hash_set_with_selection(HashSetWithKey& hash_set, size_t chunk_size) {
        hash_set.build_set(chunk_size, _group_by_columns, &_streaming_selection);
    }

    template <typename HashMapWithKey>
    void convert_hash_map_to_chunk(HashMapWithKey& hash_map_with_key, int32_t chunk_size, vectorized::ChunkPtr* chunk) {
        SCOPED_TIMER(_get_results_timer);

        auto it = std::any_cast<RawHashTableIterator>(_it_hash);
        auto end = _state_allocator.end();

        vectorized::Columns group_by_columns = _create_group_by_columns();
        vectorized::Columns agg_result_columns = _create_agg_result_columns();

        int32_t read_index = 0;
        {
            SCOPED_TIMER(_iter_timer);
            hash_map_with_key.results.resize(chunk_size);
            // get key/value from hashtable
            while ((it != end) & (read_index < chunk_size)) {
                auto* value = it.value();
                hash_map_with_key.results[read_index] = *reinterpret_cast<typename HashMapWithKey::KeyType*>(value);
                _tmp_agg_states[read_index] = value;
                ++read_index;
                it.next();
            }
        }

        {
            SCOPED_TIMER(_group_by_append_timer);
            hash_map_with_key.insert_keys_to_columns(hash_map_with_key.results, group_by_columns, read_index);
        }

        {
            SCOPED_TIMER(_agg_append_timer);
            if (_needs_finalize) {
                for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
                    _agg_functions[i]->batch_finalize(_agg_fn_ctxs[i], read_index, _tmp_agg_states,
                                                      _agg_states_offsets[i], agg_result_columns[i].get());
                }
            } else {
                for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
                    _agg_functions[i]->batch_serialize(_agg_fn_ctxs[i], read_index, _tmp_agg_states,
                                                       _agg_states_offsets[i], agg_result_columns[i].get());
                }
            }
        }

        _is_ht_eos = (it == end);

        // If there is null key, output it last
        if constexpr (HashMapWithKey::has_single_null_key) {
            if (_is_ht_eos && hash_map_with_key.null_key_data != nullptr) {
                // The output chunk size couldn't larger than _state->chunk_size()
                if (read_index < _state->chunk_size()) {
                    // For multi group by key, we don't need to special handle null key
                    DCHECK(group_by_columns.size() == 1);
                    DCHECK(group_by_columns[0]->is_nullable());
                    group_by_columns[0]->append_default();

                    if (_needs_finalize) {
                        _finalize_to_chunk(hash_map_with_key.null_key_data, agg_result_columns);
                    } else {
                        _serialize_to_chunk(hash_map_with_key.null_key_data, agg_result_columns);
                    }

                    ++read_index;
                } else {
                    // Output null key in next round
                    _is_ht_eos = false;
                }
            }
        }

        _it_hash = it;

        vectorized::ChunkPtr _result_chunk = std::make_shared<vectorized::Chunk>();
        // For different agg phase, we should use different TupleDescriptor
        if (_needs_finalize) {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                _result_chunk->append_column(group_by_columns[i], _output_tuple_desc->slots()[i]->id());
            }
            for (size_t i = 0; i < agg_result_columns.size(); i++) {
                size_t id = group_by_columns.size() + i;
                _result_chunk->append_column(agg_result_columns[i], _output_tuple_desc->slots()[id]->id());
            }
        } else {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                _result_chunk->append_column(group_by_columns[i], _intermediate_tuple_desc->slots()[i]->id());
            }
            for (size_t i = 0; i < agg_result_columns.size(); i++) {
                size_t id = group_by_columns.size() + i;
                _result_chunk->append_column(agg_result_columns[i], _intermediate_tuple_desc->slots()[id]->id());
            }
        }
        _num_rows_returned += read_index;
        *chunk = std::move(_result_chunk);
    }

    template <typename HashSetWithKey>
    void convert_hash_set_to_chunk(HashSetWithKey& hash_set, int32_t chunk_size, vectorized::ChunkPtr* chunk) {
        SCOPED_TIMER(_get_results_timer);
        using Iterator = typename HashSetWithKey::Iterator;
        auto it = std::any_cast<Iterator>(_it_hash);
        auto end = hash_set.hash_set.end();

        vectorized::Columns group_by_columns = _create_group_by_columns();

        // Computer group by columns and aggregate result column
        int32_t read_index = 0;
        hash_set.results.resize(chunk_size);
        while (it != end && read_index < chunk_size) {
            // hash_set.insert_key_to_columns(*it, group_by_columns);
            hash_set.results[read_index] = *it;
            ++read_index;
            ++it;
        }

        {
            SCOPED_TIMER(_group_by_append_timer);
            hash_set.insert_keys_to_columns(hash_set.results, group_by_columns, read_index);
        }

        _is_ht_eos = (it == end);

        // IF there is null key, output it last
        if constexpr (HashSetWithKey::has_single_null_key) {
            if (_is_ht_eos && hash_set.has_null_key) {
                // The output chunk size couldn't larger than _state->chunk_size()
                if (read_index < _state->chunk_size()) {
                    // For multi group by key, we don't need to special handle null key
                    DCHECK(group_by_columns.size() == 1);
                    DCHECK(group_by_columns[0]->is_nullable());
                    group_by_columns[0]->append_default();
                    ++read_index;
                } else {
                    // Output null key in next round
                    _is_ht_eos = false;
                }
            }
        }

        _it_hash = it;

        vectorized::ChunkPtr result_chunk = std::make_shared<vectorized::Chunk>();
        // For different agg phase, we should use different TupleDescriptor
        if (_needs_finalize) {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                result_chunk->append_column(group_by_columns[i], _output_tuple_desc->slots()[i]->id());
            }
        } else {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                result_chunk->append_column(group_by_columns[i], _intermediate_tuple_desc->slots()[i]->id());
            }
        }
        _num_rows_returned += read_index;
        *chunk = std::move(result_chunk);
    }

private:
    bool _reached_limit() { return _limit != -1 && _num_rows_returned >= _limit; }

    // initial const columns for i'th FunctionContext.
    Status _evaluate_const_columns(int i);

    // Create new aggregate function result column by type
    vectorized::Columns _create_agg_result_columns();
    vectorized::Columns _create_group_by_columns();

    void _serialize_to_chunk(vectorized::ConstAggDataPtr __restrict state,
                             const vectorized::Columns& agg_result_columns);
    void _finalize_to_chunk(vectorized::ConstAggDataPtr __restrict state,
                            const vectorized::Columns& agg_result_columns);

    void _reset_exprs(vectorized::Chunk* chunk);
    Status _evaluate_exprs(vectorized::Chunk* chunk);

    // Choose different agg hash map/set by different group by column's count, type, nullable
    template <typename HashVariantType>
    void _init_agg_hash_variant(HashVariantType& hash_variant);

    template <typename HashMapWithKey>
    void _release_agg_memory(HashMapWithKey* hash_map_with_key) {
        // If all function states are of POD type,
        // then we don't have to traverse the hash table to call destroy method.
        //
        bool skip_destroy = std::all_of(_agg_functions.begin(), _agg_functions.end(),
                                        [](auto* func) { return func->is_pod_state(); });
        if (hash_map_with_key != nullptr && !skip_destroy) {
            auto null_data_ptr = hash_map_with_key->get_null_key_data();
            if (null_data_ptr != nullptr) {
                for (int i = 0; i < _agg_functions.size(); i++) {
                    _agg_functions[i]->destroy(_agg_fn_ctxs[i], null_data_ptr + _agg_states_offsets[i]);
                }
            }
            auto it = _state_allocator.begin();
            auto end = _state_allocator.end();

            while (it != end) {
                for (int i = 0; i < _agg_functions.size(); i++) {
                    _agg_functions[i]->destroy(_agg_fn_ctxs[i], it.value() + _agg_states_offsets[i]);
                }
                it.next();
            }
        }
    }
    template <class HashMapWithKey>
    friend struct AllocateState;
};

template <class HashMapWithKey>
inline vectorized::AggDataPtr AllocateState<HashMapWithKey>::operator()(const typename HashMapWithKey::KeyType& key) {
    vectorized::AggDataPtr agg_state = aggregator->_state_allocator.allocate();
    *reinterpret_cast<typename HashMapWithKey::KeyType*>(agg_state) = key;
    for (int i = 0; i < aggregator->_agg_fn_ctxs.size(); i++) {
        aggregator->_agg_functions[i]->create(aggregator->_agg_fn_ctxs[i],
                                              agg_state + aggregator->_agg_states_offsets[i]);
    }
    return agg_state;
}

template <class HashMapWithKey>
inline vectorized::AggDataPtr AllocateState<HashMapWithKey>::operator()(std::nullptr_t) {
    vectorized::AggDataPtr agg_state = aggregator->_state_allocator.allocate_null_key_data();
    for (int i = 0; i < aggregator->_agg_fn_ctxs.size(); i++) {
        aggregator->_agg_functions[i]->create(aggregator->_agg_fn_ctxs[i],
                                              agg_state + aggregator->_agg_states_offsets[i]);
    }
    return agg_state;
}

class AggregatorFactory;
using AggregatorFactoryPtr = std::shared_ptr<AggregatorFactory>;

class AggregatorFactory {
public:
    AggregatorFactory(const TPlanNode& tnode) : _tnode(tnode) {}

    AggregatorPtr get_or_create(size_t id) {
        auto it = _aggregators.find(id);
        if (it != _aggregators.end()) {
            return it->second;
        }
        auto params = convert_to_aggregator_params(_tnode);
        auto aggregator = std::make_shared<Aggregator>(std::move(params));
        _aggregators[id] = aggregator;
        return aggregator;
    }

private:
    const TPlanNode& _tnode;
    std::unordered_map<size_t, AggregatorPtr> _aggregators;
};

} // namespace starrocks
