// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <any>
#include <atomic>
#include <mutex>
#include <queue>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/aggregate/agg_hash_variant.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks {

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

class Aggregator;
using AggregatorPtr = std::shared_ptr<Aggregator>;

// Component used to process aggregation including bloking aggregate and streaming aggregate
// it contains common data struct and algorithm of aggregation
// TODO(hcf) this component is shared by multiply sink/source operators in pipeline engine
// TODO(hcf) all the data should be protected by lightweight lock
class Aggregator {
public:
    Aggregator(const TPlanNode& tnode, const RowDescriptor& child_row_desc);

    ~Aggregator() = default;

    Status open(RuntimeState* state);
    Status prepare(RuntimeState* state, ObjectPool* pool, MemTracker* mem_tracker, MemTracker* expr_mem_tracker,
                   RuntimeProfile* runtime_profile);

    Status close(RuntimeState* state);

    std::unique_ptr<MemPool>& mem_pool() { return _mem_pool; };
    bool is_none_group_by_exprs() { return _group_by_expr_ctxs.empty(); }
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
    const vectorized::HashMapVariant& hash_map_variant() { return _hash_map_variant; }
    const vectorized::HashSetVariant& hash_set_variant() { return _hash_set_variant; }
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

    void evaluate_exprs(vectorized::Chunk* chunk);

    void output_chunk_by_streaming(vectorized::ChunkPtr* chunk);

    // Elements queried in HashTable will be added to HashTable,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    // selection[i] = 0: found in hash table
    // selection[1] = 1: not found in hash table
    void output_chunk_by_streaming_with_selection(vectorized::ChunkPtr* chunk);

    Status check_hash_map_memory_usage(RuntimeState* state);
    Status check_hash_set_memory_usage(RuntimeState* state);

    // At first, we use single hash map, if hash map is too big,
    // we convert the single hash map to two level hash map.
    // two level hash map is better in large data set.
    void try_convert_to_two_level_map();

#ifdef NDEBUG
    static constexpr size_t two_level_memory_threshold = 33554432; // 32M, L3 Cache
    static constexpr size_t streaming_hash_table_size_threshold = 10000000;
    static constexpr size_t memory_check_batch_size = 65535;
#else
    static constexpr size_t two_level_memory_threshold = 64;
    static constexpr size_t streaming_hash_table_size_threshold = 4;
    static constexpr size_t memory_check_batch_size = 1;
#endif

private:
    // used to init
    const TPlanNode _tnode;
    const RowDescriptor _child_row_desc;

    ObjectPool* _pool;
    std::unique_ptr<MemPool> _mem_pool;
    MemTracker* _mem_tracker;
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
    // memory used for hashmap or hashset
    int64_t _last_ht_memory_usage = 0;
    // memory used for agg function
    int64_t _last_agg_func_memory_usage = 0;
    int64_t _num_pass_through_rows = 0;

    TStreamingPreaggregationMode::type _streaming_preaggregation_mode;

    // The key is all group by column, the value is all agg function column
    vectorized::HashMapVariant _hash_map_variant;
    vectorized::HashSetVariant _hash_set_variant;
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
        hash_map_with_key.compute_agg_states(
                chunk_size, _group_by_columns, _mem_pool.get(),
                [this]() {
                    vectorized::AggDataPtr agg_state =
                            _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
                    for (int i = 0; i < _agg_functions.size(); i++) {
                        _agg_functions[i]->create(agg_state + _agg_states_offsets[i]);
                    }
                    return agg_state;
                },
                &_tmp_agg_states);
    }

    template <typename HashMapWithKey>
    void build_hash_map_with_selection(HashMapWithKey& hash_map_with_key, size_t chunk_size) {
        hash_map_with_key.compute_agg_states(
                chunk_size, _group_by_columns,
                [this]() {
                    vectorized::AggDataPtr agg_state =
                            _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
                    for (int i = 0; i < _agg_functions.size(); i++) {
                        _agg_functions[i]->create(agg_state + _agg_states_offsets[i]);
                    }
                    return agg_state;
                },
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
        using Iterator = typename HashMapWithKey::Iterator;
        auto it = std::any_cast<Iterator>(_it_hash);
        auto end = hash_map_with_key.hash_map.end();

        vectorized::Columns group_by_columns = _create_group_by_columns();
        vectorized::Columns agg_result_column = _create_agg_result_columns();

        int32_t read_index = 0;
        {
            SCOPED_TIMER(_iter_timer);
            hash_map_with_key.results.resize(chunk_size);
            while ((it != end) & (read_index < chunk_size)) {
                hash_map_with_key.results[read_index] = it->first;
                _tmp_agg_states[read_index] = it->second;
                ++read_index;
                ++it;
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
                                                      _agg_states_offsets[i], agg_result_column[i].get());
                }
            } else {
                for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
                    _agg_functions[i]->batch_serialize(read_index, _tmp_agg_states, _agg_states_offsets[i],
                                                       agg_result_column[i].get());
                }
            }
        }

        _is_ht_eos = (it == end);

        // If there is null key, output it last
        if constexpr (HashMapWithKey::has_single_null_key) {
            if (_is_ht_eos && hash_map_with_key.null_key_data != nullptr) {
                // The output chunk size couldn't larger than config::vector_chunk_size
                if (read_index < config::vector_chunk_size) {
                    // For multi group by key, we don't need to special handle null key
                    DCHECK(group_by_columns.size() == 1);
                    DCHECK(group_by_columns[0]->is_nullable());
                    group_by_columns[0]->append_default();

                    if (_needs_finalize) {
                        _finalize_to_chunk(hash_map_with_key.null_key_data, agg_result_column);
                    } else {
                        _serialize_to_chunk(hash_map_with_key.null_key_data, agg_result_column);
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
            for (size_t i = 0; i < agg_result_column.size(); i++) {
                size_t id = group_by_columns.size() + i;
                _result_chunk->append_column(agg_result_column[i], _output_tuple_desc->slots()[id]->id());
            }
        } else {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                _result_chunk->append_column(group_by_columns[i], _intermediate_tuple_desc->slots()[i]->id());
            }
            for (size_t i = 0; i < agg_result_column.size(); i++) {
                size_t id = group_by_columns.size() + i;
                _result_chunk->append_column(agg_result_column[i], _intermediate_tuple_desc->slots()[id]->id());
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
                // The output chunk size couldn't larger than config::vector_chunk_size
                if (read_index < config::vector_chunk_size) {
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
    void _evaluate_const_columns(int i);

    // Create new aggregate function result column by type
    vectorized::Columns _create_agg_result_columns();
    vectorized::Columns _create_group_by_columns();

    void _serialize_to_chunk(vectorized::ConstAggDataPtr state, const vectorized::Columns& agg_result_columns);
    void _finalize_to_chunk(vectorized::ConstAggDataPtr state, const vectorized::Columns& agg_result_columns);

    void _evaluate_group_by_exprs(vectorized::Chunk* chunk);
    void _evaluate_agg_fn_exprs(vectorized::Chunk* chunk);

    // Choose different agg hash map/set by different group by column's count, type, nullable
    template <typename HashVariantType>
    void _init_agg_hash_variant(HashVariantType& hash_variant);

    template <typename HashMapWithKey>
    void _release_agg_memory(HashMapWithKey& hash_map_with_key) {
        auto it = hash_map_with_key.hash_map.begin();
        auto end = hash_map_with_key.hash_map.end();
        while (it != end) {
            for (int i = 0; i < _agg_functions.size(); i++) {
                _agg_functions[i]->destroy(it->second + _agg_states_offsets[i]);
            }
            ++it;
        }
    }
};
} // namespace starrocks
