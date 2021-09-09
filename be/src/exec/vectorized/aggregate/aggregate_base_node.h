// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <any>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exec/exec_node.h"
#include "exec/vectorized/aggregate/agg_hash_variant.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {

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

class AggregateBaseNode : public ExecNode {
public:
    AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregateBaseNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    // Only for compatibility
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status close(RuntimeState* state) override;
    void push_down_join_runtime_filter(RuntimeState* state,
                                       vectorized::RuntimeFilterProbeCollector* collector) override;

protected:
    bool _should_expand_preagg_hash_tables(size_t input_chunk_size, int64_t ht_mem, int64_t ht_rows) const;

    // initial const columns for i'th FunctionContext.
    void _evaluate_const_columns(int i);

    // Choose different agg hash map/set by different group by column's count, type, nullable
    template <typename HashVariantType>
    void _init_agg_hash_variant(HashVariantType& hash_variant);

    // For aggregate without group by
    void _compute_single_agg_state(size_t chunk_size);

    // For aggregate with group by
    void _compute_batch_agg_states(size_t chunk_size);

    void _compute_batch_agg_states(size_t chunk_size, const std::vector<uint8_t>& selection);

    template <typename HashMapWithKey>
    void _build_hash_map(HashMapWithKey& hash_map_with_key, size_t chunk_size) {
        hash_map_with_key.compute_agg_states(
                chunk_size, _group_by_columns, _mem_pool.get(),
                [this]() {
                    AggDataPtr agg_state =
                            _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
                    for (int i = 0; i < _agg_functions.size(); i++) {
                        _agg_functions[i]->create(agg_state + _agg_states_offsets[i]);
                    }
                    return agg_state;
                },
                &_tmp_agg_states);
    }

    template <typename HashMapWithKey>
    void _build_hash_map(HashMapWithKey& hash_map_with_key, size_t chunk_size, std::vector<uint8_t>* selection) {
        hash_map_with_key.compute_agg_states(
                chunk_size, _group_by_columns,
                [this]() {
                    AggDataPtr agg_state =
                            _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
                    for (int i = 0; i < _agg_functions.size(); i++) {
                        _agg_functions[i]->create(agg_state + _agg_states_offsets[i]);
                    }
                    return agg_state;
                },
                &_tmp_agg_states, selection);
    }

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

    template <typename HashSetWithKey>
    void _build_hash_set(HashSetWithKey& hash_set, size_t chunk_size) {
        hash_set.build_set(chunk_size, _group_by_columns, _mem_pool.get());
    }

    template <typename HashSetWithKey>
    void _build_hash_set(HashSetWithKey& hash_set, size_t chunk_size, std::vector<uint8_t>* selection) {
        hash_set.build_set(chunk_size, _group_by_columns, selection);
    }

    // Create new aggregate function result column by type
    Columns _create_agg_result_columns();
    Columns _create_group_by_columns();

    // Convert one row agg states to chunk
    void _convert_to_chunk_no_groupby(ChunkPtr* chunk);

    template <typename HashMapWithKey>
    void _convert_hash_map_to_chunk(HashMapWithKey& hash_map_with_key, int32_t chunk_size, ChunkPtr* chunk) {
        SCOPED_TIMER(_get_results_timer);
        using Iterator = typename HashMapWithKey::Iterator;
        auto it = std::any_cast<Iterator>(_it_hash);
        auto end = hash_map_with_key.hash_map.end();

        Columns group_by_columns = _create_group_by_columns();
        Columns agg_result_column = _create_agg_result_columns();

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

        _is_finished = (it == end);
        _hash_table_eos = _is_finished;

        // If there is null key, output it last
        if constexpr (HashMapWithKey::has_single_null_key) {
            if (_is_finished && hash_map_with_key.null_key_data != nullptr) {
                // The output chunk size couldn't larger than config::vector_chunk_size
                if (read_index < config::vector_chunk_size) {
                    // For multi group by key, we don't need to special handle null key
                    DCHECK(group_by_columns.size() == 1);
                    DCHECK(group_by_columns[0]->is_nullable());
                    group_by_columns[0]->append_default();
                    (this->*_serialize_or_finalize)(hash_map_with_key.null_key_data, agg_result_column);
                    ++read_index;
                } else {
                    // Output null key in next round
                    _hash_table_eos = false;
                    _is_finished = false;
                }
            }
        }

        _it_hash = it;

        ChunkPtr _result_chunk = std::make_shared<Chunk>();
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
    void _convert_hash_set_to_chunk(HashSetWithKey& hash_set, int32_t chunk_size, ChunkPtr* chunk) {
        SCOPED_TIMER(_get_results_timer);
        using Iterator = typename HashSetWithKey::Iterator;
        auto it = std::any_cast<Iterator>(_it_hash);
        auto end = hash_set.hash_set.end();

        Columns group_by_columns = _create_group_by_columns();

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

        _is_finished = (it == end);

        // IF there is null key, output it last
        if constexpr (HashSetWithKey::has_single_null_key) {
            if (_is_finished && hash_set.has_null_key) {
                // The output chunk size couldn't larger than config::vector_chunk_size
                if (read_index < config::vector_chunk_size) {
                    // For multi group by key, we don't need to special handle null key
                    DCHECK(group_by_columns.size() == 1);
                    DCHECK(group_by_columns[0]->is_nullable());
                    group_by_columns[0]->append_default();
                    ++read_index;
                } else {
                    // Output null key in next round
                    _is_finished = false;
                }
            }
        }

        _it_hash = it;

        ChunkPtr result_chunk = std::make_shared<Chunk>();
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

    void _process_limit(ChunkPtr* chunk) {
        if (reached_limit()) {
            int64_t num_rows_over = _num_rows_returned - _limit;
            (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
            COUNTER_SET(_rows_returned_counter, _limit);
            _is_finished = true;
            LOG(INFO) << "Aggregate Node ReachedLimit " << _limit;
        }
    }

    // When convert to chunk, we serialize the aggregate state
    void _serialize_to_chunk(ConstAggDataPtr state, const Columns& agg_result_columns);

    // When convert to chunk, we finalize the aggregate state
    void _finalize_to_chunk(ConstAggDataPtr state, const Columns& agg_result_columns);

    void (AggregateBaseNode::*_serialize_or_finalize)(ConstAggDataPtr state,
                                                      const Columns& agg_result_columns) = nullptr;

    void (AggregateBaseNode::*_compute_agg_states)(size_t chunk_size) = nullptr;

    void _evaluate_exprs(Chunk* chunk);

    void _output_chunk_by_streaming(ChunkPtr* chunk);

    // Elements queried in HashTable will be added to HashTable,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    // selection[i] = 0: found in hash table
    // selection[1] = 1: not found in hash table
    void _output_chunk_by_streaming(ChunkPtr* chunk, const std::vector<uint8_t>& filter);

    Status _check_hash_map_memory_usage(RuntimeState* state);

    Status _check_hash_set_memory_usage(RuntimeState* state);

#ifdef NDEBUG
    static constexpr size_t two_level_memory_threshold = 33554432; // 32M, L3 Cache
#else
    static constexpr size_t two_level_memory_threshold = 64;
#endif

#ifdef NDEBUG
    static constexpr size_t streaming_hash_table_size_threshold = 10000000;
#else
    static constexpr size_t streaming_hash_table_size_threshold = 4;
#endif

    // At first, we use single hash map, if hash map is too big,
    // we convert the single hash map to two level hash map.
    // two level hash map is better in large data set.
    void _try_convert_to_two_level_map();

    const TPlanNode _tnode;

    // Certain aggregates require a finalize step, which is the final step of the
    // aggregate after consuming all input rows. The finalize step converts the aggregate
    // value into its final form. This is true if this node contains aggregate that requires
    // a finalize step.
    bool _needs_finalize;

    bool _is_finished = false;

    bool _is_only_group_by_columns = false;

    // At least one group by column is nullable
    bool _has_nullable_key = false;

    int64_t _num_input_rows = 0;

    // memory used for hashmap or hashset
    int64_t _last_ht_memory_usage = 0;

    // memory used for agg function
    int64_t _last_agg_func_memory_usage = 0;

    int64_t _num_pass_through_rows = 0;
    bool _hash_table_eos = false;
    bool _child_eos = false;

    TStreamingPreaggregationMode::type _streaming_preaggregation_mode;
    // The key is all group by column, the value is all agg function column
    HashMapVariant _hash_map_variant;
    HashSetVariant _hash_set_variant;
    // The Iterator for hash table
    std::any _it_hash;

    std::unique_ptr<MemPool> _mem_pool;

#ifdef NDEBUG
    static constexpr size_t memory_check_batch_size = 65535;
#else
    static constexpr size_t memory_check_batch_size = 1;
#endif

    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the aggregate function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all aggregate state
    size_t _max_agg_state_align_size = 1;
    // The followings are aggregate function information:
    std::vector<starrocks_udf::FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    // agg state when no group by columns
    AggDataPtr _single_agg_state = nullptr;
    // The expr used to evaluate agg input columns
    // one agg function could have multi input exprs
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<std::vector<ColumnPtr>> _agg_intput_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const Column*>> _agg_input_raw_columns;
    // Indicates we should use update or merge method to process aggregate column data
    std::vector<bool> _is_merge_funcs;
    // In order batch update agg states
    Buffer<AggDataPtr> _tmp_agg_states;
    std::vector<AggFunctionTypes> _agg_fn_types;

    // Exprs used to evaluate group by column
    std::vector<ExprContext*> _group_by_expr_ctxs;
    Columns _group_by_columns;
    std::vector<GroupByColumnTypes> _group_by_types;

    RuntimeProfile::Counter* _get_results_timer{};
    RuntimeProfile::Counter* _iter_timer{};
    RuntimeProfile::Counter* _agg_append_timer{};
    RuntimeProfile::Counter* _group_by_append_timer{};
    RuntimeProfile::Counter* _agg_compute_timer{};
    RuntimeProfile::Counter* _streaming_timer{};
    RuntimeProfile::Counter* _input_row_count{};
    RuntimeProfile::Counter* _hash_table_size{};
    RuntimeProfile::Counter* _pass_through_row_count{};
    RuntimeProfile::Counter* _expr_compute_timer{};
    RuntimeProfile::Counter* _expr_release_timer{};

    // Tuple into which Update()/Merge()/Serialize() results are stored.
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    // Tuple into which Finalize() results are stored. Possibly the same as
    // the intermediate tuple.
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    AggrPhase _aggr_phase = AggrPhase1;
    std::vector<uint8_t> _streaming_selection;
};

} // namespace starrocks::vectorized
