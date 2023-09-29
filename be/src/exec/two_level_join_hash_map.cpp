#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/join_hash_map.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "serde/column_array_serde.h"
#include "types/logical_type.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {
void TwoLevelSortedSerializeJoinBuildFunc::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->build_slice.resize(table_items->row_count + 1);
    table_items->build_pool = std::make_unique<MemPool>();
    // call prepare for each submap
    for (size_t i = 0; i < table_items->sub_table_size; ++i) {
        SerializedJoinBuildFunc::prepare(state, table_items->sub_items[i]);
    }
}

void TwoLevelSortedSerializeJoinBuildFunc::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                                HashTableProbeState* probe_state) {
    // call construct for each submap
    for (size_t i = 0; i < table_items->sub_table_size; ++i) {
        SerializedJoinBuildFunc::construct_hash_table(state, table_items->sub_items[i], probe_state);
    }
    // LOG(WARNING) << "TRACE1:" << this << ":" << &table_items;
    // for (size_t i = 0; i < table_items->sub_table_size; ++i) {
    //     auto& next = table_items->sub_items[i]->next;
    //     for (size_t j = 0; j < next.size(); ++j) {
    //         auto build_idx = next[j];
    //         size_t loop = 0;
    //         while (build_idx) {
    //             build_idx = next[build_idx];
    //             loop++;
    //             if (loop > 10) {
    //                 CHECK(false);
    //             }
    //         }
    //     }
    // }
}

void TwoLevelSerializeJoinProbeFunc::lookup_init(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state) {
    probe_state->probe_pool->clear();
    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < probe_state->key_columns->size(); i++) {
        if (table_items.join_keys[i].is_null_safe_equal) {
            data_columns.emplace_back((*probe_state->key_columns)[i]);
        } else if ((*probe_state->key_columns)[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if ((*probe_state->key_columns)[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back((*probe_state->key_columns)[i]);
        }
    }

    // allocate memory for serialize key columns
    size_t serialize_size = 0;
    for (const auto& data_column : data_columns) {
        serialize_size += serde::ColumnArraySerde::max_serialized_size(*data_column);
    }
    uint8_t* ptr = probe_state->probe_pool->allocate(serialize_size);

    if (!null_columns.empty()) {
        _probe_nullable_column(table_items, probe_state, data_columns, null_columns, ptr);
    } else {
        _probe_column(table_items, probe_state, data_columns, ptr);
    }
}

void TwoLevelSerializeJoinProbeFunc::_probe_column(const JoinHashTableItems& table_items,
                                                   HashTableProbeState* probe_state, const Columns& data_columns,
                                                   uint8_t* ptr) {
    uint32_t row_count = probe_state->probe_row_count;

    // serialize key
    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
        probe_state->submap_idx[i] =
                JoinHashMapHelper::calc_bucket_num<Slice>(probe_state->probe_slice[i], table_items.sub_table_size);
        ptr += probe_state->probe_slice[i].size;
        probe_state->probe_selector[i] = i;
    }

    // order by bucket id
    ::pdqsort(probe_state->probe_selector.begin(), probe_state->probe_selector.begin() + row_count,
              [&](auto l, auto r) { return probe_state->buckets[l] < probe_state->buckets[r]; });
    const auto& probe_selector = probe_state->probe_selector;
    //
    for (uint32_t i = 0; i < row_count; i++) {
        auto idx = probe_selector[i];
        probe_state->buckets[idx] =
                JoinHashMapHelper::calc_bucket_num<Slice>(probe_state->probe_slice[idx], table_items.bucket_size);
    }

    for (uint32_t i = 0; i < row_count; i++) {
        auto idx = probe_selector[i];
        auto bucket_id = probe_state->buckets[idx];
        probe_state->next[idx] = table_items.sub_items[probe_state->submap_idx[idx]]->first[bucket_id];
    }
}

void TwoLevelSerializeJoinProbeFunc::_probe_nullable_column(const JoinHashTableItems& table_items,
                                                            HashTableProbeState* probe_state,
                                                            const Columns& data_columns,
                                                            const NullColumns& null_columns, uint8_t* ptr) {
    uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[i];
        probe_state->probe_selector[i] = i;
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < row_count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[j];
        }
    }

    probe_state->null_array = &null_columns[0]->get_data();
    for (uint32_t i = 0; i < row_count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
            ptr += probe_state->probe_slice[i].size;
        }
    }

    for (uint32_t i = 0; i < row_count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            probe_state->submap_idx[i] =
                    JoinHashMapHelper::calc_bucket_num<Slice>(probe_state->probe_slice[i], table_items.sub_table_size);
            probe_state->next[i] = table_items.first[probe_state->buckets[i]];
        } else {
            probe_state->next[i] = 0;
        }
    }

    // order by bucket id
    ::pdqsort(probe_state->probe_selector.begin(), probe_state->probe_selector.end(),
              [&](auto l, auto r) { return probe_state->buckets[l] < probe_state->buckets[r]; });
    const auto& probe_selector = probe_state->probe_selector;
    //
    for (uint32_t i = 0; i < row_count; i++) {
        auto idx = probe_selector[i];
        probe_state->buckets[idx] =
                JoinHashMapHelper::calc_bucket_num<Slice>(probe_state->probe_slice[idx], table_items.bucket_size);
    }

    for (uint32_t i = 0; i < row_count; i++) {
        auto idx = probe_selector[i];
        if (probe_state->is_nulls[i] == 0) {
            auto bucket_id = probe_state->buckets[idx];
            probe_state->next[idx] = table_items.sub_items[probe_state->submap_idx[idx]]->first[bucket_id];
        } else {
            probe_state->next[idx] = 0;
        }
    }
}

void TwoLevelJoinHashMap::build_prepare(RuntimeState* state) {
    _builder.prepare(state, _table_items);
}

void TwoLevelJoinHashMap::probe_prepare(RuntimeState* state) {
    size_t chunk_size = state->chunk_size();
    _probe_state->build_index.resize(chunk_size + 8);
    _probe_state->probe_index.resize(chunk_size + 8);
    _probe_state->next.resize(chunk_size);
    _probe_state->probe_match_index.resize(chunk_size);
    _probe_state->probe_match_filter.resize(chunk_size);
    _probe_state->buckets.resize(chunk_size);
    _probe_state->submap_idx.resize(chunk_size);
    _probe_state->probe_selector.resize(chunk_size);

    _prober.prepare(state, _probe_state);
}

void TwoLevelJoinHashMap::build(RuntimeState* state) {
    // TODO: init match index to process right outer type join
    _builder.construct_hash_table(state, _table_items, _probe_state);
}

void TwoLevelJoinHashMap::probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
                                bool* has_remain) {
    _probe_state->key_columns = &key_columns;
    {
        SCOPED_TIMER(_probe_state->search_ht_timer);
        // search from hash table

        _search_ht(state, probe_chunk);
        if (_probe_state->count <= 0) {
            *has_remain = false;
            return;
        }

        *has_remain = _probe_state->has_remain;
    }
    // TODO: process semi join and right anti join
    {
        {
            SCOPED_TIMER(_probe_state->output_probe_column_timer);
            _probe_output(probe_chunk, chunk);
        }
        {
            SCOPED_TIMER(_probe_state->output_build_column_timer);
            _build_output(chunk);
        }
        {
            SCOPED_TIMER(_probe_state->output_tuple_column_timer);
            if (_table_items->need_create_tuple_columns) {
                // _probe_tuple_output(probe_chunk, chunk);
                // _build_tuple_output(chunk);
                throw std::runtime_error("unreachable path");
            }
        }
    }
}

void TwoLevelJoinHashMap::_search_ht(RuntimeState* state, ChunkPtr* probe_chunk) {
    if (!_probe_state->has_remain) {
        _probe_state->probe_row_count = (*probe_chunk)->num_rows();
        _prober.lookup_init(*_table_items, _probe_state);

        auto& build_data = _builder.get_key_data(*_table_items);
        auto& probe_data = _prober.get_key_data(*_probe_state);
        _search_ht_impl<true>(state, build_data, probe_data);
    } else {
        auto& build_data = _builder.get_key_data(*_table_items);
        auto& probe_data = _prober.get_key_data(*_probe_state);
        _search_ht_impl<false>(state, build_data, probe_data);
    }
}

template <bool first_probe>
void TwoLevelJoinHashMap::_search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data,
                                          const Buffer<CppType>& data) {
    if (!_table_items->with_other_conjunct) {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
            _probe_from_ht_for_left_outer_join<first_probe>(state, build_data, data);
            break;
        default:
            throw std::runtime_error("unimplements join type range join optimize");
        }
    } else {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
        case TJoinOp::LEFT_SEMI_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
        case TJoinOp::RIGHT_OUTER_JOIN:
        case TJoinOp::RIGHT_ANTI_JOIN:
        case TJoinOp::FULL_OUTER_JOIN:
            throw std::runtime_error("unimplements join type range join optimize");
            break;
        default:
            throw std::runtime_error("unimplements join type range join optimize");
            break;
        }
    }
}

#define LEVEL_RETURN_IF_CHUNK_FULL()                        \
    if (match_count > state->chunk_size()) {                \
        _probe_state->next[i] = sub_item.next[build_index]; \
        _probe_state->cur_probe_index = i;                  \
        _probe_state->has_remain = true;                    \
        _probe_state->count = state->chunk_size();          \
        return;                                             \
    }

template <bool first_probe>
void TwoLevelJoinHashMap::_probe_from_ht_for_left_outer_join(RuntimeState* state, const Buffer<CppType>& _,
                                                             const Buffer<CppType>& probe_data) {
    _probe_state->match_flag = JoinMatchFlag::NORMAL;
    size_t match_count = 0;
    bool one_to_many = false;
    size_t i = _probe_state->cur_probe_index;
    const auto& probe_selector = _probe_state->probe_selector;

    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->probe_index[state->chunk_size()];
        _probe_state->build_index[0] = _probe_state->build_index[state->chunk_size()];
        match_count = 1;
        if (_probe_state->next[probe_selector[i]] == 0) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        size_t build_index = _probe_state->next[probe_selector[i]];
        auto& sub_item = *_table_items->sub_items[_probe_state->submap_idx[probe_selector[i]]];
        auto& build_data = _builder.get_key_data(sub_item);
        if (build_index == 0) {
            _probe_state->probe_index[match_count] = probe_selector[i];
            _probe_state->build_index[match_count] = 0;
            match_count++;
            _probe_state->has_null_build_tuple = true;

            LEVEL_RETURN_IF_CHUNK_FULL()
        } else {
            while (build_index != 0) {
                if (SerializedJoinProbeFunc().equal(build_data[build_index], probe_data[probe_selector[i]])) {
                    _probe_state->probe_index[match_count] = probe_selector[i];
                    _probe_state->build_index[match_count] = build_index;
                    match_count++;
                    _probe_state->cur_row_match_count++;

                    LEVEL_RETURN_IF_CHUNK_FULL()
                }
                build_index = sub_item.next[build_index];
            }
            if (_probe_state->cur_row_match_count <= 0) {
                // one key of left table match none key of right table
                _probe_state->probe_index[match_count] = probe_selector[i];
                _probe_state->build_index[match_count] = 0;
                match_count++;
                _probe_state->has_null_build_tuple = true;

                LEVEL_RETURN_IF_CHUNK_FULL()
            } else if (_probe_state->cur_row_match_count > 1) {
                // one key of left table match multi key of right table
                if constexpr (first_probe) {
                    one_to_many = true;
                }
            }
        }
        _probe_state->cur_row_match_count = 0;
    }

    if constexpr (first_probe) {
        CHECK_ALL_MATCH()
    }
    PROBE_OVER()
}

void TwoLevelJoinHashMap::_probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
    bool to_nullable = _table_items->left_to_nullable;

    for (size_t i = 0; i < _table_items->probe_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;
        auto& column = (*probe_chunk)->get_column_by_slot_id(slot->id());
        if (hash_table_slot.need_output) {
            if (!column->is_nullable()) {
                _copy_probe_column(&column, chunk, slot, to_nullable);
            } else {
                _copy_probe_nullable_column(&column, chunk, slot);
            }
        } else {
            ColumnPtr default_column = ColumnHelper::create_column(slot->type(), column->is_nullable() || to_nullable);
            default_column->append_default(_probe_state->count);
            (*chunk)->append_column(std::move(default_column), slot->id());
        }
    }
}

void TwoLevelJoinHashMap::_build_output(ChunkPtr* chunk) {
    bool to_nullable = _table_items->right_to_nullable;
    for (size_t i = 0; i < _table_items->build_column_count; i++) {
        HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
        SlotDescriptor* slot = hash_table_slot.slot;
        ColumnPtr& column = _table_items->build_chunk->columns()[i];
        if (hash_table_slot.need_output) {
            if (!column->is_nullable()) {
                _copy_build_column(column, chunk, slot, to_nullable);
            } else {
                _copy_build_nullable_column(column, chunk, slot);
            }
        } else {
            ColumnPtr default_column = ColumnHelper::create_column(slot->type(), column->is_nullable() || to_nullable);
            default_column->append_default(_probe_state->count);
            (*chunk)->append_column(std::move(default_column), slot->id());
        }
    }
}

void TwoLevelJoinHashMap::_copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot,
                                             bool to_nullable) {
    ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
    dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
    (*chunk)->append_column(std::move(dest_column), slot->id());
}

void TwoLevelJoinHashMap::_copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk,
                                                      const SlotDescriptor* slot) {
    ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
    dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
    (*chunk)->append_column(std::move(dest_column), slot->id());
}

void TwoLevelJoinHashMap::_copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot,
                                             bool to_nullable) {
    if (to_nullable) {
        auto data_column = src_column->clone_empty();
        data_column->append_selective_shallow_copy(*src_column, _probe_state->build_index.data(), 0,
                                                   _probe_state->count);

        // When left outer join is executed,
        // build_index[i] Equal to 0 means it is not found in the hash table,
        // but append_selective_shallow_copy() has set item of NullColumn to not null
        // so NullColumn needs to be set back to null
        auto null_column = NullColumn::create(_probe_state->count, 0);
        size_t end = _probe_state->count;
        for (size_t i = 0; i < end; i++) {
            if (_probe_state->build_index[i] == 0) {
                null_column->get_data()[i] = 1;
            }
        }
        auto dest_column = NullableColumn::create(std::move(data_column), null_column);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    } else {
        auto dest_column = src_column->clone_empty();
        dest_column->append_selective_shallow_copy(*src_column, _probe_state->build_index.data(), 0,
                                                   _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

void TwoLevelJoinHashMap::_copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk,
                                                      const SlotDescriptor* slot) {
    ColumnPtr dest_column = src_column->clone_empty();

    dest_column->append_selective_shallow_copy(*src_column, _probe_state->build_index.data(), 0, _probe_state->count);

    // When left outer join is executed,
    // build_index[i] Equal to 0 means it is not found in the hash table,
    // but append_selective_shallow_copy() has set item of NullColumn to not null
    // so NullColumn needs to be set back to null
    auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(dest_column);
    size_t end = _probe_state->count;
    for (size_t i = 0; i < end; i++) {
        if (_probe_state->build_index[i] == 0) {
            null_column->set_null(i);
        }
    }

    (*chunk)->append_column(std::move(dest_column), slot->id());
}

void TwoLevelJoinHashMap::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {}

} // namespace starrocks
