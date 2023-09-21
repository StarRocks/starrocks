#include "exec/sorted_join_hash_map.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/join_hash_map.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "serde/column_array_serde.h"
#include "types/logical_type.h"

namespace starrocks {

void SortedSerializeJoinBuildFunc::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->bucket_size = JoinHashMapHelper::calc_bucket_size(table_items->row_count + 1);
    table_items->first.resize(table_items->bucket_size, 0);
    table_items->next.resize(table_items->row_count + 1, 0);
    table_items->range.resize(table_items->row_count + 1);
    table_items->cmps.resize(table_items->row_count + 1, 0);
    table_items->range_end_mx_data.resize(table_items->row_count + 1);
    table_items->build_slice.resize(table_items->row_count + 1);
    table_items->build_pool = std::make_unique<MemPool>();
}

void SortedSerializeJoinBuildFunc::construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                                        HashTableProbeState* probe_state) {
    uint32_t row_count = table_items->row_count;

    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < table_items->key_columns.size(); i++) {
        if (table_items->join_keys[i].is_null_safe_equal) {
            data_columns.emplace_back(table_items->key_columns[i]);
        } else if (table_items->key_columns[i]->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items->key_columns[i]);
            data_columns.emplace_back(nullable_column->data_column());
            if (table_items->key_columns[i]->has_null()) {
                null_columns.emplace_back(nullable_column->null_column());
            }
        } else {
            data_columns.emplace_back(table_items->key_columns[i]);
        }
    }

    // calc serialize size
    size_t serialize_size = 0;
    for (const auto& data_column : data_columns) {
        serialize_size += serde::ColumnArraySerde::max_serialized_size(*data_column);
    }
    uint8_t* ptr = table_items->build_pool->allocate(serialize_size);

    // TODO: build range_max

    // serialize and build hash table
    uint32_t quo = row_count / state->chunk_size();
    uint32_t rem = row_count % state->chunk_size();

    if (!null_columns.empty()) {
        for (size_t i = 0; i < quo; i++) {
            _build_nullable_columns(table_items, probe_state, data_columns, null_columns, 1 + state->chunk_size() * i,
                                    state->chunk_size(), &ptr);
        }
        if (rem > 0) {
            _build_nullable_columns(table_items, probe_state, data_columns, null_columns, 1 + state->chunk_size() * quo,
                                    rem, &ptr);
        }

    } else {
        for (size_t i = 0; i < quo; i++) {
            _build_columns(table_items, probe_state, data_columns, 1 + state->chunk_size() * i, state->chunk_size(),
                           &ptr);
        }
        if (rem > 0) {
            _build_columns(table_items, probe_state, data_columns, 1 + state->chunk_size() * quo, rem, &ptr);
        }
    }
}

void SortedSerializeJoinBuildFunc::_build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                                  const Columns& data_columns, uint32_t start, uint32_t count,
                                                  uint8_t** ptr) {
    auto range_end_column =
            ColumnRef::get_column(table_items->range_keys[1]->root()->get_child(1), table_items->build_chunk.get());
    const auto& range_end_proxy_data = ColumnHelper::get_binary_column(range_end_column.get())->get_proxy_data();

    for (size_t i = 0; i < count; i++) {
        table_items->build_slice[start + i] = JoinHashMapHelper::get_hash_key(data_columns, start + i, *ptr);
        probe_state->buckets[i] = JoinHashMapHelper::calc_bucket_num<Slice>(table_items->build_slice[start + i],
                                                                            table_items->bucket_size);
        *ptr += table_items->build_slice[start + i].size;
        table_items->cmps[start + i] = table_items->build_slice[start + i] != table_items->build_slice[start + i - 1];
    }
    table_items->cmps[start + 0] = 1;

    // assign range for table items
    uint32_t begin = start;
    for (size_t i = 0; i < count - 1; i++) {
        if (table_items->cmps[start + i] != 0) {
            Slice max_data = Slice::min_value();
            for (uint32_t j = begin; j <= start + i - 1; j++) {
                table_items->range[j] = {begin, start + i - 1};
                max_data = std::max(max_data, range_end_proxy_data[j]);
                table_items->range_end_mx_data[j] = max_data;
            }
            begin = start + i;
        }
    }
    //
    for (size_t i = count - 1; i < count; i++) {
        Slice max_data = Slice::min_value();
        for (uint32_t j = begin; j <= start + i; j++) {
            table_items->range[j] = {begin, start + i};
            max_data = std::max(max_data, range_end_proxy_data[j]);
            table_items->range_end_mx_data[j] = max_data;
        }
    }

    //
    for (size_t i = 0; i < count; i++) {
        if (table_items->cmps[start + i]) {
            table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
            table_items->first[probe_state->buckets[i]] = start + i;
        }
    }
}

void SortedSerializeJoinBuildFunc::_build_nullable_columns(JoinHashTableItems* table_items,
                                                           HashTableProbeState* probe_state,
                                                           const Columns& data_columns, const NullColumns& null_columns,
                                                           uint32_t start, uint32_t count, uint8_t** ptr) {
    auto range_end_column =
            ColumnRef::get_column(table_items->range_keys[1]->root()->get_child(1), table_items->build_chunk.get());
    const auto& range_end_proxy_data = ColumnHelper::get_binary_column(range_end_column.get())->get_proxy_data();

    for (uint32_t i = 0; i < count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[start + i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->get_data()[start + j];
        }
    }

    for (size_t i = 1; i < count; i++) {
        table_items->cmps[start + i] = probe_state->is_nulls[i] != probe_state->is_nulls[i - 1];
    }

    for (size_t i = 0; i < count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            table_items->build_slice[start + i] = JoinHashMapHelper::get_hash_key(data_columns, start + i, *ptr);
            probe_state->buckets[i] = JoinHashMapHelper::calc_bucket_num<Slice>(table_items->build_slice[start + i],
                                                                                table_items->bucket_size);
            *ptr += table_items->build_slice[start + i].size;
            table_items->cmps[start + i] =
                    table_items->cmps[start + i] ||
                    table_items->build_slice[start + i] != table_items->build_slice[start + i - 1];
        }
    }
    table_items->cmps[start + 0] = 1;
    // assign range for table items
    uint32_t begin = start;
    for (size_t i = 0; i < count; i++) {
        if (table_items->cmps[start + i] != 0 && probe_state->is_nulls[i] == 0) {
            Slice max_data = Slice::min_value();
            for (uint32_t j = begin; j <= start + i - 1; j++) {
                table_items->range[j] = {begin, start + i - 1};
                max_data = std::max(max_data, range_end_proxy_data[j]);
                table_items->range_end_mx_data[j] = max_data;
            }
            begin = start + i;
        }
    }

    for (size_t i = count - 1; i < count; i++) {
        Slice max_data = Slice::min_value();
        for (uint32_t j = begin; j <= start + i; j++) {
            table_items->range[j] = {begin, start + i};
            max_data = std::max(max_data, range_end_proxy_data[j]);
            table_items->range_end_mx_data[j] = max_data;
        }
    }

    for (size_t i = 0; i < count; i++) {
        if ((probe_state->is_nulls[i] == 0) & table_items->cmps[start + i]) {
            // if (probe_state->is_nulls[i] == 0) {
            table_items->next[start + i] = table_items->first[probe_state->buckets[i]];
            table_items->first[probe_state->buckets[i]] = start + i;
        }
    }
}

void SortedSerializeJoinProbeFunc::lookup_init(const JoinHashTableItems& table_items,
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

    // serialize and init search
    if (!null_columns.empty()) {
        _probe_nullable_column(table_items, probe_state, data_columns, null_columns, ptr);
    } else {
        _probe_column(table_items, probe_state, data_columns, ptr);
    }
}

std::pair<uint32_t, uint32_t> SortedSerializeJoinProbeFunc::seek(const JoinHashTableItems& table_items, Slice* range_st,
                                                                 Slice* range_ed, uint32_t build_index, Slice data) {
    // should copy
    auto range = table_items.range[build_index];
    if (LIKELY(range.second - range.first < 10)) {
        uint32_t i;
        for (i = range.second; i >= range.first; i--) {
            if (data.compare(range_st[i]) >= 0) {
                break;
            }
        }
        range.second = i;

        for (i = range.first; i <= range.second; i++) {
            if (data.compare(range_ed[i]) <= 0) {
                break;
            }
        }
        range.first = i;
    } else {
        range.second = std::upper_bound(range_st + range.first, range_st + range.second + 1, data) - range_st - 1;
        range.first = std::lower_bound(range_ed + range.first, range_ed + range.second + 1, data) - range_ed;
    }

    return range;
}

void SortedSerializeJoinProbeFunc::_probe_column(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state, const Columns& data_columns,
                                                 uint8_t* ptr) {
    uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
        probe_state->buckets[i] =
                JoinHashMapHelper::calc_bucket_num<Slice>(probe_state->probe_slice[i], table_items.bucket_size);
        ptr += probe_state->probe_slice[i].size;
    }

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->next[i] = table_items.first[probe_state->buckets[i]];
    }
}

void SortedSerializeJoinProbeFunc::_probe_nullable_column(const JoinHashTableItems& table_items,
                                                          HashTableProbeState* probe_state, const Columns& data_columns,
                                                          const NullColumns& null_columns, uint8_t* ptr) {
    uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->get_data()[i];
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
            probe_state->buckets[i] =
                    JoinHashMapHelper::calc_bucket_num<Slice>(probe_state->probe_slice[i], table_items.bucket_size);
            probe_state->next[i] = table_items.first[probe_state->buckets[i]];
        } else {
            probe_state->next[i] = 0;
        }
    }
}

void SortedJoinHashMap::build_prepare(RuntimeState* state) {
    _build_func.prepare(state, _table_items);
}

void SortedJoinHashMap::probe_prepare(RuntimeState* state) {
    size_t chunk_size = state->chunk_size();
    _probe_state->build_index.resize(chunk_size + 8);
    _probe_state->probe_index.resize(chunk_size + 8);
    _probe_state->next.resize(chunk_size);
    _probe_state->probe_match_index.resize(chunk_size);
    _probe_state->probe_match_filter.resize(chunk_size);
    _probe_state->buckets.resize(chunk_size);
    _probe_state->probe_ranges.resize(chunk_size + 8);

    // TODO: init match index to process right outer type join
    _probe_func.prepare(state, _probe_state);
}

void SortedJoinHashMap::build(RuntimeState* state) {
    _build_func.construct_hash_table(state, _table_items, _probe_state);
}

void SortedJoinHashMap::_search_ht(RuntimeState* state, ChunkPtr* probe_chunk) {
    if (!_probe_state->has_remain) {
        _probe_state->probe_row_count = (*probe_chunk)->num_rows();
        _probe_func.lookup_init(*_table_items, _probe_state);

        // get_build_range_data
        // get_probe_range_data
        auto& build_data = _build_func.get_key_data(*_table_items);
        auto& probe_data = _probe_func.get_key_data(*_probe_state);

        _search_ht_impl<true>(state, build_data, probe_data);
    } else {
        auto& build_data = _build_func.get_key_data(*_table_items);
        auto& probe_data = _probe_func.get_key_data(*_probe_state);
        _search_ht_impl<false>(state, build_data, probe_data);
    }
}

template <bool first_probe>
void SortedJoinHashMap::_search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data,
                                        const Buffer<CppType>& data) {
    if (!_table_items->with_other_conjunct) {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
            _probe_from_ht_for_left_outer_join_with_other_conjunct<first_probe>(state, build_data, data);
            break;
        default:
            throw std::runtime_error("unimplements join type range join optimize");
        }
    } else {
        switch (_table_items->join_type) {
        case TJoinOp::LEFT_OUTER_JOIN:
            _probe_from_ht_for_left_outer_join_with_other_conjunct<first_probe>(state, build_data, data);
            break;
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

#define RANGE_RETURN_IF_CHUNK_FULL()               \
    if (match_count > state->chunk_size()) {       \
        _probe_state->cur_probe_index = i;         \
        _probe_state->has_remain = true;           \
        _probe_state->count = state->chunk_size(); \
        return;                                    \
    }

std::pair<uint32_t, uint32_t> inner_seek(const JoinHashTableItems& table_items, Slice* range_st, Slice* range_mx_ed,
                                         Slice* range_ed, std::pair<uint32_t, uint32_t> range, Slice data) {
    // CHECK(std::is_sorted(range_ed + range.first, range_ed + range.second));
    // CHECK(std::is_sorted(range_st + range.first, range_st + range.second));
    if (LIKELY(range.second - range.first < 10)) {
        uint32_t i;
        for (i = range.second; i >= range.first; i--) {
            if (data.compare(range_st[i]) >= 0) {
                break;
            }
        }
        range.second = i;

        for (i = range.first; i <= range.second; i++) {
            if (data.compare(range_mx_ed[i]) <= 0) {
                break;
            }
        }
        range.first = i;
    } else {
        range.second = std::upper_bound(range_st + range.first, range_st + range.second + 1, data) - range_st - 1;
        range.first = std::lower_bound(range_mx_ed + range.first, range_mx_ed + range.second + 1, data) - range_mx_ed;
    }

    for (; range.second >= range.first; range.second--) {
        if (data.compare(range_ed[range.second]) <= 0) {
            break;
        }
    }

    return range;
}

template <bool first_probe>
void SortedJoinHashMap::_probe_from_ht_for_left_outer_join_with_other_conjunct(RuntimeState* state,
                                                                               const Buffer<CppType>& build_data,
                                                                               const Buffer<CppType>& probe_data) {
    // probe_range
    auto& probe_range = ColumnHelper::get_binary_column(_probe_state->probe_range_key_column.get())->get_proxy_data();

    auto& range_keys = _table_items->range_keys;
    auto range_start = ColumnRef::get_column(range_keys[0]->root()->get_child(1), _table_items->build_chunk.get());
    auto range_end = ColumnRef::get_column(range_keys[1]->root()->get_child(1), _table_items->build_chunk.get());

    Slice* range_start_data = ColumnHelper::get_binary_column(range_start.get())->get_data().data();
    Slice* range_end_data = ColumnHelper::get_binary_column(range_end.get())->get_data().data();
    Slice* range_end_mx_data = _table_items->range_end_mx_data.data();

    size_t match_count = 0;

    size_t i = _probe_state->cur_probe_index;
    if constexpr (!first_probe) {
        _probe_state->probe_index[0] = _probe_state->probe_index[state->chunk_size()];
        _probe_state->build_index[0] = _probe_state->build_index[state->chunk_size()];
        match_count = 1;
        // Note here
        if (_probe_state->next[i] == 0 && _probe_state->build_range.first > _probe_state->build_range.second) {
            i++;
            _probe_state->cur_row_match_count = 0;
        }
    } else {
        _probe_state->cur_row_match_count = 0;
        for (size_t j = 0; j < state->chunk_size(); j++) {
            _probe_state->probe_match_index[j] = 0;
        }

        // try probe once for range
        size_t probe_row_count = _probe_state->probe_row_count;
        _probe_state->probe_ranges.resize(probe_row_count);
        for (int j = i; j < probe_row_count; j++) {
            size_t build_index = _probe_state->next[j];
            _probe_state->probe_ranges[j] = {1, 0};
            while (build_index != 0) {
                if (_probe_func.equal(build_data[build_index], probe_data[j])) {
                    _probe_state->probe_ranges[j] = _table_items->range[build_index];
                    break;
                }
                build_index = _table_items->next[build_index];
            }
        }
        // final
    }

    size_t probe_row_count = _probe_state->probe_row_count;
    for (; i < probe_row_count; i++) {
        if (_probe_state->probe_ranges[i].first > _probe_state->probe_ranges[i].second) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;
            RANGE_RETURN_IF_CHUNK_FULL()
            continue;
        }

        // seek
        _probe_state->build_range = inner_seek(*_table_items, range_start_data, range_end_mx_data, range_end_data,
                                               _probe_state->probe_ranges[i], probe_range[i]);
        // _probe_state->build_range = _probe_state->probe_ranges[i];
        while (UNLIKELY(_probe_state->build_range.first <= _probe_state->build_range.second)) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = _probe_state->build_range.first;
            _probe_state->probe_match_index[i]++;
            match_count++;
            _probe_state->cur_row_match_count++;
            _probe_state->build_range.first++;
            RANGE_RETURN_IF_CHUNK_FULL()
        }

        if (_probe_state->cur_row_match_count <= 0) {
            _probe_state->probe_index[match_count] = i;
            _probe_state->build_index[match_count] = 0;
            match_count++;

            RANGE_RETURN_IF_CHUNK_FULL()
        }

        _probe_state->cur_row_match_count = 0;
    }

    _probe_state->has_null_build_tuple = true;
    PROBE_OVER()
}

void SortedJoinHashMap::probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
                              bool* has_remain) {
    _probe_state->key_columns = &key_columns;
    _probe_state->probe_range_key_column = (*probe_chunk)->get_column_by_slot_id(_table_items->probe_range_key_id);
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

void SortedJoinHashMap::_probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
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

void SortedJoinHashMap::_build_output(ChunkPtr* chunk) {
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

void SortedJoinHashMap::_copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot,
                                           bool to_nullable) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        if (to_nullable) {
            ColumnPtr dest_column = NullableColumn::create(*src_column, NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        if (to_nullable) {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            ColumnPtr dest_column = NullableColumn::create(*src_column, NullColumn::create((*src_column)->size()));
            (*chunk)->append_column(std::move(dest_column), slot->id());
        } else {
            (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
            (*chunk)->append_column(*src_column, slot->id());
        }
    } else {
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
        dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

void SortedJoinHashMap::_copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk,
                                                    const SlotDescriptor* slot) {
    if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
        (*chunk)->append_column(*src_column, slot->id());
    } else if (_probe_state->match_flag == JoinMatchFlag::MOST_MATCH_ONE) {
        (*src_column)->filter(_probe_state->probe_match_filter, _probe_state->probe_row_count);
        (*chunk)->append_column(*src_column, slot->id());
    } else {
        ColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
        dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
        (*chunk)->append_column(std::move(dest_column), slot->id());
    }
}

void SortedJoinHashMap::_copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot,
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

void SortedJoinHashMap::_copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk,
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

void SortedJoinHashMap::probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {}

} // namespace starrocks
