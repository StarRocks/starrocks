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

#include <optional>

#include "column/column.h"
#include "join_key_constructor.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// KeyConstructorForOneKey
// ------------------------------------------------------------------------------------

template <LogicalType LT>
auto BuildKeyConstructorForOneKey<LT>::get_key_data(const JoinHashTableItems& table_items) -> const ImmBuffer<CppType> {
    ColumnPtr data_column;
    if (table_items.key_columns[0]->is_nullable()) {
        auto* null_column = ColumnHelper::as_raw_column<NullableColumn>(table_items.key_columns[0]);
        data_column = null_column->data_column();
    } else {
        data_column = table_items.key_columns[0];
    }

    if constexpr (lt_is_string<LT>) {
        if (UNLIKELY(data_column->is_large_binary())) {
            return ColumnHelper::as_raw_column<LargeBinaryColumn>(data_column)->get_data();
        } else {
            return ColumnHelper::as_raw_column<BinaryColumn>(data_column)->get_data();
        }
    } else {
        return ColumnHelper::as_raw_column<ColumnType>(data_column)->get_data();
    }
}

template <LogicalType LT>
const std::optional<ImmBuffer<uint8_t>> BuildKeyConstructorForOneKey<LT>::get_is_nulls(
        const JoinHashTableItems& table_items) {
    if (table_items.key_columns[0]->is_nullable() && table_items.key_columns[0]->has_null()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(table_items.key_columns[0]);
        return nullable_column->immutable_null_column_data();
    } else {
        return std::nullopt;
    }
}

template <LogicalType LT>
void ProbeKeyConstructorForOneKey<LT>::build_key(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state) {
    const auto& key_column = (*probe_state->key_columns)[0];
    if (key_column->is_nullable() && key_column->has_null()) {
        const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state->key_columns)[0]);
        probe_state->null_array = nullable_column->immutable_null_column_data();
    } else {
        probe_state->null_array = std::nullopt;
    }
}

template <LogicalType LT>
auto ProbeKeyConstructorForOneKey<LT>::get_key_data(const HashTableProbeState& probe_state)
        -> const ImmBuffer<CppType> {
    if ((*probe_state.key_columns)[0]->is_nullable()) {
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>((*probe_state.key_columns)[0]);
        return ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data();
    }

    return ColumnHelper::as_raw_column<ColumnType>((*probe_state.key_columns)[0])->get_data();
}

// ------------------------------------------------------------------------------------
// KeyConstructorForSerializedFixedSize
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void BuildKeyConstructorForSerializedFixedSize<LT>::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->build_key_column = ColumnType::create(table_items->row_count + 1);
}

template <LogicalType LT>
void BuildKeyConstructorForSerializedFixedSize<LT>::build_key(RuntimeState* state, JoinHashTableItems* table_items) {
    const uint32_t row_count = table_items->row_count;

    // Prepare data and null columns.
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
    // Serialize to key columns.
    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, table_items->build_key_column.get(), 1,
                                                           row_count);
    // Build key is_nulls.
    if (!null_columns.empty()) {
        table_items->build_key_nulls.resize(row_count + 1);
        auto* dest_is_nulls = table_items->build_key_nulls.data();
        const auto& null_data_0 = null_columns[0]->immutable_data();
        std::memcpy(dest_is_nulls, null_data_0.data(), (row_count + 1) * sizeof(NullColumn::ValueType));
        for (uint32_t i = 1; i < null_columns.size(); i++) {
            const auto& null_data_i = null_columns[i]->immutable_data();
            for (uint32_t j = 1; j < 1 + row_count; j++) {
                dest_is_nulls[j] |= null_data_i[j];
            }
        }
    }
}

template <LogicalType LT>
void ProbeKeyConstructorForSerializedFixedSize<LT>::build_key(const JoinHashTableItems& table_items,
                                                              HashTableProbeState* probe_state) {
    // Prepare columns.
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < probe_state->key_columns->size(); i++) {
        if (table_items.join_keys[i].is_null_safe_equal) {
            if ((*probe_state->key_columns)[i]->is_nullable()) {
                data_columns.emplace_back((*probe_state->key_columns)[i]);
            } else {
                auto tmp_column = NullableColumn::create((*probe_state->key_columns)[i],
                                                         NullColumn::create(probe_state->probe_row_count, 0));
                data_columns.emplace_back(tmp_column);
            }
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

    // Build key and is_nulls.
    const uint32_t row_count = probe_state->probe_row_count;
    JoinHashMapHelper::serialize_fixed_size_key_column<LT>(data_columns, probe_state->probe_key_column.get(), 0,
                                                           row_count);

    if (null_columns.empty()) {
        probe_state->null_array = std::nullopt;
    } else {
        const auto& null_data_0 = null_columns[0]->immutable_data();
        for (uint32_t i = 0; i < row_count; i++) {
            probe_state->is_nulls[i] = null_data_0[i];
        }
        for (uint32_t i = 1; i < null_columns.size(); i++) {
            const auto& null_data_i = null_columns[i]->immutable_data();
            for (uint32_t j = 0; j < row_count; j++) {
                probe_state->is_nulls[j] |= null_data_i[j];
            }
        }

        probe_state->null_array = probe_state->is_nulls;
    }
}

} // namespace starrocks