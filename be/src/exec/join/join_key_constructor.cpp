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

#include "join_key_constructor.hpp"

#include <optional>

#include "serde/column_array_serde.h"

namespace starrocks {

void BuildKeyConstructorForSerialized::prepare(RuntimeState* state, JoinHashTableItems* table_items) {
    table_items->build_slice.resize(table_items->row_count + 1);
    table_items->build_pool = std::make_unique<MemPool>();
}

void BuildKeyConstructorForSerialized::build_key(RuntimeState* state, JoinHashTableItems* table_items) {
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

    // Calc serialize size.
    size_t serialize_size = 0;
    for (const auto& data_column : data_columns) {
        serialize_size += serde::ColumnArraySerde::max_serialized_size(*data_column);
    }
    uint8_t* ptr = table_items->build_pool->allocate(serialize_size);

    // Serialize to key columns and build key is_nulls.
    if (null_columns.empty()) {
        for (uint32_t i = 1; i < 1 + row_count; i++) {
            table_items->build_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
            ptr += table_items->build_slice[i].size;
        }
    } else {
        table_items->build_key_nulls.resize(row_count + 1);
        auto* dest_is_nulls = table_items->build_key_nulls.data();
        std::memcpy(dest_is_nulls, null_columns[0]->immutable_data().data(),
                    (row_count + 1) * sizeof(NullColumn::ValueType));
        for (uint32_t i = 1; i < null_columns.size(); i++) {
            for (uint32_t j = 1; j < 1 + row_count; j++) {
                dest_is_nulls[j] |= null_columns[i]->immutable_data()[j];
            }
        }

        for (uint32_t i = 1; i < 1 + row_count; i++) {
            if (dest_is_nulls[i] == 0) {
                table_items->build_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
                ptr += table_items->build_slice[i].size;
            }
        }
    }
}

void ProbeKeyConstructorForSerialized::build_key(const JoinHashTableItems& table_items,
                                                 HashTableProbeState* probe_state) {
    probe_state->probe_pool->clear();

    // prepare columns
    Columns data_columns;
    NullColumns null_columns;

    for (size_t i = 0; i < probe_state->key_columns->size(); i++) {
        if (table_items.join_keys[i].is_null_safe_equal) {
            // this means build column is a nullable column and join condition is null safe equal
            // we need convert the probe column to a nullable column when it's a non-nullable column
            // to align the type between build and probe columns.
            data_columns.emplace_back(NullableColumn::wrap_if_necessary((*probe_state->key_columns)[i]));
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

void ProbeKeyConstructorForSerialized::_probe_column(const JoinHashTableItems& table_items,
                                                     HashTableProbeState* probe_state, const Columns& data_columns,
                                                     uint8_t* ptr) {
    const uint32_t row_count = probe_state->probe_row_count;
    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
        ptr += probe_state->probe_slice[i].size;
    }

    probe_state->null_array = std::nullopt;
}

void ProbeKeyConstructorForSerialized::_probe_nullable_column(const JoinHashTableItems& table_items,
                                                              HashTableProbeState* probe_state,
                                                              const Columns& data_columns,
                                                              const NullColumns& null_columns, uint8_t* ptr) {
    const uint32_t row_count = probe_state->probe_row_count;

    for (uint32_t i = 0; i < row_count; i++) {
        probe_state->is_nulls[i] = null_columns[0]->immutable_data()[i];
    }
    for (uint32_t i = 1; i < null_columns.size(); i++) {
        for (uint32_t j = 0; j < row_count; j++) {
            probe_state->is_nulls[j] |= null_columns[i]->immutable_data()[j];
        }
    }

    for (uint32_t i = 0; i < row_count; i++) {
        if (probe_state->is_nulls[i] == 0) {
            probe_state->probe_slice[i] = JoinHashMapHelper::get_hash_key(data_columns, i, ptr);
            ptr += probe_state->probe_slice[i].size;
        }
    }

    probe_state->null_array = probe_state->is_nulls;
}
} // namespace starrocks