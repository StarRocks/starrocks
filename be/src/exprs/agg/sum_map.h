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

#include <fmt/format.h>

#include "base/phmap/phmap.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/hash_set.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/sum.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "types/logical_type.h"

namespace starrocks {

// SumMapAggregateFunctionState: Compile-time templated state for key and value types
template <LogicalType KT, LogicalType VT, typename MyHashMap = std::map<int, size_t>>
struct SumMapAggregateFunctionState : public AggregateFunctionEmptyState {
    using KeyType = typename SliceHashSet::key_type;
    using ValueResultColumnType = RunTimeColumnType<SumResultLT<VT>>;

    MyHashMap hash_map;
    // Use column to store the summed values
    typename ValueResultColumnType::MutablePtr value_result_column = ValueResultColumnType::create();
    // Track null keys separately
    bool has_null_key = false;
    size_t null_key_value_idx = 0;

    template <typename KeyViewer, typename ValueViewer>
    void update(MemPool* mem_pool, const KeyViewer& key_viewer, const ValueViewer& value_viewer, size_t offset,
                size_t count) {
        if constexpr (!lt_is_string<KT>) {
            // Numeric keys
            for (size_t i = offset; i < offset + count; i++) {
                // If value is null, skip this entry entirely
                if (value_viewer.is_null(i)) {
                    continue;
                }

                auto value = value_viewer.value(i);

                // Check if key is null
                if (key_viewer.is_null(i)) {
                    // Null key with non-null value: include as {null: value}
                    if (has_null_key) {
                        // Sum with existing null key value
                        value_result_column->get_data()[null_key_value_idx] += value;
                    } else {
                        // First null key
                        value_result_column->get_data().push_back(value);
                        null_key_value_idx = value_result_column->size() - 1;
                        has_null_key = true;
                    }
                } else {
                    // Non-null key
                    auto key = key_viewer.value(i);

                    if (hash_map.contains(key)) {
                        // Sum the value with existing value (compile-time dispatch)
                        auto idx = hash_map[key];
                        value_result_column->get_data()[idx] += value;
                    } else {
                        // Insert new key-value pair
                        value_result_column->get_data().push_back(value);
                        hash_map.emplace(key, value_result_column->size() - 1);
                    }
                }
            }
        } else {
            // String keys
            for (size_t i = offset; i < offset + count; i++) {
                // If value is null, skip this entry entirely
                if (value_viewer.is_null(i)) {
                    continue;
                }

                auto value = value_viewer.value(i);

                // Check if key is null
                if (key_viewer.is_null(i)) {
                    // Null key with non-null value: include as {null: value}
                    if (has_null_key) {
                        // Sum with existing null key value
                        value_result_column->get_data()[null_key_value_idx] += value;
                    } else {
                        // First null key
                        value_result_column->get_data().push_back(value);
                        null_key_value_idx = value_result_column->size() - 1;
                        has_null_key = true;
                    }
                } else {
                    // Non-null key
                    auto raw_key = key_viewer.value(i);
                    KeyType key(raw_key);

                    if (hash_map.contains(key)) {
                        // Sum the value with existing value (compile-time dispatch)
                        auto idx = hash_map[key];
                        value_result_column->get_data()[idx] += value;

                    } else {
                        // Insert new key-value pair
                        uint8_t* pos = mem_pool->allocate(key.size);
                        memcpy(pos, key.data, key.size);
                        value_result_column->get_data().push_back(value);
                        hash_map.emplace(Slice(pos, key.size), value_result_column->size() - 1);
                    }
                }
            }
        }
    }
};

template <LogicalType KT, LogicalType VT, typename MyHashMap = std::map<int, size_t>>
class SumMapAggregateFunction final
        : public AggregateFunctionBatchHelper<SumMapAggregateFunctionState<KT, VT, MyHashMap>,
                                              SumMapAggregateFunction<KT, VT, MyHashMap>> {
public:
    using KeyColumnType = RunTimeColumnType<KT>;
    using ValueColumnType = RunTimeColumnType<VT>;
    using ValueCppType = RunTimeCppType<VT>;
    using ValueResultColumnType = RunTimeColumnType<SumResultLT<VT>>;
    using ValueResultCppType = RunTimeCppType<SumResultLT<VT>>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[0]->is_nullable());

        const auto* map_column = down_cast<const MapColumn*>(ColumnHelper::get_data_column(columns[0]));
        auto& offsets = map_column->offsets().immutable_data();

        if (offsets[row_num + 1] > offsets[row_num]) {
            // Pass the key and value columns as ColumnPtr (they may be nullable)
            ColumnViewer<KT> key_viewer(map_column->keys_column());
            ColumnViewer<VT> value_viewer(map_column->values_column());

            this->data(state).update(ctx->mem_pool(), key_viewer, value_viewer, offsets[row_num],
                                     offsets[row_num + 1] - offsets[row_num]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(!column->is_nullable());
        const auto* map_column = down_cast<const MapColumn*>(ColumnHelper::get_data_column(column));
        auto& offsets = map_column->offsets().immutable_data();

        if (offsets[row_num + 1] > offsets[row_num]) {
            // Pass the key and value columns as ColumnPtr (they may be nullable)
            ColumnViewer<KT> key_viewer(map_column->keys_column());
            ColumnViewer<SumResultLT<VT>> value_viewer(map_column->values_column());
            this->data(state).update(ctx->mem_pool(), key_viewer, value_viewer, offsets[row_num],
                                     offsets[row_num + 1] - offsets[row_num]);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable());
        auto& state_impl = this->data(state);
        auto* map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));

        size_t total_entries = state_impl.hash_map.size() + (state_impl.has_null_key ? 1 : 0);
        auto* key_column = map_column->keys_column_raw_ptr();
        auto* value_column = map_column->values_column_raw_ptr();

        // Serialize null key entry if it exists
        if (state_impl.has_null_key) {
            append_null_key(key_column);
            append_value_data(value_column, state_impl.value_result_column->get_data()[state_impl.null_key_value_idx]);
        }

        // Serialize regular key-value entries
        for (const auto& entry : state_impl.hash_map) {
            append_key(key_column, entry.first);
            append_value_data(value_column, state_impl.value_result_column->get_data()[entry.second]);
        }

        // Update null flags and offsets for the map column
        finalize_map_column(to, map_column, total_entries);
    }

private:
    // Helper: Append a null key to the key column
    void append_null_key(Column* key_column) const {
        DCHECK(key_column->is_nullable());
        key_column->append_nulls(1);
    }

    // Helper: Append a regular (non-null) key to the key column
    template <typename KeyValue>
    void append_key(Column* key_column, const KeyValue& key) const {
        DCHECK(key_column->is_nullable());
        auto* nullable_key = down_cast<NullableColumn*>(key_column);
        auto* key_data_col = down_cast<KeyColumnType*>(nullable_key->data_column()->as_mutable_raw_ptr());
        append_key_data(key_data_col, key);
        nullable_key->null_column_data().push_back(0);
    }

    // Helper: Append key value based on key type (string vs non-string)
    template <typename KeyValue>
    void append_key_data(KeyColumnType* key_data_col, const KeyValue& key) const {
        if constexpr (lt_is_string<KT>) {
            key_data_col->append(Slice(key.data, key.size));
        } else {
            key_data_col->append(key);
        }
    }

    void append_value_data(Column* value_data_col, ValueResultCppType value) const {
        DCHECK(value_data_col->is_nullable());
        auto* nullable_value_col = down_cast<NullableColumn*>(value_data_col);
        auto* data_column = down_cast<ValueResultColumnType*>(nullable_value_col->data_column()->as_mutable_raw_ptr());
        data_column->get_data().push_back(value);
        nullable_value_col->null_column_data().push_back(0);
    }

    // Helper: Finalize the map column by updating null flags and offsets
    void finalize_map_column(Column* to, MapColumn* map_column, size_t total_entries) const {
        // Update map offsets
        auto* offsets_col = map_column->offsets_column_raw_ptr();
        offsets_col->append(offsets_col->immutable_data().back() + total_entries);
    }

public:
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        DCHECK(!src[0]->is_nullable());
        DCHECK(!dst->is_nullable());

        const auto* src_map_column = down_cast<const MapColumn*>(ColumnHelper::get_data_column(src[0]));
        auto* dst_map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(dst.get()));
        ColumnViewer<VT> value_viewer(src_map_column->values_column());

        size_t element_size = src_map_column->keys().size();
        // append keys
        dst_map_column->keys_column()->as_mutable_raw_ptr()->append(*src_map_column->keys_column(), 0, element_size);

        DCHECK(dst_map_column->values_column()->is_nullable());

        NullableColumn* nullable_dst_values_column =
                down_cast<NullableColumn*>(dst_map_column->values_column_raw_ptr());
        auto& null_data = nullable_dst_values_column->null_column_data();
        ValueResultColumnType* dst_values_column =
                down_cast<ValueResultColumnType*>(nullable_dst_values_column->data_column()->as_mutable_raw_ptr());
        auto& dst_value_data = dst_values_column->get_data();
        for (size_t i = 0; i < element_size; ++i) {
            bool is_null = value_viewer.is_null(i);
            null_data.push_back(is_null);
            dst_value_data.push_back(value_viewer.value(i));
            nullable_dst_values_column->set_has_null(is_null);
        }

        auto& dst_offsets = dst_map_column->offsets().get_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            dst_offsets.push_back(dst_offsets.back() + src_map_column->get_map_size(i));
        }

        dst_map_column->check_or_die();
    }

    std::string get_name() const override { return "sum_map"; }
};

} // namespace starrocks
