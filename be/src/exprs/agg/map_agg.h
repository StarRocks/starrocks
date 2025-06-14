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

#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/hash_set.h"
#include "column/map_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "util/phmap/phmap.h"
#include "util/time.h"

namespace starrocks {

template <LogicalType KT, typename MyHashMap = std::map<int, size_t>>
struct MapAggAggregateFunctionState : public AggregateFunctionEmptyState {
    using KeyColumnType = RunTimeColumnType<KT>;
    using KeyType = typename SliceHashSet::key_type;

    MyHashMap hash_map;
    // Use column to store the values in case that the reference of the Slices disappears.
    ColumnPtr value_column;

    void update(MemPool* mem_pool, const KeyColumnType& arg_key_column, const Column& arg_value_column, size_t offset,
                size_t count) {
        if constexpr (!lt_is_string<KT>) {
            for (int i = offset; i < offset + count; i++) {
                auto key = arg_key_column.get_data()[i];
                if (!hash_map.contains(key)) {
                    auto value = arg_value_column.get(i);
                    value_column->append_datum(value);
                    hash_map.emplace(key, value_column->size() - 1);
                }
            }
        } else {
            for (int i = offset; i < offset + count; i++) {
                auto raw_key = arg_key_column.get_slice(i);
                KeyType key(raw_key);
                if (!hash_map.contains(key)) {
                    uint8_t* pos = mem_pool->allocate(key.size);
                    memcpy(pos, key.data, key.size);
                    auto value = arg_value_column.get(i);
                    value_column->append_datum(value);
                    hash_map.emplace(Slice(pos, key.size), value_column->size() - 1);
                }
            }
        }
    }
};

template <LogicalType KT, typename MyHashMap = std::map<int, size_t>>
class MapAggAggregateFunction final : public AggregateFunctionBatchHelper<MapAggAggregateFunctionState<KT, MyHashMap>,
                                                                          MapAggAggregateFunction<KT, MyHashMap>> {
public:
    using KeyColumnType = RunTimeColumnType<KT>;

    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        auto* state = new (ptr) MapAggAggregateFunctionState<KT, MyHashMap>;
        state->value_column = ctx->create_column(*ctx->get_arg_type(1), true);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        // Key could not be null.
        if ((columns[0]->is_nullable() && columns[0]->is_null(row_num)) || columns[0]->only_null()) {
            return;
        }
        const auto& key_column = down_cast<const KeyColumnType&>(*ColumnHelper::get_data_column(columns[0]));
        this->data(state).update(ctx->mem_pool(), key_column, *columns[1], row_num, 1);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto map_column = down_cast<const MapColumn*>(ColumnHelper::get_data_column(column));
        auto& offsets = map_column->offsets().get_data();
        if (offsets[row_num + 1] > offsets[row_num]) {
            this->data(state).update(
                    ctx->mem_pool(),
                    *down_cast<const KeyColumnType*>(ColumnHelper::get_data_column(map_column->keys_column().get())),
                    map_column->values(), offsets[row_num], offsets[row_num + 1] - offsets[row_num]);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        auto* map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));

        auto elem_size = state_impl.hash_map.size();
        auto* key_column = down_cast<KeyColumnType*>(ColumnHelper::get_data_column(map_column->keys_column().get()));
        if constexpr (lt_is_string<KT>) {
            for (const auto& entry : state_impl.hash_map) {
                key_column->append(Slice(entry.first.data, entry.first.size));
                map_column->values_column()->append_datum(state_impl.value_column->get(entry.second));
            }
        } else {
            for (const auto& entry : state_impl.hash_map) {
                key_column->append(entry.first);
                map_column->values_column()->append_datum(state_impl.value_column->get(entry.second));
            }
        }

        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
        if (map_column->keys_column()->is_nullable()) {
            // Key could not be NULL.
            auto* nullable_column = down_cast<NullableColumn*>(map_column->keys_column().get());
            nullable_column->null_column_data().resize(nullable_column->null_column_data().size() + elem_size);
        }

        auto& offsets = map_column->offsets_column()->get_data();
        offsets.push_back(offsets.back() + elem_size);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<MapColumn*>(ColumnHelper::get_data_column(dst->get()));
        auto key_column = column->keys_column();
        auto value_column = column->values_column();
        auto& offsets = column->offsets_column()->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            if ((src[0]->is_nullable() && src[0]->is_null(i)) || src[0]->only_null()) {
                offsets.push_back(offsets.back());
                continue;
            }
            key_column->append(*src[0], i, 1);
            value_column->append(*src[1], i, 1);
            offsets.push_back(offsets.back() + 1);
        }
        if (dst->get()->is_nullable()) {
            down_cast<NullableColumn*>(dst->get())->null_column_data().resize(column->size());
        }
    }

    std::string get_name() const override { return "map_agg"; }
};

} // namespace starrocks
