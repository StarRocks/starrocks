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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exec/sorting/sorting.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/defer_op.h"

namespace starrocks {

template <LogicalType PT, bool is_distinct, typename MyHashSet = std::set<int>>
struct ArrayUnionAggAggregateState {
    using ColumnType = RunTimeColumnType<PT>;
    using CppType = RunTimeCppType<PT>;
    using KeyType = typename SliceHashSet::key_type;
    void update(MemPool* mem_pool, const ColumnType& column, size_t offset, size_t count) {
        if constexpr (is_distinct) {
            if constexpr (lt_is_string<PT>) {
                for (int i = 0; i < count; i++) {
                    auto raw_key = column.get_slice(offset + i);
                    KeyType key(raw_key);
                    set.template lazy_emplace(key, [&](const auto& ctor) {
                        uint8_t* pos = mem_pool->allocate(key.size);
                        assert(pos != nullptr);
                        memcpy(pos, key.data, key.size);
                        ctor(pos, key.size, key.hash);
                    });
                }
            } else {
                for (int i = 0; i < count; i++) {
                    set.emplace(column.get_data()[offset + i]);
                }
            }
        } else {
            data_column.append(column, offset, count);
        }
    }

    void append_null() {
        if constexpr (is_distinct) {
            null_count = 1;
        } else {
            null_count++;
        }
    }

    void append_null(size_t count) {
        if constexpr (is_distinct) {
            if (count > 0) {
                null_count = 1;
            }
        } else {
            null_count += count;
        }
    }

    ColumnType* get_data_column() {
        auto size = set.size();
        if (data_column.size() > 0 || size == 0) {
            return &data_column;
        }
        data_column.get_data().reserve(size);
        if constexpr (is_distinct) {
            if constexpr (lt_is_string<PT>) {
                for (auto& key : set) {
                    data_column.append(Slice(key.data, key.size));
                }
            } else {
                for (auto& key : set) {
                    data_column.append(key);
                }
            }
        }
        return &data_column;
    }

    ColumnType data_column; // Aggregated elements for array_agg
    size_t null_count = 0;
    MyHashSet set;
};

template <LogicalType LT, bool is_distinct, typename MyHashSet = std::set<int>>
class ArrayUnionAggAggregateFunction
        : public AggregateFunctionBatchHelper<ArrayUnionAggAggregateState<LT, is_distinct, MyHashSet>,
                                              ArrayUnionAggAggregateFunction<LT, is_distinct, MyHashSet>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void update_state(FunctionContext* ctx, const ArrayColumn* input_column, AggDataPtr __restrict state,
                      size_t row_num) const {
        // Array element is nullable, so we need to extract the data from nullable column first
        auto offset_size = input_column->get_element_offset_size(row_num);
        auto& array_element = down_cast<const NullableColumn&>(input_column->elements());
        auto* element_data_column = down_cast<const InputColumnType*>(ColumnHelper::get_data_column(&array_element));
        size_t element_null_count = array_element.null_count(offset_size.first, offset_size.second);
        DCHECK_LE(element_null_count, offset_size.second);

        if (element_null_count == 0) {
            this->data(state).update(ctx->mem_pool(), *element_data_column, offset_size.first, offset_size.second);
        } else {
            for (size_t i = offset_size.first; i < offset_size.first + offset_size.second; i++) {
                if (!array_element.is_null(i)) {
                    this->data(state).update(ctx->mem_pool(), *element_data_column, i, 1);
                }
            }
        }

        this->data(state).append_null(element_null_count);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(columns[0]);
        update_state(ctx, input_column, state, row_num);
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        this->data(state).append_null();
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        update_state(ctx, input_column, state, row_num);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(const_cast<AggDataPtr>(state));
        auto* column = down_cast<ArrayColumn*>(to);
        column->append_array_element(*(state_impl.get_data_column()), state_impl.null_count);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        return serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        (*dst)->append(*(src[0].get()));
    }

    std::string get_name() const override { return is_distinct ? "array_unique_agg" : "array_union_agg"; }
};

} // namespace starrocks
