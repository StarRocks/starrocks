// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <set>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace starrocks::vectorized {

template <PrimitiveType PT, bool is_distinct, typename MyHashSet = std::set<int>>
struct ArrayAggAggregateState {
    using ColumnType = RunTimeColumnType<PT>;
    using CppType = RunTimeCppType<PT>;
    using KeyType = typename SliceHashSet::key_type;
    void update(MemPool* mem_pool, const ColumnType& column, size_t offset, size_t count) {
        if constexpr (is_distinct) {
            if constexpr (pt_is_string<PT>) {
                for (int i = 0; i < count; i++) {
                    auto raw_key = column.get_slice(offset + i);
                    if (set.find(raw_key) == set.end()) {
                        data_column.append(column, offset + i, 1);
                        KeyType key(raw_key);
                        set.template lazy_emplace(key, [&](const auto& ctor) {
                            uint8_t* pos = mem_pool->allocate(key.size);
                            assert(pos != nullptr);
                            memcpy(pos, key.data, key.size);
                            ctor(pos, key.size, key.hash);
                        });
                    }
                }
            } else {
                for (int i = 0; i < count; i++) {
                    if (set.find(column.get_data()[offset + i]) == set.end()) {
                        data_column.append(column, offset + i, 1);
                        set.emplace(data_column.get_data()[data_column.size() - 1]);
                    }
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

    ColumnType data_column; // Aggregated elements for array_agg
    size_t null_count = 0;
    MyHashSet set;
};

template <PrimitiveType PT, bool is_distinct, typename MyHashSet = std::set<int>>
class ArrayAggAggregateFunction
        : public AggregateFunctionBatchHelper<ArrayAggAggregateState<PT, is_distinct, MyHashSet>,
                                              ArrayAggAggregateFunction<PT, is_distinct, MyHashSet>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        // TODO: update is random access, so we could not pre-reserve memory for State, which is the bottleneck
        this->data(state).update(ctx->impl()->mem_pool(), column, row_num, 1);
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        this->data(state).append_null();
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        // Array element is nullable, so we need to extract the data from nullable column first
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        auto offset_size = input_column->get_element_offset_size(row_num);
        auto& array_element = down_cast<const NullableColumn&>(input_column->elements());
        auto* element_data_column = down_cast<const InputColumnType*>(ColumnHelper::get_data_column(&array_element));
        size_t element_null_count = array_element.null_count(offset_size.first, offset_size.second);
        DCHECK_LE(element_null_count, offset_size.second);

        this->data(state).update(ctx->impl()->mem_pool(), *element_data_column, offset_size.first,
                                 offset_size.second - element_null_count);
        this->data(state).append_null(element_null_count);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(state);
        auto* column = down_cast<ArrayColumn*>(to);
        column->append_array_element(state_impl.data_column, state_impl.null_count);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        return serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<ArrayColumn*>(dst->get());
        auto& offsets = column->offsets_column()->get_data();
        auto& elements_column = column->elements_column();

        for (size_t i = 0; i < chunk_size; i++) {
            elements_column->append_datum(src[0]->get(i));
            offsets.emplace_back(offsets.back() + 1);
        }
    }

    std::string get_name() const override { return is_distinct ? "array_agg_distinct" : "array_agg"; }
};

} // namespace starrocks::vectorized
