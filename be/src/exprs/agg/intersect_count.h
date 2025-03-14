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

#include <cmath>

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"
#include "util/bitmap_intersect.h"

namespace starrocks {
template <LogicalType LT, typename = guard::Guard>
inline constexpr LogicalType IntersectCountResultLT = TYPE_BIGINT;

template <typename T>
struct BitmapIntersectAggregateState {
    mutable BitmapIntersect<T> intersect;
    bool initial = false;
};

template <LogicalType LT>
struct BitmapIntersectInternalKey {
    using InternalKeyType = RunTimeCppType<LT>;
};

template <>
struct BitmapIntersectInternalKey<TYPE_VARCHAR> {
    using InternalKeyType = std::string;
};

template <>
struct BitmapIntersectInternalKey<TYPE_CHAR> {
    using InternalKeyType = std::string;
};

template <LogicalType LT>
using BitmapRuntimeCppType = typename BitmapIntersectInternalKey<LT>::InternalKeyType;

template <LogicalType LT, typename T = BitmapRuntimeCppType<LT>, LogicalType ResultLT = IntersectCountResultLT<LT>,
          typename TResult = RunTimeCppType<ResultLT>>
class IntersectCountAggregateFunction final
        : public AggregateFunctionBatchHelper<BitmapIntersectAggregateState<BitmapRuntimeCppType<LT>>,
                                              IntersectCountAggregateFunction<LT, T, ResultLT, TResult>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {}

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_object());

        auto& intersect = this->data(state).intersect;
        if (!this->data(state).initial) {
            for (int i = 2; i < ctx->get_num_constant_columns(); ++i) {
                auto arg_column = ctx->get_constant_column(i);
                auto arg_value = ColumnHelper::get_const_value<LT>(arg_column);
                if constexpr (LT != TYPE_VARCHAR && LT != TYPE_CHAR) {
                    intersect.add_key(arg_value);
                } else {
                    std::string key(arg_value.data, arg_value.size);
                    intersect.add_key(key);
                }
            }
            this->data(state).initial = true;
        }

        const auto* bitmap_column = down_cast<const BitmapColumn*>(columns[0]);

        // based on NullableAggregateFunctionVariadic.
        const auto* key_column = down_cast<const InputColumnType*>(columns[1]);

        auto bimtap_value = bitmap_column->get_pool()[row_num];
        auto key_value = key_column->get_data()[row_num];

        if constexpr (LT != TYPE_VARCHAR && LT != TYPE_CHAR) {
            intersect.update(key_value, bimtap_value);
        } else {
            std::string key(key_value.data, key_value.size);
            intersect.update(key, bimtap_value);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto& intersect = this->data(state).intersect;
        Slice slice = column->get(row_num).get_slice();
        intersect.merge(BitmapIntersect<T>((char*)slice.data));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto& intersect = this->data(state).intersect;

        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        size_t old_size = bytes.size();
        size_t new_size = old_size + intersect.size();
        bytes.resize(new_size);

        intersect.serialize(reinterpret_cast<char*>(bytes.data() + old_size));

        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK(src[0]->is_object());

        // initial keys in BitmapIntersect.
        BitmapIntersect<BitmapRuntimeCppType<LT>> intersect;
        for (int i = 2; i < src.size(); ++i) {
            auto arg_value = ColumnHelper::get_const_value<LT>(src[i]);
            if constexpr (LT != TYPE_VARCHAR && LT != TYPE_CHAR) {
                intersect.add_key(arg_value);
            } else {
                std::string key(arg_value.data, arg_value.size);
                intersect.add_key(key);
            }
        }

        const auto* bitmap_column = down_cast<const BitmapColumn*>(src[0].get());
        const auto* key_column = down_cast<const InputColumnType*>(src[1].get());

        // compute bytes for serialization for this chunk.
        int new_size = 0;
        std::vector<BitmapIntersect<BitmapRuntimeCppType<LT>>> intersect_chunks;
        intersect_chunks.reserve(chunk_size);
        for (int i = 0; i < chunk_size; ++i) {
            BitmapIntersect<BitmapRuntimeCppType<LT>> intersect_per_row(intersect);

            auto bimtap_value = bitmap_column->get_pool()[i];
            auto key_value = key_column->get_data()[i];

            if constexpr (LT != TYPE_VARCHAR && LT != TYPE_CHAR) {
                intersect_per_row.update(key_value, bimtap_value);
            } else {
                std::string key(key_value.data, key_value.size);
                intersect_per_row.update(key, bimtap_value);
            }

            new_size += intersect_per_row.size();

            // intersect_per_row has one bitmapValue at most.
            // Or has one empty bitmap.
            intersect_chunks.emplace_back(intersect_per_row);
        }

        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        size_t old_size = bytes.size();
        bytes.resize(new_size);

        dst_column->get_offset().resize(chunk_size + 1);

        // serialize for every row of this chunk.
        for (int i = 0; i < chunk_size; ++i) {
            auto& intersect = intersect_chunks[i];
            intersect.serialize(reinterpret_cast<char*>(bytes.data() + old_size));
            old_size += intersect.size();
            dst_column->get_offset()[i + 1] = old_size;
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& intersect = this->data(state).intersect;
        down_cast<ResultColumnType*>(to)->append(intersect.intersect_count());
    }

    std::string get_name() const override { return "intersect count"; }
};

} // namespace starrocks
