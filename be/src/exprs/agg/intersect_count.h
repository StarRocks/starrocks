// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cmath>

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/bitmap_value.h"
#include "udf/udf.h"
#include "util/bitmap_intersect.h"

namespace starrocks::vectorized {
template <PrimitiveType PT, typename = guard::Guard>
inline constexpr PrimitiveType IntersectCountResultPT = TYPE_BIGINT;

template <typename T>
struct BitmapIntersectAggregateState {
    mutable BitmapIntersect<T> intersect;
    bool initial = false;
};

template <PrimitiveType PT>
struct BitmapIntersectInternalKey {
    using InternalKeyType = RunTimeCppType<PT>;
};

template <>
struct BitmapIntersectInternalKey<TYPE_VARCHAR> {
    using InternalKeyType = std::string;
};

template <>
struct BitmapIntersectInternalKey<TYPE_CHAR> {
    using InternalKeyType = std::string;
};

template <PrimitiveType PT>
using BitmapRuntimeCppType = typename BitmapIntersectInternalKey<PT>::InternalKeyType;

template <PrimitiveType PT, typename T = BitmapRuntimeCppType<PT>, PrimitiveType ResultPT = IntersectCountResultPT<PT>,
          typename TResult = RunTimeCppType<ResultPT>>
class IntersectCountAggregateFunction
        : public AggregateFunctionBatchHelper<BitmapIntersectAggregateState<BitmapRuntimeCppType<PT>>,
                                              IntersectCountAggregateFunction<PT, T, ResultPT, TResult>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;
    using ResultColumnType = RunTimeColumnType<ResultPT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {}

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_object());

        auto& intersect = this->data(state).intersect;
        if (!this->data(state).initial) {
            for (int i = 2; i < ctx->get_num_constant_columns(); ++i) {
                auto arg_column = ctx->get_constant_column(i);
                auto arg_value = ColumnHelper::get_const_value<PT>(arg_column);
                if constexpr (PT != TYPE_VARCHAR && PT != TYPE_CHAR) {
                    intersect.add_key(arg_value);
                } else {
                    std::string key(arg_value.data, arg_value.size);
                    intersect.add_key(key);
                }
            }
            this->data(state).initial = true;
        }

        const BitmapColumn* bitmap_column = down_cast<const BitmapColumn*>(columns[0]);

        // based on NullableAggregateFunctionVariadic.
        const InputColumnType* key_column = down_cast<const InputColumnType*>(columns[1]);

        auto bimtap_value = bitmap_column->get_pool()[row_num];
        auto key_value = key_column->get_data()[row_num];

        if constexpr (PT != TYPE_VARCHAR && PT != TYPE_CHAR) {
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
        BitmapIntersect<BitmapRuntimeCppType<PT>> intersect;
        for (int i = 2; i < src.size(); ++i) {
            auto arg_value = ColumnHelper::get_const_value<PT>(src[i]);
            if constexpr (PT != TYPE_VARCHAR && PT != TYPE_CHAR) {
                intersect.add_key(arg_value);
            } else {
                std::string key(arg_value.data, arg_value.size);
                intersect.add_key(key);
            }
        }

        const BitmapColumn* bitmap_column = down_cast<const BitmapColumn*>(src[0].get());
        const InputColumnType* key_column = down_cast<const InputColumnType*>(src[1].get());

        // compute bytes for serialization for this chunk.
        int new_size = 0;
        std::vector<BitmapIntersect<BitmapRuntimeCppType<PT>>> intersect_chunks;
        intersect_chunks.reserve(chunk_size);
        for (int i = 0; i < chunk_size; ++i) {
            BitmapIntersect<BitmapRuntimeCppType<PT>> intersect_per_row(intersect);

            auto bimtap_value = bitmap_column->get_pool()[i];
            auto key_value = key_column->get_data()[i];

            if constexpr (PT != TYPE_VARCHAR && PT != TYPE_CHAR) {
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

} // namespace starrocks::vectorized
