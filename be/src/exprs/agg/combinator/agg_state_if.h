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

#include <utility>

#include "column/column_helper.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/combinator/agg_state_combinator.h"
#ifdef __x86_64__
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

namespace starrocks {
struct AggStateIfState {};

class AggStateIf final : public AggStateCombinator<AggStateIfState, AggStateIf> {
public:
    AggStateIf(AggStateDesc agg_state_desc, const AggregateFunction* function)
            : AggStateCombinator(std::move(agg_state_desc), function) {
        DCHECK(_function != nullptr);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        throw std::runtime_error("agg if doesn't implement update");
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        _function->merge(ctx, column, state, row_num);
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _function->serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& srcs, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto column_size = ctx->get_num_args() + 1;
        const Column* data_columns[column_size - 1];
        ColumnPtr new_nullable_column;
        std::vector<const Column*> column_ptrs(column_size);
        for (size_t i = 0; i < srcs.size(); ++i) {
            column_ptrs[i] = srcs[i].get();
        }
        update_help(ctx, chunk_size, column_ptrs.data(), data_columns, new_nullable_column);

        Columns filtered_columns;
        for (size_t i = 0; i < column_size - 1; ++i) {
            filtered_columns.push_back(data_columns[i]->get_ptr());
        }

        _function->convert_to_serialize_format(ctx, filtered_columns, chunk_size, dst);
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        _function->finalize_to_column(ctx, state, to);
    }

    // override batch interface for better performance
    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        auto column_size = ctx->get_num_args() + 1;
        const Column* data_columns[column_size - 1];
        ColumnPtr new_nullable_column;
        update_help(ctx, chunk_size, columns, data_columns, new_nullable_column);
        _function->update_batch(ctx, chunk_size, state_offset, data_columns, states);
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const Filter& filter) const override {
        auto column_size = ctx->get_num_args() + 1;
        const Column* data_columns[column_size - 1];
        ColumnPtr new_nullable_column;
        update_help(ctx, chunk_size, columns, data_columns, new_nullable_column);
        _function->update_batch_selectively(ctx, chunk_size, state_offset, data_columns, states, filter);
    }

    void update_help(FunctionContext* ctx, size_t chunk_size, const Column** columns, const Column** replace_columns,
                     ColumnPtr& new_nullable_column) const {
        auto fake_null_column = NullColumn::create(columns[0]->size(), 0);
        uint8_t* __restrict fake_null_column_raw_data = fake_null_column->mutable_raw_data();

        auto column_size = ctx->get_num_args() + 1;
        DCHECK(column_size >= 2);

        // step 1: merge predicate_column into fake_null_column
        size_t predicate_col_index = column_size - 1;
        ColumnPtr predicate_column = ColumnHelper::unpack_and_duplicate_const_column(
                chunk_size, columns[predicate_col_index]->as_mutable_ptr());
        if (predicate_column->is_nullable()) {
            const NullableColumn* nullable_predicate_column = down_cast<const NullableColumn*>(predicate_column.get());
            size_t nullCount = nullable_predicate_column->null_count();

            if (nullCount == 0) {
                const uint8_t* __restrict nullable_predicate_data_col_raw_data;
                nullable_predicate_data_col_raw_data =
                        ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(nullable_predicate_column->data_column())->raw_data();
                for (size_t i = 0; i < chunk_size; ++i) {
                    // false is 0, but null is 1
                    fake_null_column_raw_data[i] = !nullable_predicate_data_col_raw_data[i];
                }
            } else if (nullCount == predicate_column->size()) {
                fake_null_column = NullColumn::create(columns[0]->size(), 1);
            } else {
                const auto nullable_predicate_null_col_data = nullable_predicate_column->immutable_null_column_data();
                const auto nullable_predicate_data_col_data =
                        down_cast<const UInt8Column*>(nullable_predicate_column->immutable_data_column())
                                ->immutable_data();
                // we treat false(0) as null(which is 1)
                for (size_t i = 0; i < chunk_size; ++i) {
                    fake_null_column_raw_data[i] = static_cast<uint8_t>((!nullable_predicate_data_col_data[i]) ||
                                                                        nullable_predicate_null_col_data[i]);
                }
            }
        } else {
            const uint8_t* __restrict predicate_column_raw_data =
                    ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(predicate_column)->raw_data();
            for (size_t i = 0; i < chunk_size; ++i) {
                fake_null_column_raw_data[i] = !predicate_column_raw_data[i];
            }
        }

        // agg_state_if's _function always be nullable version of agg function(like NullableAggregateFunctionUnary)
        // so we merge filter column into agg function's arg column, and leave others to nullable agg function
        // by this, we can only call batch interface to avoid too many virtual function call
        const NullableColumn* first_nullable_arg_col = nullptr;
        size_t first_nullable_arg_col_index = 0;
        // pick the first nullable arg
        for (int i = 0; i < column_size - 1; i++) {
            if (columns[i]->is_nullable()) {
                first_nullable_arg_col = down_cast<const NullableColumn*>(columns[i]);
                first_nullable_arg_col_index = i;
                break;
            }
        }

        ColumnPtr data_column;
        if (first_nullable_arg_col == nullptr) {
            // if all not-null, pick the first column
            data_column = const_cast<Column*>(columns[0])->get_ptr();
        } else {
            data_column = first_nullable_arg_col;
            if (first_nullable_arg_col->has_null()) {
                // step 2: merge first_nullable_arg_col(if exsited)'s null_column into fake_null_column
                const uint8_t* __restrict nulls = first_nullable_arg_col->immutable_null_column_data().data();
                // merge two null column
                ColumnHelper::or_two_filters(&fake_null_column->get_data(), nulls);
                data_column = first_nullable_arg_col;
            }
        }

        data_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, data_column);
        if (data_column->is_nullable()) {
            NullableColumn* original_nullable_column =
                    const_cast<NullableColumn*>(down_cast<const NullableColumn*>(data_column.get()));
            new_nullable_column =
                    NullableColumn::create(original_nullable_column->data_column_mutable_ptr(), fake_null_column);
        } else {
            new_nullable_column = NullableColumn::create(data_column, fake_null_column);
        }

        for (int i = 0; i < column_size - 1; i++) {
            if (i == first_nullable_arg_col_index) {
                replace_columns[i] = new_nullable_column.get();
            } else {
                replace_columns[i] = columns[i];
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto column_size = ctx->get_num_args() + 1;
        const Column* data_columns[column_size - 1];
        ColumnPtr new_nullable_column;
        update_help(ctx, chunk_size, columns, data_columns, new_nullable_column);
        return _function->update_batch_single_state(ctx, chunk_size, data_columns, state);
    }

    // merge‘s logic is same with _function
    void merge_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        _function->merge_batch(ctx, chunk_size, state_offset, column, states);
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const Filter& filter) const override {
        _function->merge_batch_selectively(ctx, chunk_size, state_offset, column, states, filter);
    }

    void merge_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column* input, size_t start,
                                  size_t size) const override {
        _function->merge_batch_single_state(ctx, state, input, start, size);
    }

    std::string get_name() const override { return "agg_state_if"; }
};
} // namespace starrocks