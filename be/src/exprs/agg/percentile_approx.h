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

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/percentile_value.h"
#include "util/tdigest.h"

namespace starrocks {

struct PercentileApproxState {
public:
    PercentileApproxState() : percentile(new PercentileValue()) {}
    ~PercentileApproxState() = default;

    std::unique_ptr<PercentileValue> percentile;
    double targetQuantile = -1.0;
    bool is_null = true;
};

class PercentileApproxAggregateFunction final
        : public AggregateFunctionBatchHelper<PercentileApproxState, PercentileApproxAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        double column_value;
        if (columns[0]->is_nullable()) {
            if (columns[0]->is_null(row_num)) {
                return;
            }
            column_value = down_cast<const NullableColumn*>(columns[0])->data_column()->get(row_num).get_double();
        } else {
            column_value = down_cast<const DoubleColumn*>(columns[0])->get_data()[row_num];
        }

        DCHECK(!columns[1]->only_null());
        DCHECK(!columns[1]->is_null(0));

        data(state).percentile->add(implicit_cast<float>(column_value));
        data(state).targetQuantile = columns[1]->get(0).get_double();
        data(state).is_null = false;
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        Slice src;
        if (column->is_nullable()) {
            if (column->is_null(row_num)) {
                return;
            }
            const auto* nullable_column = down_cast<const NullableColumn*>(column);
            src = nullable_column->data_column()->get(row_num).get_slice();
        } else {
            const auto* binary_column = down_cast<const BinaryColumn*>(column);
            src = binary_column->get_slice(row_num);
        }
        double quantile;
        memcpy(&quantile, src.data, sizeof(double));

        PercentileApproxState src_percentile;
        src_percentile.targetQuantile = quantile;
        src_percentile.percentile->deserialize((char*)src.data + sizeof(double));

        data(state).percentile->merge(src_percentile.percentile.get());
        data(state).targetQuantile = quantile;
        data(state).is_null = false;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        size_t size = data(state).percentile->serialize_size();
        uint8_t result[size + sizeof(double)];
        memcpy(result, &(data(state).targetQuantile), sizeof(double));
        data(state).percentile->serialize(result + sizeof(double));

        if (to->is_nullable()) {
            auto* column = down_cast<NullableColumn*>(to);
            if (data(state).is_null) {
                column->append_default();
            } else {
                down_cast<BinaryColumn*>(column->data_column().get())->append(Slice(result, size));
                column->null_column_data().push_back(0);
            }
        } else {
            auto* column = down_cast<BinaryColumn*>(to);
            column->append(Slice(result, size));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const DoubleColumn* input = nullptr;
        BinaryColumn* result = nullptr;
        // get input data column
        if (src[0]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(src[0].get());
            input = down_cast<const DoubleColumn*>(nullable_column->data_column().get());

            auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
            result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());
            dst_nullable_column->null_column_data() = nullable_column->immutable_null_column_data();
        } else {
            input = down_cast<const DoubleColumn*>(src[0].get());
            // Even if the input column is non-nullable, the result column still could be nullable
            if ((*dst)->is_nullable()) {
                auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());
                dst_nullable_column->null_column_data().resize(chunk_size, 0);
            } else {
                result = down_cast<BinaryColumn*>((*dst).get());
            }
        }

        // get const arg
        DCHECK(src[1]->is_constant());
        const auto* const_column = down_cast<const ConstColumn*>(src[1].get());
        double quantile = const_column->get(0).get_double();

        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 20);
        result->get_offset().resize(chunk_size + 1);

        // serialize percentile one by one
        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (src[0]->is_null(i)) {
                auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                dst_nullable_column->set_has_null(true);
                result->get_offset()[i + 1] = old_size;
            } else {
                PercentileValue percentile;
                percentile.add(input->get_data()[i]);

                size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
                bytes.resize(new_size);
                memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                percentile.serialize(bytes.data() + old_size + sizeof(double));

                result->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(to);
            if (data(state).is_null) {
                nullable_column->append_default();
                return;
            }

            double result = data(state).percentile->quantile(data(state).targetQuantile);
            (void)nullable_column->data_column()->append_numbers(&result, sizeof(result));
            nullable_column->null_column_data().push_back(0);
        } else {
            auto* data_column = down_cast<DoubleColumn*>(to);
            if (data(state).is_null) {
                return;
            }

            double result = data(state).percentile->quantile(data(state).targetQuantile);
            data_column->append_numbers(&result, sizeof(result));
        }
    }

    std::string get_name() const override { return "percentile_approx"; }
};
} // namespace starrocks
