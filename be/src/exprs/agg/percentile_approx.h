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
    explicit PercentileApproxState(double compression) : percentile(new PercentileValue(compression)) {}
    ~PercentileApproxState() = default;

    int64_t mem_usage() const { return percentile->mem_usage(); }

    std::unique_ptr<PercentileValue> percentile;
    double targetQuantile = -1.0;
    bool is_null = true;
};

class PercentileApproxAggregateFunctionBase
        : public AggregateFunctionBatchHelper<PercentileApproxState, PercentileApproxAggregateFunctionBase> {
protected:
    static constexpr double MIN_COMPRESSION = 2048.0;
    static constexpr double MAX_COMPRESSION = 10000.0;
    static constexpr double DEFAULT_COMPRESSION_FACTOR = 10000.0;

public:
    virtual double get_compression_factor(FunctionContext* ctx) const = 0;

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

        PercentileApproxState src_percentile(get_compression_factor(ctx));
        src_percentile.targetQuantile = quantile;
        src_percentile.percentile->deserialize((char*)src.data + sizeof(double));

        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->merge(src_percentile.percentile.get());
        data(state).targetQuantile = quantile;
        data(state).is_null = false;
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
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
};

// PercentileApproxAggregateFunction: percentile_approx(expr, DOUBLE p[, DOUBLE compression])
class PercentileApproxAggregateFunction final : public PercentileApproxAggregateFunctionBase {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        double compression = (ctx == nullptr) ? DEFAULT_COMPRESSION_FACTOR : get_compression_factor(ctx);
        new (ptr) PercentileApproxState(compression);
    }

    double get_compression_factor(FunctionContext* ctx) const override {
        double compression = DEFAULT_COMPRESSION_FACTOR;
        if (ctx->get_num_args() > 2) {
            compression = ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
            if (compression < MIN_COMPRESSION || compression > MAX_COMPRESSION) {
                LOG(WARNING) << "Compression factor out of range. Using default compression factor: "
                             << DEFAULT_COMPRESSION_FACTOR;
                compression = DEFAULT_COMPRESSION_FACTOR;
            }
        }
        return compression;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1
        if (columns[1]->only_null()) {
            ctx->set_error("For percentile_approx the second argument is expected to be non-null.", false);
            return;
        }
        DCHECK(!columns[1]->is_null(0));

        double column_value = data_column->get_data()[row_num];
        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->add(implicit_cast<float>(column_value));
        data(state).targetQuantile = columns[1]->get(0).get_double();
        data(state).is_null = false;
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 1
        DCHECK(src[1]->is_constant());
        const auto* const_column = down_cast<const ConstColumn*>(src[1].get());
        double quantile = const_column->get(0).get_double();
        // result
        BinaryColumn* result = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 20);
        result->get_offset().resize(chunk_size + 1);

        // serialize percentile one by one
        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            PercentileValue percentile;
            percentile.add(data_column->get_data()[i]);

            size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
            bytes.resize(new_size);
            memcpy(bytes.data() + old_size, &quantile, sizeof(double));
            percentile.serialize(bytes.data() + old_size + sizeof(double));

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }
    std::string get_name() const override { return "percentile_approx"; }
};

// PercentileApproxWeightedAggregateFunction: percentile_approx_weighted(expr, weight, DOUBLE p[, DOUBLE compression])
class PercentileApproxWeightedAggregateFunction final : public PercentileApproxAggregateFunctionBase {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        double compression = (ctx == nullptr) ? DEFAULT_COMPRESSION_FACTOR : get_compression_factor(ctx);
        new (ptr) PercentileApproxState(compression);
    }

    double get_compression_factor(FunctionContext* ctx) const override {
        double compression = DEFAULT_COMPRESSION_FACTOR;
        if (ctx->get_num_args() > 3) {
            compression = ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
            if (compression < MIN_COMPRESSION || compression > MAX_COMPRESSION) {
                LOG(WARNING) << "Compression factor out of range. Using default compression factor: "
                             << DEFAULT_COMPRESSION_FACTOR;
                compression = DEFAULT_COMPRESSION_FACTOR;
            }
        }
        return compression;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1: weight can be const or int64 column
        auto weight_viewer = ColumnViewer<TYPE_BIGINT>(columns[1]);
        int64_t weight = weight_viewer.value(row_num);
        // argument 2
        if (columns[2]->only_null()) {
            ctx->set_error("For percentile_approx the second argument is expected to be non-null.", false);
            return;
        }
        DCHECK(!columns[2]->is_null(0));
        double column_value = data_column->get_data()[row_num];
        int64_t prev_memory = data(state).percentile->mem_usage();

        // add value with weight
        data(state).targetQuantile = columns[2]->get(0).get_double();
        data(state).is_null = false;
        if (LIKELY(weight != 0)) {
            data(state).percentile->add(implicit_cast<float>(column_value), weight);
        }
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 1, weight column can be int64 or const column
        auto weight_viewer = ColumnViewer<TYPE_BIGINT>(src[1]);
        // argument 2
        DCHECK(src[2]->is_constant());
        const auto* const_column = down_cast<const ConstColumn*>(src[2].get());
        double quantile = const_column->get(0).get_double();
        // result
        BinaryColumn* result = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 20);
        result->get_offset().resize(chunk_size + 1);

        // serialize percentile one by one
        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            PercentileValue percentile;
            double value = data_column->get_data()[i];
            int64_t weight = weight_viewer.value(i);
            if (LIKELY(weight != 0)) {
                percentile.add(value, weight);
            }
            size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
            bytes.resize(new_size);
            memcpy(bytes.data() + old_size, &quantile, sizeof(double));
            percentile.serialize(bytes.data() + old_size + sizeof(double));

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }
    std::string get_name() const override { return "percentile_approx_weighted"; }
};
} // namespace starrocks
