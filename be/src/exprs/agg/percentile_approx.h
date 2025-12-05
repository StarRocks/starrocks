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
    explicit PercentileApproxState(double compression)
            : percentile(new PercentileValue(compression)), compression_initialized(true) {}
    ~PercentileApproxState() = default;

    // Reinitialize the PercentileValue with a new compression factor
    // This is used when the state is already constructed (e.g., as part of NullableAggregateFunctionState)
    // but we need to apply a different compression factor from FunctionContext
    void reinit_with_compression(double compression) {
        percentile.reset(new PercentileValue(compression));
        compression_initialized = true;
    }

    int64_t mem_usage() const { return percentile->mem_usage(); }

    std::unique_ptr<PercentileValue> percentile;
    bool compression_initialized = false; // Flag to track if compression has been initialized from FunctionContext
    std::vector<double>
            targetQuantiles; // Stores target quantile(s), single value for scalar mode, multiple for array mode
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
        double compression = get_compression_factor(ctx);
        // Lazy initialization of compression factor on first merge
        if (UNLIKELY(!data(state).compression_initialized)) {
            data(state).reinit_with_compression(compression);
        }

        const auto* binary_column = down_cast<const BinaryColumn*>(column);
        Slice src = binary_column->get_slice(row_num);
        double quantile;
        memcpy(&quantile, src.data, sizeof(double));

        PercentileApproxState src_percentile(compression);
        src_percentile.percentile->deserialize((char*)src.data + sizeof(double));

        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->merge(src_percentile.percentile.get());
        if (data(state).targetQuantiles.empty()) {
            data(state).targetQuantiles.push_back(quantile);
        }
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        size_t size = data(state).percentile->serialize_size();
        uint8_t result[size + sizeof(double)];
        double quantile = data(state).targetQuantiles.empty() ? 0.0 : data(state).targetQuantiles[0];
        memcpy(result, &quantile, sizeof(double));
        data(state).percentile->serialize(result + sizeof(double));
        auto* column = down_cast<BinaryColumn*>(to);
        column->append(Slice(result, size + sizeof(double)));
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* data_column = down_cast<DoubleColumn*>(to);
        double quantile = data(state).targetQuantiles.empty() ? 0.0 : data(state).targetQuantiles[0];
        double result = data(state).percentile->quantile(quantile);
        data_column->append_numbers(&result, sizeof(result));
    }
};

// PercentileApproxAggregateFunction: percentile_approx(expr, DOUBLE p[, DOUBLE compression])
class PercentileApproxAggregateFunction : public PercentileApproxAggregateFunctionBase {
public:
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
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);
        }

        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1
        DCHECK(columns[1]->is_constant());
        DCHECK(!columns[1]->is_null(0));
        // first update
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.push_back(columns[1]->get(0).get_double());
        }
        double column_value = data_column->immutable_data()[row_num];
        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->add(implicit_cast<float>(column_value));
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 1
        DCHECK(src[1]->is_constant());
        const auto* const_column = down_cast<const ConstColumn*>(src[1].get());
        double quantile = const_column->get(0).get_double();
        // result
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 20);
        result->get_offset().resize(chunk_size + 1);

        // serialize percentile one by one
        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            PercentileValue percentile;
            percentile.add(implicit_cast<float>(data_column->immutable_data()[i]));

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
class PercentileApproxWeightedAggregateFunction : public PercentileApproxAggregateFunctionBase {
public:
    // SplitAggregateRule pass the const args to merge phase aggregator for performance.
    // Compression parameter is always the last constant parameter (if provided)
    double get_compression_factor(FunctionContext* ctx) const override {
        double compression = DEFAULT_COMPRESSION_FACTOR;
        int num_args = ctx->get_num_args();
        if (num_args > 3) {
            for (int i = num_args - 1; i >= 0; i--) {
                auto const_col = ctx->get_constant_column(i);
                if (const_col != nullptr) {
                    // Found the last constant column, this should be compression
                    compression = ColumnHelper::get_const_value<TYPE_DOUBLE>(const_col);
                    if (compression < MIN_COMPRESSION || compression > MAX_COMPRESSION) {
                        LOG(WARNING) << "Compression factor out of range. Using default compression factor: "
                                     << DEFAULT_COMPRESSION_FACTOR;
                        compression = DEFAULT_COMPRESSION_FACTOR;
                    }
                    break;
                }
            }
        }
        return compression;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);
        }
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1: weight can be const or int64 column
        size_t real_row_num = columns[1]->is_constant() ? 0 : row_num;
        int64_t weight = columns[1]->get(real_row_num).get_int64();
        // argument 2
        DCHECK(columns[2]->is_constant());
        DCHECK(!columns[2]->is_null(0));
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.push_back(columns[2]->get(0).get_double());
        }

        double column_value = data_column->immutable_data()[row_num];
        int64_t prev_memory = data(state).percentile->mem_usage();
        // add value with weight
        if (LIKELY(weight != 0)) {
            data(state).percentile->add(implicit_cast<float>(column_value), weight);
        }
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 2
        DCHECK(src[2]->is_constant());
        double quantile = src[2]->get(0).get_double();
        // result
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        bytes.reserve(chunk_size * 20);
        result->get_offset().resize(chunk_size + 1);

        // argument 1, weight column can be int64 or const column
        // serialize percentile one by one
        size_t old_size = bytes.size();
        if (src[1]->is_constant()) {
            int64_t weight = src[1]->get(0).get_int64();
            if (LIKELY(weight != 0)) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    PercentileValue percentile;
                    double value = data_column->immutable_data()[i];
                    percentile.add(implicit_cast<float>(value), weight);
                    size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
                    bytes.resize(new_size);
                    memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                    percentile.serialize(bytes.data() + old_size + sizeof(double));
                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            } else {
                // TODO: optimize for empty weight but should not happen frequently
                PercentileValue empty_percentile;
                static size_t delta_size = sizeof(double) + empty_percentile.serialize_size();
                for (size_t i = 0; i < chunk_size; ++i) {
                    size_t new_size = old_size + delta_size;
                    bytes.resize(new_size);
                    memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                    empty_percentile.serialize(bytes.data() + old_size + sizeof(double));
                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            }
        } else {
            const auto* weight_column = down_cast<const Int64Column*>(src[1].get());
            for (size_t i = 0; i < chunk_size; ++i) {
                int64_t weight = weight_column->immutable_data()[i];
                PercentileValue percentile;
                double value = data_column->immutable_data()[i];
                if (LIKELY(weight != 0)) {
                    percentile.add(implicit_cast<float>(value), weight);
                }
                size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
                bytes.resize(new_size);
                memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                percentile.serialize(bytes.data() + old_size + sizeof(double));
                old_size = new_size;
                result->get_offset()[i + 1] = new_size;
            }
        }
    }
    std::string get_name() const override { return "percentile_approx_weighted"; }
};

// PercentileApproxArrayAggregateFunction: percentile_approx(expr, ARRAY<DOUBLE> p[, DOUBLE compression])
// Returns ARRAY<DOUBLE>, using new serialization format
class PercentileApproxArrayAggregateFunction final : public PercentileApproxAggregateFunction {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // argument 1: array column wrapped in ConstColumn, no need to check is_null
        DCHECK(columns[1]->is_constant());
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);

            const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[1]));
            const auto* elements = down_cast<const DoubleColumn*>(
                    ColumnHelper::get_data_column(array_column->elements_column().get()));
            auto offsets = array_column->offsets().immutable_data();
            size_t start = offsets[0];
            size_t end = offsets[1];
            DCHECK(end > start) << "Array length cannot be 0";
            auto elements_data = elements->immutable_data();
            auto sub = elements_data.subspan(start, end - start);
            data(state).targetQuantiles.assign(sub.begin(), sub.end());
        }

        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);

        double column_value = data_column->immutable_data()[row_num];
        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->add(implicit_cast<float>(column_value));
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    // Override merge method, deserialize using new format: [count(4 bytes), q1...qn(8*n bytes), TDigest_data]
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        double compression = get_compression_factor(ctx);
        // Lazy initialization of compression factor on first merge
        if (UNLIKELY(!data(state).compression_initialized)) {
            data(state).reinit_with_compression(compression);
        }

        const auto* binary_column = down_cast<const BinaryColumn*>(column);
        Slice src = binary_column->get_slice(row_num);

        // Read quantile count
        uint32_t count;
        memcpy(&count, src.data, sizeof(uint32_t));

        // Initialize targetQuantiles if empty (first merge without prior update)
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.resize(count);
            memcpy(data(state).targetQuantiles.data(), (char*)src.data + sizeof(uint32_t), count * sizeof(double));
        }

        // Deserialize TDigest (skip quantiles array, only need TDigest for merging)
        PercentileApproxState src_percentile(compression);
        src_percentile.percentile->deserialize((char*)src.data + sizeof(uint32_t) + count * sizeof(double));

        // Merge into current state
        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->merge(src_percentile.percentile.get());
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    // Override serialize_to_column method, serialize using new format: [count(4 bytes), q1...qn(8*n bytes), TDigest_data]
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        size_t tdigest_size = data(state).percentile->serialize_size();
        uint32_t count = static_cast<uint32_t>(data(state).targetQuantiles.size());
        size_t total_size = sizeof(uint32_t) + count * sizeof(double) + tdigest_size;

        uint8_t result[total_size];

        // Write quantile count
        memcpy(result, &count, sizeof(uint32_t));

        // Write all quantiles
        memcpy(result + sizeof(uint32_t), data(state).targetQuantiles.data(), count * sizeof(double));

        // Write TDigest data
        data(state).percentile->serialize(result + sizeof(uint32_t) + count * sizeof(double));

        auto* column = down_cast<BinaryColumn*>(to);
        column->append(Slice(result, total_size));
    }

    // Override finalize_to_column method, returns ARRAY<DOUBLE>
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        // Calculate all quantiles and build DatumArray
        DatumArray result_array;
        result_array.reserve(data(state).targetQuantiles.size());
        for (double quantile : data(state).targetQuantiles) {
            double result = data(state).percentile->quantile(quantile);
            result_array.emplace_back(result);
        }

        array_column->append_datum(Datum(result_array));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 1: ARRAY<DOUBLE>
        DCHECK(src[1]->is_constant());
        const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[1].get()));
        const auto* elements =
                down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));

        auto offsets = array_column->offsets().immutable_data();
        size_t start = offsets[0];
        size_t end = offsets[1];
        uint32_t count = static_cast<uint32_t>(end - start);

        // Extract quantiles
        std::vector<double> quantiles(count);
        auto elements_data = elements->immutable_data();
        for (size_t i = 0; i < count; ++i) {
            quantiles[i] = elements_data[start + i];
        }

        // result
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        // Calculate estimated size per row: count(4) + quantiles(8*n) + TDigest data(~16 bytes)
        // 20 = sizeof(uint32_t) + percentile.serialize_size()
        size_t estimated_size_per_row = count * sizeof(double) + 20;
        bytes.reserve(chunk_size * estimated_size_per_row);
        result->get_offset().resize(chunk_size + 1);

        // serialize percentile one by one
        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            PercentileValue percentile;
            percentile.add(implicit_cast<float>(data_column->immutable_data()[i]));

            size_t new_size = old_size + sizeof(uint32_t) + count * sizeof(double) + percentile.serialize_size();
            bytes.resize(new_size);

            // Write count
            memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
            // Write quantiles
            memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
            // Write TDigest
            percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }
};

// PercentileApproxWeightedArrayAggregateFunction: percentile_approx_weighted(expr, weight, ARRAY<DOUBLE> p[, DOUBLE compression])
// Returns ARRAY<DOUBLE>, using new serialization format
class PercentileApproxWeightedArrayAggregateFunction final : public PercentileApproxWeightedAggregateFunction {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // argument 2: array column wrapped in ConstColumn, no need to check is_null
        DCHECK(columns[2]->is_constant());
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);

            const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[2]));
            const auto* elements = down_cast<const DoubleColumn*>(
                    ColumnHelper::get_data_column(array_column->elements_column().get()));
            auto offsets = array_column->offsets().immutable_data();
            size_t start = offsets[0];
            size_t end = offsets[1];
            DCHECK(end > start) << "Array length cannot be 0";
            auto elements_data = elements->immutable_data();
            auto sub = elements_data.subspan(start, end - start);
            data(state).targetQuantiles.assign(sub.begin(), sub.end());
        }

        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1: weight can be const or int64 column
        size_t real_row_num = columns[1]->is_constant() ? 0 : row_num;
        int64_t weight = columns[1]->get(real_row_num).get_int64();

        double column_value = data_column->immutable_data()[row_num];
        int64_t prev_memory = data(state).percentile->mem_usage();
        // add value with weight
        if (LIKELY(weight != 0)) {
            data(state).percentile->add(implicit_cast<float>(column_value), weight);
        }
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    // Override merge method, deserialize using new format: [count(4 bytes), q1...qn(8*n bytes), TDigest_data]
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        double compression = get_compression_factor(ctx);
        // Lazy initialization of compression factor on first merge
        if (UNLIKELY(!data(state).compression_initialized)) {
            data(state).reinit_with_compression(compression);
        }

        const auto* binary_column = down_cast<const BinaryColumn*>(column);
        Slice src = binary_column->get_slice(row_num);

        // Read quantile count
        uint32_t count;
        memcpy(&count, src.data, sizeof(uint32_t));

        // Initialize targetQuantiles if empty (first merge without prior update)
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.resize(count);
            memcpy(data(state).targetQuantiles.data(), (char*)src.data + sizeof(uint32_t), count * sizeof(double));
        }

        // Deserialize TDigest (skip quantiles array, only need TDigest for merging)
        PercentileApproxState src_percentile(compression);
        src_percentile.percentile->deserialize((char*)src.data + sizeof(uint32_t) + count * sizeof(double));

        // Merge into current state
        int64_t prev_memory = data(state).percentile->mem_usage();
        data(state).percentile->merge(src_percentile.percentile.get());
        ctx->add_mem_usage(data(state).percentile->mem_usage() - prev_memory);
    }

    // Override serialize_to_column method, serialize using new format: [count(4 bytes), q1...qn(8*n bytes), TDigest_data]
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        size_t tdigest_size = data(state).percentile->serialize_size();
        uint32_t count = static_cast<uint32_t>(data(state).targetQuantiles.size());
        size_t total_size = sizeof(uint32_t) + count * sizeof(double) + tdigest_size;

        uint8_t result[total_size];

        // Write quantile count
        memcpy(result, &count, sizeof(uint32_t));

        // Write all quantiles
        memcpy(result + sizeof(uint32_t), data(state).targetQuantiles.data(), count * sizeof(double));

        // Write TDigest data
        data(state).percentile->serialize(result + sizeof(uint32_t) + count * sizeof(double));

        auto* column = down_cast<BinaryColumn*>(to);
        column->append(Slice(result, total_size));
    }

    // Override finalize_to_column method, returns ARRAY<DOUBLE>
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        // Calculate all quantiles and build DatumArray
        DatumArray result_array;
        result_array.reserve(data(state).targetQuantiles.size());
        for (double quantile : data(state).targetQuantiles) {
            double result = data(state).percentile->quantile(quantile);
            result_array.emplace_back(result);
        }

        array_column->append_datum(Datum(result_array));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 2: ARRAY<DOUBLE>
        DCHECK(src[2]->is_constant());
        const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[2].get()));
        const auto* elements =
                down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));

        auto offsets = array_column->offsets().immutable_data();
        size_t start = offsets[0];
        size_t end = offsets[1];
        uint32_t count = static_cast<uint32_t>(end - start);

        // Extract quantiles
        std::vector<double> quantiles(count);
        auto elements_data = elements->immutable_data();
        for (size_t i = 0; i < count; ++i) {
            quantiles[i] = elements_data[start + i];
        }

        // result
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        // Calculate estimated size per row: count(4) + quantiles(8*n) + TDigest data(~16 bytes)
        // 20 = sizeof(uint32_t) + percentile.serialize_size()
        size_t estimated_size_per_row = count * sizeof(double) + 20;
        bytes.reserve(chunk_size * estimated_size_per_row);
        result->get_offset().resize(chunk_size + 1);

        // argument 1, weight column can be int64 or const column
        // serialize percentile one by one
        size_t old_size = bytes.size();
        if (src[1]->is_constant()) {
            int64_t weight = src[1]->get(0).get_int64();
            if (LIKELY(weight != 0)) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    PercentileValue percentile;
                    double value = data_column->immutable_data()[i];
                    percentile.add(implicit_cast<float>(value), weight);

                    size_t new_size =
                            old_size + sizeof(uint32_t) + count * sizeof(double) + percentile.serialize_size();
                    bytes.resize(new_size);

                    // Write count
                    memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
                    // Write quantiles
                    memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
                    // Write TDigest
                    percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            } else {
                // TODO: optimize for empty weight but should not happen frequently
                PercentileValue empty_percentile;
                size_t delta_size = sizeof(uint32_t) + count * sizeof(double) + empty_percentile.serialize_size();
                for (size_t i = 0; i < chunk_size; ++i) {
                    size_t new_size = old_size + delta_size;
                    bytes.resize(new_size);

                    // Write count
                    memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
                    // Write quantiles
                    memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
                    // Write TDigest
                    empty_percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            }
        } else {
            const auto* weight_column = down_cast<const Int64Column*>(src[1].get());
            for (size_t i = 0; i < chunk_size; ++i) {
                int64_t weight = weight_column->immutable_data()[i];
                PercentileValue percentile;
                double value = data_column->immutable_data()[i];
                if (LIKELY(weight != 0)) {
                    percentile.add(implicit_cast<float>(value), weight);
                }

                size_t new_size = old_size + sizeof(uint32_t) + count * sizeof(double) + percentile.serialize_size();
                bytes.resize(new_size);

                // Write count
                memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
                // Write quantiles
                memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
                // Write TDigest
                percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

                old_size = new_size;
                result->get_offset()[i + 1] = new_size;
            }
        }
    }
};

} // namespace starrocks
