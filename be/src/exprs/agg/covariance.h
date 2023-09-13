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

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "types/logical_type.h"
namespace starrocks {

// aditional state for Corelation
struct CorelationAggregateState {
    double m2X{};
    double m2Y{};
};

struct EmptyAggregateState {};

template <bool isCorelation>
struct CovarianceCorelationAggregateState
        : public std::conditional_t<isCorelation, CorelationAggregateState, EmptyAggregateState> {
    // Average value of first column.
    double meanX{};
    // Average value of second column.
    double meanY{};

    double c2{};
    // Items.
    int64_t count{0};
};

// use Welford's online algorithm to calculate covariance, refer to https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online
template <LogicalType LT, bool isCorelation, typename T = RunTimeCppType<LT>>
class CorVarianceBaseAggregateFunction
        : public AggregateFunctionBatchHelper<CovarianceCorelationAggregateState<isCorelation>,
                                              CorVarianceBaseAggregateFunction<LT, isCorelation, T>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using InputCppType = T;
    using ResultColumnType = RunTimeColumnType<TYPE_DOUBLE>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).meanX = {};
        this->data(state).meanY = {};
        this->data(state).c2 = {};
        this->data(state).count = 0;

        if constexpr (isCorelation) {
            this->data(state).m2X = 0;
            this->data(state).m2Y = 0;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(ctx->get_num_args() == 2);

        const auto* column0 = down_cast<const InputColumnType*>(columns[0]);
        const auto* column1 = down_cast<const InputColumnType*>(columns[1]);

        this->data(state).count += 1;

        double oldMeanX = this->data(state).meanX;
        InputCppType rowX = column0->get_data()[row_num];

        double oldMeanY = this->data(state).meanY;
        InputCppType rowY = column1->get_data()[row_num];

        double newMeanX = (oldMeanX + (rowX - oldMeanX) / this->data(state).count);
        double newMeanY = (oldMeanY + (rowY - oldMeanY) / this->data(state).count);

        this->data(state).c2 = this->data(state).c2 + (rowX - oldMeanX) * (rowY - newMeanY);

        this->data(state).meanX = newMeanX;
        this->data(state).meanY = newMeanY;

        if constexpr (isCorelation) {
            this->data(state).m2X += (rowX - oldMeanX) * (rowX - newMeanX);
            this->data(state).m2Y += (rowY - oldMeanY) * (rowY - newMeanY);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice slice = column->get(row_num).get_slice();

        auto meanX = *reinterpret_cast<double*>(slice.data);
        auto meanY = *reinterpret_cast<double*>(slice.data + sizeof(double));
        auto c2 = *reinterpret_cast<double*>(slice.data + 2 * sizeof(double));
        int64_t count = *reinterpret_cast<int64_t*>(slice.data + sizeof(double) * 3);

        double deltaX = this->data(state).meanX - meanX;
        double deltaY = this->data(state).meanY - meanY;

        double sum_count = this->data(state).count + count;
        double factor = (this->data(state).count / sum_count);

        this->data(state).meanX = meanX + deltaX * factor;
        this->data(state).meanY = meanY + deltaY * factor;

        this->data(state).c2 = c2 + this->data(state).c2 + (deltaX * deltaY) * factor;
        this->data(state).count = sum_count;

        if constexpr (isCorelation) {
            double m2X = *reinterpret_cast<double*>(slice.data + sizeof(double) * 3 + sizeof(int64_t));
            double m2Y = *reinterpret_cast<double*>(slice.data + sizeof(double) * 3 + sizeof(int64_t) + sizeof(double));
            this->data(state).m2X = m2X + this->data(state).m2X + (deltaX * deltaX) * factor;
            this->data(state).m2Y = m2Y + this->data(state).m2Y + (deltaY * deltaY) * factor;
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        size_t old_size = bytes.size();
        size_t new_size = old_size + sizeof(double) * 3 + sizeof(int64_t);
        if constexpr (isCorelation) {
            new_size += (sizeof(double) * 2);
        }
        bytes.resize(new_size);

        memcpy(bytes.data() + old_size, &(this->data(state).meanX), sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double), &(this->data(state).meanY), sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double) * 2, &(this->data(state).c2), sizeof(double));
        memcpy(bytes.data() + old_size + sizeof(double) * 3, &(this->data(state).count), sizeof(int64_t));

        if constexpr (isCorelation) {
            memcpy(bytes.data() + old_size + sizeof(double) * 3 + sizeof(int64_t), &(this->data(state).m2X),
                   sizeof(double));
            memcpy(bytes.data() + old_size + sizeof(double) * 3 + sizeof(int64_t) + sizeof(double),
                   &(this->data(state).m2Y), sizeof(double));
        }
        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        size_t old_size = bytes.size();

        size_t one_element_size = sizeof(double) * 3 + sizeof(int64_t);
        if constexpr (isCorelation) {
            one_element_size += (sizeof(double) * 2);
        }
        bytes.resize(one_element_size * chunk_size);
        dst_column->get_offset().resize(chunk_size + 1);

        const auto* src_column0 = down_cast<const InputColumnType*>(src[0].get());
        const auto* src_column1 = down_cast<const InputColumnType*>(src[1].get());

        double meanX = {};
        double meanY = {};
        double c2 = 0;

        int64_t count = 1;
        for (size_t i = 0; i < chunk_size; ++i) {
            meanX = src_column0->get_data()[i];
            meanY = src_column1->get_data()[i];
            memcpy(bytes.data() + old_size, &meanX, sizeof(double));
            memcpy(bytes.data() + old_size + sizeof(double), &meanY, sizeof(double));
            memcpy(bytes.data() + old_size + sizeof(double) * 2, &c2, sizeof(double));
            memcpy(bytes.data() + old_size + sizeof(double) * 3, &count, sizeof(int64_t));
            if constexpr (isCorelation) {
                double m2X = 0;
                double m2Y = 0;
                memcpy(bytes.data() + old_size + sizeof(double) * 3 + sizeof(int64_t), &m2X, sizeof(double));
                memcpy(bytes.data() + old_size + sizeof(double) * 3 + sizeof(int64_t) + sizeof(double), &m2Y,
                       sizeof(double));
            }
            old_size += one_element_size;

            dst_column->get_offset()[i + 1] = old_size;
        }
    }
};

template <LogicalType LT, bool isSample, typename T = RunTimeCppType<LT>>
class CorVarianceAggregateFunction final : public CorVarianceBaseAggregateFunction<LT, false> {
    using InputColumnType = RunTimeColumnType<LT>;
    using InputCppType = T;
    using ResultColumnType = RunTimeColumnType<TYPE_DOUBLE>;
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());

        int64_t count = this->data(state).count;
        if constexpr (isSample) {
            if (count > 1) {
                down_cast<ResultColumnType*>(to)->append(this->data(state).c2 / (count - 1));
            } else {
                down_cast<ResultColumnType*>(to)->append(0);
            }
        } else {
            if (count > 0) {
                down_cast<ResultColumnType*>(to)->append(this->data(state).c2 / count);
            } else {
                down_cast<ResultColumnType*>(to)->append(0);
            }
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);

        double result;
        int64_t count = this->data(state).count;
        if constexpr (isSample) {
            if (count > 1) {
                result = this->data(state).c2 / (count - 1);
            } else {
                result = 0;
            }
        } else {
            if (count > 0) {
                result = this->data(state).c2 / count;
            } else {
                result = 0;
            }
        }

        auto* column = down_cast<ResultColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    std::string get_name() const override { return "corvariance"; }
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class CorelationAggregateFunction final : public CorVarianceBaseAggregateFunction<LT, true> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using InputCppType = T;
    using ResultColumnType = RunTimeColumnType<TYPE_DOUBLE>;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());

        double result;

        // count < 2 means m2X and m2Y is zero
        if (this->data(state).count < 2) {
            result = std::numeric_limits<double>::max();
        } else {
            result = this->data(state).c2 / sqrt(this->data(state).m2X) / sqrt(this->data(state).m2Y);
        }

        down_cast<ResultColumnType*>(to)->append(result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);

        double result;
        if (this->data(state).count < 2) {
            result = std::numeric_limits<double>::max();
        } else {
            result = this->data(state).c2 / sqrt(this->data(state).m2X) / sqrt(this->data(state).m2Y);
        }

        auto* column = down_cast<ResultColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    std::string get_name() const override { return "corelation"; }
};

} // namespace starrocks
