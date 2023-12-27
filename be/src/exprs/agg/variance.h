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
#include "gutil/casts.h"

namespace starrocks {

template <LogicalType LT, typename = guard::Guard>
inline constexpr LogicalType DevFromAveResultLT = TYPE_DOUBLE;

template <>
inline constexpr LogicalType DevFromAveResultLT<TYPE_DECIMALV2, guard::Guard> = TYPE_DECIMALV2;

template <LogicalType LT>
inline constexpr LogicalType DevFromAveResultLT<LT, DecimalLTGuard<LT>> = TYPE_DECIMAL128;

template <typename T>
struct DevFromAveAggregateState {
    // Average value.
    T mean{};
    // The square of the difference between
    // each sample value and the average of all sample values.
    // It's calculated incrementally.
    T m2{};
    // Items.
    int64_t count = 0;
};

template <LogicalType LT, bool is_sample, typename T = RunTimeCppType<LT>,
          LogicalType ResultLT = DevFromAveResultLT<LT>, typename TResult = RunTimeCppType<ResultLT>>
class DevFromAveAggregateFunction
        : public AggregateFunctionBatchHelper<DevFromAveAggregateState<TResult>,
                                              DevFromAveAggregateFunction<LT, is_sample, T, ResultLT, TResult>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).mean = {};
        this->data(state).m2 = {};
        this->data(state).count = 0;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_numeric() || columns[0]->is_decimal());

        const auto* column = down_cast<const InputColumnType*>(columns[0]);

        int64_t temp = 1 + this->data(state).count;

        TResult delta;
        delta = column->get_data()[row_num] - this->data(state).mean;

        TResult r;
        if constexpr (lt_is_decimalv2<LT>) {
            r = delta / DecimalV2Value(temp, 0);
        } else if constexpr (lt_is_decimal128<LT>) {
            r = (Decimal128P38S9(delta) / temp).value();
        } else {
            r = delta / temp;
        }

        this->data(state).mean += r;
        if constexpr (lt_is_decimalv2<LT>) {
            this->data(state).m2 += DecimalV2Value(this->data(state).count, 0) * delta * r;
        } else if constexpr (lt_is_decimal128<LT>) {
            this->data(state).m2 += this->data(state).count * (Decimal128P38S9(delta) * Decimal128P38S9(r)).value();
        } else {
            this->data(state).m2 += this->data(state).count * delta * r;
        }

        this->data(state).count = temp;
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

        auto mean = unaligned_load<TResult>(slice.data);
        auto m2 = unaligned_load<TResult>(slice.data + sizeof(TResult));
        int64_t count = *reinterpret_cast<int64_t*>(slice.data + sizeof(TResult) * 2);

        TResult delta = this->data(state).mean - mean;

        if constexpr (lt_is_decimalv2<LT>) {
            DecimalV2Value count_state_decimal = DecimalV2Value(this->data(state).count, 0);
            DecimalV2Value count_decimal = DecimalV2Value(count, 0);

            TResult sum_count = count_state_decimal + count_decimal;
            this->data(state).mean = mean + delta * (count_state_decimal / sum_count);
            this->data(state).m2 =
                    m2 + this->data(state).m2 + (delta * delta) * (count_decimal * count_state_decimal / sum_count);
            this->data(state).count = sum_count;
        } else if constexpr (lt_is_decimal128<LT>) {
            TResult sum_count = this->data(state).count + count;
            this->data(state).mean = (Decimal128P38S9(mean) +
                                      Decimal128P38S9(delta) * (Decimal128P38S9(TResult(this->data(state).count)) /
                                                                Decimal128P38S9(TResult(sum_count))))
                                             .value();
            this->data(state).m2 =
                    m2 + this->data(state).m2 +
                    ((Decimal128P38S9(delta) * Decimal128P38S9(delta)) *
                     (Decimal128P38S9(TResult(count * this->data(state).count)) / Decimal128P38S9(TResult(sum_count))))
                            .value();
            this->data(state).count = sum_count;
        } else {
            TResult sum_count = this->data(state).count + count;
            this->data(state).mean = mean + delta * (this->data(state).count / sum_count);
            this->data(state).m2 =
                    m2 + this->data(state).m2 + (delta * delta) * (count * this->data(state).count / sum_count);
            this->data(state).count = sum_count;
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        size_t old_size = bytes.size();
        size_t new_size = old_size + sizeof(TResult) * 2 + sizeof(int64_t);
        bytes.resize(new_size);

        memcpy(bytes.data() + old_size, &(this->data(state).mean), sizeof(TResult));
        memcpy(bytes.data() + old_size + sizeof(TResult), &(this->data(state).m2), sizeof(TResult));
        memcpy(bytes.data() + old_size + sizeof(TResult) * 2, &(this->data(state).count), sizeof(int64_t));

        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        size_t old_size = bytes.size();

        size_t one_element_size = sizeof(TResult) * 2 + sizeof(int64_t);
        bytes.resize(one_element_size * chunk_size);
        dst_column->get_offset().resize(chunk_size + 1);

        const auto* src_column = down_cast<const InputColumnType*>(src[0].get());

        TResult mean = {};
        TResult m2;
        if constexpr (lt_is_decimalv2<LT>) {
            m2 = DecimalV2Value(0, 0);
        } else {
            m2 = 0;
        }

        int64_t count = 1;
        for (size_t i = 0; i < chunk_size; ++i) {
            mean = src_column->get_data()[i];
            memcpy(bytes.data() + old_size, &mean, sizeof(TResult));
            memcpy(bytes.data() + old_size + sizeof(TResult), &m2, sizeof(TResult));
            memcpy(bytes.data() + old_size + sizeof(TResult) * 2, &count, sizeof(int64_t));
            old_size += one_element_size;
            dst_column->get_offset()[i + 1] = old_size;
        }
    }

    std::string get_name() const override { return "deviation from average"; }
};

template <LogicalType LT, bool is_sample, typename T = RunTimeCppType<LT>,
          LogicalType ResultLT = DevFromAveResultLT<LT>, typename TResult = RunTimeCppType<ResultLT>>
class VarianceAggregateFunction final : public DevFromAveAggregateFunction<LT, is_sample, T, ResultLT, TResult> {
public:
    using ResultColumnType =
            typename DevFromAveAggregateFunction<LT, is_sample, T, ResultLT, TResult>::ResultColumnType;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());

        int64_t count = this->data(state).count;
        if constexpr (lt_is_decimalv2<LT>) {
            if constexpr (is_sample) {
                if (count > 1) {
                    down_cast<ResultColumnType*>(to)->append(this->data(state).m2 / DecimalV2Value(count - 1, 0));
                } else {
                    down_cast<ResultColumnType*>(to)->append(DecimalV2Value(0));
                }
            } else {
                if (count > 0) {
                    down_cast<ResultColumnType*>(to)->append(this->data(state).m2 / DecimalV2Value(count, 0));
                } else {
                    down_cast<ResultColumnType*>(to)->append(DecimalV2Value(0));
                }
            }
        } else if constexpr (lt_is_decimal128<LT>) {
            if constexpr (is_sample) {
                if (count > 1) {
                    auto result = (Decimal128P38S9(this->data(state).m2) / (count - 1)).value();
                    down_cast<ResultColumnType*>(to)->append(result);
                } else {
                    down_cast<ResultColumnType*>(to)->append(TResult(0));
                }
            } else {
                if (count > 0) {
                    auto result = (Decimal128P38S9(this->data(state).m2) / count).value();
                    down_cast<ResultColumnType*>(to)->append(result);
                } else {
                    down_cast<ResultColumnType*>(to)->append(TResult(0));
                }
            }
        } else {
            if constexpr (is_sample) {
                if (count > 1) {
                    down_cast<ResultColumnType*>(to)->append(this->data(state).m2 / (count - 1));
                } else {
                    down_cast<ResultColumnType*>(to)->append(0);
                }
            } else {
                if (count > 0) {
                    down_cast<ResultColumnType*>(to)->append(this->data(state).m2 / count);
                } else {
                    down_cast<ResultColumnType*>(to)->append(0);
                }
            }
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);

        TResult result;
        int64_t count = this->data(state).count;
        if constexpr (lt_is_decimalv2<LT>) {
            if constexpr (is_sample) {
                if (count > 1) {
                    result = this->data(state).m2 / DecimalV2Value(count - 1, 0);
                } else {
                    result = DecimalV2Value(0, 0);
                }
            } else {
                if (count > 0) {
                    result = this->data(state).m2 / DecimalV2Value(count, 0);
                } else {
                    result = DecimalV2Value(0, 0);
                }
            }
        } else if constexpr (lt_is_decimal128<LT>) {
            if constexpr (is_sample) {
                if (count > 1) {
                    result = (Decimal128P38S9(this->data(state).m2) / (count - 1)).value();
                } else {
                    result = TResult(0);
                }
            } else {
                if (count > 0) {
                    result = (Decimal128P38S9(this->data(state).m2) / count).value();
                } else {
                    result = TResult(0);
                }
            }
        } else {
            if constexpr (is_sample) {
                if (count > 1) {
                    result = this->data(state).m2 / (count - 1);
                } else {
                    result = 0;
                }
            } else {
                if (count > 0) {
                    result = this->data(state).m2 / count;
                } else {
                    result = 0;
                }
            }
        }

        auto* column = down_cast<ResultColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    std::string get_name() const override { return "variance"; }
};

template <LogicalType LT, bool is_sample, typename T = RunTimeCppType<LT>,
          LogicalType ResultLT = DevFromAveResultLT<LT>, typename TResult = RunTimeCppType<ResultLT>>
class StddevAggregateFunction final : public DevFromAveAggregateFunction<LT, is_sample, T, ResultLT, TResult> {
public:
    using ResultColumnType =
            typename DevFromAveAggregateFunction<LT, is_sample, T, ResultLT, TResult>::ResultColumnType;

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());

        int64_t count = this->data(state).count;
        if constexpr (lt_is_decimalv2<LT>) {
            TResult result;
            if constexpr (is_sample) {
                if (count > 1) {
                    result = this->data(state).m2 / DecimalV2Value(count - 1, 0);
                    const double value = sqrt(static_cast<double>(result));
                    result.assign_from_double(value);
                    down_cast<ResultColumnType*>(to)->append(result);
                } else {
                    down_cast<ResultColumnType*>(to)->append(DecimalV2Value(0));
                }
            } else {
                if (count > 0) {
                    result = this->data(state).m2 / DecimalV2Value(count, 0);
                    const double value = sqrt(static_cast<double>(result));
                    result.assign_from_double(value);
                    down_cast<ResultColumnType*>(to)->append(result);
                } else {
                    down_cast<ResultColumnType*>(to)->append(DecimalV2Value(0));
                }
            }
        } else if constexpr (lt_is_decimal128<LT>) {
            TResult result;
            if constexpr (is_sample) {
                if (count > 1) {
                    auto double_val = (Decimal128P38S9(this->data(state).m2) / (count - 1)).double_value();
                    result = Decimal128P38S9(sqrt(double_val)).value();
                    down_cast<ResultColumnType*>(to)->append(result);
                } else {
                    down_cast<ResultColumnType*>(to)->append(TResult(0));
                }
            } else {
                if (count > 0) {
                    auto double_val = (Decimal128P38S9(this->data(state).m2) / count).double_value();
                    result = Decimal128P38S9(sqrt(double_val)).value();
                    down_cast<ResultColumnType*>(to)->append(result);
                } else {
                    down_cast<ResultColumnType*>(to)->append(TResult(0));
                }
            }
        } else {
            if constexpr (is_sample) {
                if (count > 1) {
                    down_cast<ResultColumnType*>(to)->append(sqrt(this->data(state).m2 / (count - 1)));
                } else {
                    down_cast<ResultColumnType*>(to)->append(0);
                }
            } else {
                if (count > 0) {
                    down_cast<ResultColumnType*>(to)->append(sqrt(this->data(state).m2 / count));
                } else {
                    down_cast<ResultColumnType*>(to)->append(0);
                }
            }
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);

        TResult result;

        int64_t count = this->data(state).count;
        if constexpr (lt_is_decimalv2<LT>) {
            if constexpr (is_sample) {
                if (count > 1) {
                    result = this->data(state).m2 / DecimalV2Value(count - 1, 0);
                    const double value = sqrt(static_cast<double>(result));
                    result.assign_from_double(value);
                } else {
                    result = DecimalV2Value(0, 0);
                }
            } else {
                if (count > 0) {
                    result = this->data(state).m2 / DecimalV2Value(count, 0);
                    const double value = sqrt(static_cast<double>(result));
                    result.assign_from_double(value);
                } else {
                    result = DecimalV2Value(0, 0);
                }
            }
        } else if constexpr (lt_is_decimal128<LT>) {
            if constexpr (is_sample) {
                if (count > 1) {
                    auto double_val = (Decimal128P38S9(this->data(state).m2) / (count - 1)).double_value();
                    result = Decimal128P38S9(sqrt(double_val)).value();
                } else {
                    result = TResult(0);
                }
            } else {
                if (count > 0) {
                    auto double_val = (Decimal128P38S9(this->data(state).m2) / count).double_value();
                    result = Decimal128P38S9(sqrt(double_val)).value();
                } else {
                    result = TResult(0);
                }
            }
        } else {
            if constexpr (is_sample) {
                if (count > 1) {
                    result = sqrt(this->data(state).m2 / (count - 1));
                } else {
                    result = 0;
                }
            } else {
                if (count > 0) {
                    result = sqrt(this->data(state).m2 / count);
                } else {
                    result = 0;
                }
            }
        }

        auto* column = down_cast<ResultColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    std::string get_name() const override { return "stddev"; }
};

} // namespace starrocks
