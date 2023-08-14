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
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "runtime/large_int_value.h"

namespace starrocks {

template <typename T>
struct Bucket {
public:
    Bucket() = default;
    Bucket(T lower, T upper, size_t count, size_t upper_repeats)
            : lower(lower), upper(upper), count(count), upper_repeats(upper_repeats), count_in_bucket(1) {}
    T lower;
    T upper;
    // Up to this bucket, the total value
    int64_t count;
    // the number of values that on the upper boundary
    int64_t upper_repeats;
    // total value count in this bucket
    int64_t count_in_bucket;
};

template <typename T>
struct HistogramState {
    HistogramState() = default;
    std::vector<T> data;
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class HistogramAggregationFunction final
        : public AggregateFunctionBatchHelper<HistogramState<T>, HistogramAggregationFunction<LT, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        T v;
        if (columns[0]->is_nullable()) {
            if (columns[0]->is_null(row_num)) {
                return;
            }

            const auto* data_column = down_cast<const NullableColumn*>(columns[0]);
            v = down_cast<const ColumnType*>(data_column->data_column().get())->get_data()[row_num];
        } else {
            v = down_cast<const ColumnType*>(columns[0])->get_data()[row_num];
        }

        this->data(state).data.emplace_back(v);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        //Histogram aggregation function only support one stage Agg
        DCHECK(false);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        //Histogram aggregation function only support one stage Agg
        DCHECK(false);
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override {
        //Histogram aggregation function only support one stage Agg
        DCHECK(false);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        //Histogram aggregation function only support one stage Agg
        DCHECK(false);
    }

    std::string toBucketJson(const std::string& lower, const std::string& upper, size_t count, size_t upper_repeats,
                             double sample_ratio) const {
        return fmt::format(R"(["{}","{}","{}","{}"])", lower, upper, std::to_string((int64_t)(count * sample_ratio)),
                           std::to_string((int64_t)(upper_repeats * sample_ratio)));
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        auto bucket_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        [[maybe_unused]] double sample_ratio =
                1 / ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
        int bucket_size = this->data(state).data.size() / bucket_num;

        //Build bucket
        std::vector<Bucket<T>> buckets;
        for (int i = 0; i < this->data(state).data.size(); ++i) {
            T v = this->data(state).data[i];
            if (buckets.empty()) {
                Bucket<T> bucket(v, v, 1, 1);
                buckets.emplace_back(bucket);
            } else {
                Bucket<T>* lastBucket = &buckets.back();

                if (lastBucket->upper == v) {
                    lastBucket->count++;
                    lastBucket->count_in_bucket++;
                    lastBucket->upper_repeats++;
                } else {
                    if (lastBucket->count_in_bucket >= bucket_size) {
                        Bucket<T> bucket(v, v, lastBucket->count + 1, 1);
                        buckets.emplace_back(bucket);
                    } else {
                        lastBucket->upper = v;
                        lastBucket->count++;
                        lastBucket->count_in_bucket++;
                        lastBucket->upper_repeats = 1;
                    }
                }
            }
        }

        std::string bucket_json;
        if (buckets.empty()) {
            bucket_json = "[]";
        } else {
            bucket_json = "[";
            if constexpr (lt_is_largeint<LT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(LargeIntValue::to_string(buckets[i].lower),
                                                LargeIntValue::to_string(buckets[i].upper), buckets[i].count,
                                                buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (lt_is_arithmetic<LT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(std::to_string(buckets[i].lower), std::to_string(buckets[i].upper),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (lt_is_date_or_datetime<LT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(buckets[i].lower.to_string(), buckets[i].upper.to_string(),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (lt_is_decimal<LT>) {
                int scale = ctx->get_arg_type(0)->scale;
                int precision = ctx->get_arg_type(0)->precision;
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(DecimalV3Cast::to_string<T>(buckets[i].lower, precision, scale),
                                                DecimalV3Cast::to_string<T>(buckets[i].upper, precision, scale),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (lt_is_string<LT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(buckets[i].lower.to_string(), buckets[i].upper.to_string(),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            }
            bucket_json[bucket_json.size() - 1] = ']';
        }

        Slice slice(bucket_json);
        to->append_datum(slice);
    }

    std::string get_name() const override { return "histogram"; }
};

} // namespace starrocks
