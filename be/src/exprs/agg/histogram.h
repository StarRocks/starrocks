// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "runtime/large_int_value.h"

namespace starrocks::vectorized {

template <typename T>
struct Bucket {
public:
    Bucket() {}
    Bucket(T lower, T upper, size_t count, size_t upper_repeats)
            : lower(lower), upper(upper), count(count), upper_repeats(upper_repeats), count_in_bucket(1) {}
    T lower;
    T upper;
    // Up to this bucket, the total value
    int32_t count;
    // the number of values that on the upper boundary
    int32_t upper_repeats;
    // total value count in this bucket
    int32_t count_in_bucket;
};

template <typename T>
struct HistogramState {
    HistogramState() {}
    std::vector<Bucket<T>> buckets;
    double sample_ratio;
};

template <PrimitiveType PT, typename T = RunTimeCppType<PT>>
class HistogramAggregationFunction final
        : public AggregateFunctionBatchHelper<HistogramState<T>, HistogramAggregationFunction<PT, T>> {
public:
    using ColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        int32_t total_rows = columns[1]->get(0).get_int32();
        int32_t sample_rows = columns[2]->get(0).get_int32();
        int32_t bucket_num = columns[3]->get(0).get_int32();
        int32_t bucket_size = ceil(sample_rows / bucket_num);

        double sample_ratio;
        if (sample_rows == 0) {
            sample_ratio = 1;
        } else {
            sample_ratio = total_rows / sample_rows;
        }
        this->data(state).sample_ratio = sample_ratio;
        this->data(state).buckets.reserve(bucket_num);

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

        if (this->data(state).buckets.empty()) {
            Bucket<T> bucket(v, v, 1, 1);
            this->data(state).buckets.emplace_back(bucket);
        } else {
            Bucket<T>* lastBucket = &this->data(state).buckets.back();

            if (lastBucket->upper == v) {
                lastBucket->count++;
                lastBucket->count_in_bucket++;
                lastBucket->upper_repeats++;
            } else {
                if (lastBucket->count_in_bucket >= bucket_size) {
                    Bucket<T> bucket(v, v, lastBucket->count + 1, 1);
                    this->data(state).buckets.emplace_back(bucket);
                } else {
                    lastBucket->upper = v;
                    lastBucket->count++;
                    lastBucket->count_in_bucket++;
                    lastBucket->upper_repeats = 1;
                }
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
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

    std::string toBucketJson(std::string lower, std::string upper, size_t count, size_t upper_repeats,
                             double sample_ratio) const {
        return fmt::format("[\"{}\",\"{}\",\"{}\",\"{}\"]", lower, upper,
                           std::to_string((size_t)(count * sample_ratio)),
                           std::to_string((size_t)(upper_repeats * sample_ratio)));
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        std::string histogram_json = "{ \"buckets\" : [";
        if constexpr (pt_is_largeint<PT>) {
            for (int i = 0; i < this->data(state).buckets.size(); ++i) {
                histogram_json +=
                        toBucketJson(LargeIntValue::to_string(this->data(state).buckets[i].lower),
                                     LargeIntValue::to_string(this->data(state).buckets[i].upper),
                                     this->data(state).buckets[i].count, this->data(state).buckets[i].upper_repeats,
                                     this->data(state).sample_ratio) +
                        ",";
            }
        } else if constexpr (pt_is_arithmetic<PT>) {
            for (int i = 0; i < this->data(state).buckets.size(); ++i) {
                histogram_json +=
                        toBucketJson(std::to_string(this->data(state).buckets[i].lower),
                                     std::to_string(this->data(state).buckets[i].upper),
                                     this->data(state).buckets[i].count, this->data(state).buckets[i].upper_repeats,
                                     this->data(state).sample_ratio) +
                        ",";
            }

        } else if constexpr (pt_is_date_or_datetime<PT>) {
            for (int i = 0; i < this->data(state).buckets.size(); ++i) {
                histogram_json +=
                        toBucketJson(this->data(state).buckets[i].lower.to_string(),
                                     this->data(state).buckets[i].upper.to_string(), this->data(state).buckets[i].count,
                                     this->data(state).buckets[i].upper_repeats, this->data(state).sample_ratio) +
                        ",";
            }
        } else if constexpr (pt_is_decimal<PT>) {
            int scale = ctx->get_arg_type(0)->scale;
            int precision = ctx->get_arg_type(0)->precision;
            for (int i = 0; i < this->data(state).buckets.size(); ++i) {
                histogram_json +=
                        toBucketJson(DecimalV3Cast::to_string<T>(this->data(state).buckets[i].lower, scale, precision),
                                     DecimalV3Cast::to_string<T>(this->data(state).buckets[i].upper, scale, precision),
                                     this->data(state).buckets[i].count, this->data(state).buckets[i].upper_repeats,
                                     this->data(state).sample_ratio) +
                        ",";
            }
        }

        histogram_json[histogram_json.size() - 1] = ']';
        histogram_json = histogram_json + " }";
        Slice slice(histogram_json);
        to->append_datum(slice);
    }

    std::string get_name() const override { return "histogram"; }
};

} // namespace starrocks::vectorized