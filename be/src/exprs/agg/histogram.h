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

template <PrimitiveType PT, typename = guard::Guard>
inline constexpr PrimitiveType ImmediateHistogramResultPT = PT;

template <PrimitiveType PT>
inline constexpr PrimitiveType ImmediateHistogramResultPT<PT, BinaryPTGuard<PT>> = TYPE_DOUBLE;

template <typename T>
struct Bucket {
public:
    Bucket() {}
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
    HistogramState() {}
    std::vector<T> data;
};

template <PrimitiveType PT, typename T = RunTimeCppType<PT>>
class HistogramAggregationFunction final
        : public AggregateFunctionBatchHelper<HistogramState<T>, HistogramAggregationFunction<PT, T>> {
public:
    using ColumnType = RunTimeColumnType<PT>;

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

    std::string toBucketJson(std::string lower, std::string upper, size_t count, size_t upper_repeats,
                             double sample_ratio) const {
        return fmt::format("[\"{}\",\"{}\",\"{}\",\"{}\"]", lower, upper,
                           std::to_string((int64_t)(count * sample_ratio)),
                           std::to_string((int64_t)(upper_repeats * sample_ratio)));
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        auto bucket_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        [[maybe_unused]] double sample_ratio =
                1 / ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
        int bucket_size = this->data(state).data.size() / bucket_num;

        if (this->data(state).data.empty()) {
            std::string histogram_json = "{ \"buckets\" : [], \"topn\" : [] }";
            Slice slice(histogram_json);
            to->append_datum(slice);
            return;
        }

        //Calculate the frequency of each value in the data
        std::vector<std::pair<T, int>> frequency_vec;
        frequency_vec.reserve(this->data(state).data.size());
        T last_value = this->data(state).data[0];
        int count = 1;
        for (int i = 1; i < this->data(state).data.size(); ++i) {
            T v = this->data(state).data[i];
            if (v == last_value) {
                count++;
            } else {
                frequency_vec.emplace_back(std::make_pair(last_value, count));
                count = 1;
                last_value = v;
            }
        }
        frequency_vec.emplace_back(std::make_pair(last_value, count));
        //Sort by frequency
        std::sort(frequency_vec.begin(), frequency_vec.end(),
                  [](const std::pair<T, int>& lhs, const std::pair<T, int>& rhs) -> bool {
                      return lhs.second > rhs.second;
                  });

        int top_n_size = std::min((int)ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(3)),
                                  (int)frequency_vec.size());
        //Build a top-n map so that values are excluded from subsequent buckets
        std::unordered_map<T, int> topn;
        for (int i = 0; i < top_n_size; ++i) {
            topn.insert(frequency_vec[i]);
        }

        std::string topn_json;
        if (top_n_size == 0) {
            topn_json = "[]";
        } else {
            topn_json = "[";
            if constexpr (pt_is_largeint<PT>) {
                for (int i = 0; i < top_n_size; ++i) {
                    topn_json += fmt::format("[\"{}\",\"{}\"]", LargeIntValue::to_string(frequency_vec[i].first),
                                             (int64_t)(frequency_vec[i].second * sample_ratio)) +
                                 ",";
                }
            } else if constexpr (pt_is_arithmetic<PT>) {
                for (int i = 0; i < top_n_size; ++i) {
                    topn_json += fmt::format("[\"{}\",\"{}\"]", frequency_vec[i].first,
                                             (int64_t)(frequency_vec[i].second * sample_ratio)) +
                                 ",";
                }
            } else if constexpr (pt_is_date_or_datetime<PT>) {
                for (int i = 0; i < top_n_size; ++i) {
                    topn_json += fmt::format("[\"{}\",\"{}\"]", frequency_vec[i].first.to_string(),
                                             (int64_t)(frequency_vec[i].second * sample_ratio)) +
                                 ",";
                }
            } else if constexpr (pt_is_decimal<PT>) {
                for (int i = 0; i < top_n_size; ++i) {
                    int scale = ctx->get_arg_type(0)->scale;
                    int precision = ctx->get_arg_type(0)->precision;
                    topn_json += fmt::format("[\"{}\",\"{}\"]",
                                             DecimalV3Cast::to_string<T>(frequency_vec[i].first, scale, precision),
                                             (int64_t)(frequency_vec[i].second * sample_ratio)) +
                                 ",";
                }
            }
            topn_json[topn_json.size() - 1] = ']';
        }

        //Build bucket
        std::vector<Bucket<T>> buckets;
        for (int i = 0; i < this->data(state).data.size(); ++i) {
            T v = this->data(state).data[i];
            if (topn.find(v) != topn.end()) {
                continue;
            }

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
            if constexpr (pt_is_largeint<PT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(LargeIntValue::to_string(buckets[i].lower),
                                                LargeIntValue::to_string(buckets[i].upper), buckets[i].count,
                                                buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (pt_is_arithmetic<PT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(std::to_string(buckets[i].lower), std::to_string(buckets[i].upper),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (pt_is_date_or_datetime<PT>) {
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(buckets[i].lower.to_string(), buckets[i].upper.to_string(),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            } else if constexpr (pt_is_decimal<PT>) {
                int scale = ctx->get_arg_type(0)->scale;
                int precision = ctx->get_arg_type(0)->precision;
                for (int i = 0; i < buckets.size(); ++i) {
                    bucket_json += toBucketJson(DecimalV3Cast::to_string<T>(buckets[i].lower, scale, precision),
                                                DecimalV3Cast::to_string<T>(buckets[i].upper, scale, precision),
                                                buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                                   ",";
                }
            }
            bucket_json[bucket_json.size() - 1] = ']';
        }

        std::string histogram_json =
                "{" + fmt::format(" \"buckets\" : {}, \"top-n\" : {} ", bucket_json, topn_json) + "}";
        Slice slice(histogram_json);
        to->append_datum(slice);
    }

    std::string get_name() const override { return "histogram"; }
};

} // namespace starrocks::vectorized