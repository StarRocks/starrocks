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
#include "column/column_viewer.h"
#include "column/datum_convert.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_traits.h"
#include "gutil/casts.h"
#include "simdjson.h"
#include "storage/types.h"
#include "util/ndv_estimator.h"

namespace starrocks {

template <LogicalType LT>
struct Bucket {
    using RefType = AggDataRefType<LT>;
    using ValueType = AggDataValueType<LT>;

    static Bucket from_json(simdjson::ondemand::array bucket_json, const FunctionContext::TypeDesc* type_desc,
                            MemPool* mem_pool) {
        TypeInfoPtr type_info = get_type_info(LT, type_desc->precision, type_desc->scale);
        auto bucket_iter = bucket_json.begin();

        Datum lower_datum;
        std::ignore =
                datum_from_string(type_info.get(), &lower_datum, std::string{std::string_view{*bucket_iter}}, mem_pool);
        ++bucket_iter;

        Datum upper_datum;
        std::ignore =
                datum_from_string(type_info.get(), &upper_datum, std::string{std::string_view{*bucket_iter}}, mem_pool);
        ++bucket_iter;

        int64_t count = std::stoll(std::string{std::string_view{*bucket_iter}});
        ++bucket_iter;
        int64_t upper_repeats = std::stoll(std::string{std::string_view{*bucket_iter}});

        return Bucket(lower_datum.get<RefType>(), upper_datum.get<RefType>(), count, upper_repeats);
    }

    Bucket() = default;

    Bucket(RefType input_lower, RefType input_upper, size_t count, size_t upper_repeats)
            : count(count), upper_repeats(upper_repeats), count_in_bucket(1) {
        AggDataTypeTraits<LT>::assign_value(lower, input_lower);
        AggDataTypeTraits<LT>::assign_value(upper, input_upper);
    }

    bool is_equals_to_upper(RefType value) {
        return AggDataTypeTraits<LT>::is_equal(value, AggDataTypeTraits<LT>::get_ref(upper));
    }

    bool is_greater_equal_to_lower(RefType value) {
        return AggDataTypeTraits<LT>::is_greater_equal(value, AggDataTypeTraits<LT>::get_ref(lower));
    }

    bool is_less_equal_to_upper(RefType value) {
        return AggDataTypeTraits<LT>::is_less_equal(value, AggDataTypeTraits<LT>::get_ref(upper));
    }

    void update_upper(RefType value) { AggDataTypeTraits<LT>::assign_value(upper, value); }

    Datum get_lower_datum() const { return Datum(AggDataTypeTraits<LT>::get_ref(lower)); }
    Datum get_upper_datum() const { return Datum(AggDataTypeTraits<LT>::get_ref(upper)); }

    ValueType lower;
    ValueType upper;
    // Up to this bucket, the total value
    int64_t count;
    // the number of values that on the upper boundary
    int64_t upper_repeats;
    // total value count in this bucket
    int64_t count_in_bucket;
    // distinct value count estimation in this bucket
    int64_t distinct_count;
};

template <LogicalType LT>
struct HistogramState {
    HistogramState() {
        auto data = RunTimeColumnType<LT>::create();
        column = NullableColumn::create(std::move(data), NullColumn::create());
    }

    ColumnPtr column;
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class HistogramAggregationFunction final
        : public AggregateFunctionBatchHelper<HistogramState<LT>, HistogramAggregationFunction<LT, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        CHECK(false);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        this->data(state).column->append(*columns[0], 0, chunk_size);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        //Histogram aggregation function only support one stage Agg
        CHECK(false);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        //Histogram aggregation function only support one stage Agg
        CHECK(false);
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override {
        //Histogram aggregation function only support one stage Agg
        CHECK(false);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        //Histogram aggregation function only support one stage Agg
        CHECK(false);
    }

    std::string toBucketJson(const std::string& lower, const std::string& upper, size_t count, size_t upper_repeats,
                             double sample_ratio) const {
        return fmt::format(R"(["{}","{}","{}","{}"])", lower, upper, (int64_t)(count * sample_ratio),
                           (int64_t)(upper_repeats * sample_ratio));
    }

    std::string toBucketJson(const std::string& lower, const std::string& upper, size_t count, size_t upper_repeats,
                             int64_t distinct_count, double sample_ratio) const {
        return fmt::format(R"(["{}","{}","{}","{}","{}"])", lower, upper, (int64_t)(count * sample_ratio),
                           (int64_t)(upper_repeats * sample_ratio), distinct_count);
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        if (ctx->get_num_args() == 4) {
            finalize_to_column_with_ndv(ctx, state, to);
        } else {
            finalize_to_column_without_ndv(ctx, state, to);
        }
    }

    std::string get_name() const override { return "histogram"; }

private:
    void finalize_to_column_without_ndv(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                                        Column* to) const {
        auto bucket_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        [[maybe_unused]] double sample_ratio =
                1 / ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
        int bucket_size = this->data(state).column->size() / bucket_num;

        // Build bucket
        std::vector<Bucket<LT>> buckets;
        ColumnViewer<LT> viewer(this->data(state).column);
        for (size_t i = 0; i < viewer.size(); ++i) {
            auto v = viewer.value(i);
            if (viewer.is_null(i)) {
                continue;
            }
            if (buckets.empty()) {
                Bucket<LT> bucket(v, v, 1, 1);
                buckets.emplace_back(bucket);
            } else {
                Bucket<LT>* last_bucket = &buckets.back();

                if (last_bucket->is_equals_to_upper(v)) {
                    last_bucket->count++;
                    last_bucket->count_in_bucket++;
                    last_bucket->upper_repeats++;
                } else {
                    if (last_bucket->count_in_bucket >= bucket_size) {
                        Bucket<LT> bucket(v, v, last_bucket->count + 1, 1);
                        buckets.emplace_back(bucket);
                    } else {
                        last_bucket->update_upper(v);
                        last_bucket->count++;
                        last_bucket->count_in_bucket++;
                        last_bucket->upper_repeats = 1;
                    }
                }
            }
        }

        const auto& type_desc = ctx->get_arg_type(0);
        TypeInfoPtr type_info = get_type_info(LT, type_desc->precision, type_desc->scale);
        std::string bucket_json;
        if (buckets.empty()) {
            bucket_json = "[]";
        } else {
            bucket_json = "[";

            for (int i = 0; i < buckets.size(); ++i) {
                std::string lower_str = datum_to_string(type_info.get(), buckets[i].get_lower_datum());
                std::string upper_str = datum_to_string(type_info.get(), buckets[i].get_upper_datum());
                bucket_json +=
                        toBucketJson(lower_str, upper_str, buckets[i].count, buckets[i].upper_repeats, sample_ratio) +
                        ",";
            }

            bucket_json[bucket_json.size() - 1] = ']';
        }

        Slice slice(bucket_json);
        to->append_datum(slice);
    }

    void finalize_to_column_with_ndv(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                                     Column* to) const {
        auto bucket_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        auto sample_ratio = ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
        auto ndv_estimator_name = ColumnHelper::get_const_value<TYPE_VARCHAR>(ctx->get_constant_column(3)).to_string();
        [[maybe_unused]] double sample_factor = 1 / sample_ratio;
        int bucket_size = this->data(state).column->size() / bucket_num;

        // prepare ndv estimation
        auto ndv_estimator = NDVEstimatorBuilder::build(ndv_estimator_name);
        int64_t sample_distinct = 0;
        int64_t count_once = 0;
        bool new_upper = false;

        // Build bucket
        std::vector<Bucket<LT>> buckets;
        ColumnViewer<LT> viewer(this->data(state).column);
        for (size_t i = 0; i < viewer.size(); ++i) {
            auto v = viewer.value(i);
            if (viewer.is_null(i)) {
                continue;
            }
            if (buckets.empty()) {
                Bucket<LT> bucket(v, v, 1, 1);
                buckets.emplace_back(bucket);
                sample_distinct = 1;
                count_once = 1;
                new_upper = true;
            } else {
                Bucket<LT>* last_bucket = &buckets.back();

                if (last_bucket->is_equals_to_upper(v)) {
                    last_bucket->count++;
                    last_bucket->count_in_bucket++;
                    last_bucket->upper_repeats++;
                    if (new_upper) {
                        count_once--;
                        new_upper = false;
                    }
                } else {
                    if (last_bucket->count_in_bucket >= bucket_size) {
                        int64_t last_ndv = ndv_estimator->estimate(last_bucket->count_in_bucket, sample_distinct,
                                                                  count_once, sample_ratio);
                        last_bucket->distinct_count = last_ndv;
                        Bucket<LT> bucket(v, v, last_bucket->count + 1, 1);
                        buckets.emplace_back(bucket);
                        sample_distinct = 1;
                        count_once = 1;
                        new_upper = true;
                    } else {
                        last_bucket->update_upper(v);
                        last_bucket->count++;
                        last_bucket->count_in_bucket++;
                        last_bucket->upper_repeats = 1;
                        sample_distinct++;
                        count_once++;
                    }
                }
            }
        }

        Bucket<LT>* last_bucket = &buckets.back();
        double last_ndv = ndv_estimator->estimate(last_bucket->count_in_bucket, sample_distinct, count_once,
                                                  sample_ratio);
        last_bucket->distinct_count = last_ndv;

        const auto& type_desc = ctx->get_arg_type(0);
        TypeInfoPtr type_info = get_type_info(LT, type_desc->precision, type_desc->scale);
        std::string bucket_json;
        if (buckets.empty()) {
            bucket_json = "[]";
        } else {
            bucket_json = "[";

            for (int i = 0; i < buckets.size(); ++i) {
                std::string lower_str = datum_to_string(type_info.get(), buckets[i].get_lower_datum());
                std::string upper_str = datum_to_string(type_info.get(), buckets[i].get_upper_datum());
                bucket_json +=
                        toBucketJson(lower_str, upper_str, buckets[i].count, buckets[i].upper_repeats,
                        buckets[i].distinct_count, sample_factor) +
                        ",";
            }

            bucket_json[bucket_json.size() - 1] = ']';
        }

        Slice slice(bucket_json);
        to->append_datum(slice);
    }
};

} // namespace starrocks
