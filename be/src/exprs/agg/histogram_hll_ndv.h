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

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "types/hll.h"

namespace starrocks {

template <LogicalType LT>
struct HistogramHllNdvState {
    bool initialized = false;
    MemPool mem_pool;
    std::vector<Bucket<LT>> buckets;
    std::vector<HyperLogLog> hlls;
};

template <LogicalType LT, typename T = RunTimeCppType<LT>>
class HistogramHllNdvAggregateFunction final
        : public AggregateFunctionBatchHelper<HistogramHllNdvState<LT>, HistogramHllNdvAggregateFunction<LT, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void create_impl(FunctionContext* ctx, HistogramHllNdvState<LT>& state) const {
        DCHECK_EQ(ctx->get_num_args(), 2);
        state.initialized = true;
        std::vector<Bucket<LT>> buckets;

        try {
            simdjson::padded_string padded_buckets_string = ctx->get_constant_column(1)->get(0).get_slice().to_string();
            simdjson::ondemand::parser simdjson_parser;
            simdjson::ondemand::document doc;
            simdjson_parser.iterate(padded_buckets_string).get(doc);
            simdjson::ondemand::array outer_array = doc.get_array();
            for (auto bucket_json : outer_array) {
                simdjson::ondemand::array inner_array = bucket_json.get_array();
                buckets.push_back(Bucket<LT>::from_json(inner_array, ctx->get_arg_type(0), &state.mem_pool));
            }
        } catch (const simdjson::simdjson_error& e) {
            throw std::runtime_error("histogram_hll_ndv: can't parse JSON specification of histogram buckets.");
        }

        state.buckets = buckets;
        state.hlls = std::vector<HyperLogLog>(buckets.size());
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        auto& state_impl = this->data(state);
        state_impl.initialized = false;
        state_impl.buckets.clear();
        state_impl.hlls.clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto& state_impl = this->data(state);
        if (!state_impl.initialized) {
            create_impl(ctx, state_impl);
        }

        const auto* column = down_cast<const ColumnType*>(columns[0]);
        auto& buckets = this->data(state).buckets;
        auto bucket_it{buckets.end()};
        uint64_t hash = 0;

        // find the correct bucket for the current value.
        if constexpr (lt_is_string_or_binary<LT>) {
            Slice s = column->get_slice(row_num);
            bucket_it = std::upper_bound(buckets.begin(), buckets.end(), s,
                                         [](auto& s, Bucket<LT>& bucket) { return bucket.is_less_equal_to_upper(s); });
            if (bucket_it != buckets.end() && !bucket_it->is_greater_equal_to_lower(s)) {
                bucket_it = buckets.end();
            }

            hash = HashUtil::murmur_hash64A(s.data, s.size, HashUtil::MURMUR_SEED);
        } else {
            const auto v = column->get_data();
            bucket_it = std::upper_bound(
                    buckets.begin(), buckets.end(), v[row_num],
                    [](auto& value, Bucket<LT>& bucket) { return bucket.is_less_equal_to_upper(value); });
            if (bucket_it != buckets.end() && !bucket_it->is_greater_equal_to_lower(v[row_num])) {
                bucket_it = buckets.end();
            }

            hash = HashUtil::murmur_hash64A(&v[row_num], sizeof(v[row_num]), HashUtil::MURMUR_SEED);
        }

        if (hash != 0 && bucket_it != buckets.end()) {
            int index = std::distance(buckets.begin(), bucket_it);
            this->data(state).hlls[index].update(hash);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        CHECK(false);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        CHECK(false);
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        CHECK(false);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        CHECK(false);
    }

    std::string toBucketJson(const std::string& lower, const std::string& upper, size_t count, size_t upper_repeats,
                             size_t distinct_count) const {
        return fmt::format(R"(["{}","{}","{}","{}","{}"])", lower, upper, count, upper_repeats, distinct_count);
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        auto& buckets = this->data(state).buckets;
        auto& hlls = this->data(state).hlls;

        const auto& type_desc = ctx->get_arg_type(0);
        TypeInfoPtr type_info = get_type_info(LT, type_desc->precision, type_desc->scale);
        std::string buckets_json;
        if (buckets.empty()) {
            buckets_json = "[]";
        } else {
            buckets_json = "[";

            for (int i = 0; i < buckets.size(); ++i) {
                int64_t distinct_count = hlls[i].estimate_cardinality();
                if (distinct_count == 0) {
                    distinct_count = 1;
                }

                std::string lower_str = datum_to_string(type_info.get(), buckets[i].get_lower_datum());
                std::string upper_str = datum_to_string(type_info.get(), buckets[i].get_upper_datum());
                buckets_json +=
                        toBucketJson(lower_str, upper_str, buckets[i].count, buckets[i].upper_repeats, distinct_count) +
                        ",";
            }

            buckets_json[buckets_json.size() - 1] = ']';
        }

        Slice slice(buckets_json);
        to->append_datum(slice);
    }

    std::string get_name() const override { return "histogram_hll_ndv"; }
};

} // namespace starrocks
