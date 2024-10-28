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
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_traits.h"
#include "gutil/casts.h"
#include "storage/types.h"

namespace starrocks {

// TODO(murphy) optimize performance for string column, whose has slowness at comparison
// TODO(murphy) optimize the algorithm, downsample the data instead of holding all input data
// TODO(muprhy) parallelise the algorithm
template <LogicalType LT>
struct Bucket {
    using RefType = AggDataRefType<LT>;
    using ValueType = AggDataValueType<LT>;

    Bucket() = default;

    Bucket(RefType input_lower, RefType input_upper, size_t count, size_t upper_repeats)
            : upper_repeats(upper_repeats), count_in_bucket(1) {
        AggDataTypeTraits<LT>::assign_value(lower, input_lower);
        AggDataTypeTraits<LT>::assign_value(upper, input_upper);
    }

    bool is_equals_to_upper(RefType value) {
        return AggDataTypeTraits<LT>::is_equal(value, AggDataTypeTraits<LT>::get_ref(upper));
    }

    void update_upper(RefType value) { AggDataTypeTraits<LT>::assign_value(upper, value); }

    Datum get_lower_datum() { return Datum(AggDataTypeTraits<LT>::get_ref(lower)); }
    Datum get_upper_datum() { return Datum(AggDataTypeTraits<LT>::get_ref(upper)); }

    ValueType lower;
    ValueType upper;
    // Up to this bucket, the total value
    int64_t count;
    // the number of values that on the upper boundary
    int64_t upper_repeats;
    // total value count in this bucket
    int64_t count_in_bucket;
};

template <LogicalType LT>
struct HistogramState {
    HistogramState() { column = RunTimeColumnType<LT>::create(); }

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
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);
        this->data(state).column->append(*column, 0, chunk_size);
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

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        auto bucket_num = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(1));
        [[maybe_unused]] double sample_ratio =
                1 / ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
        int bucket_size = this->data(state).column->size() / bucket_num;

        // Build bucket
        std::vector<Bucket<LT>> buckets;
        ColumnViewer<LT> viewer(this->data(state).column);
        for (size_t i = 0; i < viewer.size(); ++i) {
            auto v = viewer.value(i);
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

    std::string get_name() const override { return "histogram"; }
};

/*
      │     be/build_RELEASE/be/src/exprs/agg/histogram.h:136
  9.44 │6b0:   incq       -0x8(%r15)
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:135
  7.16 │       vmovdqu    -0x18(%r15),%xmm0
 10.67 │       vpsubq     -0x3323452(%rip),%xmm0,%xmm0        # 32e2300 <orc::DEC_32_TABLE+0x940>
  3.86 │       vmovdqu    %xmm0,-0x18(%r15)
  1.48 │6c8:   mov        %r14,%r13
       │     starrocks::ColumnViewer<(starrocks::LogicalType)11>::size() const:
       │     be/build_RELEASE/be/src/column/column_viewer.h:50
  0.53 │6cb:   mov        -0x178(%rbp),%rax
       │     std::vector<double, starrocks::ColumnAllocator<double> >::size() const:
  5.51 │       mov        0x18(%rax),%rcx
  4.54 │       sub        0x10(%rax),%rcx
       │     starrocks::HistogramAggregationFunction<(starrocks::LogicalType)11, double>::finalize_to_column(starrocks::FunctionContext*, unsigned char const*, starrocks::Column*) const:
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:126
  1.36 │       inc        %rbx
       │     std::vector<double, starrocks::ColumnAllocator<double> >::size() const:
  1.10 │       sar        $0x3,%rcx
  5.17 │       mov        %r13,%r14
       │     starrocks::HistogramAggregationFunction<(starrocks::LogicalType)11, double>::finalize_to_column(starrocks::FunctionContext*, unsigned char const*, starrocks::Column*) const:
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:126
  4.39 │       cmp        %rcx,%rbx
       │     ↑ jae        17e
       │     starrocks::ColumnViewer<(starrocks::LogicalType)11>::value(unsigned long) const:
       │     be/build_RELEASE/be/src/column/column_viewer.h:46
  1.02 │6ed:   mov        -0x170(%rbp),%rax
  0.60 │       mov        -0x160(%rbp),%rcx
  5.43 │       and        %rbx,%rcx
  7.85 │       vmovsd     (%rax,%rcx,8),%xmm1
       │     _ZN9__gnu_cxxeqIPKN9starrocks6BucketILNS1_11LogicalTypeE11EEESt6vectorIS4_SaIS4_EEEEbRKNS_17__normal_iteratorIT_T0_EESF_QrqXeqcldtfp_4baseEcldtfp0_4baseERSt14convertible_toIbEE():
  1.16 │       cmp        %r15,%r14
       │     starrocks::HistogramAggregationFunction<(starrocks::LogicalType)11, double>::finalize_to_column(starrocks::FunctionContext*, unsigned char const*, starrocks::Column*) const:
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:128
       │     ↓ je         750
       │     starrocks::AggDataTypeTraits<(starrocks::LogicalType)11, int>::get_ref(double const&):
       │     be/build_RELEASE/be/src/exprs/agg/aggregate_traits.h:41
  0.49 │       vmovsd     -0x20(%r15),%xmm0
       │     starrocks::AggDataTypeTraits<(starrocks::LogicalType)11, int>::is_equal(double const&, double const&):
       │     be/build_RELEASE/be/src/exprs/agg/aggregate_traits.h:46
  7.74 │       vucomisd   %xmm1,%xmm0
       │     starrocks::HistogramAggregationFunction<(starrocks::LogicalType)11, double>::finalize_to_column(starrocks::FunctionContext*, unsigned char const*, starrocks::Column*) const:
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:134
  5.37 │     ↓ jne        716
  2.68 │     ↑ jnp        6b0
  7.07 │716:   vmovsd     %xmm1,-0x30(%rbp)
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:139
  0.06 │       mov        -0x8(%r15),%rax
  0.83 │       cmp        -0x108(%rbp),%rax
       │     ↓ jge        7a1
  1.54 │       vmovq      -0x30(%rbp),%xmm0
       │     starrocks::AggDataTypeTraits<(starrocks::LogicalType)11, int>::assign_value(double&, double const&):
       │     be/build_RELEASE/be/src/exprs/agg/aggregate_traits.h:35
  1.71 │       vmovq      %xmm0,-0x20(%r15)
       │     starrocks::HistogramAggregationFunction<(starrocks::LogicalType)11, double>::finalize_to_column(starrocks::FunctionContext*, unsigned char const*, starrocks::Column*) const:
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:144
  0.23 │       incq       -0x18(%r15)
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:145
  0.73 │       inc        %rax
  0.02 │       mov        %rax,-0x8(%r15)
       │     be/build_RELEASE/be/src/exprs/agg/histogram.h:146
  0.12 │       movq       $0x1,-0x10(%r15)
  0.12 │     ↑ jmp        6c8*/

} // namespace starrocks
