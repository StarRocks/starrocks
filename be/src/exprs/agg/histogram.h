// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {

template <typename T>
struct Bucket {
public:
    Bucket() {}
    Bucket(T lower, T upper, size_t count) : lower(lower), upper(upper), count(count) {}
    T lower;
    T upper;
    size_t count;
    size_t upper_value_count;
};

template <typename T>
struct MostCommonValue {
    T value;
    size_t count;
};

template <typename T>
struct HistogramState {
    HistogramState() {}
    std::vector<Bucket<T>> buckets;
    std::vector<MostCommonValue<T>> mcv;
};

template <PrimitiveType PT, typename T = RunTimeCppType<PT>>
class HistogramAggregationFunction final
        : public AggregateFunctionBatchHelper<HistogramState<T>, HistogramAggregationFunction<PT, T>> {
public:
    using ColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        std::cout << "update" << std::endl;
        int64_t total_rows = columns[1]->get(0).get_int64();
        int64_t bucket_num = columns[2]->get(0).get_int64();
        int64_t bucket_size = total_rows / bucket_num;
        std::cout << "total_rows " << total_rows << "bucket_num " << bucket_num << "bucket_size " << bucket_size;

        //if constexpr (pt_is_arithmetic<PT>) {
            T v;
            //std::cout << "row " << row_num << " " << v << std::endl;

            if (columns[0]->is_nullable()) {
                if (columns[0]->is_null(row_num)) {
                    return;
                }

                const auto* data_column = down_cast<const NullableColumn*>(columns[0]);
                //const Column* data_column = &column->data_column_ref();
                v = down_cast<const ColumnType*>(data_column->data_column().get())->get_data()[row_num];
                //v = down_cast<const ColumnType*>(data_column)[row_num];
            } else {
                v = down_cast<const ColumnType*>(columns[0])->get_data()[row_num];
            }

            if (this->data(state).buckets.size() == 0) {
                Bucket<T> bucket(v, v, 1);
                this->data(state).buckets.push_back(bucket);
            } else {
                Bucket<T> lastBucket = this->data(state).buckets.back();

                if (lastBucket.upper == v) {
                    this->data(state).buckets.back().count++;
                } else {
                    if (lastBucket.count >= bucket_size) {
                        Bucket<T> bucket(v, v, 1);
                        this->data(state).buckets.push_back(bucket);
                    } else {
                        this->data(state).buckets.back().upper = v;
                        this->data(state).buckets.back().count++;
                    }
                }
            }
        //}

        /*
        if constexpr (pt_is_bigint<PT>) {
            const auto& column = down_cast<const ColumnType&>(*columns[0]);
            std::cout << "row " << row_num << " ";
            std::cout << column.get_data()[row_num] << std::endl;
        }
         */

        //if constexpr (pt_is_binary<PT>) {
        //Slice s = column->get_slice(row_num);
        //} else {
        //const auto& v = column->get_data();
        //if constexpr (pt_is_integer<PT>) {
        //    std::cout << "row_num : " << row_num << column->get_data()[row_num] << std::endl;
        //}
        //}
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                   int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) const override {
        std::cout << "update_batch_single_state" << std::endl;
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        std::cout << "merge" << std::endl;
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override {
        std::cout << "serialize_to_column" << std::endl;
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        std::cout << "convert_to_serialize_format" << std::endl;
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        std::cout << "finalize_to_column" << std::endl;
        HistogramState<T> histogramState = this->data(state);
        std::cout << "histogram state" << histogramState.buckets.size() << std::endl;

        if constexpr (pt_is_bigint<PT>) {
            std::string histogram_json = "{ \"buckets\" : [";
            for (int i = 0; i < histogramState.buckets.size(); ++i) {
                std::string bucket_json_array = "[" + std::to_string(histogramState.buckets[i].lower) + "," +
                                                std::to_string(histogramState.buckets[i].upper) + "," +
                                                std::to_string(histogramState.buckets[i].count) + "]";
                histogram_json = histogram_json + bucket_json_array + ",";
            }
            histogram_json[histogram_json.size() - 1] = ']';
            histogram_json = histogram_json + " }";

            std::cout << histogram_json << std::endl;
            Slice slice(histogram_json);

            if (to->is_nullable()) {
                auto* nullable_column = down_cast<NullableColumn*>(to);
                nullable_column->data_column()->append_datum(slice);
            } else {
                BinaryColumn* binary_column = down_cast<BinaryColumn*>(to);
                binary_column->append(slice);
            }
        }
    }

    std::string get_name() const override { return "histogram"; }
};

} // namespace starrocks::vectorized