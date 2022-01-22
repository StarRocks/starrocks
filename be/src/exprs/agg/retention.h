// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "udf/udf_internal.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks::vectorized {
struct RetentionState {
    void merge_array_element(const DatumArray& datum_array) {
        auto size = datum_array.size();
        if (!is_initial) {
            is_initial = true;
            boolean_vector.resize(size);
        }

        // All array conditions' size is equal.
        DCHECK_EQ(boolean_vector.size(), datum_array.size());

        for (int i = 0; i < size; ++i) {
            // Use union operation between different rows of the same condition.
            boolean_vector[i] |= datum_array[i].is_null() ? 0 : datum_array[i].get_uint8();
        }
    }

    template <bool finalize>
    void serialize_to_array_column(ArrayColumn* array_column) const {
        if (is_initial) {
            size_t size = boolean_vector.size();
            DatumArray array;
            array.reserve(size);
            for (int i = 0; i < size; i++) {
                if constexpr (finalize) {
                    // Get final result through remove values that first condition is not satisfied.
                    array.emplace_back((uint8_t)(boolean_vector[0] & boolean_vector[i]));
                } else {
                    array.emplace_back(boolean_vector[i]);
                }
            }
            array_column->append_datum(array);
        }
    }

    bool is_initial = false;

    /*
     * The nth element of boolean_vector is the partial result of 
     * nth condition in retention definition:
     * 
     * ARRAY retention(Array[cond1, cond2, cond3, cond4...]);
     * 
     */
    std::vector<uint8_t> boolean_vector;
};

/*
 * retention is a aggregate function to compute from input(array) to output(array), result is consist of (0|1) value, 
 * It's definition is follows:
 * 
 *  ARRAY retention(Array[cond1, cond2, cond3, cond4...])
 * 
 *  cond is predicates, and 
 *  The 1th element of output ARRAY is compute with cond1.
 *  The nth(n > 1) element of output ARRAY is compute with(cond1 && condn).
 * 
 * for example, we could compute the Retention of 2020-01-02 from 2020-01-01, through following sql:
 * 
 *              select sum(retention[2]) / sum(retention[1]) as retention from (select retention([event_type = '点击' and time
 *              = '2020-01-01', event_type = '支付' and time = '2020-01-02']) as retention from test_retention 
 *              where time = '2020-01-01' or time = '2020-01-02' group by uid) t;
 */
class RetentionAggregateFunction final
        : public AggregateFunctionBatchHelper<RetentionState, RetentionAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto* column = down_cast<const ArrayColumn*>(columns[0]);
        auto ele_vector = column->get(row_num).get_array();
        this->data(state).merge_array_element(ele_vector);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        auto ele_vector = input_column->get(row_num).get_array();
        this->data(state).merge_array_element(ele_vector);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<ArrayColumn*>(to);
        this->data(state).serialize_to_array_column<false>(column);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        this->data(state).serialize_to_array_column<true>(array_column);
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        const auto* src_column = down_cast<const ArrayColumn*>(src[0].get());
        for (size_t i = 0; i < chunk_size; ++i) {
            auto ele_vector = src_column->get(i).get_array();
            dst_column->append_datum(ele_vector);
        }
    }

    std::string get_name() const override { return "retention"; }
};

} // namespace starrocks::vectorized
