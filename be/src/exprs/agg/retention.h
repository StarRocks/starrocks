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
#include "exprs/function_context.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks {
struct RetentionState {
    static void udpate(uint64_t* value_ptr, const ArrayColumn* column, size_t row_num) {
        const auto& ele_col = column->elements();
        const auto& offsets = column->offsets().get_data();

        size_t array_size = 0;
        if (ele_col.is_nullable()) {
            const auto& null_column = down_cast<const NullableColumn&>(ele_col);
            auto data_column = down_cast<BooleanColumn*>(null_column.data_column().get());
            size_t offset = offsets[row_num];
            array_size = offsets[row_num + 1] - offset;

            // We allow 31 conditions at most, so limit it
            if (array_size > MAX_CONDITION_SIZE) {
                array_size = MAX_CONDITION_SIZE;
            }

            for (size_t i = 0; i < array_size; ++i) {
                auto ele_offset = offset + i;
                if (!null_column.is_null(ele_offset) && data_column->get_data()[ele_offset]) {
                    // Set right bit for condition.
                    (*value_ptr) |= RetentionState::bool_values[i];
                }
            }
        } else {
            const auto& data_column = down_cast<const BooleanColumn&>(ele_col);
            size_t offset = offsets[row_num];
            array_size = offsets[row_num + 1] - offset;

            if (array_size > MAX_CONDITION_SIZE) {
                array_size = MAX_CONDITION_SIZE;
            }

            for (size_t i = 0; i < array_size; ++i) {
                if (data_column.get_data()[offset + i]) {
                    (*value_ptr) |= RetentionState::bool_values[i];
                }
            }
        }

        (*value_ptr) |= array_size;
    }

    void udpate(const ArrayColumn* column, size_t row_num) {
        uint64_t* value_ptr = &boolean_value;
        RetentionState::udpate(value_ptr, column, row_num);
    }

    void finalize_to_array_column(ArrayColumn* array_column) const {
        auto size = (boolean_value & MAX_CONDITION_SIZE);
        DatumArray array;
        if (size > 0) {
            array.reserve(size);
            auto first_condition = ((boolean_value & bool_values[0]) > 0);
            array.emplace_back((uint8_t)first_condition);
            if (first_condition) {
                for (int i = 1; i < size; ++i) {
                    array.emplace_back((uint8_t)((boolean_value & bool_values[i]) > 0));
                }
            } else {
                for (int i = 1; i < size; ++i) {
                    array.emplace_back((uint8_t)0);
                }
            }
        }
        array_column->append_datum(array);
    }

    // We use top 31 bits of boolean_value to indicate which condition is true;
    // We use the last 5 bits of boolean_value to indicate size of conditions(so 31 conditions at most).
    uint64_t boolean_value;

    // Mask is used to identify top 31 bits.
    static inline uint64_t bool_values[] = {1UL << 63, 1UL << 62, 1UL << 61, 1UL << 60, 1UL << 59, 1UL << 58, 1UL << 57,
                                            1UL << 56, 1UL << 55, 1UL << 54, 1UL << 53, 1UL << 52, 1UL << 51, 1UL << 50,
                                            1UL << 49, 1UL << 48, 1UL << 47, 1UL << 46, 1UL << 45, 1UL << 44, 1UL << 43,
                                            1UL << 42, 1UL << 41, 1UL << 40, 1UL << 39, 1UL << 38, 1UL << 37, 1UL << 36,
                                            1UL << 35, 1UL << 34, 1UL << 33};
    static constexpr int MAX_CONDITION_SIZE_BIT = 5;
    // Mask is used to identify the last 5 bits.
    static constexpr int MAX_CONDITION_SIZE = (1 << MAX_CONDITION_SIZE_BIT) - 1;
};

/*
 * retention is a aggregate function to compute from input(array) to output(array), result is consist of (0|1) value, 
 * It's definition is follows:
 * 
 *  ARRAY retention(Array[cond1, cond2, cond3, cond4...]) (at most 31 conditions)
 * 
 *  cond is predicates, and 
 *  The 1th element of output ARRAY is compute with cond1.
 *  The nth(n > 1) element of output ARRAY is compute with(cond1 && condn).
 * 
 * for example, we could compute the Retention of 2020-01-02 from 2020-01-01, through following sql:
 * 
 *              select sum(retention[2]) / sum(retention[1]) as retention from (select retention([event_type = 'click' and time
 *              = '2020-01-01', event_type = 'payment' and time = '2020-01-02']) as retention from test_retention 
 *              where time = '2020-01-01' or time = '2020-01-02' group by uid) t;
 */
class RetentionAggregateFunction final
        : public AggregateFunctionBatchHelper<RetentionState, RetentionAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto column = down_cast<const ArrayColumn*>(columns[0]);
        this->data(state).udpate(column, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const Int64Column*>(column);
        this->data(state).boolean_value |= input_column->get_data()[row_num];
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        down_cast<Int64Column*>(to)->append(this->data(state).boolean_value);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        this->data(state).finalize_to_array_column(array_column);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<Int64Column*>((*dst).get());
        dst_column->reserve(chunk_size);

        const auto* src_column = down_cast<const ArrayColumn*>(src[0].get());
        for (size_t i = 0; i < chunk_size; ++i) {
            uint64_t boolean_value = 0;
            RetentionState::udpate(&boolean_value, src_column, i);
            dst_column->append(boolean_value);
        }
    }

    std::string get_name() const override { return "retention"; }
};

} // namespace starrocks
