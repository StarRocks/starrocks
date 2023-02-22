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

#include <type_traits>

#include "column/column.h"

namespace starrocks {
class FunctionContext;
}

namespace starrocks {

/**
 * For each aggregate function, it may use different agg state kind for Incremental MV:
 *  - RESULT                : Use result table data as AggStateData, eg sum/count
 *  - INTERMEDIATE          : Use intermediate table data as AggStateData which is a common state
 *                              for Accumulate mode just as agg_data in OLAP mode.
 *  - DETAIL_RESULT         : Use detail table data as AggStateData to retract in retract mode and
 *                              use result table data as intermediate result.
 *  - DETAIL_INTERMEDIATE   : Use detail table data as AggStateData to retract in retract mode and
 *                              use intermediate table data as intermediate result.
 *
 *  Optimizer will decide which kind of agg state data is used for each agg function in different retract mode.
 */
enum AggStateTableKind { RESULT = 0, INTERMEDIATE = 1, DETAIL_RESULT = 2, DETAIL_INTERMEDIATE = 3, UNSUPPORTED = 4 };

using AggDataPtr = uint8_t*;
using ConstAggDataPtr = const uint8_t*;

// Aggregate function interface
// Aggregate function instances don't contain aggregation state, the aggregation state is stored in
// other objects
// Aggregate function instances contain aggregate function description and state management methods
// Keyword __restrict is added everywhere AggDataPtr appears, which used to solve the problem of
// pointer aliasing and improve the performance of auto-vectorization. For better understanding,
// some micro-benchmark results are listed as follows
//      1. https://quick-bench.com/q/ZQoR7xloXdKcqLC-rPFLhSuHEf0
//      2. https://quick-bench.com/q/E5SfW3gn2IjJl4q0YIVMBPW8Ja8
//      3. https://quick-bench.com/q/yniGOh4CIz6YRGj85HFwu4WoHME
class AggregateFunction {
public:
    virtual ~AggregateFunction() = default;

    // Reset the aggregation state, for aggregate window functions
    virtual void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const {}

    // Update the aggregation state
    // columns points to columns containing arguments of aggregation function.
    // row_num is number of row which should be updated.
    virtual void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                        size_t row_num) const = 0;

    // Update/Merge the aggregation state with null
    virtual void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const {}

    // Merge the aggregation state
    // columns points to columns containing merge input,
    // We maybe need deserialize input data firstly.
    // row_num is number of row which should be merged.
    virtual void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
                       size_t row_num) const = 0;

    // When transmit data over network, we need to serialize agg data.
    // We serialize the agg data to |to| column
    // @param[out] to: maybe nullable
    virtual void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const = 0;

    // batch serialize aggregate state to reduce virtual function call
    virtual void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                                 size_t state_offsets, Column* to) const = 0;

    // Change the aggregation state to final result if necessary
    virtual void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const = 0;

    // batch finalize aggregate state to reduce virtual function call
    virtual void batch_finalize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                                size_t state_offsets, Column* to) const = 0;

    // For streaming aggregation, we directly convert column data to serialize format
    virtual void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                             ColumnPtr* dst) const = 0;

    // Insert current aggregation state into dst column from start to end
    // For aggregation window functions
    virtual void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                            size_t end) const {}

    virtual std::string get_name() const = 0;

    // State management methods:
    virtual size_t size() const = 0;
    virtual size_t alignof_size() const = 0;
    virtual bool is_pod_state() const { return false; }
    virtual void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const = 0;
    virtual void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const = 0;

    virtual void batch_create_with_selection(FunctionContext* ctx, size_t chunk_size, Buffer<AggDataPtr>& states,
                                             size_t state_offset, const std::vector<uint8_t>& selection) const {
        for (size_t i = 0; i < chunk_size; i++) {
            if (selection[i] == 0) {
                create(ctx, states[i] + state_offset);
            }
        }
    }

    virtual void batch_destroy_with_selection(FunctionContext* ctx, size_t chunk_size, Buffer<AggDataPtr>& states,
                                              size_t state_offset, const std::vector<uint8_t>& selection) const {
        for (size_t i = 0; i < chunk_size; i++) {
            if (selection[i] == 0) {
                destroy(ctx, states[i] + state_offset);
            }
        }
    }

    virtual void batch_finalize_with_selection(FunctionContext* ctx, size_t chunk_size,
                                               const Buffer<AggDataPtr>& agg_states, size_t state_offset, Column* to,
                                               const std::vector<uint8_t>& selection) const {
        for (size_t i = 0; i < chunk_size; i++) {
            if (selection[i] == 0) {
                this->finalize_to_column(ctx, agg_states[i] + state_offset, to);
            }
        }
    }

    // Contains a loop with calls to "update" function.
    // You can collect arguments into array "states"
    // and do a single call to "update_batch" for devirtualization and inlining.
    virtual void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                              AggDataPtr* states) const = 0;

    // filter[i] = 0, will be update
    virtual void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset,
                                          const Column** columns, AggDataPtr* states,
                                          const std::vector<uint8_t>& filter) const = 0;

    // update result to single state
    virtual void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                           AggDataPtr __restrict state) const = 0;

    // For window functions
    // A peer group is all of the rows that are peers within the specified ordering.
    // Rows are peers if they compare equal to each other using the specified ordering expression.
    virtual void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state,
                                                      const Column** columns, int64_t peer_group_start,
                                                      int64_t peer_group_end, int64_t frame_start,
                                                      int64_t frame_end) const {}

    // For window functions
    // A peer group is all of the rows that are peers within the specified ordering.
    // Rows are peers if they compare equal to each other using the specified ordering expression.
    // Update batch single state with null
    virtual void update_single_state_null(FunctionContext* ctx, AggDataPtr __restrict state, int64_t peer_group_start,
                                          int64_t peer_group_end) const {}

    // For window functions with slide frame
    // For example, given a slide frame, i.e. "sum() over (m preceding and n following)",
    // if we had already evaluated the state of (i-1)-th frame, then we can get the i-th frame through reusing
    // the sum of (i-1)-th frame, i.e. "sum(i) = sum(i-1) - v[i-1+rows_start_offset] +  v[i+rows_end_offset]"
    // Ignore subtraction if ignore_subtraction is true
    // Ignore addition if ignore_addition is true
    virtual void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state,
                                                     const Column** columns, int64_t current_row_position,
                                                     int64_t partition_start, int64_t partition_end,
                                                     int64_t rows_start_offset, int64_t rows_end_offset,
                                                     bool ignore_subtraction, bool ignore_addition) const {}

    // Contains a loop with calls to "merge" function.
    // You can collect arguments into array "states"
    // and do a single call to "merge_batch" for devirtualization and inlining.
    virtual void merge_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                             AggDataPtr* states) const = 0;

    // filter[i] = 0, will be merged
    virtual void merge_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset,
                                         const Column* column, AggDataPtr* states,
                                         const std::vector<uint8_t>& filter) const = 0;

    // Merge some continuous portion of a chunk to a given state.
    // This will be useful for sorted streaming aggregation.
    // 'start': the start position of the continuous portion
    // 'size': the length of the continuous portion
    virtual void merge_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column* column,
                                          size_t start, size_t size) const = 0;

    ///////////////// STREAM MV METHODS /////////////////

    // Return stream agg function's state table kind, see AggStateTableKind's description.
    // NOTE: `is_append_only` indicates whether the input only has append-only messages(eg, binlog of duplicated table)
    //       or has retract messages (eg, primary key tables). This rules should keep the same with FE/Optimizer.
    // NOTE: All stream mv agg function should implement this method to define where the state is stored and
    //       then can be used for incremental compute.
    // Example: min/max can use Result state table directly to save its intermediate state if the
    //          input is only append-only, however need use Detail state table to save detail inputs if
    //          the input is not append-only.
    virtual AggStateTableKind agg_state_table_kind(bool is_append_only) const { return AggStateTableKind::UNSUPPORTED; }

    // When the input is retract message(eg, DELETE/UPDATE_BEFORE ops), call this function to
    // generate the recording incremental retract messages for Agg.
    // NOTE: All state table kinds(result/intermediate/detail) need to implement this method to
    // support final consistency in Stream MV.
    virtual void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                         size_t row_num) const {
        throw std::runtime_error("retract function in aggregate is not supported for now.");
    }

    // NOTE: Below Methods for agg functions of Detail_Result/Detail_Intermediate kind,
    // other kinds no need to implement.

    // Agg functions of detail kind(Detail_Result/Detail_Intermediate) will store detail message for each
    // group-by-keys which can be used to generate retract messages. The detail message is a k-v map,
    // the k-v map is the recording of the specific key which `k` is the agg value and `v` is the count.
    // Restore previous input from the detail state table for the specific input group_by_keys+agg_key,
    // `columns` have two columns:
    // columns[0] is the `k`, the agg function's column,
    // columns[1] is the `v`, the count column for the `k`
    // To avoid restoring all details for the specific group_by_keys:
    // 1. use `restore_detail` to only restore details for the specific input group_by_keys+agg_key.
    // 2. then `restore_all_details` to restore details for the speicif input group_by_keys for
    //  all need `detail_sync` group_by_keys;
    virtual void restore_detail(FunctionContext* ctx, const Column* agg_column, size_t agg_row_idx,
                                const Column* count_column, size_t count_row_idx, AggDataPtr __restrict state) const {
        throw std::runtime_error("restore_detail function in aggregate is not supported for now.");
    }

    // NOTE: Methods for agg functions of Detail_Result/Detail_Intermediate kind, other kinds no need to implement.
    // To avoid restoring all details for the specific group_by_keys:
    // 1. use `restore_detail` to only restore details for the specific input group_by_keys+agg_key.
    // 2. then `restore_all_details` to restore details for the speicif input group_by_keys for
    //  all need `detail_sync` group_by_keys;
    virtual void restore_all_details(FunctionContext* ctx, AggDataPtr __restrict state, size_t chunk_size,
                                     const Columns& columns) const {
        throw std::runtime_error("restore_all_details function in aggregate is not supported for now.");
    }

    // NOTE: Methods for agg functions of Detail_Result/Detail_Intermediate kind,
    // other kinds no need to implement.
    // For each detail agg function, not every group by keys need to use detail state to generate results because:
    //  - the specific group by keys have no retract messages (even the input may have retract messages);
    //  - the specific group by keys have retract messages but have no affects with the agg result.
    //    eg, min(y) group by x, the input is +(1, 3), +(1, 4), the new retract message is -(1, 4)
    //    now the intermediate result is not affected because 4 > 3, so no need use detail state to generate retracts.
    // `is_sync` indicates whether to sync detail state(iter the state map) to genreate the
    // final result. Output need sync to `to` column(uint8_t column) for the specific group by keys.
    virtual void output_is_sync(FunctionContext* ctx, size_t chunk_size, Column* to,
                                AggDataPtr __restrict state) const {
        throw std::runtime_error("output_is_sync function in aggregate is not supported for now.");
    }

    // NOTE: Methods for agg functions of Detail_Result/Detail_Intermediate kind, other kinds no need to implement.
    // Output the state map to state table to be reused for the next runs when the agg is over.
    // Output the detail map to `tos` columns, and save the map's size to `count` column. Columns `tos`
    // contain two columns:
    //  columns[0] is the aggregate function column(`k`);
    //  columns[1] is the count column(`v`);
    // Column `count` is output to indicate how many detail rows each key has.
    virtual void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& tos,
                               Column* count) const {
        throw std::runtime_error("output_detail function in aggregate is not supported for now.");
    }
};

template <typename State>
class AggregateFunctionStateHelper : public AggregateFunction {
protected:
    static State& data(AggDataPtr __restrict place) { return *reinterpret_cast<State*>(place); }
    static const State& data(ConstAggDataPtr __restrict place) { return *reinterpret_cast<const State*>(place); }

public:
    static constexpr bool pod_state() { return std::is_trivially_destructible_v<State>; }

    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const final { new (ptr) State; }

    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const final { data(ptr).~State(); }

    size_t size() const final { return sizeof(State); }

    size_t alignof_size() const final { return alignof(State); }

    bool is_pod_state() const override { return pod_state(); }
};

template <typename State, typename Derived>
class AggregateFunctionBatchHelper : public AggregateFunctionStateHelper<State> {
public:
    void batch_create_with_selection(FunctionContext* ctx, size_t chunk_size, Buffer<AggDataPtr>& states,
                                     size_t state_offset, const std::vector<uint8_t>& selection) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            if (selection[i] == 0) {
                static_cast<const Derived*>(this)->create(ctx, states[i] + state_offset);
            }
        }
    }

    void batch_destroy_with_selection(FunctionContext* ctx, size_t chunk_size, Buffer<AggDataPtr>& states,
                                      size_t state_offset, const std::vector<uint8_t>& selection) const override {
        if constexpr (AggregateFunctionStateHelper<State>::pod_state()) {
            // nothing TODO
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                if (selection[i] == 0) {
                    static_cast<const Derived*>(this)->destroy(ctx, states[i] + state_offset);
                }
            }
        }
    }

    void batch_finalize_with_selection(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                                       size_t state_offset, Column* to,
                                       const std::vector<uint8_t>& selection) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            if (selection[i] == 0) {
                static_cast<const Derived*>(this)->finalize_to_column(ctx, agg_states[i] + state_offset, to);
            }
        }
    }

    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            static_cast<const Derived*>(this)->update(ctx, columns, states[i] + state_offset, i);
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            // TODO: optimize with simd ?
            if (filter[i] == 0) {
                static_cast<const Derived*>(this)->update(ctx, columns, states[i] + state_offset, i);
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            static_cast<const Derived*>(this)->update(ctx, columns, state, i);
        }
    }

    void merge_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            static_cast<const Derived*>(this)->merge(ctx, column, states[i] + state_offset, i);
        }
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            // TODO: optimize with simd ?
            if (filter[i] == 0) {
                static_cast<const Derived*>(this)->merge(ctx, column, states[i] + state_offset, i);
            }
        }
    }

    void merge_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column* input, size_t start,
                                  size_t size) const override {
        for (size_t i = start; i < start + size; ++i) {
            static_cast<const Derived*>(this)->merge(ctx, input, state, i);
        }
    }

    void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            static_cast<const Derived*>(this)->serialize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }

    void batch_finalize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < chunk_size; i++) {
            static_cast<const Derived*>(this)->finalize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }
};

using AggregateFunctionPtr = std::shared_ptr<AggregateFunction>;

struct AggregateFunctionEmptyState {};

} // namespace starrocks
