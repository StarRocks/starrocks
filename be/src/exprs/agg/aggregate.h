// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <type_traits>

#include "column/column.h"

namespace starrocks_udf {
class FunctionContext;
}

using starrocks_udf::FunctionContext;

namespace starrocks {
namespace vectorized {

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

    // For aggregation window functions
    // For certain window functions, such as "rank," the process will be executed in streaming mode,
    // and the columns that were previously buffered but are no longer needed will be contracted.
    // And if the implementation of the window function record some certain information, such as partition boundary,
    // this information should be updated as well.
    virtual void reset_state_for_contraction(FunctionContext* ctx, AggDataPtr __restrict state, size_t count) const {}

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

    // merge result to single state
    virtual void merge_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column* column,
                                          AggDataPtr __restrict state) const = 0;
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

    void merge_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column* column,
                                  AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            static_cast<const Derived*>(this)->merge(ctx, column, state, i);
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

} // namespace vectorized
} // namespace starrocks
