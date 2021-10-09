// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

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
class AggregateFunction {
public:
    virtual ~AggregateFunction() = default;

    // Reset the aggregation state, for aggregate window functions
    virtual void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const {}

    // Update the aggregation state
    // columns points to columns containing arguments of aggregation function.
    // row_num is number of row which should be updated.
    virtual void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const = 0;

    // Merge the aggregation state
    // columns points to columns containing merge input,
    // We maybe need deserialize input data firstly.
    // row_num is number of row which should be merged.
    virtual void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const = 0;

    // When transmit data over network, we need to serialize agg data.
    // We serialize the agg data to |to| column
    // @param[out] to: maybe nullable
    virtual void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const = 0;

    // batch serialize aggregate state to reduce virtual function call
    virtual void batch_serialize(size_t batch_size, const Buffer<AggDataPtr>& agg_states, size_t state_offsets,
                                 Column* to) const = 0;

    // Change the aggregation state to final result if necessary
    virtual void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const = 0;

    // batch finalize aggregate state to reduce virtual function call
    virtual void batch_finalize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                                size_t state_offsets, Column* to) const = 0;

    // For streaming aggregation, we directly convert column data to serialize format
    virtual void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const = 0;

    // Insert current aggregation state into dst column from start to end
    // For aggregation window functions
    virtual void get_values(FunctionContext* ctx, ConstAggDataPtr state, Column* dst, size_t start, size_t end) const {}

    virtual std::string get_name() const = 0;

    // State management methods:
    virtual size_t size() const = 0;
    virtual size_t alignof_size() const = 0;
    virtual void create(AggDataPtr ptr) const = 0;
    virtual void destroy(AggDataPtr ptr) const = 0;

    // Contains a loop with calls to "update" function.
    // You can collect arguments into array "states"
    // and do a single call to "update_batch" for devirtualization and inlining.
    virtual void update_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                              AggDataPtr* states) const = 0;

    // filter[i] = 0, will be update
    virtual void update_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset,
                                          const Column** columns, AggDataPtr* states,
                                          const std::vector<uint8_t>& filter) const = 0;

    // update result to single state
    virtual void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                           AggDataPtr state) const = 0;

    // For window functions
    // A peer group is all of the rows that are peers within the specified ordering.
    // Rows are peers if they compare equal to each other using the specified ordering expression.
    virtual void update_batch_single_state(FunctionContext* ctx, AggDataPtr state, const Column** columns,
                                           int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                           int64_t frame_end) const {}

    // Contains a loop with calls to "merge" function.
    // You can collect arguments into array "states"
    // and do a single call to "merge_batch" for devirtualization and inlining.
    virtual void merge_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                             AggDataPtr* states) const = 0;

    // filter[i] = 0, will be merged
    virtual void merge_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset,
                                         const Column* column, AggDataPtr* states,
                                         const std::vector<uint8_t>& filter) const = 0;

    // merge result to single state
    virtual void merge_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column* column,
                                          AggDataPtr state) const = 0;
};

template <typename State>
class AggregateFunctionStateHelper : public AggregateFunction {
protected:
    static State& data(AggDataPtr place) { return *reinterpret_cast<State*>(place); }
    static const State& data(ConstAggDataPtr place) { return *reinterpret_cast<const State*>(place); }

public:
    void create(AggDataPtr ptr) const final { new (ptr) State; }

    void destroy(AggDataPtr ptr) const final { data(ptr).~State(); }

    size_t size() const final { return sizeof(State); }

    size_t alignof_size() const final { return alignof(State); }
};

template <typename State, typename Derived>
class AggregateFunctionBatchHelper : public AggregateFunctionStateHelper<State> {
    void update_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->update(ctx, columns, states[i] + state_offset, i);
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < batch_size; i++) {
            // TODO: optimize with simd ?
            if (filter[i] == 0) {
                static_cast<const Derived*>(this)->update(ctx, columns, states[i] + state_offset, i);
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                   AggDataPtr state) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->update(ctx, columns, state, i);
        }
    }

    void merge_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->merge(ctx, column, states[i] + state_offset, i);
        }
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < batch_size; i++) {
            // TODO: optimize with simd ?
            if (filter[i] == 0) {
                static_cast<const Derived*>(this)->merge(ctx, column, states[i] + state_offset, i);
            }
        }
    }

    void merge_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column* column,
                                  AggDataPtr state) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->merge(ctx, column, state, i);
        }
    }

    void batch_serialize(size_t batch_size, const Buffer<AggDataPtr>& agg_states, size_t state_offset,
                         Column* to) const override {
        for (size_t i = 0; i < batch_size; i++) {
            static_cast<const Derived*>(this)->serialize_to_column(nullptr, agg_states[i] + state_offset, to);
        }
    }

    void batch_finalize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < batch_size; i++) {
            static_cast<const Derived*>(this)->finalize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }
};

// Helper class that properly invokes destructor when state goes out of scope.
class ManagedAggregateState {
public:
    ManagedAggregateState(const AggregateFunction* desc, std::shared_ptr<Buffer<uint8_t>>&& buffer)
            : _desc(desc), _state(std::move(buffer)) {
        _desc->create(_state->data());
    }

    ~ManagedAggregateState() { _desc->destroy(_state->data()); }

    uint8_t* mutable_data() { return _state->data(); }

    uint8_t* data() const { return _state->data(); }

    static std::unique_ptr<ManagedAggregateState> Make(const AggregateFunction* desc) {
        std::shared_ptr<Buffer<uint8_t>> buf = std::make_shared<Buffer<uint8_t>>();
        buf->reserve(desc->size());
        return std::make_unique<ManagedAggregateState>(desc, std::move(buf));
    }

private:
    const AggregateFunction* _desc;
    std::shared_ptr<Buffer<uint8_t>> _state;
};

using AggregateFunctionPtr = std::shared_ptr<AggregateFunction>;

} // namespace vectorized
} // namespace starrocks