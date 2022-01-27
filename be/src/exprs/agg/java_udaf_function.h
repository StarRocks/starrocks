// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include "column/binary_column.h"
#include "exprs/agg/aggregate.h"
#include "gen_cpp/Opcodes_types.h"
#include "jni.h"
#include "udf/java/java_udf.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace starrocks::vectorized {

struct JavaUDAFState {
    JavaUDAFState(jobject&& handle_) : handle(std::move(handle_)){};
    ~JavaUDAFState() = default;
    // UDAF State
    jobject handle;
};

template <bool handle_null>
jvalue cast_to_jvalue(MethodTypeDescriptor method_type_desc, const Column* col, int row_num);
void release_jvalue(MethodTypeDescriptor method_type_desc, jvalue val);
void append_jvalue(MethodTypeDescriptor method_type_desc, Column* col, jvalue val);
Status get_java_udaf_function(int fid, const std::string& url, const std::string& checksum, const std::string& symbol,
                              starrocks_udf::FunctionContext* context, AggregateFunction** func);
// Not Support Nullable

template <bool handle_null = true>
class JavaUDAFAggregateFunction : public AggregateFunction {
public:
    using State = JavaUDAFState;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override final {
        int num_args = ctx->get_num_args();
        jvalue args[num_args + 1];
        args[0].l = this->data(state).handle;
        for (int i = 0; i < num_args; ++i) {
            args[i + 1] = cast_to_jvalue<handle_null>(ctx->impl()->udaf_ctxs()->update->method_desc[i + 2], columns[i],
                                                      row_num);
        }

        ctx->impl()->udaf_ctxs()->_func->update(args);

        for (int i = 0; i < num_args; ++i) {
            release_jvalue(ctx->impl()->udaf_ctxs()->update->method_desc[i + 2], args[i + 1]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
               size_t row_num) const override final {
        // TODO merge
        DCHECK(!column->is_nullable());
        DCHECK(column->is_binary());
        const auto* input_column = down_cast<const BinaryColumn*>(column);
        Slice slice = input_column->get_slice(row_num);
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();

        if (udaf_ctx->buffer->capacity() < slice.get_size()) {
            udaf_ctx->buffer_data.resize(slice.get_size());
            udaf_ctx->buffer =
                    std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());
        }

        memcpy(udaf_ctx->buffer_data.data(), slice.get_data(), slice.get_size());
        udaf_ctx->_func->merge(this->data(state).handle, udaf_ctx->buffer->handle());
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override final {
        // TODO serialize
        DCHECK(!to->is_nullable());
        DCHECK(to->is_binary());

        auto* column = down_cast<BinaryColumn*>(to);
        size_t old_size = column->get_bytes().size();
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();
        int serialize_size = udaf_ctx->_func->serialize_size(this->data(state).handle);
        if (udaf_ctx->buffer->capacity() < serialize_size) {
            udaf_ctx->buffer_data.resize(serialize_size);
            udaf_ctx->buffer =
                    std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());
        }

        udaf_ctx->_func->serialize(this->data(state).handle, udaf_ctx->buffer->handle());
        size_t new_size = old_size + serialize_size;
        column->get_bytes().resize(new_size);
        column->get_offset().emplace_back(new_size);
        memcpy(column->get_bytes().data(), udaf_ctx->buffer_data.data(), serialize_size);
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override final {
        // TODO finalize
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();
        jvalue val = udaf_ctx->_func->finalize(this->data(state).handle);
        append_jvalue(udaf_ctx->finalize->method_desc[0], to, val);
        release_jvalue(udaf_ctx->finalize->method_desc[0], val);
    }

    //Now Java UDAF don't Not Support Streaming Aggregate
    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override final {
        DCHECK(false) << "Now Java UDAF Not Support Streaming Mode";
    }

    // State Data
    static State& data(AggDataPtr __restrict place) { return *reinterpret_cast<State*>(place); }
    static const State& data(ConstAggDataPtr __restrict place) { return *reinterpret_cast<const State*>(place); }

    // jclass
    // newInstance -> handle
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        new (ptr) State(ctx->impl()->udaf_ctxs()->_func->create());
    }

    // Call Destroy method
    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        ctx->impl()->udaf_ctxs()->_func->destroy(data(ptr).handle);
        data(ptr).~State();
    }

    size_t size() const override { return sizeof(State); }

    size_t alignof_size() const override { return alignof(State); }

    // batch interface

    void update_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            this->update(ctx, columns, states[i] + state_offset, i);
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < batch_size; i++) {
            if (filter[i] == 0) {
                this->update(ctx, columns, states[i] + state_offset, i);
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            this->update(ctx, columns, state, i);
        }
    }

    void merge_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            this->merge(ctx, column, states[i] + state_offset, i);
        }
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < batch_size; i++) {
            if (filter[i] == 0) {
                this->merge(ctx, column, states[i] + state_offset, i);
            }
        }
    }

    void merge_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column* column,
                                  AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            this->merge(ctx, column, state, i);
        }
    }

    void batch_serialize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < batch_size; i++) {
            this->serialize_to_column(nullptr, agg_states[i] + state_offset, to);
        }
    }

    void batch_finalize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < batch_size; i++) {
            this->finalize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }

    std::string get_name() const override { return "java_udaf"; }
};
} // namespace starrocks::vectorized