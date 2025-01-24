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
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "jni.h"
#include "types/logical_type.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks {

class JavaUDAFAggregateFunction : public AggregateFunction {
public:
    using State = JavaUDAFState;

    bool is_exception_safe() const override { return false; }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state, size_t row_num) const final {
        CHECK(false) << "unreadable path";
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const final {
        // TODO merge
        const BinaryColumn* input_column = nullptr;
        if (column->is_nullable()) {
            auto* null_column = down_cast<const NullableColumn*>(column);
            input_column = down_cast<const BinaryColumn*>(null_column->data_column().get());
        } else {
            input_column = down_cast<const BinaryColumn*>(column);
        }
        Slice slice = input_column->get_slice(row_num);
        auto* udaf_ctx = ctx->udaf_ctxs();

        if (udaf_ctx->buffer->capacity() < slice.get_size()) {
            udaf_ctx->buffer_data.resize(slice.get_size());
            udaf_ctx->buffer =
                    std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());
        }
        JVMFunctionHelper::getInstance().clear(udaf_ctx->buffer.get(), ctx);
        memcpy(udaf_ctx->buffer_data.data(), slice.get_data(), slice.get_size());
        udaf_ctx->_func->merge(this->data(state).handle, udaf_ctx->buffer->handle());
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const final {
        BinaryColumn* column = nullptr;
        // TODO serialize
        if (to->is_nullable()) {
            auto* null_column = down_cast<NullableColumn*>(to);
            null_column->null_column()->append(DATUM_NOT_NULL);
            column = down_cast<BinaryColumn*>(null_column->data_column().get());
        } else {
            DCHECK(to->is_binary());
            column = down_cast<BinaryColumn*>(to);
        }

        size_t old_size = column->get_bytes().size();
        auto* udaf_ctx = ctx->udaf_ctxs();
        int serialize_size = udaf_ctx->_func->serialize_size(this->data(state).handle);
        if (udaf_ctx->buffer->capacity() < serialize_size) {
            udaf_ctx->buffer_data.resize(serialize_size);
            udaf_ctx->buffer =
                    std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());
        }
        JVMFunctionHelper::getInstance().clear(udaf_ctx->buffer.get(), ctx);

        udaf_ctx->_func->serialize(this->data(state).handle, udaf_ctx->buffer->handle());
        size_t new_size = old_size + serialize_size;
        column->get_bytes().resize(new_size);
        column->get_offset().emplace_back(new_size);
        memcpy(column->get_bytes().data() + old_size, udaf_ctx->buffer_data.data(), serialize_size);
    }

    void finalize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                            Column* to) const final {
        auto* udaf_ctx = ctx->udaf_ctxs();
        jvalue val = udaf_ctx->_func->finalize(this->data(state).handle);
        append_jvalue(udaf_ctx->finalize->method_desc[0], to, val);
        release_jvalue(udaf_ctx->finalize->method_desc[0].is_box, val);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t batch_size,
                                     ColumnPtr* dst) const final {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        auto* udf_ctxs = ctx->udaf_ctxs();
        // 1 convert input as state
        // 1.1 create state list
        auto rets = helper.batch_call(ctx, udf_ctxs->handle.handle(), udf_ctxs->create->method.handle(), batch_size);
        RETURN_IF_UNLIKELY_NULL(rets, (void)0);
        // 1.2 convert input as input array
        int num_cols = ctx->get_num_args();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        DeferOp defer = DeferOp([&]() {
            // clean up arrays
            for (auto& arg : args) {
                if (arg) {
                    env->DeleteLocalRef(arg);
                }
            }
        });
        args.emplace_back(rets);
        std::vector<const Column*> raw_input_ptrs(src.size());
        for (int i = 0; i < src.size(); ++i) {
            raw_input_ptrs[i] = src[i].get();
        }
        auto st = JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, raw_input_ptrs.data(), num_cols,
                                                                batch_size, &args);
        RETURN_IF_UNLIKELY(!st.ok(), (void)0);

        // 2 batch call update
        helper.batch_update_state(ctx, ctx->udaf_ctxs()->handle.handle(), ctx->udaf_ctxs()->update->method.handle(),
                                  args.data(), args.size());
        // 3 get serialize size
        auto serialize_szs = (jintArray)helper.int_batch_call(
                ctx, rets, ctx->udaf_ctxs()->serialize_size->method.handle(), batch_size);
        RETURN_IF_UNLIKELY_NULL(serialize_szs, (void)0);
        LOCAL_REF_GUARD_ENV(env, serialize_szs);

        int length = env->GetArrayLength(serialize_szs);
        std::vector<int> slice_sz(length);
        helper.getEnv()->GetIntArrayRegion(serialize_szs, 0, length, slice_sz.data());
        int totalLength = std::accumulate(slice_sz.begin(), slice_sz.end(), 0, [](auto l, auto r) { return l + r; });
        // 4 prepare serialize buffer
        udf_ctxs->buffer_data.resize(totalLength);
        udf_ctxs->buffer =
                std::make_unique<DirectByteBuffer>(udf_ctxs->buffer_data.data(), udf_ctxs->buffer_data.size());
        // chunk size
        auto buffer_array = helper.create_object_array(udf_ctxs->buffer->handle(), batch_size);
        RETURN_IF_UNLIKELY_NULL(buffer_array, (void)0);
        LOCAL_REF_GUARD_ENV(env, buffer_array);
        jobject state_and_buffer[2] = {rets, buffer_array};
        helper.batch_update_state(ctx, ctx->udaf_ctxs()->handle.handle(), ctx->udaf_ctxs()->serialize->method.handle(),
                                  state_and_buffer, 2);

        // 5 ready
        std::vector<Slice> slices(batch_size);
        int offsets = 0;
        for (int i = 0; i < batch_size; ++i) {
            slices[i] = Slice(udf_ctxs->buffer_data.data() + offsets, slice_sz[i]);
            offsets += slice_sz[i];
        }
        // append result to dst column
        CHECK((*dst)->append_strings(slices));
    }

    // State Data
    static State& data(AggDataPtr __restrict place) { return *reinterpret_cast<State*>(place); }
    static const State& data(ConstAggDataPtr __restrict place) { return *reinterpret_cast<const State*>(place); }

    // jclass
    // newInstance -> handle
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        new (ptr) State(ctx->udaf_ctxs()->_func->create());
    }

    // Call Destroy method
    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        ctx->udaf_ctxs()->_func->destroy(data(ptr).handle);
        data(ptr).~State();
    }

    size_t size() const override { return sizeof(State); }

    size_t alignof_size() const override { return alignof(State); }

    // batch interface

    void update_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        int num_cols = ctx->get_num_args();
        helper.getEnv()->PushLocalFrame(num_cols * 3 + 1);
        auto defer = DeferOp([&helper]() { helper.getEnv()->PopLocalFrame(nullptr); });

        {
            auto states_arr = JavaDataTypeConverter::convert_to_states(ctx, states, state_offset, batch_size);
            RETURN_IF_UNLIKELY_NULL(states_arr, (void)0);
            auto st =
                    JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);
            RETURN_IF_UNLIKELY(!st.ok(), (void)0);
            helper.batch_update(ctx, ctx->udaf_ctxs()->handle.handle(), ctx->udaf_ctxs()->update->method.handle(),
                                states_arr, args.data(), args.size());
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const Filter& filter) const override {
        auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        int num_cols = ctx->get_num_args();
        helper.getEnv()->PushLocalFrame(num_cols * 3 + 1);
        auto defer = DeferOp([env = env]() { env->PopLocalFrame(nullptr); });
        {
            auto states_arr = JavaDataTypeConverter::convert_to_states_with_filter(ctx, states, state_offset,
                                                                                   filter.data(), batch_size);
            RETURN_IF_UNLIKELY_NULL(states_arr, (void)0);
            auto st =
                    JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);
            RETURN_IF_UNLIKELY(!st.ok(), (void)0);
            helper.batch_update_if_not_null(ctx, ctx->udaf_ctxs()->handle.handle(),
                                            ctx->udaf_ctxs()->update->method.handle(), states_arr, args.data(),
                                            args.size());
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        std::vector<jobject> args;
        std::vector<DirectByteBuffer> buffers;
        int num_cols = ctx->get_num_args();
        env->PushLocalFrame(num_cols * 3 + 1);
        auto defer = DeferOp([env = env]() { env->PopLocalFrame(nullptr); });
        {
            auto st =
                    JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);
            RETURN_IF_UNLIKELY(!st.ok(), (void)0);

            auto* stub = ctx->udaf_ctxs()->update_batch_call_stub.get();
            auto state_handle = this->data(state).handle;
            helper.batch_update_single(stub, state_handle, args.data(), num_cols, batch_size);
        }
    }

    // This is only used to get portion of the entire binary column
    template <class StatesProvider, class MergeCaller>
    void _merge_batch_process(FunctionContext* ctx, StatesProvider&& states_provider, MergeCaller&& caller,
                              const Column* column, size_t start, size_t size, bool need_multi_buffer) const {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        // get state lists
        auto state_array = states_provider();
        RETURN_IF_UNLIKELY_NULL(state_array, (void)0);
        LOCAL_REF_GUARD_ENV(env, state_array);

        // prepare buffer
        auto serialized_column =
                ColumnHelper::get_binary_column(const_cast<Column*>(ColumnHelper::get_data_column(column)));

        auto& serialized_bytes = serialized_column->get_bytes();
        const auto& offsets = serialized_column->get_offset();

        if (serialized_bytes.size() > std::numeric_limits<int>::max()) {
            ctx->set_error("serialized column size is too large");
            return;
        }

        if (!need_multi_buffer) {
            size_t start_offset = serialized_column->get_offset()[start];
            size_t end_offset = serialized_column->get_offset()[start + size];
            // create one buffer will be ok
            auto buffer = std::make_unique<DirectByteBuffer>(serialized_bytes.data() + start_offset,
                                                             end_offset - start_offset);
            auto buffer_array = helper.create_object_array(buffer->handle(), size);
            RETURN_IF_UNLIKELY_NULL(buffer_array, (void)0);
            LOCAL_REF_GUARD_ENV(env, buffer_array);
            // batch call merge
            caller(state_array, buffer_array);
        } else {
            auto buffer_array =
                    helper.batch_create_bytebuf(serialized_bytes.data(), offsets.data(), start, start + size);
            RETURN_IF_UNLIKELY_NULL(buffer_array, (void)0);
            LOCAL_REF_GUARD_ENV(env, buffer_array);
            // batch call merge
            caller(state_array, buffer_array);
        }
    }

    void merge_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        // batch merge
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();

        auto provider = [&]() {
            auto state_id_list = JavaDataTypeConverter::convert_to_states(ctx, states, state_offset, batch_size);
            RETURN_IF_UNLIKELY_NULL(state_id_list, state_id_list);
            LOCAL_REF_GUARD_ENV(env, state_id_list);
            auto state_array = helper.convert_handles_to_jobjects(ctx, state_id_list);
            return state_array;
        };
        auto merger = [&](jobject state_array, jobject buffer_array) {
            jobject state_and_buffer[2] = {state_array, buffer_array};
            helper.batch_update_state(ctx, ctx->udaf_ctxs()->handle.handle(), ctx->udaf_ctxs()->merge->method.handle(),
                                      state_and_buffer, 2);
        };
        _merge_batch_process(ctx, std::move(provider), std::move(merger), column, 0, batch_size, false);
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const Filter& filter) const override {
        // batch merge
        auto& helper = JVMFunctionHelper::getInstance();

        auto provider = [&]() {
            auto state_id_list = JavaDataTypeConverter::convert_to_states_with_filter(ctx, states, state_offset,
                                                                                      filter.data(), batch_size);
            return state_id_list;
        };
        auto merger = [&](jobject state_array, jobject buffer_array) {
            jobject state_and_buffer[] = {buffer_array};
            helper.batch_update_if_not_null(ctx, ctx->udaf_ctxs()->handle.handle(),
                                            ctx->udaf_ctxs()->merge->method.handle(), state_array, state_and_buffer, 1);
        };
        _merge_batch_process(ctx, std::move(provider), std::move(merger), column, 0, batch_size, true);
    }

    void merge_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column* column, size_t start,
                                  size_t size) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        auto provider = [&]() {
            auto state_handle = reinterpret_cast<JavaUDAFState*>(state)->handle;
            auto res = helper.convert_handle_to_jobject(ctx, state_handle);
            LOCAL_REF_GUARD_ENV(env, res);
            return helper.create_object_array(res, size);
        };
        auto merger = [&](jobject state_array, jobject buffer_array) {
            jobject state_and_buffer[2] = {state_array, buffer_array};
            helper.batch_update_state(ctx, ctx->udaf_ctxs()->handle.handle(), ctx->udaf_ctxs()->merge->method.handle(),
                                      state_and_buffer, 2);
        };
        _merge_batch_process(ctx, std::move(provider), std::move(merger), column, start, size, false);
    }

    void batch_serialize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        auto* udf_ctxs = ctx->udaf_ctxs();

        const size_t origin_chunk_size = to->size();
        auto defer = DeferOp([&]() {
            // we must keep column num_rows equals with expected numbers
            if (to->size() != batch_size + origin_chunk_size) {
                DCHECK(ctx->has_error());
                to->append_default(origin_chunk_size + batch_size - to->size());
            }
        });

        // step 1 get state lists
        auto states = const_cast<AggDataPtr*>(agg_states.data());
        auto state_id_list = JavaDataTypeConverter::convert_to_states(ctx, states, state_offset, batch_size);
        RETURN_IF_UNLIKELY_NULL(state_id_list, (void)0);
        LOCAL_REF_GUARD_ENV(env, state_id_list);

        auto state_array = helper.convert_handles_to_jobjects(ctx, state_id_list);
        RETURN_IF_UNLIKELY_NULL(state_array, (void)0);
        LOCAL_REF_GUARD_ENV(env, state_array);

        // step 2 serialize size
        auto serialize_szs = (jintArray)helper.int_batch_call(
                ctx, state_array, ctx->udaf_ctxs()->serialize_size->method.handle(), batch_size);
        RETURN_IF_UNLIKELY_NULL(serialize_szs, (void)0);
        LOCAL_REF_GUARD_ENV(env, serialize_szs);

        int length = env->GetArrayLength(serialize_szs);
        std::vector<int> slice_sz(length);
        helper.getEnv()->GetIntArrayRegion(serialize_szs, 0, length, slice_sz.data());
        int totalLength = std::accumulate(slice_sz.begin(), slice_sz.end(), 0, [](auto l, auto r) { return l + r; });
        // step 3 prepare serialize buffer
        udf_ctxs->buffer_data.resize(totalLength);
        udf_ctxs->buffer =
                std::make_unique<DirectByteBuffer>(udf_ctxs->buffer_data.data(), udf_ctxs->buffer_data.size());

        // step 4 call serialize
        auto buffer_array = helper.create_object_array(udf_ctxs->buffer->handle(), batch_size);
        LOCAL_REF_GUARD_ENV(env, buffer_array);
        jobject state_and_buffer[2] = {state_array, buffer_array};
        helper.batch_update_state(ctx, ctx->udaf_ctxs()->handle.handle(), ctx->udaf_ctxs()->serialize->method.handle(),
                                  state_and_buffer, 2);

        int offsets = 0;
        std::vector<Slice> slices(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            slices[i] = Slice(udf_ctxs->buffer_data.data() + offsets, slice_sz[i]);
            offsets += slice_sz[i];
        }
        CHECK(to->append_strings(slices));
    }

    void batch_finalize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        auto* udf_ctxs = ctx->udaf_ctxs();

        const size_t origin_chunk_size = to->size();
        auto defer = DeferOp([&]() {
            // we must keep column num_rows equals with expected numbers
            if (to->size() != batch_size + origin_chunk_size) {
                DCHECK(ctx->has_error());
                to->append_default(origin_chunk_size + batch_size - to->size());
            }
        });

        // 1. get state list
        auto states = const_cast<AggDataPtr*>(agg_states.data());
        auto state_id_list = JavaDataTypeConverter::convert_to_states(ctx, states, state_offset, batch_size);
        RETURN_IF_UNLIKELY_NULL(state_id_list, (void)0);
        LOCAL_REF_GUARD_ENV(env, state_id_list);

        auto state_array = helper.convert_handles_to_jobjects(ctx, state_id_list);
        RETURN_IF_UNLIKELY_NULL(state_array, (void)0);
        LOCAL_REF_GUARD_ENV(env, state_array);
        // 2. batch call finalize
        CHECK(to->empty());
        // 3. get result from column
        auto res = helper.batch_call(ctx, udf_ctxs->handle.handle(), udf_ctxs->finalize->method.handle(), &state_array,
                                     1, batch_size);
        RETURN_IF_UNLIKELY_NULL(res, (void)0);
        LOCAL_REF_GUARD_ENV(env, res);

        LogicalType type = udf_ctxs->finalize->method_desc[0].type;
        // For nullable inputs, our UDAF does not produce nullable results
        if (!to->is_nullable()) {
            ColumnPtr wrapper(const_cast<Column*>(to), [](auto p) {});
            auto output = NullableColumn::create(wrapper, NullColumn::create());
            helper.get_result_from_boxed_array(ctx, type, output.get(), res, batch_size);
        } else {
            helper.get_result_from_boxed_array(ctx, type, to, res, batch_size);
            down_cast<NullableColumn*>(to)->update_has_null();
        }
    }

    std::string get_name() const override { return "java_udaf"; }
};
} // namespace starrocks
