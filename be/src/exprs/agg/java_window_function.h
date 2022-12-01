// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstring>
#include <limits>
#include <vector>

#include "common/compiler_util.h"
#include "exprs/agg/java_udaf_function.h"
#include "jni.h"
#include "runtime/primitive_type.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"

namespace starrocks::vectorized {
void assign_jvalue(MethodTypeDescriptor method_type_desc, Column* col, int row_num, jvalue val);

class JavaWindowFunction final : public JavaUDAFAggregateFunction {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        ctx->impl()->udaf_ctxs()->_func->reset(data(state).handle);
    }

    std::string get_name() const override { return "java_window"; }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                   int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) const override {
        int num_rows = columns[0]->size();
        int num_args = ctx->get_num_args();
        if (UNLIKELY(frame_start > std::numeric_limits<int32_t>::max() ||
                     frame_end > std::numeric_limits<int32_t>::max())) {
            ctx->set_error(fmt::format("too big window: start:{}, end:{}", frame_start, frame_end).c_str());
        }

        std::vector<jobject> args;
        std::vector<DirectByteBuffer> buffers;
        ConvertDirectBufferVistor vistor(buffers);
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_args, num_rows, &args);
        ctx->impl()->udaf_ctxs()->_func->window_update_batch(data(state).handle, peer_group_start, peer_group_end,
                                                             frame_start, frame_end, num_args, args.data());
        // release input cols
        for (int i = 0; i < num_args; ++i) {
            env->DeleteLocalRef(args[i]);
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        jvalue val = ctx->impl()->udaf_ctxs()->_func->finalize(this->data(state).handle);
        // insert values to column
        JNIEnv* env = helper.getEnv();
        MethodTypeDescriptor desc = {(PrimitiveType)ctx->get_return_type().type, true};
        int sz = end - start;
        for (int i = 0; i < sz; ++i) {
            assign_jvalue(desc, dst, start + i, val);
        }
        env->DeleteLocalRef(val.l);
    }
};
} // namespace starrocks::vectorized