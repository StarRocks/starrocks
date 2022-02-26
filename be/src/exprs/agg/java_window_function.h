// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <stdint.h>
#include <cstring>
#include <vector>
#include <memory>
#include <string>

#include "exprs/agg/java_udaf_function.h"
#include "jni.h"
#include "runtime/primitive_type.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/strings/numbers.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace starrocks::vectorized {
void assign_jvalue(MethodTypeDescriptor method_type_desc, Column* col, int row_num, jvalue val);

class JavaWindowFunction final : public JavaUDAFAggregateFunction<true> {
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

        jobject input_cols[num_args];
        PrimitiveType types[num_rows];
        std::vector<DirectByteBuffer> buffers;
        ConvertDirectBufferVistor vistor(buffers);
        auto& helper = JVMFunctionHelper::getInstance();

        for (int i = 0; i < num_args; ++i) {
            types[i] = ctx->get_arg_type(i)->type;
            int buffers_idx = buffers.size();
            columns[i]->accept(&vistor);
            int buffers_sz = buffers.size() - buffers_idx;
            input_cols[i] = helper.create_boxed_array(types[i], num_rows, columns[i]->is_nullable(),
                                                      &buffers[buffers_idx], buffers_sz);
        }
        ctx->impl()->udaf_ctxs()->_func->window_update_batch(data(state).handle, peer_group_start, peer_group_end,
                                                             frame_start, frame_end, num_args, input_cols);
        // release input cols
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        jobject val = ctx->impl()->udaf_ctxs()->_func->get_values(this->data(state).handle, start, end);
        // insert values to column
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        helper.getEnv()->PushLocalFrame(end - start);
        MethodTypeDescriptor desc = {(PrimitiveType)ctx->get_return_type().type, true};
        int sz = end - start;
        for (int i = 0; i < sz; ++i) {
            jobject vi = env->GetObjectArrayElement((jobjectArray)val, i);
            assign_jvalue(desc, dst, start + i, {.l = vi});
        }
        helper.getEnv()->PopLocalFrame(nullptr);
        env->DeleteLocalRef(val);
    }
};
} // namespace starrocks::vectorized