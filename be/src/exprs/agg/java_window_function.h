// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstring>
#include <vector>

#include "column/column_visitor.h"
#include "column/column_visitor_adapter.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/java_udaf_function.h"
#include "jni.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {
void assign_jvalue(MethodTypeDescriptor method_type_desc, Column* col, int row_num, jvalue val);

class ConvertDirectBufferVistor : public ColumnVisitorAdapter<ConvertDirectBufferVistor> {
public:
    ConvertDirectBufferVistor(std::vector<DirectByteBuffer>& buffers) : ColumnVisitorAdapter(this), _buffers(buffers) {}
    Status do_visit(const NullableColumn& column);
    Status do_visit(const BinaryColumn& column);

    template <typename T>
    Status do_visit(const vectorized::FixedLengthColumnBase<T>& column) {
        get_buffer_data(column, &_buffers);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        return Status::NotSupported("UDF Not Support Type");
    }

private:
    template <class ColumnType>
    void get_buffer_data(const ColumnType& column, std::vector<DirectByteBuffer>* buffers) {
        const auto& container = column.get_data();
        buffers->emplace_back((void*)container.data(), container.size() * sizeof(typename ColumnType::ValueType));
    }

private:
    std::vector<DirectByteBuffer>& _buffers;
};

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
        std::vector<DirectByteBuffer> buffers;
        ConvertDirectBufferVistor vistor(buffers);

        for (int i = 0; i < num_args; ++i) {
            auto type = ctx->get_arg_type(i)->type;
            int buffers_idx = buffers.size();
            columns[i]->accept(&vistor);
            int buffers_sz = buffers.size() - buffers_idx;
            input_cols[i] = ctx->impl()->udaf_ctxs()->udf_helper->create_boxed_array(type, num_rows, columns[i],
                                                                                     &buffers[buffers_idx], buffers_sz);
        }
        ctx->impl()->udaf_ctxs()->_func->window_update_batch(data(state).handle, peer_group_start, peer_group_end,
                                                             frame_start, frame_end, num_args, input_cols);
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