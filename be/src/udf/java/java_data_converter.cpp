#include "udf/java/java_data_converter.h"

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks::vectorized {

Status ConvertDirectBufferVistor::do_visit(const NullableColumn& column) {
    const auto& null_data = column.immutable_null_column_data();
    _buffers.emplace_back((void*)null_data.data(), null_data.size());
    return column.data_column()->accept(this);
}

Status ConvertDirectBufferVistor::do_visit(const BinaryColumn& column) {
    const auto& offsets = column.get_offset();
    _buffers.emplace_back((void*)offsets.data(), offsets.size() * 4);
    const auto& bytes = column.get_bytes();
    _buffers.emplace_back((void*)bytes.data(), bytes.size());
    return Status::OK();
}

jobject JavaDataTypeConverter::convert_to_object_array(uint8_t** data, size_t offset, int num_rows) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();
    jobjectArray arr = env->NewObjectArray(num_rows, helper.object_class(), nullptr);
    for (int i = 0; i < num_rows; ++i) {
        env->SetObjectArrayElement(arr, i, reinterpret_cast<JavaUDAFState*>(data[i] + offset)->handle);
    }
    return arr;
}

void JavaDataTypeConverter::convert_to_boxed_array(FunctionContext* ctx, std::vector<DirectByteBuffer>* buffers,
                                                   const Column** columns, int num_cols, int num_rows,
                                                   std::vector<jobject>* res) {
    auto& helper = JVMFunctionHelper::getInstance();
    ConvertDirectBufferVistor vistor(*buffers);
    PrimitiveType types[num_cols];
    for (int i = 0; i < num_cols; ++i) {
        types[i] = ctx->get_arg_type(i)->type;
        int buffers_offset = buffers->size();
        columns[i]->accept(&vistor);
        int buffers_sz = buffers->size() - buffers_offset;
        auto arg = helper.create_boxed_array(types[i], num_rows, columns[i]->is_nullable(), &(*buffers)[buffers_offset],
                                             buffers_sz);
        res->emplace_back(arg);
    }
}
} // namespace starrocks::vectorized