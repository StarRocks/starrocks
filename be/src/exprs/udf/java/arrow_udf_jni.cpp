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

#include "exprs/udf/java/arrow_udf_jni.h"

#include <arrow/c/bridge.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <memory>
#include <mutex>
#include <string>

#include "base/utility/arrow_utils.h"
#include "column/arrow/record_batch_converter.h"
#include "column/arrow/type_to_arrow_converter.h"
#include "jni.h"
#include "runtime/java/jvm_helper.h"
#include "types/type_descriptor.h"

namespace starrocks {

ExportedArrowBatch::~ExportedArrowBatch() {
    if (_array.release != nullptr) {
        _array.release(&_array);
    }
    if (_schema.release != nullptr) {
        _schema.release(&_schema);
    }
}

Status ExportedArrowBatch::export_record_batch(const arrow::RecordBatch& batch) {
    return to_status(arrow::ExportRecordBatch(batch, &_array, &_schema));
}

Status ExportedArrowBatch::export_columns(size_t num_rows, const Columns& columns, const TypeDescriptor* arg_types) {
    arrow::SchemaBuilder schema_builder;
    for (size_t i = 0; i < columns.size(); ++i) {
        std::shared_ptr<arrow::Field> field;
        RETURN_IF_ERROR(convert_to_arrow_field(arg_types[i], std::to_string(i), columns[i]->is_nullable(), &field));
        RETURN_IF_ERROR(to_status(schema_builder.AddField(field)));
    }
    std::shared_ptr<arrow::Schema> schema;
    auto finished = schema_builder.Finish();
    RETURN_IF_ERROR(to_status(std::move(finished).Value(&schema)));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_columns_to_arrow_batch(num_rows, columns, arrow::default_memory_pool(), arg_types, schema,
                                                   &record_batch));
    return export_record_batch(*record_batch);
}

Status ExportedArrowBatch::export_columns(size_t num_rows, const Column* const* columns, size_t num_cols,
                                          const TypeDescriptor* arg_types) {
    // Non-owning views over the raw input columns (owned by the caller for the duration of the
    // export) so we can reuse the Columns-based converter without copying column data.
    Columns view(num_cols);
    for (size_t i = 0; i < num_cols; ++i) {
        view[i] = ColumnPtr(const_cast<Column*>(columns[i]), [](Column*) {});
    }
    return export_columns(num_rows, view, arg_types);
}

StatusOr<jclass> arrow_udf_helper_class() {
    static std::mutex mtx;
    static JavaGlobalRef cached_class = nullptr;
    std::lock_guard<std::mutex> guard(mtx);
    if (cached_class.handle() == nullptr) {
        JNIEnv* env = JVMHelper::getInstance().getEnv();
        std::string cls_name = JVMHelper::to_jni_class_name("com.starrocks.udf.ArrowUDFHelper");
        jclass local = env->FindClass(cls_name.c_str());
        RETURN_ERROR_IF_JNI_EXCEPTION(env);
        if (local == nullptr) {
            return Status::InternalError("cannot find class com.starrocks.udf.ArrowUDFHelper");
        }
        cached_class = JavaGlobalRef(env->NewGlobalRef(local));
        env->DeleteLocalRef(local);
    }
    return reinterpret_cast<jclass>(cached_class.handle());
}

} // namespace starrocks
