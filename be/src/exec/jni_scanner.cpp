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

#include "jni_scanner.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "fmt/core.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks {

Status JniScanner::_check_jni_exception(JNIEnv* _jni_env, const std::string& message) {
    if (jthrowable thr = _jni_env->ExceptionOccurred(); thr) {
        std::string jni_error_message = JVMFunctionHelper::getInstance().dumpExceptionString(thr);
        _jni_env->ExceptionDescribe();
        _jni_env->ExceptionClear();
        _jni_env->DeleteLocalRef(thr);
        return Status::InternalError(message + " java exception details: " + jni_error_message);
    }
    return Status::OK();
}

Status JniScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _init_profile(scanner_params);
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    JNIEnv* _jni_env = JVMFunctionHelper::getInstance().getEnv();
    if (_jni_env->EnsureLocalCapacity(_jni_scanner_params.size() * 2 + 6) < 0) {
        RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to ensure the local capacity."));
    }
    RETURN_IF_ERROR(_init_jni_table_scanner(_jni_env, runtime_state));
    RETURN_IF_ERROR(_init_jni_method(_jni_env));
    return Status::OK();
}

Status JniScanner::do_open(RuntimeState* state) {
    JNIEnv* _jni_env = JVMFunctionHelper::getInstance().getEnv();
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_open);
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to open the off-heap table scanner."));
    return Status::OK();
}

void JniScanner::do_update_counter(HdfsScanProfile* profile) {}

void JniScanner::do_close(RuntimeState* runtime_state) noexcept {
    JNIEnv* _jni_env = JVMFunctionHelper::getInstance().getEnv();
    if (_jni_scanner_obj != nullptr) {
        if (_jni_scanner_close != nullptr) {
            _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_close);
        }
        _jni_env->DeleteLocalRef(_jni_scanner_obj);
        _jni_scanner_obj = nullptr;
    }
    if (_jni_scanner_cls != nullptr) {
        _jni_env->DeleteLocalRef(_jni_scanner_cls);
        _jni_scanner_cls = nullptr;
    }
}

void JniScanner::_init_profile(const HdfsScannerParams& scanner_params) {}

Status JniScanner::_init_jni_method(JNIEnv* _jni_env) {
    // init jmethod
    _jni_scanner_open = _jni_env->GetMethodID(_jni_scanner_cls, "open", "()V");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get `open` jni method"));

    _jni_scanner_get_next_chunk = _jni_env->GetMethodID(_jni_scanner_cls, "getNextOffHeapChunk", "()J");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get `getNextOffHeapChunk` jni method"));

    _jni_scanner_close = _jni_env->GetMethodID(_jni_scanner_cls, "close", "()V");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get `close` jni method"));

    _jni_scanner_release_column = _jni_env->GetMethodID(_jni_scanner_cls, "releaseOffHeapColumnVector", "(I)V");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get `releaseOffHeapColumnVector` jni method"));

    _jni_scanner_release_table = _jni_env->GetMethodID(_jni_scanner_cls, "releaseOffHeapTable", "()V");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get `releaseOffHeapTable` jni method"));
    return Status::OK();
}

Status JniScanner::_init_jni_table_scanner(JNIEnv* _jni_env, RuntimeState* runtime_state) {
    jclass scanner_factory_class = _jni_env->FindClass(_jni_scanner_factory_class.c_str());
    jmethodID scanner_factory_constructor = _jni_env->GetMethodID(scanner_factory_class, "<init>", "()V");
    jobject scanner_factory_obj = _jni_env->NewObject(scanner_factory_class, scanner_factory_constructor);
    jmethodID get_scanner_method =
            _jni_env->GetMethodID(scanner_factory_class, "getScannerClass", "()Ljava/lang/Class;");
    _jni_scanner_cls = (jclass)_jni_env->CallObjectMethod(scanner_factory_obj, get_scanner_method);
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to init the scanner class."));
    _jni_env->DeleteLocalRef(scanner_factory_class);
    _jni_env->DeleteLocalRef(scanner_factory_obj);

    jmethodID scanner_constructor = _jni_env->GetMethodID(_jni_scanner_cls, "<init>", "(ILjava/util/Map;)V");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get a scanner class constructor."));

    jclass hashmap_class = _jni_env->FindClass("java/util/HashMap");
    jmethodID hashmap_constructor = _jni_env->GetMethodID(hashmap_class, "<init>", "(I)V");
    jobject hashmap_object = _jni_env->NewObject(hashmap_class, hashmap_constructor, _jni_scanner_params.size());
    jmethodID hashmap_put =
            _jni_env->GetMethodID(hashmap_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to get the HashMap methods."));

    std::string message = "Initialize a scanner with parameters: ";
    for (const auto& it : _jni_scanner_params) {
        jstring key = _jni_env->NewStringUTF(it.first.c_str());
        jstring value = _jni_env->NewStringUTF(it.second.c_str());
        message.append(it.first);
        message.append("->");
        message.append(it.second);
        message.append(", ");

        _jni_env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
        _jni_env->DeleteLocalRef(key);
        _jni_env->DeleteLocalRef(value);
    }
    _jni_env->DeleteLocalRef(hashmap_class);
    LOG(INFO) << message;

    int fetch_size = runtime_state->chunk_size();
    _jni_scanner_obj = _jni_env->NewObject(_jni_scanner_cls, scanner_constructor, fetch_size, hashmap_object);
    _jni_env->DeleteLocalRef(hashmap_object);
    DCHECK(_jni_scanner_obj != nullptr);
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to initialize a scanner instance."));

    return Status::OK();
}

Status JniScanner::_get_next_chunk(JNIEnv* _jni_env, long* chunk_meta) {
    SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats.io_ns);
    _app_stats.io_count += 1;
    *chunk_meta = _jni_env->CallLongMethod(_jni_scanner_obj, _jni_scanner_get_next_chunk);
    RETURN_IF_ERROR(
            _check_jni_exception(_jni_env, "Failed to call the nextChunkOffHeap method of off-heap table scanner."));
    return Status::OK();
}

template <LogicalType type>
Status JniScanner::_append_primitive_data(const FillColumnArgs& args) {
    char* column_ptr = static_cast<char*>(next_chunk_meta_as_ptr());
    using ColumnType = typename starrocks::RunTimeColumnType<type>;
    using CppType = typename starrocks::RunTimeCppType<type>;
    auto* runtime_column = down_cast<ColumnType*>(args.column);
    runtime_column->resize_uninitialized(args.num_rows);
    memcpy(runtime_column->get_data().data(), column_ptr, args.num_rows * sizeof(CppType));
    return Status::OK();
}

template <LogicalType type>
Status JniScanner::_append_string_data(const FillColumnArgs& args) {
    int* offset_ptr = static_cast<int*>(next_chunk_meta_as_ptr());
    char* column_ptr = static_cast<char*>(next_chunk_meta_as_ptr());

    auto* data_column = args.column;
    using ColumnType = typename starrocks::RunTimeColumnType<type>;
    auto* runtime_column = down_cast<ColumnType*>(data_column);
    Bytes& bytes = runtime_column->get_bytes();
    Offsets& offsets = runtime_column->get_offset();

    int total_length = offset_ptr[args.num_rows];
    bytes.resize(total_length);
    offsets.resize(args.num_rows + 1);

    memcpy(offsets.data(), offset_ptr, (args.num_rows + 1) * sizeof(uint32_t));
    memcpy(bytes.data(), column_ptr, total_length);
    return Status::OK();
}

Status JniScanner::_append_date_data(const FillColumnArgs& args) {
    int* offset_ptr = static_cast<int*>(next_chunk_meta_as_ptr());
    char* column_ptr = static_cast<char*>(next_chunk_meta_as_ptr());

    using ColumnType = typename starrocks::RunTimeColumnType<TYPE_DATE>;
    auto* runtime_column = down_cast<ColumnType*>(args.column);
    runtime_column->resize_uninitialized(args.num_rows);
    DateValue* runtime_data = runtime_column->get_data().data();

    for (int i = 0; i < args.num_rows; i++) {
        if (args.nulls && args.nulls[i]) {
            // NULL
        } else {
            std::string date_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i + 1]);
            DateValue dv;
            if (!dv.from_string(date_str.c_str(), date_str.size())) {
                return Status::DataQualityError(fmt::format("Invalid date value occurs on column[{}], value is [{}]",
                                                            args.slot_name, date_str));
            }
            runtime_data[i] = dv;
        }
    }
    return Status::OK();
}

Status JniScanner::_append_datetime_data(const FillColumnArgs& args) {
    int* offset_ptr = static_cast<int*>(next_chunk_meta_as_ptr());
    char* column_ptr = static_cast<char*>(next_chunk_meta_as_ptr());

    using ColumnType = typename starrocks::RunTimeColumnType<TYPE_DATETIME>;
    auto* runtime_column = down_cast<ColumnType*>(args.column);
    runtime_column->resize_uninitialized(args.num_rows);
    TimestampValue* runtime_data = runtime_column->get_data().data();

    for (int i = 0; i < args.num_rows; i++) {
        if (args.nulls && args.nulls[i]) {
            // NULL
        } else {
            std::string origin_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i + 1]);
            std::string datetime_str = origin_str.substr(0, origin_str.find('.'));
            TimestampValue tsv;
            if (!tsv.from_datetime_format_str(datetime_str.c_str(), datetime_str.size(), "%Y-%m-%d %H:%i:%s")) {
                return Status::DataQualityError(fmt::format(
                        "Invalid datetime value occurs on column[{}], value is [{}]", args.slot_name, origin_str));
            }
            runtime_data[i] = tsv;
        }
    }
    return Status::OK();
}

Status JniScanner::_append_array_data(const FillColumnArgs& args) {
    DCHECK(args.slot_type.is_array_type());

    auto* array_column = down_cast<ArrayColumn*>(args.column);
    int* offset_ptr = static_cast<int*>(next_chunk_meta_as_ptr());

    auto* offsets = array_column->offsets_column().get();
    offsets->resize_uninitialized(args.num_rows + 1);
    memcpy(offsets->get_data().data(), offset_ptr, (args.num_rows + 1) * sizeof(uint32_t));

    int total_length = offset_ptr[args.num_rows];
    Column* elements = array_column->elements_column().get();
    std::string name = args.slot_name + ".$0";
    FillColumnArgs sub_args = {.num_rows = total_length,
                               .slot_name = name,
                               .slot_type = args.slot_type.children[0],
                               .nulls = nullptr,
                               .column = elements,
                               .must_nullable = false};
    RETURN_IF_ERROR(_fill_column(&sub_args));
    return Status::OK();
}

Status JniScanner::_append_map_data(const FillColumnArgs& args) {
    DCHECK(args.slot_type.is_map_type());

    auto* map_column = down_cast<MapColumn*>(args.column);
    int* offset_ptr = static_cast<int*>(next_chunk_meta_as_ptr());

    auto* offsets = map_column->offsets_column().get();
    offsets->resize_uninitialized(args.num_rows + 1);
    memcpy(offsets->get_data().data(), offset_ptr, (args.num_rows + 1) * sizeof(uint32_t));

    int total_length = offset_ptr[args.num_rows];
    {
        Column* keys = map_column->keys_column().get();
        if (!args.slot_type.children[0].is_unknown_type()) {
            std::string name = args.slot_name + ".$0";
            FillColumnArgs sub_args = {.num_rows = total_length,
                                       .slot_name = name,
                                       .slot_type = args.slot_type.children[0],
                                       .nulls = nullptr,
                                       .column = keys,
                                       .must_nullable = false};
            RETURN_IF_ERROR(_fill_column(&sub_args));
        } else {
            keys->append_default(total_length);
        }
    }

    {
        Column* values = map_column->values_column().get();
        if (!args.slot_type.children[1].is_unknown_type()) {
            std::string name = args.slot_name + ".$1";
            FillColumnArgs sub_args = {.num_rows = total_length,
                                       .slot_name = name,
                                       .slot_type = args.slot_type.children[1],
                                       .nulls = nullptr,
                                       .column = values,
                                       .must_nullable = true};
            RETURN_IF_ERROR(_fill_column(&sub_args));
        } else {
            values->append_default(total_length);
        }
    }
    return Status::OK();
}

Status JniScanner::_append_struct_data(const FillColumnArgs& args) {
    DCHECK(args.slot_type.is_struct_type());

    auto* struct_column = down_cast<StructColumn*>(args.column);
    const TypeDescriptor& type = args.slot_type;
    for (int i = 0; i < type.children.size(); i++) {
        Column* column = struct_column->fields_column()[i].get();
        std::string name = args.slot_name + "." + type.field_names[i];
        FillColumnArgs sub_args = {.num_rows = args.num_rows,
                                   .slot_name = name,
                                   .slot_type = type.children[i],
                                   .nulls = nullptr,
                                   .column = column,
                                   .must_nullable = true};
        RETURN_IF_ERROR(_fill_column(&sub_args));
    }
    return Status::OK();
}

Status JniScanner::_fill_column(FillColumnArgs* pargs) {
    FillColumnArgs& args = *pargs;
    if (args.must_nullable && !args.column->is_nullable()) {
        return Status::DataQualityError(fmt::format("NOT NULL column[{}] is not supported.", args.slot_name));
    }

    void* ptr = next_chunk_meta_as_ptr();
    if (ptr == nullptr) {
        // struct field mismatch.
        args.column->append_default(args.num_rows);
        return Status::OK();
    }

    if (args.column->is_nullable()) {
        // if column is nullable, we parse `null_column`,
        // and update `args.nulls` and set `data_column` to `args.column`
        bool* null_column_ptr = static_cast<bool*>(ptr);
        auto* nullable_column = down_cast<NullableColumn*>(args.column);

        NullData& null_data = nullable_column->null_column_data();
        null_data.resize(args.num_rows);
        memcpy(null_data.data(), null_column_ptr, args.num_rows);
        nullable_column->update_has_null();

        auto* data_column = nullable_column->data_column().get();
        pargs->column = data_column;
        pargs->nulls = null_data.data();
    } else {
        // otherwise we skip this chunk meta, because in Java side
        // we assume every column starts with `null_column`.
    }

    LogicalType column_type = args.slot_type.type;
    if (column_type == LogicalType::TYPE_BOOLEAN) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_BOOLEAN>(args)));
    } else if (column_type == LogicalType::TYPE_TINYINT) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_TINYINT>(args)));
    } else if (column_type == LogicalType::TYPE_SMALLINT) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_SMALLINT>(args)));
    } else if (column_type == LogicalType::TYPE_INT) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_INT>(args)));
    } else if (column_type == LogicalType::TYPE_FLOAT) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_FLOAT>(args)));
    } else if (column_type == LogicalType::TYPE_BIGINT) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_BIGINT>(args)));
    } else if (column_type == LogicalType::TYPE_DOUBLE) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_DOUBLE>(args)));
    } else if (column_type == LogicalType::TYPE_VARCHAR) {
        RETURN_IF_ERROR((_append_string_data<TYPE_VARCHAR>(args)));
    } else if (column_type == LogicalType::TYPE_CHAR) {
        RETURN_IF_ERROR((_append_string_data<TYPE_CHAR>(args)));
    } else if (column_type == LogicalType::TYPE_VARBINARY) {
        RETURN_IF_ERROR((_append_string_data<TYPE_VARBINARY>(args)));
    } else if (column_type == LogicalType::TYPE_DATE) {
        RETURN_IF_ERROR((_append_date_data(args)));
    } else if (column_type == LogicalType::TYPE_DATETIME) {
        RETURN_IF_ERROR((_append_datetime_data(args)));
    } else if (column_type == LogicalType::TYPE_DECIMAL32) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_DECIMAL32>(args)));
    } else if (column_type == LogicalType::TYPE_DECIMAL64) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_DECIMAL64>(args)));
    } else if (column_type == LogicalType::TYPE_DECIMAL128) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_DECIMAL128>(args)));
    } else if (column_type == LogicalType::TYPE_ARRAY) {
        RETURN_IF_ERROR((_append_array_data(args)));
    } else if (column_type == LogicalType::TYPE_MAP) {
        RETURN_IF_ERROR((_append_map_data(args)));
    } else if (column_type == LogicalType::TYPE_STRUCT) {
        RETURN_IF_ERROR((_append_struct_data(args)));
    } else {
        return Status::InternalError(fmt::format("Type {} is not supported for off-heap table scanner", column_type));
    }
    return Status::OK();
}

Status JniScanner::_fill_chunk(JNIEnv* _jni_env, ChunkPtr* chunk, const std::vector<SlotDescriptor*>& slot_desc_list) {
    SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);

    long num_rows = next_chunk_meta_as_long();
    if (num_rows == 0) {
        return Status::EndOfFile("");
    }
    _app_stats.raw_rows_read += num_rows;

    for (size_t col_idx = 0; col_idx < slot_desc_list.size(); col_idx++) {
        SlotDescriptor* slot_desc = slot_desc_list[col_idx];
        const std::string& slot_name = slot_desc->col_name();
        const TypeDescriptor& slot_type = slot_desc->type();
        ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        FillColumnArgs args{.num_rows = num_rows,
                            .slot_name = slot_name,
                            .slot_type = slot_type,
                            .nulls = nullptr,
                            .column = column.get(),
                            .must_nullable = true};
        RETURN_IF_ERROR(_fill_column(&args));
        _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_column, col_idx);
        RETURN_IF_ERROR(_check_jni_exception(
                _jni_env, "Failed to call the releaseOffHeapColumnVector method of off-heap table scanner."));
    }
    return Status::OK();
}

Status JniScanner::_release_off_heap_table(JNIEnv* _jni_env) {
    _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_table);
    RETURN_IF_ERROR(
            _check_jni_exception(_jni_env, "Failed to call the releaseOffHeapTable method of off-heap table scanner."));
    return Status::OK();
}

Status JniScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    // fill chunk with all wanted column(include partition columns)
    Status status = fill_empty_chunk(runtime_state, chunk, _scanner_params.tuple_desc->slots());

    // ====== conjunct evaluation ======
    // important to add columns before evaluation
    // because ctxs_by_slot maybe refers to some non-existed slot or partition slot.
    size_t chunk_size = (*chunk)->num_rows();
    _scanner_ctx.append_not_existed_columns_to_chunk(chunk, chunk_size);

    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    return status;
}

Status JniScanner::fill_empty_chunk(RuntimeState* runtime_state, ChunkPtr* chunk,
                                    const std::vector<SlotDescriptor*>& slot_desc_list) {
    JNIEnv* _jni_env = JVMFunctionHelper::getInstance().getEnv();
    long chunk_meta;
    RETURN_IF_ERROR(_get_next_chunk(_jni_env, &chunk_meta));
    reset_chunk_meta(chunk_meta);
    Status status = _fill_chunk(_jni_env, chunk, slot_desc_list);
    RETURN_IF_ERROR(_release_off_heap_table(_jni_env));

    return status;
}

Status HiveJniScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    // fill chunk with all wanted column exclude partition columns
    Status status = fill_empty_chunk(runtime_state, chunk, _scanner_params.materialize_slots);

    // ====== conjunct evaluation ======
    // important to add columns before evaluation
    // because ctxs_by_slot maybe refers to some non-existed slot or partition slot.
    size_t chunk_size = (*chunk)->num_rows();
    _scanner_ctx.append_not_existed_columns_to_chunk(chunk, chunk_size);
    // right now only hive table need append partition columns explictly, paimon and hudi reader will append partition columns in Java side
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, chunk_size);
    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    return status;
}

} // namespace starrocks
