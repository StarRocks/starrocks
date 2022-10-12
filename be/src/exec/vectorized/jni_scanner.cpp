// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "jni_scanner.h"

#include "column/type_traits.h"
#include "fmt/core.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

Status JniScanner::_check_jni_exception(JNIEnv* _jni_env, const std::string& message) {
    if (_jni_env->ExceptionCheck()) {
        _jni_env->ExceptionDescribe();
        _jni_env->ExceptionClear();
        return Status::InternalError(message);
    }
    return Status::OK();
}

Status JniScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    _init_profile(scanner_params);
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
    SCOPED_TIMER(_profile.open_timer);
    _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_open);
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to open the off-heap table scanner."));
    return Status::OK();
}

void JniScanner::do_close(RuntimeState* runtime_state) noexcept {
    JNIEnv* _jni_env = JVMFunctionHelper::getInstance().getEnv();
    _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_close);
    _check_jni_exception(_jni_env, "Failed to close the off-heap table scanner.");
    _jni_env->DeleteLocalRef(_jni_scanner_obj);
    _jni_env->DeleteLocalRef(_jni_scanner_cls);
}

void JniScanner::_init_profile(const HdfsScannerParams& scanner_params) {
    auto _runtime_profile = scanner_params.profile->runtime_profile;
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "JniScannerRowsRead", TUnit::UNIT);
    _profile.io_timer = ADD_TIMER(_runtime_profile, "JniScannerIOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "JniScannerIOCounter", TUnit::UNIT);
    _profile.fill_chunk_timer = ADD_TIMER(_runtime_profile, "JniScannerFillChunkTime");
    _profile.open_timer = ADD_TIMER(_runtime_profile, "JniScannerOpenTime");
}

Status JniScanner::_init_jni_method(JNIEnv* _jni_env) {
    // init jmethod
    _jni_scanner_open = _jni_env->GetMethodID(_jni_scanner_cls, "open", "()V");
    DCHECK(_jni_scanner_open != nullptr);
    _jni_scanner_get_next_chunk = _jni_env->GetMethodID(_jni_scanner_cls, "getNextOffHeapChunk", "()J");
    DCHECK(_jni_scanner_get_next_chunk != nullptr);
    _jni_scanner_close = _jni_env->GetMethodID(_jni_scanner_cls, "close", "()V");
    DCHECK(_jni_scanner_close != nullptr);
    _jni_scanner_release_column = _jni_env->GetMethodID(_jni_scanner_cls, "releaseOffHeapColumnVector", "(I)V");
    DCHECK(_jni_scanner_release_column != nullptr);
    _jni_scanner_release_table = _jni_env->GetMethodID(_jni_scanner_cls, "releaseOffHeapTable", "()V");
    DCHECK(_jni_scanner_release_table != nullptr);
    RETURN_IF_ERROR(_check_jni_exception(_jni_env, "Failed to init off-heap table jni methods."));

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

    string message = "Initialize a scanner with parameters: ";
    for (auto it : _jni_scanner_params) {
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
    SCOPED_TIMER(_profile.io_timer);
    COUNTER_UPDATE(_profile.io_counter, 1);
    *chunk_meta = _jni_env->CallLongMethod(_jni_scanner_obj, _jni_scanner_get_next_chunk);
    RETURN_IF_ERROR(
            _check_jni_exception(_jni_env, "Failed to call the nextChunkOffHeap method of off-heap table scanner."));
    return Status::OK();
}

template <PrimitiveType type, typename CppType>
void JniScanner::_append_data(Column* column, CppType& value) {
    auto appender = [](auto* column, CppType& value) {
        using ColumnType = typename vectorized::RunTimeColumnType<type>;
        ColumnType* runtime_column = down_cast<ColumnType*>(column);
        runtime_column->append(value);
    };

    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto* data_column = nullable_column->data_column().get();
        NullData& null_data = nullable_column->null_column_data();
        null_data.push_back(0);
        appender(data_column, value);
    } else {
        appender(column, value);
    }
}

template <PrimitiveType type, typename CppType>
Status JniScanner::_append_primitive_data(long num_rows, long* chunk_meta_ptr, int& chunk_meta_index,
                                          ColumnPtr& column) {
    bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
    CppType* column_ptr = reinterpret_cast<CppType*>(chunk_meta_ptr[chunk_meta_index++]);

    auto* nullable_column = down_cast<NullableColumn*>(column.get());
    nullable_column->resize(num_rows);

    NullData& null_data = nullable_column->null_column_data();
    memcpy(null_data.data(), null_column_ptr, num_rows);

    auto* data_column = nullable_column->data_column().get();
    using ColumnType = typename vectorized::RunTimeColumnType<type>;
    ColumnType* runtime_column = down_cast<ColumnType*>(data_column);
    memcpy(runtime_column->get_data().data(), column_ptr, num_rows * sizeof(CppType));

    nullable_column->update_has_null();
    return Status::OK();
}

template <PrimitiveType type, typename CppType>
Status JniScanner::_append_decimal_data(long num_rows, long* chunk_meta_ptr, int& chunk_meta_index, ColumnPtr& column,
                                        SlotDescriptor* slot_desc) {
    bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
    int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
    char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);

    int precision = slot_desc->type().precision;
    int scale = slot_desc->type().scale;
    for (int i = 0; i < num_rows; i++) {
        if (null_column_ptr[i]) {
            column->append_nulls(1);
        } else {
            std::string decimal_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i + 1]);
            CppType cpp_val;
            if (DecimalV3Cast::from_string<CppType>(&cpp_val, precision, scale, decimal_str.data(),
                                                    decimal_str.size())) {
                return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                            slot_desc->col_name(), decimal_str));
            }
            _append_data<type, CppType>(column.get(), cpp_val);
        }
    }
    return Status::OK();
}

template <PrimitiveType type>
Status JniScanner::_append_string_data(long num_rows, long* chunk_meta_ptr, int& chunk_meta_index, ColumnPtr& column) {
    bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
    int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
    char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);

    auto* nullable_column = down_cast<NullableColumn*>(column.get());
    nullable_column->resize(num_rows);

    NullData& null_data = nullable_column->null_column_data();
    memcpy(null_data.data(), null_column_ptr, num_rows);

    auto* data_column = nullable_column->data_column().get();
    using ColumnType = typename vectorized::RunTimeColumnType<type>;
    ColumnType* runtime_column = down_cast<ColumnType*>(data_column);

    int total_length = offset_ptr[num_rows];
    runtime_column->get_bytes().resize(total_length);

    memcpy(runtime_column->get_offset().data(), offset_ptr, (num_rows + 1) * sizeof(uint32_t));
    memcpy(runtime_column->get_bytes().data(), column_ptr, total_length);

    nullable_column->update_has_null();
    return Status::OK();
}

Status JniScanner::_fill_chunk(JNIEnv* _jni_env, long chunk_meta, ChunkPtr* chunk) {
    SCOPED_TIMER(_profile.fill_chunk_timer);

    long* chunk_meta_ptr = reinterpret_cast<long*>(chunk_meta);
    int chunk_meta_index = 0;
    long num_rows = chunk_meta_ptr[chunk_meta_index++];
    if (num_rows == 0) {
        return Status::EndOfFile("");
    }
    COUNTER_UPDATE(_profile.rows_read_counter, num_rows);
    auto slot_desc_list = _scanner_params.tuple_desc->slots();
    for (size_t col_idx = 0; col_idx < slot_desc_list.size(); col_idx++) {
        SlotDescriptor* slot_desc = slot_desc_list[col_idx];
        ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        PrimitiveType column_type = slot_desc->type().type;
        if (!column->is_nullable()) {
            return Status::DataQualityError(
                    fmt::format("NOT NULL column[{}] is not supported.", slot_desc->col_name()));
        }
        if (column_type == PrimitiveType::TYPE_BOOLEAN) {
            RETURN_IF_ERROR((
                    _append_primitive_data<TYPE_BOOLEAN, uint8_t>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_SMALLINT) {
            RETURN_IF_ERROR((_append_primitive_data<TYPE_SMALLINT, int16_t>(num_rows, chunk_meta_ptr, chunk_meta_index,
                                                                            column)));
        } else if (column_type == PrimitiveType::TYPE_INT) {
            RETURN_IF_ERROR(
                    (_append_primitive_data<TYPE_INT, int32_t>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_FLOAT) {
            RETURN_IF_ERROR(
                    (_append_primitive_data<TYPE_FLOAT, float>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_BIGINT) {
            RETURN_IF_ERROR(
                    (_append_primitive_data<TYPE_BIGINT, int64_t>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_DOUBLE) {
            RETURN_IF_ERROR(
                    (_append_primitive_data<TYPE_DOUBLE, double>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_VARCHAR) {
            RETURN_IF_ERROR((_append_string_data<TYPE_VARCHAR>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_CHAR) {
            RETURN_IF_ERROR((_append_string_data<TYPE_CHAR>(num_rows, chunk_meta_ptr, chunk_meta_index, column)));
        } else if (column_type == PrimitiveType::TYPE_DATE) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    column->append_nulls(1);
                } else {
                    std::string date_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i + 1]);
                    DateValue dv;
                    if (!dv.from_string(date_str.c_str(), date_str.size())) {
                        return Status::DataQualityError(
                                fmt::format("Invalid date value occurs on column[{}], value is [{}]",
                                            slot_desc->col_name(), date_str));
                    }
                    _append_data<TYPE_DATE, DateValue>(column.get(), dv);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_DATETIME) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    column->append_nulls(1);
                } else {
                    std::string origin_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i + 1]);
                    std::string datetime_str = origin_str.substr(0, origin_str.find('.'));
                    TimestampValue tsv;
                    if (!tsv.from_datetime_format_str(datetime_str.c_str(), datetime_str.size(), "%Y-%m-%d %H:%i:%s")) {
                        return Status::DataQualityError(
                                fmt::format("Invalid datetime value occurs on column[{}], value is [{}]",
                                            slot_desc->col_name(), origin_str));
                    }
                    _append_data<TYPE_DATETIME, TimestampValue>(column.get(), tsv);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_DECIMAL32) {
            RETURN_IF_ERROR((_append_decimal_data<TYPE_DECIMAL32, int32_t>(num_rows, chunk_meta_ptr, chunk_meta_index,
                                                                           column, slot_desc)));
        } else if (column_type == PrimitiveType::TYPE_DECIMAL64) {
            RETURN_IF_ERROR((_append_decimal_data<TYPE_DECIMAL64, int64_t>(num_rows, chunk_meta_ptr, chunk_meta_index,
                                                                           column, slot_desc)));
        } else if (column_type == PrimitiveType::TYPE_DECIMAL128) {
            RETURN_IF_ERROR((_append_decimal_data<TYPE_DECIMAL128, int128_t>(num_rows, chunk_meta_ptr, chunk_meta_index,
                                                                             column, slot_desc)));
        } else {
            return Status::InternalError(
                    fmt::format("Type {} is not supported for off-heap table scanner", column_type));
        }
        _jni_env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_column, col_idx);
        RETURN_IF_ERROR(_check_jni_exception(
                _jni_env, "Failed to call the releaseOffHeapColumnVector method of off-heap table scanner."));
    }
    if (num_rows < _runtime_state->chunk_size()) {
        return Status::EndOfFile("");
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
    JNIEnv* _jni_env = JVMFunctionHelper::getInstance().getEnv();
    long chunk_meta;
    RETURN_IF_ERROR(_get_next_chunk(_jni_env, &chunk_meta));
    Status status = _fill_chunk(_jni_env, chunk_meta, chunk);
    RETURN_IF_ERROR(_release_off_heap_table(_jni_env));
    return status;
}

} // namespace starrocks::vectorized
