// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "hudi_mor_scanner.h"

#include "column/type_traits.h"
#include "fmt/core.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

#define CHECK_JAVA_EXCEPTION(error_message)                                             \
    if (jthrowable thr = _jni_env->ExceptionOccurred(); thr) {                          \
        std::string err = JVMFunctionHelper::getInstance().dumpExceptionString(thr);    \
        _jni_env->ExceptionClear();                                                     \
        _jni_env->DeleteLocalRef(thr);                                                  \
        return Status::InternalError(fmt::format("{}, error: {}", error_message, err)); \
    }

Status HudiMORScanner::reset_jni_env() {
    _jni_env = JVMFunctionHelper::getInstance().getEnv();

    if (_jni_env == nullptr) {
        return Status::InternalError("Cannot get jni env");
    }
    return Status::OK();
}

Status HudiMORScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    RETURN_IF_ERROR(reset_jni_env());

    _init_profile(scanner_params);

    RETURN_IF_ERROR(_init_hudi_mor_scanner(runtime_state));
    return Status::OK();
}

Status HudiMORScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(reset_jni_env());
    if (_opened) {
        return Status::OK();
    }
    _build_scanner_context();
    auto status = do_open(state);
    if (status.ok()) {
        _opened = true;
        if (_scanner_params.open_limit != nullptr) {
            _scanner_params.open_limit->fetch_add(1, std::memory_order_relaxed);
        }
        LOG(INFO) << "open file success: " << _scanner_params.path;
    }
    return status;
}

Status HudiMORScanner::do_open(RuntimeState* state) {
    SCOPED_TIMER(_profile.open_timer);
    // open scanner
    jmethodID scanner_open = _jni_env->GetMethodID(_hudi_mor_scanner_cls, "open", "()V");
    DCHECK(scanner_open != nullptr);

    _jni_env->CallVoidMethod(_hudi_mor_scanner, scanner_open);
    CHECK_JAVA_EXCEPTION("open hudi MOR scanner failed")
    return Status::OK();
}

void HudiMORScanner::do_close(RuntimeState* runtime_state) noexcept {
    if (_hudi_mor_scanner == nullptr) {
        return;
    }
    // close scanner
    jmethodID scanner_close = _jni_env->GetMethodID(_hudi_mor_scanner_cls, "close", "()V");
    DCHECK(scanner_close != nullptr);

    _jni_env->CallVoidMethod(_hudi_mor_scanner, scanner_close);
    _jni_env->DeleteLocalRef(_hudi_mor_scanner);
    _jni_env->DeleteLocalRef(_hudi_mor_scanner_cls);
}

void HudiMORScanner::_init_profile(const HdfsScannerParams& scanner_params) {
    auto _runtime_profile = scanner_params.profile->runtime_profile;
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "HudiMORRowsRead", TUnit::UNIT);
    _profile.io_timer = ADD_TIMER(_runtime_profile, "HudiMORIOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "HudiMORIOCounter", TUnit::UNIT);
    _profile.fill_chunk_timer = ADD_TIMER(_runtime_profile, "HudiMORFillChunkTime");
    _profile.open_timer = ADD_TIMER(_runtime_profile, "HudiMOROpenTime");
}

Status HudiMORScanner::_init_hudi_mor_scanner(RuntimeState* state) {
    jclass hudi_mor_scanner_util_cls = _jni_env->FindClass("com/starrocks/hudi/reader/HudiSliceReaderUtil");
    jmethodID util_get_class_method = _jni_env->GetStaticMethodID(
            hudi_mor_scanner_util_cls, "getHudiSliceRecordReaderClass", "()Ljava/lang/Class;");
    _hudi_mor_scanner_cls = (jclass)_jni_env->CallStaticObjectMethod(hudi_mor_scanner_util_cls, util_get_class_method);
    CHECK_JAVA_EXCEPTION("get hudi MOR scanner class failed")

    jmethodID constructor = _jni_env->GetMethodID(
            _hudi_mor_scanner_cls, "<init>",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;[Ljava/lang/"
            "String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;I)V");
    CHECK_JAVA_EXCEPTION("get hudi MOR scanner constructor failed")

    jclass jstring_class = _jni_env->FindClass("java/lang/String");
    jstring jbase_path = _jni_env->NewStringUTF(_scan_ctx.base_path.c_str());
    jstring jhive_column_names = _jni_env->NewStringUTF(_scan_ctx.hive_column_names.c_str());
    jstring jhive_column_types = _jni_env->NewStringUTF(_scan_ctx.hive_column_types.c_str());
    jobjectArray jrequired_fields = _jni_env->NewObjectArray(_scan_ctx.required_fields.size(), jstring_class, nullptr);
    for (int i = 0; i < _scan_ctx.required_fields.size(); i++) {
        jstring field = _jni_env->NewStringUTF(_scan_ctx.required_fields[i].c_str());
        _jni_env->SetObjectArrayElement(jrequired_fields, i, field);
    }
    jstring jinstant_time = _jni_env->NewStringUTF(_scan_ctx.instant_time.c_str());
    jobjectArray jlog_paths = _jni_env->NewObjectArray(_scan_ctx.delta_log_paths.size(), jstring_class, nullptr);
    for (int i = 0; i < _scan_ctx.delta_log_paths.size(); i++) {
        jstring log = _jni_env->NewStringUTF(_scan_ctx.delta_log_paths[i].c_str());
        _jni_env->SetObjectArrayElement(jlog_paths, i, log);
    }
    jstring jdata_file_path = _jni_env->NewStringUTF(_scan_ctx.data_file_path.c_str());
    jstring jserde = _jni_env->NewStringUTF(_scan_ctx.serde_lib.c_str());
    jstring jinput_format = _jni_env->NewStringUTF(_scan_ctx.input_format.c_str());

    string message = fmt::format(
            "init hudi mor scanner with parameters: base_path -> {}, jhive_column_names -> {}, jhive_column_types -> "
            "{}, required_fields count -> {},"
            "instant_time -> {}, data_file_path -> {}, data_file_lenth -> {}, delta_log_paths count -> {}, serde_lib "
            "-> {}, input_format -> {}",
            _scan_ctx.base_path, _scan_ctx.hive_column_names, _scan_ctx.hive_column_types,
            _scan_ctx.required_fields.size(), _scan_ctx.instant_time, _scan_ctx.data_file_path,
            _scan_ctx.data_file_lenth, _scan_ctx.delta_log_paths.size(), _scan_ctx.serde_lib, _scan_ctx.input_format);
    for (int i = 0; i < _scan_ctx.required_fields.size(); i++) {
        message.append(fmt::format(" required_field{} -> {},", i, _scan_ctx.required_fields[i]));
    }
    for (int i = 0; i < _scan_ctx.delta_log_paths.size(); i++) {
        message.append(fmt::format(" log{} -> {},", i, _scan_ctx.delta_log_paths[i]));
    }
    LOG(INFO) << message;

    int fetch_size = state->chunk_size();
    _hudi_mor_scanner = _jni_env->NewObject(
            _hudi_mor_scanner_cls, constructor, jbase_path, jhive_column_names, jhive_column_types, jrequired_fields,
            jinstant_time, jlog_paths, jdata_file_path, _scan_ctx.data_file_lenth, jserde, jinput_format, fetch_size);

    DCHECK(_hudi_mor_scanner != nullptr);
    CHECK_JAVA_EXCEPTION("get hudi MOR scanner failed")
    // init jmethod
    _scanner_has_next = _jni_env->GetMethodID(_hudi_mor_scanner_cls, "hasNext", "()Z");
    DCHECK(_scanner_has_next != nullptr);
    _scanner_get_next_chunk = _jni_env->GetMethodID(_hudi_mor_scanner_cls, "nextChunkOffHeap", "()J");
    DCHECK(_scanner_get_next_chunk != nullptr);
    _scanner_column_release = _jni_env->GetMethodID(_hudi_mor_scanner_cls, "releaseOffHeapColumnVector", "(I)V");
    DCHECK(_scanner_column_release != nullptr);

    _jni_env->DeleteLocalRef(jbase_path);
    _jni_env->DeleteLocalRef(jhive_column_names);
    _jni_env->DeleteLocalRef(jhive_column_types);
    _jni_env->DeleteLocalRef(jrequired_fields);
    _jni_env->DeleteLocalRef(jinstant_time);
    _jni_env->DeleteLocalRef(jlog_paths);
    _jni_env->DeleteLocalRef(jdata_file_path);
    _jni_env->DeleteLocalRef(jserde);
    _jni_env->DeleteLocalRef(jinstant_time);
    _jni_env->DeleteLocalRef(jstring_class);
    _jni_env->DeleteLocalRef(hudi_mor_scanner_util_cls);
    return Status::OK();
}

Status HudiMORScanner::_has_next(bool* result) {
    jboolean ret = _jni_env->CallBooleanMethod(_hudi_mor_scanner, _scanner_has_next);
    CHECK_JAVA_EXCEPTION("call hudi MOR scanner hasNext failed")
    *result = ret;
    return Status::OK();
}

Status HudiMORScanner::_get_next_chunk(long* chunk_meta) {
    SCOPED_TIMER(_profile.io_timer);
    COUNTER_UPDATE(_profile.io_counter, 1);
    *chunk_meta = _jni_env->CallLongMethod(_hudi_mor_scanner, _scanner_get_next_chunk);
    CHECK_JAVA_EXCEPTION("getNextChunk failed")
    return Status::OK();
}

template <PrimitiveType type, typename CppType>
void HudiMORScanner::_append_data(Column* column, CppType& value) {
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

Status HudiMORScanner::_process_null_value(Column* column, SlotDescriptor* slot_desc) {
    if (!column->is_nullable()) {
        return Status::DataQualityError(
                fmt::format("Unexpected NULL value occurs on NOT NULL column[{}]", slot_desc->col_name()));
    }
    column->append_nulls(1);
    return Status::OK();
}

Status HudiMORScanner::_fill_chunk(long chunk_meta, ChunkPtr* chunk) {
    SCOPED_TIMER(_profile.fill_chunk_timer);

    long* chunk_meta_ptr = reinterpret_cast<long*>(chunk_meta);
    int chunk_meta_index = 0;
    long num_rows = chunk_meta_ptr[chunk_meta_index++];
    COUNTER_UPDATE(_profile.rows_read_counter, num_rows);
    auto slot_desc_list = _scanner_params.tuple_desc->slots();
    for (size_t col_idx = 0; col_idx < slot_desc_list.size(); col_idx++) {
        SlotDescriptor* slot_desc = slot_desc_list[col_idx];
        ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        PrimitiveType column_type = slot_desc->type().type;
        if (column_type == PrimitiveType::TYPE_BOOLEAN) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            uint8_t* column_ptr = reinterpret_cast<uint8_t*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    _append_data<TYPE_BOOLEAN, uint8_t>(column.get(), column_ptr[i]);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_SMALLINT) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int16_t* column_ptr = reinterpret_cast<int16_t*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    _append_data<TYPE_SMALLINT, int16_t>(column.get(), column_ptr[i]);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_INT) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int32_t* column_ptr = reinterpret_cast<int32_t*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    _append_data<TYPE_INT, int32_t>(column.get(), column_ptr[i]);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_FLOAT) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            float* column_ptr = reinterpret_cast<float*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    _append_data<TYPE_FLOAT, float>(column.get(), column_ptr[i]);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_BIGINT) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int64_t* column_ptr = reinterpret_cast<int64_t*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    _append_data<TYPE_BIGINT, int64_t>(column.get(), column_ptr[i]);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_DOUBLE) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            double* column_ptr = reinterpret_cast<double*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    _append_data<TYPE_DOUBLE, double>(column.get(), column_ptr[i]);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_VARCHAR || column_type == PrimitiveType::TYPE_CHAR) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            int* length_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    std::string str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i] + length_ptr[i]);
                    Slice val(str);
                    _append_data<TYPE_VARCHAR, Slice>(column.get(), val);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_DATE) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            int* length_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    std::string date_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i] + length_ptr[i]);
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
            int* length_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    std::string origin_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i] + length_ptr[i]);
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
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            int* length_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);

            int precision = slot_desc->type().precision;
            int scale = slot_desc->type().scale;
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    std::string decimal_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i] + length_ptr[i]);
                    int32_t cpp_val;
                    if (DecimalV3Cast::from_string<int32_t>(&cpp_val, precision, scale, decimal_str.data(),
                                                            decimal_str.size())) {
                        return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                                    slot_desc->col_name(), decimal_str));
                    }
                    _append_data<TYPE_DECIMAL32, int32_t>(column.get(), cpp_val);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_DECIMAL64) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            int* length_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);

            int precision = slot_desc->type().precision;
            int scale = slot_desc->type().scale;
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    std::string decimal_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i] + length_ptr[i]);
                    int64_t cpp_val;
                    if (DecimalV3Cast::from_string<int64_t>(&cpp_val, precision, scale, decimal_str.data(),
                                                            decimal_str.size())) {
                        return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                                    slot_desc->col_name(), decimal_str));
                    }
                    _append_data<TYPE_DECIMAL64, int64_t>(column.get(), cpp_val);
                }
            }
        } else if (column_type == PrimitiveType::TYPE_DECIMAL128) {
            bool* null_column_ptr = reinterpret_cast<bool*>(chunk_meta_ptr[chunk_meta_index++]);
            int* offset_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            int* length_ptr = reinterpret_cast<int*>(chunk_meta_ptr[chunk_meta_index++]);
            char* column_ptr = reinterpret_cast<char*>(chunk_meta_ptr[chunk_meta_index++]);

            int precision = slot_desc->type().precision;
            int scale = slot_desc->type().scale;
            for (int i = 0; i < num_rows; i++) {
                if (null_column_ptr[i]) {
                    _process_null_value(column.get(), slot_desc);
                } else {
                    std::string decimal_str(column_ptr + offset_ptr[i], column_ptr + offset_ptr[i] + length_ptr[i]);
                    int128_t cpp_val;
                    if (DecimalV3Cast::from_string<int128_t>(&cpp_val, precision, scale, decimal_str.data(),
                                                             decimal_str.size())) {
                        return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                                    slot_desc->col_name(), decimal_str));
                    }
                    _append_data<TYPE_DECIMAL128, int128_t>(column.get(), cpp_val);
                }
            }
        } else {
            do_close(_runtime_state);
            return Status::InternalError(fmt::format("not support type {}", column_type));
        }
        _jni_env->CallVoidMethod(_hudi_mor_scanner, _scanner_column_release, col_idx);
    }
    return Status::OK();
}

Status HudiMORScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(reset_jni_env());
    bool has_next = false;
    RETURN_IF_ERROR(_has_next(&has_next));
    if (!has_next) {
        return Status::EndOfFile("");
    }
    long chunk_meta;

    RETURN_IF_ERROR(_get_next_chunk(&chunk_meta));
    RETURN_IF_ERROR(_fill_chunk(chunk_meta, chunk));
    _stats.raw_rows_read += (*chunk)->num_rows();
    return Status::OK();
}

} // namespace starrocks::vectorized