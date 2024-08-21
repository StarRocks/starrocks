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

#include "exec/jni_scanner.h"

#include <utility>

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "fmt/core.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks {

Status JniScanner::_check_jni_exception(JNIEnv* env, const std::string& message) {
    if (jthrowable thr = env->ExceptionOccurred(); thr) {
        std::string jni_error_message = JVMFunctionHelper::getInstance().dumpExceptionString(thr);
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(thr);
        return Status::InternalError(message + " java exception details: " + jni_error_message);
    }
    return Status::OK();
}

Status JniScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    RETURN_IF_ERROR(detect_java_runtime());
    return Status::OK();
}

Status JniScanner::do_open(RuntimeState* state) {
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();
    RETURN_IF_ERROR(update_jni_scanner_params());
    if (env->EnsureLocalCapacity(_jni_scanner_params.size() * 2 + 6) < 0) {
        RETURN_IF_ERROR(_check_jni_exception(env, "Failed to ensure the local capacity."));
    }
    RETURN_IF_ERROR(_init_jni_table_scanner(env, state));
    RETURN_IF_ERROR(_init_jni_method(env));
    env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_open);
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to open the off-heap table scanner."));
    return Status::OK();
}

void JniScanner::do_close(RuntimeState* runtime_state) noexcept {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();
    if (_jni_scanner_obj != nullptr) {
        if (_jni_scanner_close != nullptr) {
            env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_close);
        }
        env->DeleteLocalRef(_jni_scanner_obj);
        _jni_scanner_obj = nullptr;
    }
    if (_jni_scanner_cls != nullptr) {
        env->DeleteLocalRef(_jni_scanner_cls);
        _jni_scanner_cls = nullptr;
    }
}

Status JniScanner::_init_jni_method(JNIEnv* env) {
    // init jmethod
    _jni_scanner_open = env->GetMethodID(_jni_scanner_cls, "open", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get `open` jni method"));

    _jni_scanner_get_next_chunk = env->GetMethodID(_jni_scanner_cls, "getNextOffHeapChunk", "()J");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get `getNextOffHeapChunk` jni method"));

    _jni_scanner_close = env->GetMethodID(_jni_scanner_cls, "close", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get `close` jni method"));

    _jni_scanner_release_column = env->GetMethodID(_jni_scanner_cls, "releaseOffHeapColumnVector", "(I)V");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get `releaseOffHeapColumnVector` jni method"));

    _jni_scanner_release_table = env->GetMethodID(_jni_scanner_cls, "releaseOffHeapTable", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get `releaseOffHeapTable` jni method"));
    return Status::OK();
}

Status JniScanner::_init_jni_table_scanner(JNIEnv* env, RuntimeState* runtime_state) {
    jclass scanner_factory_class = env->FindClass(_jni_scanner_factory_class.c_str());
    jmethodID scanner_factory_constructor = env->GetMethodID(scanner_factory_class, "<init>", "()V");
    jobject scanner_factory_obj = env->NewObject(scanner_factory_class, scanner_factory_constructor);
    jmethodID get_scanner_method = env->GetMethodID(scanner_factory_class, "getScannerClass", "()Ljava/lang/Class;");
    _jni_scanner_cls = (jclass)env->CallObjectMethod(scanner_factory_obj, get_scanner_method);
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to init the scanner class."));
    env->DeleteLocalRef(scanner_factory_class);
    env->DeleteLocalRef(scanner_factory_obj);

    jmethodID scanner_constructor = env->GetMethodID(_jni_scanner_cls, "<init>", "(ILjava/util/Map;)V");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get a scanner class constructor."));

    jclass hashmap_class = env->FindClass("java/util/HashMap");
    jmethodID hashmap_constructor = env->GetMethodID(hashmap_class, "<init>", "(I)V");
    jobject hashmap_object = env->NewObject(hashmap_class, hashmap_constructor, _jni_scanner_params.size());
    jmethodID hashmap_put =
            env->GetMethodID(hashmap_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to get the HashMap methods."));

    std::string message = "Initialize a scanner with parameters: ";
    for (const auto& it : _jni_scanner_params) {
        jstring key = env->NewStringUTF(it.first.c_str());
        jstring value = env->NewStringUTF(it.second.c_str());
        // skip encoded object
        if (_skipped_log_jni_scanner_params.find(it.first) == _skipped_log_jni_scanner_params.end()) {
            message.append(it.first);
            message.append("->");
            message.append(it.second);
            message.append(", ");
        }

        env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
    }
    env->DeleteLocalRef(hashmap_class);
    LOG(INFO) << message;

    int fetch_size = runtime_state->chunk_size();
    _jni_scanner_obj = env->NewObject(_jni_scanner_cls, scanner_constructor, fetch_size, hashmap_object);
    env->DeleteLocalRef(hashmap_object);
    DCHECK(_jni_scanner_obj != nullptr);
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to initialize a scanner instance."));

    return Status::OK();
}

Status JniScanner::_get_next_chunk(JNIEnv* env, long* chunk_meta) {
    SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats.io_ns);
    _app_stats.io_count += 1;
    *chunk_meta = env->CallLongMethod(_jni_scanner_obj, _jni_scanner_get_next_chunk);
    RETURN_IF_ERROR(_check_jni_exception(env, "Failed to call the nextChunkOffHeap method of off-heap table scanner."));
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
        RETURN_IF_ERROR((_append_primitive_data<TYPE_DATE>(args)));
    } else if (column_type == LogicalType::TYPE_DATETIME) {
        RETURN_IF_ERROR((_append_primitive_data<TYPE_DATETIME>(args)));
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

StatusOr<size_t> JniScanner::_fill_chunk(JNIEnv* env, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);

    long num_rows = next_chunk_meta_as_long();
    if (num_rows == 0) {
        return Status::EndOfFile("");
    }
    _app_stats.raw_rows_read += num_rows;

    for (size_t col_idx = 0; col_idx < _scanner_ctx.materialized_columns.size(); col_idx++) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[col_idx].slot_desc;
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
        env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_column, col_idx);
        RETURN_IF_ERROR(_check_jni_exception(
                env, "Failed to call the releaseOffHeapColumnVector method of off-heap table scanner."));
    }
    return num_rows;
}

Status JniScanner::_release_off_heap_table(JNIEnv* env) {
    env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_table);
    RETURN_IF_ERROR(
            _check_jni_exception(env, "Failed to call the releaseOffHeapTable method of off-heap table scanner."));
    return Status::OK();
}

Status JniScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    // fill chunk with all wanted column.
    ASSIGN_OR_RETURN(size_t chunk_size, fill_empty_chunk(chunk));
    // ====== conjunct evaluation ======
    // important to add columns before evaluation
    // because ctxs_by_slot maybe refers to some non-existed slot or partition slot.
    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, chunk_size));
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, chunk_size);
    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    return Status::OK();
}

StatusOr<size_t> JniScanner::fill_empty_chunk(ChunkPtr* chunk) {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();
    long chunk_meta;
    RETURN_IF_ERROR(_get_next_chunk(env, &chunk_meta));
    reset_chunk_meta(chunk_meta);
    auto status = _fill_chunk(env, chunk);
    RETURN_IF_ERROR(_release_off_heap_table(env));

    return status;
}

static void build_nested_fields(const TypeDescriptor& type, const std::string& parent, std::string* sb) {
    for (int i = 0; i < type.children.size(); i++) {
        const auto& t = type.children[i];
        if (t.is_unknown_type()) continue;
        std::string p = parent + "." + (type.is_struct_type() ? type.field_names[i] : fmt::format("${}", i));
        if (t.is_complex_type()) {
            build_nested_fields(t, p, sb);
        } else {
            sb->append(p);
            sb->append(",");
        }
    }
}

static std::string build_fs_options_properties(const FSOptions& options) {
    const TCloudConfiguration* cloud_configuration = options.cloud_configuration;
    static constexpr char KV_SEPARATOR = 0x1;
    static constexpr char PROP_SEPARATOR = 0x2;
    std::string data;

    if (cloud_configuration != nullptr && cloud_configuration->__isset.cloud_properties) {
        for (const auto& [key, value] : cloud_configuration->cloud_properties) {
            data += key;
            data += KV_SEPARATOR;
            data += value;
            data += PROP_SEPARATOR;
        }
    }

    if (data.size() > 0 && data.back() == PROP_SEPARATOR) {
        data.pop_back();
    }
    return data;
}

Status JniScanner::update_jni_scanner_params() {
    // update materialized columns.
    {
        std::unordered_set<std::string> names;
        for (const auto& column : _scanner_ctx.materialized_columns) {
            if (column.name() == "___count___") continue;
            names.insert(column.name());
        }
        RETURN_IF_ERROR(_scanner_ctx.update_materialized_columns(names));
    }

    std::string required_fields;
    for (const auto& column : _scanner_ctx.materialized_columns) {
        required_fields.append(column.name());
        required_fields.append(",");
    }
    if (!required_fields.empty()) {
        required_fields = required_fields.substr(0, required_fields.size() - 1);
    }

    std::string nested_fields;
    for (const auto& column : _scanner_ctx.materialized_columns) {
        const TypeDescriptor& type = column.slot_type();
        if (type.is_complex_type()) {
            build_nested_fields(type, column.name(), &nested_fields);
        }
    }
    if (!nested_fields.empty()) {
        nested_fields = nested_fields.substr(0, nested_fields.size() - 1);
    }

    _jni_scanner_params["required_fields"] = required_fields;
    _jni_scanner_params["nested_fields"] = nested_fields;
    return Status::OK();
}

// -------------------------------hive jni scanner-------------------------------

class HiveJniScanner : public JniScanner {
public:
    HiveJniScanner(std::string factory_class, std::map<std::string, std::string> params)
            : JniScanner(std::move(factory_class), std::move(params)) {}
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
};

Status HiveJniScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    // fill chunk with all wanted column exclude partition columns
    ASSIGN_OR_RETURN(size_t chunk_size, fill_empty_chunk(chunk));
    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, chunk_size));
    // right now only hive table need append partition columns explictly, paimon and hudi reader will append partition columns in Java side
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, chunk_size);
    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    return Status::OK();
}

std::unique_ptr<JniScanner> create_hive_jni_scanner(const JniScanner::CreateOptions& options) {
    const auto& scan_range = *(options.scan_range);
    const HiveTableDescriptor* hive_table = options.hive_table;
    static const char* serde_property_prefix = "SerDe.";

    std::string data_file_path;
    std::string hive_column_names;
    std::string hive_column_types;
    std::string serde;
    std::string input_format;
    std::map<std::string, std::string> serde_properties;
    std::string time_zone;

    if (dynamic_cast<const FileTableDescriptor*>(hive_table)) {
        const auto* file_table = down_cast<const FileTableDescriptor*>(hive_table);

        data_file_path = scan_range.full_path;

        hive_column_names = file_table->get_hive_column_names();
        hive_column_types = file_table->get_hive_column_types();
        serde = file_table->get_serde_lib();
        input_format = file_table->get_input_format();
        time_zone = file_table->get_time_zone();
    } else if (dynamic_cast<const HdfsTableDescriptor*>(hive_table)) {
        const auto* hdfs_table = down_cast<const HdfsTableDescriptor*>(hive_table);
        auto* partition_desc = hdfs_table->get_partition(scan_range.partition_id);
        std::string partition_full_path = partition_desc->location();
        data_file_path = fmt::format("{}/{}", partition_full_path, scan_range.relative_path);

        hive_column_names = hdfs_table->get_hive_column_names();
        hive_column_types = hdfs_table->get_hive_column_types();
        serde = hdfs_table->get_serde_lib();
        input_format = hdfs_table->get_input_format();
        serde_properties = hdfs_table->get_serde_properties();
        time_zone = hdfs_table->get_time_zone();
    } else {
        return nullptr;
    }

    std::map<std::string, std::string> jni_scanner_params;

    jni_scanner_params["hive_column_names"] = hive_column_names;
    jni_scanner_params["hive_column_types"] = hive_column_types;
    jni_scanner_params["data_file_path"] = data_file_path;
    jni_scanner_params["block_offset"] = std::to_string(scan_range.offset);
    jni_scanner_params["block_length"] = std::to_string(scan_range.length);
    jni_scanner_params["serde"] = serde;
    jni_scanner_params["input_format"] = input_format;
    jni_scanner_params["time_zone"] = time_zone;
    jni_scanner_params["fs_options_props"] = build_fs_options_properties(*(options.fs_options));

    for (const auto& pair : serde_properties) {
        jni_scanner_params[serde_property_prefix + pair.first] = pair.second;
    }

    std::string scanner_factory_class = "com/starrocks/hive/reader/HiveScannerFactory";

    return std::make_unique<HiveJniScanner>(scanner_factory_class, jni_scanner_params);
}

// ---------------paimon jni scanner------------------
std::unique_ptr<JniScanner> create_paimon_jni_scanner(const JniScanner::CreateOptions& options) {
    const auto& scan_range = *(options.scan_range);
    const HiveTableDescriptor* hive_table = options.hive_table;
    const auto* paimon_table = dynamic_cast<const PaimonTableDescriptor*>(hive_table);

    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["split_info"] = scan_range.paimon_split_info;
    jni_scanner_params["predicate_info"] = scan_range.paimon_predicate_info;
    jni_scanner_params["native_table"] = paimon_table->get_paimon_native_table();
    jni_scanner_params["time_zone"] = paimon_table->get_time_zone();

    std::string scanner_factory_class = "com/starrocks/paimon/reader/PaimonSplitScannerFactory";
    return std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);
}

// -------------hudi jni scanner---------------------
std::unique_ptr<JniScanner> create_hudi_jni_scanner(const JniScanner::CreateOptions& options) {
    const auto& scan_range = *(options.scan_range);
    const auto* hudi_table = dynamic_cast<const HudiTableDescriptor*>(options.hive_table);
    auto* partition_desc = hudi_table->get_partition(scan_range.partition_id);
    std::string partition_full_path = partition_desc->location();

    std::string delta_file_paths;
    if (!scan_range.hudi_logs.empty()) {
        for (const std::string& log : scan_range.hudi_logs) {
            delta_file_paths.append(fmt::format("{}/{}", partition_full_path, log));
            delta_file_paths.append(",");
        }
        delta_file_paths = delta_file_paths.substr(0, delta_file_paths.size() - 1);
    }

    std::string data_file_path;
    if (scan_range.relative_path.empty()) {
        data_file_path = "";
    } else {
        data_file_path = fmt::format("{}/{}", partition_full_path, scan_range.relative_path);
    }

    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["base_path"] = hudi_table->get_base_path();
    jni_scanner_params["hive_column_names"] = hudi_table->get_hive_column_names();
    jni_scanner_params["hive_column_types"] = hudi_table->get_hive_column_types();
    jni_scanner_params["instant_time"] = hudi_table->get_instant_time();
    jni_scanner_params["delta_file_paths"] = delta_file_paths;
    jni_scanner_params["data_file_path"] = data_file_path;
    jni_scanner_params["data_file_length"] = std::to_string(scan_range.file_length);
    jni_scanner_params["serde"] = hudi_table->get_serde_lib();
    jni_scanner_params["input_format"] = hudi_table->get_input_format();
    jni_scanner_params["fs_options_props"] = build_fs_options_properties(*(options.fs_options));
    jni_scanner_params["time_zone"] = hudi_table->get_time_zone();

    std::string scanner_factory_class = "com/starrocks/hudi/reader/HudiSliceScannerFactory";
    return std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);
}

// ---------------odps jni scanner------------------
std::unique_ptr<JniScanner> create_odps_jni_scanner(const JniScanner::CreateOptions& options) {
    const auto& scan_range = *(options.scan_range);
    const auto* odps_table = dynamic_cast<const OdpsTableDescriptor*>(options.hive_table);

    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["project_name"] = odps_table->get_database_name();
    jni_scanner_params["table_name"] = odps_table->get_table_name();
    jni_scanner_params.insert(scan_range.odps_split_infos.begin(), scan_range.odps_split_infos.end());
    jni_scanner_params["time_zone"] = odps_table->get_time_zone();

    const AliyunCloudConfiguration aliyun_cloud_configuration =
            CloudConfigurationFactory::create_aliyun(*options.fs_options->cloud_configuration);
    AliyunCloudCredential aliyun_cloud_credential = aliyun_cloud_configuration.aliyun_cloud_credential;
    jni_scanner_params["endpoint"] = aliyun_cloud_credential.endpoint;
    jni_scanner_params["access_id"] = aliyun_cloud_credential.access_key;
    jni_scanner_params["access_key"] = aliyun_cloud_credential.secret_key;

    std::string scanner_factory_class = "com/starrocks/odps/reader/OdpsSplitScannerFactory";
    return std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);
}

// ---------------iceberg metadata jni scanner------------------
std::unique_ptr<JniScanner> create_iceberg_metadata_jni_scanner(const JniScanner::CreateOptions& options) {
    const auto& scan_range = *(options.scan_range);
    const auto* hdfs_table = dynamic_cast<const IcebergMetadataTableDescriptor*>(options.hive_table);
    std::map<std::string, std::string> jni_scanner_params;

    jni_scanner_params["required_fields"] = hdfs_table->get_hive_column_names();
    jni_scanner_params["metadata_column_types"] = hdfs_table->get_hive_column_types();
    jni_scanner_params["serialized_predicate"] = options.scan_node->serialized_predicate;

    jni_scanner_params["serialized_table"] = options.scan_node->serialized_table;
    jni_scanner_params["split_info"] = scan_range.serialized_split;
    jni_scanner_params["load_column_stats"] = options.scan_node->load_column_stats ? "true" : "false";

    const std::string scanner_factory_class = "com/starrocks/connector/iceberg/IcebergMetadataScannerFactory";
    return std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);
}

// ---------------kudu jni scanner------------------
std::unique_ptr<JniScanner> create_kudu_jni_scanner(const JniScanner::CreateOptions& options) {
    const auto& scan_range = *(options.scan_range);

    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["kudu_scan_token"] = scan_range.kudu_scan_token;
    jni_scanner_params["kudu_master"] = scan_range.kudu_master;

    std::string scanner_factory_class = "com/starrocks/kudu/reader/KuduSplitScannerFactory";
    return std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);
}

} // namespace starrocks
