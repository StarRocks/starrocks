// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/jdbc_scanner.h"

#include <type_traits>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/cast_expr.h"
#include "jni_md.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

#define CHECK_JAVA_EXCEPTION(error_message)                                             \
    if (jthrowable thr = _jni_env->ExceptionOccurred(); thr) {                          \
        std::string err = JVMFunctionHelper::getInstance().dumpExceptionString(thr);    \
        _jni_env->ExceptionClear();                                                     \
        _jni_env->DeleteLocalRef(thr);                                                  \
        return Status::InternalError(fmt::format("{}, error: {}", error_message, err)); \
    }

#define PROCESS_NULL_VALUE(val, column)                                                                         \
    if (val == nullptr) {                                                                                       \
        if (!column->is_nullable()) {                                                                           \
            return Status::DataQualityError(                                                                    \
                    fmt::format("Unexpected NULL value occurs on NOT NULL column[{}]", slot_desc->col_name())); \
        }                                                                                                       \
        column->append_nulls(1);                                                                                \
        return Status::OK();                                                                                    \
    }

JDBCScanner::~JDBCScanner() {}

Status JDBCScanner::reset_jni_env() {
    _jni_env = JVMFunctionHelper::getInstance().getEnv();
    if (_jni_env == nullptr) {
        return Status::InternalError("Cannot get jni env");
    }
    return Status::OK();
}

Status JDBCScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(detect_java_runtime());

    RETURN_IF_ERROR(reset_jni_env());

    _init_profile();

    RETURN_IF_ERROR(_init_jdbc_bridge());

    RETURN_IF_ERROR(_init_jdbc_scan_context(state));

    RETURN_IF_ERROR(_init_jdbc_scanner());

    RETURN_IF_ERROR(_init_column_class_name());

    // init JDBCUtil method
    _jdbc_util_cls = _jni_env->FindClass(JDBC_UTIL_CLASS_NAME);
    DCHECK(_jdbc_util_cls != nullptr);
    _util_format_date =
            _jni_env->GetStaticMethodID(_jdbc_util_cls, "formatDate", "(Ljava/sql/Date;)Ljava/lang/String;");
    DCHECK(_util_format_date != nullptr);
    _util_format_localdatetime = _jni_env->GetStaticMethodID(_jdbc_util_cls, "formatLocalDatetime",
                                                             "(Ljava/time/LocalDateTime;)Ljava/lang/String;");
    DCHECK(_util_format_localdatetime != nullptr);

    return Status::OK();
}

Status JDBCScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    bool has_next = false;
    RETURN_IF_ERROR(_has_next(&has_next));
    if (!has_next) {
        *eos = true;
        return Status::OK();
    }
    jobject jchunk;
    DeferOp defer([&jchunk, this]() {
        if (jchunk != nullptr) {
            _jni_env->DeleteLocalRef(jchunk);
        }
    });

    RETURN_IF_ERROR(_get_next_chunk(&jchunk));
    RETURN_IF_ERROR(_fill_chunk(jchunk, chunk));
    return Status::OK();
}

Status JDBCScanner::close(RuntimeState* state) {
    return _close_jdbc_scanner();
}

Status JDBCScanner::_init_jdbc_bridge() {
    // 1. construct JDBCBridge
    _jdbc_bridge_cls = _jni_env->FindClass(JDBC_BRIDGE_CLASS_NAME);
    DCHECK(_jdbc_bridge_cls != nullptr);

    jmethodID constructor = _jni_env->GetMethodID(_jdbc_bridge_cls, "<init>", "()V");
    DCHECK(constructor != nullptr);

    _jdbc_bridge = _jni_env->NewObject(_jdbc_bridge_cls, constructor);

    // 2. set class loader
    jmethodID set_class_loader = _jni_env->GetMethodID(_jdbc_bridge_cls, "setClassLoader", "(Ljava/lang/String;)V");
    DCHECK(set_class_loader != nullptr);

    jstring driver_location = _jni_env->NewStringUTF(_scan_ctx.driver_path.c_str());
    _jni_env->CallVoidMethod(_jdbc_bridge, set_class_loader, driver_location);
    _jni_env->DeleteLocalRef(driver_location);
    CHECK_JAVA_EXCEPTION("set class loader failed")

    return Status::OK();
}

Status JDBCScanner::_init_jdbc_scan_context(RuntimeState* state) {
    jclass scan_context_cls = _jni_env->FindClass(JDBC_SCAN_CONTEXT_CLASS_NAME);
    DCHECK(scan_context_cls != nullptr);

    jmethodID constructor = _jni_env->GetMethodID(
            scan_context_cls, "<init>",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;I)V");
    jstring driver_class_name = _jni_env->NewStringUTF(_scan_ctx.driver_class_name.c_str());
    jstring jdbc_url = _jni_env->NewStringUTF(_scan_ctx.jdbc_url.c_str());
    jstring user = _jni_env->NewStringUTF(_scan_ctx.user.c_str());
    jstring passwd = _jni_env->NewStringUTF(_scan_ctx.passwd.c_str());
    jstring sql = _jni_env->NewStringUTF(_scan_ctx.sql.c_str());
    int statement_fetch_size = state->chunk_size();

    _jdbc_scan_context = _jni_env->NewObject(scan_context_cls, constructor, driver_class_name, jdbc_url, user, passwd,
                                             sql, statement_fetch_size);

    _jni_env->DeleteLocalRef(driver_class_name);
    _jni_env->DeleteLocalRef(jdbc_url);
    _jni_env->DeleteLocalRef(user);
    _jni_env->DeleteLocalRef(passwd);
    _jni_env->DeleteLocalRef(sql);
    CHECK_JAVA_EXCEPTION("construct JDBCScanContext failed")

    return Status::OK();
}

Status JDBCScanner::_init_jdbc_scanner() {
    jmethodID get_scanner =
            _jni_env->GetMethodID(_jdbc_bridge_cls, "getScanner",
                                  "(Lcom/starrocks/jdbcbridge/JDBCScanContext;)Lcom/starrocks/jdbcbridge/JDBCScanner;");
    DCHECK(get_scanner != nullptr);

    _jdbc_scanner = _jni_env->CallObjectMethod(_jdbc_bridge, get_scanner, _jdbc_scan_context);
    _jni_env->DeleteLocalRef(_jdbc_scan_context);
    _jni_env->DeleteLocalRef(_jdbc_bridge);
    CHECK_JAVA_EXCEPTION("get JDBCScanner failed")

    _jdbc_scanner_cls = _jni_env->FindClass(JDBC_SCANNER_CLASS_NAME);
    DCHECK(_jdbc_scanner_cls != nullptr);
    // init jmethod
    _scanner_has_next = _jni_env->GetMethodID(_jdbc_scanner_cls, "hasNext", "()Z");
    DCHECK(_scanner_has_next != nullptr);
    _scanner_get_next_chunk = _jni_env->GetMethodID(_jdbc_scanner_cls, "getNextChunk", "()Ljava/util/List;");
    DCHECK(_scanner_get_next_chunk != nullptr);
    _scanner_close = _jni_env->GetMethodID(_jdbc_scanner_cls, "close", "()V");
    DCHECK(_scanner_close != nullptr);

    // open scanner
    jmethodID scanner_open = _jni_env->GetMethodID(_jdbc_scanner_cls, "open", "()V");
    DCHECK(scanner_open != nullptr);

    _jni_env->CallVoidMethod(_jdbc_scanner, scanner_open);
    CHECK_JAVA_EXCEPTION("open JDBCScanner failed")

    return Status::OK();
}

void JDBCScanner::_init_profile() {
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "IOCounter", TUnit::UNIT);
    _profile.fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunkTime");
    _runtime_profile->add_info_string("Query", _scan_ctx.sql);
}

Status JDBCScanner::_precheck_data_type(const std::string& java_class, SlotDescriptor* slot_desc) {
    auto type = slot_desc->type().type;
    if (java_class == "java.lang.Short") {
        if (type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Short, please set the type to "
                                "one of tinyint,smallint,int,bigint",
                                slot_desc->col_name()));
        }
    } else if (java_class == "java.lang.Integer") {
        if (type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Integer, please set the type to "
                                "one of tinyint,smallint,int,bigint",
                                slot_desc->col_name()));
        }
    } else if (java_class == "java.lang.String") {
        if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is String, please set the type to varchar or char",
                    slot_desc->col_name()));
        }
    } else if (java_class == "java.lang.Long") {
        if (type != TYPE_BIGINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Long, please set the type to bigint",
                    slot_desc->col_name()));
        }
    } else if (java_class == "java.lang.Boolean") {
        if (type != TYPE_BOOLEAN && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Boolean, please set the type to "
                                "one of boolean,smallint,int,bigint",
                                slot_desc->col_name()));
        }
    } else if (java_class == "java.lang.Float") {
        if (type != TYPE_FLOAT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Float, please set the type to float",
                    slot_desc->col_name()));
        }
    } else if (java_class == "java.lang.Double") {
        if (type != TYPE_DOUBLE) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Double, please set the type to double",
                    slot_desc->col_name()));
        }
    } else if (java_class == "java.sql.Timestamp") {
        if (type != TYPE_DATETIME) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Timestamp, please set the type to datetime",
                    slot_desc->col_name()));
        }
    } else if (java_class == "java.sql.Date") {
        if (type != TYPE_DATE) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Date, please set the type to date",
                                slot_desc->col_name()));
        }
    } else if (java_class == "java.time.LocalDateTime") {
        if (type != TYPE_DATETIME) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is LocalDateTime, please set the type to datetime",
                    slot_desc->col_name()));
        }
    } else if (java_class == "java.math.BigDecimal") {
        if (type != TYPE_DECIMAL32 && type != TYPE_DECIMAL64 && type != TYPE_DECIMAL128) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is BigDecimal, please set the type to decimal",
                    slot_desc->col_name()));
        }
    } else {
        return Status::NotSupported(fmt::format("Type is not supported on column[{}], JDBC result type is [{}]",
                                                slot_desc->col_name(), java_class));
    }
    return Status::OK();
}

Status JDBCScanner::_init_column_class_name() {
    jmethodID get_result_column_class_names =
            _jni_env->GetMethodID(_jdbc_scanner_cls, "getResultColumnClassNames", "()Ljava/util/List;");
    DCHECK(get_result_column_class_names != nullptr);

    jobject column_class_names = _jni_env->CallObjectMethod(_jdbc_scanner, get_result_column_class_names);
    CHECK_JAVA_EXCEPTION("get JDBC result column class name failed")

    auto& helper = JVMFunctionHelper::getInstance();
    int len = helper.list_size(column_class_names);

    for (int i = 0; i < len; i++) {
        jobject jelement = helper.list_get(column_class_names, i);
        DeferOp defer([&jelement, this]() { _jni_env->DeleteLocalRef(jelement); });
        std::string class_name = helper.to_string((jstring)(jelement));
        RETURN_IF_ERROR(_precheck_data_type(class_name, _slot_descs[i]));
        _column_class_name.emplace_back(class_name);
    }
    _jni_env->DeleteLocalRef(column_class_names);

    return Status::OK();
}

Status JDBCScanner::_has_next(bool* result) {
    jboolean ret = _jni_env->CallBooleanMethod(_jdbc_scanner, _scanner_has_next);
    CHECK_JAVA_EXCEPTION("call JDBCScanner hasNext failed")
    *result = ret;
    return Status::OK();
}

Status JDBCScanner::_get_next_chunk(jobject* chunk) {
    SCOPED_TIMER(_profile.io_timer);
    COUNTER_UPDATE(_profile.io_counter, 1);
    *chunk = _jni_env->CallObjectMethod(_jdbc_scanner, _scanner_get_next_chunk);
    CHECK_JAVA_EXCEPTION("getNextChunk failed")
    return Status::OK();
}

Status JDBCScanner::_close_jdbc_scanner() {
    if (_jdbc_scanner == nullptr) {
        return Status::OK();
    }
    _jni_env->CallVoidMethod(_jdbc_scanner, _scanner_close);
    _jni_env->DeleteLocalRef(_jdbc_scanner);
    CHECK_JAVA_EXCEPTION("close JDBCScanner failed")
    return Status::OK();
}

template <PrimitiveType type, typename CppType>
void JDBCScanner::_append_data(Column* column, CppType& value) {
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

template <>
Status JDBCScanner::_append_value_from_result<std::string>(jobject jval,
                                                           std::function<std::string(jobject)> get_value_func,
                                                           SlotDescriptor* slot_desc, Column* column) {
    DeferOp defer([&jval, this]() { _jni_env->DeleteLocalRef(jval); });
    PROCESS_NULL_VALUE(jval, column)

    std::string cpp_val = get_value_func(jval);
    int max_len = slot_desc->type().len;
    if (cpp_val.size() > max_len) {
        return Status::DataQualityError(
                fmt::format("Value length exceeds limit on column[{}], max length is [{}], value is [{}]",
                            slot_desc->col_name(), max_len, cpp_val));
    }
    switch (slot_desc->type().type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        Slice val(cpp_val);
        _append_data<TYPE_VARCHAR, Slice>(column, val);
        break;
    }
    default: {
        DCHECK(false) << "unreachable path, unknown type:" << slot_desc->type().type;
        return Status::InternalError(fmt::format("unknown type {}", slot_desc->type().type));
    }
    }
    return Status::OK();
}

template <typename CppType>
Status JDBCScanner::_append_value_from_result(jobject jval, std::function<CppType(jobject)> get_value_func,
                                              SlotDescriptor* slot_desc, Column* column) {
    DeferOp defer([&jval, this]() { _jni_env->DeleteLocalRef(jval); });
    PROCESS_NULL_VALUE(jval, column)

#define CHECK_DATA_OVERFLOW(val, min_val, max_val)                                                                  \
    if (val > max_val || val < min_val) {                                                                           \
        return Status::DataQualityError(                                                                            \
                fmt::format("Data out of range on column[{}], invalid value is [{}]", slot_desc->col_name(), val)); \
    }

    CppType cpp_val = get_value_func(jval);
    switch (slot_desc->type().type) {
    case TYPE_INT: {
        CHECK_DATA_OVERFLOW(cpp_val, INT_MIN, INT_MAX)
        _append_data<TYPE_INT, CppType>(column, cpp_val);
        break;
    }
    case TYPE_TINYINT: {
        CHECK_DATA_OVERFLOW(cpp_val, INT8_MIN, INT8_MAX)
        _append_data<TYPE_TINYINT, CppType>(column, cpp_val);
        break;
    }
    case TYPE_SMALLINT: {
        CHECK_DATA_OVERFLOW(cpp_val, INT16_MIN, INT16_MAX)
        _append_data<TYPE_SMALLINT, CppType>(column, cpp_val);
        break;
    }
    case TYPE_BIGINT: {
        CHECK_DATA_OVERFLOW(cpp_val, INT64_MIN, INT64_MAX)
        _append_data<TYPE_BIGINT, CppType>(column, cpp_val);
        break;
    }
    case TYPE_DOUBLE: {
        _append_data<TYPE_DOUBLE, CppType>(column, cpp_val);
        break;
    }
    case TYPE_FLOAT: {
        _append_data<TYPE_FLOAT, CppType>(column, cpp_val);
        break;
    }
    case TYPE_BOOLEAN: {
        _append_data<TYPE_BOOLEAN, CppType>(column, cpp_val);
        break;
    }
    default: {
        DCHECK(false) << "unknown type:" << slot_desc->type().type;
        return Status::InternalError(fmt::format("unknown type {}", slot_desc->type().type));
    }
    }
    return Status::OK();
}

Status JDBCScanner::_fill_chunk(jobject jchunk, ChunkPtr* chunk) {
    SCOPED_TIMER(_profile.fill_chunk_timer);
    auto& helper = JVMFunctionHelper::getInstance();
    jobject first_column = helper.list_get(jchunk, 0);
    int num_rows = helper.list_size(first_column);
    _jni_env->DeleteLocalRef(first_column);
    COUNTER_UPDATE(_profile.rows_read_counter, num_rows);

    for (size_t col_idx = 0; col_idx < _slot_descs.size(); col_idx++) {
        SlotDescriptor* slot_desc = _slot_descs[col_idx];
        ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        const auto& column_class = _column_class_name[col_idx];

        jobject jcolumn = helper.list_get(jchunk, col_idx);
        DeferOp defer([&jcolumn, this]() { _jni_env->DeleteLocalRef(jcolumn); });

#define FILL_COLUMN(get_value_func, cpp_type)                                                                         \
    {                                                                                                                 \
        auto func = std::bind(&JVMFunctionHelper::get_value_func, &helper, std::placeholders::_1);                    \
        for (int i = 0; i < num_rows; i++) {                                                                          \
            RETURN_IF_ERROR(                                                                                          \
                    _append_value_from_result<cpp_type>(helper.list_get(jcolumn, i), func, slot_desc, column.get())); \
        }                                                                                                             \
    }
        if (column_class == "java.lang.Short") {
            FILL_COLUMN(valint16_t, int16_t);
        } else if (column_class == "java.lang.Integer") {
            FILL_COLUMN(valint32_t, int32_t);
        } else if (column_class == "java.lang.String") {
            FILL_COLUMN(to_string, std::string);
        } else if (column_class == "java.lang.Long") {
            FILL_COLUMN(valint64_t, int64_t);
        } else if (column_class == "java.lang.Boolean") {
            FILL_COLUMN(valuint8_t, uint8_t);
        } else if (column_class == "java.lang.Float") {
            FILL_COLUMN(valfloat, float)
        } else if (column_class == "java.lang.Double") {
            FILL_COLUMN(valdouble, double);
        } else if (column_class == "java.sql.Timestamp") {
            DCHECK(slot_desc->type().type == TYPE_DATETIME);
            for (int i = 0; i < num_rows; i++) {
                RETURN_IF_ERROR(_append_datetime_val(helper.list_get(jcolumn, i), slot_desc, column.get()));
            }
        } else if (column_class == "java.sql.Date") {
            DCHECK(slot_desc->type().type == TYPE_DATE);
            for (int i = 0; i < num_rows; i++) {
                RETURN_IF_ERROR(_append_date_val(helper.list_get(jcolumn, i), slot_desc, column.get()));
            }
        } else if (column_class == "java.time.LocalDateTime") {
            DCHECK(slot_desc->type().type == TYPE_DATETIME);
            for (int i = 0; i < num_rows; i++) {
                RETURN_IF_ERROR(_append_localdatetime_val(helper.list_get(jcolumn, i), slot_desc, column.get()));
            }
        } else if (column_class == "java.math.BigDecimal") {
            for (int i = 0; i < num_rows; i++) {
                RETURN_IF_ERROR(_append_decimal_val(helper.list_get(jcolumn, i), slot_desc, column.get()));
            }
        } else {
            return Status::InternalError(fmt::format("not support type {}", column_class));
        }
    }
    return Status::OK();
}

Status JDBCScanner::_append_datetime_val(jobject jval, SlotDescriptor* slot_desc, Column* column) {
    PROCESS_NULL_VALUE(jval, column)
    DeferOp defer([&jval, this]() { _jni_env->DeleteLocalRef(jval); });

    std::string origin_str = JVMFunctionHelper::getInstance().to_string(jval);
    std::string datetime_str = origin_str.substr(0, origin_str.find('.'));
    TimestampValue tsv;
    if (!tsv.from_datetime_format_str(datetime_str.c_str(), datetime_str.size(), "%Y-%m-%d %H:%i:%s")) {
        return Status::DataQualityError(fmt::format("Invalid datetime value occurs on column[{}], value is [{}]",
                                                    slot_desc->col_name(), origin_str));
    }
    _append_data<TYPE_DATETIME, TimestampValue>(column, tsv);
    return Status::OK();
}

Status JDBCScanner::_append_localdatetime_val(jobject jval, SlotDescriptor* slot_desc, Column* column) {
    PROCESS_NULL_VALUE(jval, column)
    DeferOp defer([&jval, this]() { _jni_env->DeleteLocalRef(jval); });

    std::string localdatetime_str = _get_localdatetime_string(jval);
    TimestampValue tsv;
    if (!tsv.from_datetime_format_str(localdatetime_str.c_str(), localdatetime_str.size(), "%Y-%m-%d %H:%i:%s")) {
        return Status::DataQualityError(fmt::format("Invalid datetime value occurs on column[{}], value is [{}]",
                                                    slot_desc->col_name(), localdatetime_str));
    }
    _append_data<TYPE_DATETIME, TimestampValue>(column, tsv);
    return Status::OK();
}

Status JDBCScanner::_append_date_val(jobject jval, SlotDescriptor* slot_desc, Column* column) {
    PROCESS_NULL_VALUE(jval, column)
    DeferOp defer([&jval, this]() { _jni_env->DeleteLocalRef(jval); });

    std::string date_str = _get_date_string(jval);
    DateValue dv;
    if (!dv.from_string(date_str.c_str(), date_str.size())) {
        return Status::DataQualityError(
                fmt::format("Invalid date value occurs on column[{}], value is [{}]", slot_desc->col_name(), date_str));
    }
    _append_data<TYPE_DATE, DateValue>(column, dv);
    return Status::OK();
}

std::string JDBCScanner::_get_date_string(jobject jval) {
    jstring date_str = (jstring)_jni_env->CallStaticObjectMethod(_jdbc_util_cls, _util_format_date, jval);
    std::string result = JVMFunctionHelper::getInstance().to_string(date_str);
    _jni_env->DeleteLocalRef(date_str);
    return result;
}

std::string JDBCScanner::_get_localdatetime_string(jobject jval) {
    jstring datetime_str = (jstring)_jni_env->CallStaticObjectMethod(_jdbc_util_cls, _util_format_localdatetime, jval);
    std::string result = JVMFunctionHelper::getInstance().to_string(datetime_str);
    _jni_env->DeleteLocalRef(datetime_str);
    return result;
}

Status JDBCScanner::_append_decimal_val(jobject jval, SlotDescriptor* slot_desc, Column* column) {
    PROCESS_NULL_VALUE(jval, column)
    DeferOp defer([&jval, this]() { _jni_env->DeleteLocalRef(jval); });

    auto type = slot_desc->type().type;
    int precision = slot_desc->type().precision;
    int scale = slot_desc->type().scale;

    std::string decimal_str = JVMFunctionHelper::getInstance().to_string(jval);
    switch (type) {
    case TYPE_DECIMAL32: {
        int32_t cpp_val;
        if (DecimalV3Cast::from_string<int32_t>(&cpp_val, precision, scale, decimal_str.data(), decimal_str.size())) {
            return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                        slot_desc->col_name(), decimal_str));
        }
        _append_data<TYPE_DECIMAL32, int32_t>(column, cpp_val);
        break;
    }
    case TYPE_DECIMAL64: {
        int64_t cpp_val;
        if (DecimalV3Cast::from_string<int64_t>(&cpp_val, precision, scale, decimal_str.data(), decimal_str.size())) {
            return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                        slot_desc->col_name(), decimal_str));
        }
        _append_data<TYPE_DECIMAL64, int64_t>(column, cpp_val);
        break;
    }
    case TYPE_DECIMAL128: {
        int128_t cpp_val;
        if (DecimalV3Cast::from_string<int128_t>(&cpp_val, precision, scale, decimal_str.data(), decimal_str.size())) {
            return Status::DataQualityError(fmt::format("Invalid value occurs in column[{}], value is [{}]",
                                                        slot_desc->col_name(), decimal_str));
        }
        _append_data<TYPE_DECIMAL128, int128_t>(column, cpp_val);
        break;
    }
    default: {
        DCHECK(false) << "unreachable path";
        return Status::InternalError("unreachable path");
    }
    }

    return Status::OK();
}

} // namespace starrocks::vectorized
