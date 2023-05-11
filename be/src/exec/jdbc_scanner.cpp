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

#include "exec/jdbc_scanner.h"

#include <memory>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks {

#define CHECK_JAVA_EXCEPTION(env, error_message)                                        \
    if (jthrowable thr = env->ExceptionOccurred(); thr) {                               \
        std::string err = JVMFunctionHelper::getInstance().dumpExceptionString(thr);    \
        env->ExceptionClear();                                                          \
        env->DeleteLocalRef(thr);                                                       \
        return Status::InternalError(fmt::format("{}, error: {}", error_message, err)); \
    }

Status JDBCScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(detect_java_runtime());

    _init_profile();

    RETURN_IF_ERROR(_init_jdbc_bridge());

    RETURN_IF_ERROR(_init_jdbc_scan_context(state));

    RETURN_IF_ERROR(_init_jdbc_scanner());

    RETURN_IF_ERROR(_init_column_class_name(state));

    RETURN_IF_ERROR(_init_jdbc_util());

    return Status::OK();
}

Status JDBCScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    bool has_next = false;
    RETURN_IF_ERROR(_has_next(&has_next));
    if (!has_next) {
        *eos = true;
        return Status::OK();
    }
    jobject jchunk = nullptr;
    size_t jchunk_rows = 0;
    LOCAL_REF_GUARD(jchunk);
    RETURN_IF_ERROR(_get_next_chunk(&jchunk, &jchunk_rows));
    RETURN_IF_ERROR(_fill_chunk(jchunk, jchunk_rows, chunk));
    return Status::OK();
}

Status JDBCScanner::close(RuntimeState* state) {
    return _close_jdbc_scanner();
}

Status JDBCScanner::_init_jdbc_bridge() {
    // 1. construct JDBCBridge
    auto& h = JVMFunctionHelper::getInstance();
    auto* env = h.getEnv();

    auto jdbc_bridge_cls = env->FindClass(JDBC_BRIDGE_CLASS_NAME);
    DCHECK(jdbc_bridge_cls != nullptr);
    _jdbc_bridge_cls = std::make_unique<JVMClass>(env->NewGlobalRef(jdbc_bridge_cls));
    LOCAL_REF_GUARD_ENV(env, jdbc_bridge_cls);

    ASSIGN_OR_RETURN(_jdbc_bridge, _jdbc_bridge_cls->newInstance());

    // 2. set class loader
    jmethodID set_class_loader = env->GetMethodID(_jdbc_bridge_cls->clazz(), "setClassLoader", "(Ljava/lang/String;)V");
    DCHECK(set_class_loader != nullptr);

    jstring driver_location = env->NewStringUTF(_scan_ctx.driver_path.c_str());
    LOCAL_REF_GUARD_ENV(env, driver_location);
    env->CallVoidMethod(_jdbc_bridge.handle(), set_class_loader, driver_location);
    CHECK_JAVA_EXCEPTION(env, "set class loader failed")

    return Status::OK();
}

Status JDBCScanner::_init_jdbc_scan_context(RuntimeState* state) {
    // scan
    auto* env = JVMFunctionHelper::getInstance().getEnv();

    jclass scan_context_cls = env->FindClass(JDBC_SCAN_CONTEXT_CLASS_NAME);
    DCHECK(scan_context_cls != nullptr);
    LOCAL_REF_GUARD_ENV(env, scan_context_cls);

    jmethodID constructor = env->GetMethodID(
            scan_context_cls, "<init>",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IIII)V");
    jstring driver_class_name = env->NewStringUTF(_scan_ctx.driver_class_name.c_str());
    LOCAL_REF_GUARD_ENV(env, driver_class_name);
    jstring jdbc_url = env->NewStringUTF(_scan_ctx.jdbc_url.c_str());
    LOCAL_REF_GUARD_ENV(env, jdbc_url);
    jstring user = env->NewStringUTF(_scan_ctx.user.c_str());
    LOCAL_REF_GUARD_ENV(env, user);
    jstring passwd = env->NewStringUTF(_scan_ctx.passwd.c_str());
    LOCAL_REF_GUARD_ENV(env, passwd);
    jstring sql = env->NewStringUTF(_scan_ctx.sql.c_str());
    LOCAL_REF_GUARD_ENV(env, sql);
    int statement_fetch_size = state->chunk_size();
    int connection_pool_size = config::jdbc_connection_pool_size;
    if (UNLIKELY(connection_pool_size <= 0)) {
        connection_pool_size = DEFAULT_JDBC_CONNECTION_POOL_SIZE;
    }
    int minimum_idle_connections = config::jdbc_minimum_idle_connections;
    if (UNLIKELY(minimum_idle_connections < 0 || minimum_idle_connections > connection_pool_size)) {
        minimum_idle_connections = connection_pool_size;
    }
    int idle_timeout_ms = config::jdbc_connection_idle_timeout_ms;
    if (UNLIKELY(idle_timeout_ms < MINIMUM_ALLOWED_JDBC_CONNECTION_IDLE_TIMEOUT_MS)) {
        idle_timeout_ms = MINIMUM_ALLOWED_JDBC_CONNECTION_IDLE_TIMEOUT_MS;
    }

    auto scan_ctx =
            env->NewObject(scan_context_cls, constructor, driver_class_name, jdbc_url, user, passwd, sql,
                           statement_fetch_size, connection_pool_size, minimum_idle_connections, idle_timeout_ms);
    _jdbc_scan_context = env->NewGlobalRef(scan_ctx);
    LOCAL_REF_GUARD_ENV(env, scan_ctx);
    CHECK_JAVA_EXCEPTION(env, "construct JDBCScanContext failed")

    return Status::OK();
}

Status JDBCScanner::_init_jdbc_scanner() {
    auto* env = JVMFunctionHelper::getInstance().getEnv();

    jmethodID get_scanner =
            env->GetMethodID(_jdbc_bridge_cls->clazz(), "getScanner",
                             "(Lcom/starrocks/jdbcbridge/JDBCScanContext;)Lcom/starrocks/jdbcbridge/JDBCScanner;");
    DCHECK(get_scanner != nullptr);

    auto jdbc_scanner = env->CallObjectMethod(_jdbc_bridge.handle(), get_scanner, _jdbc_scan_context.handle());
    _jdbc_scanner = env->NewGlobalRef(jdbc_scanner);
    LOCAL_REF_GUARD_ENV(env, jdbc_scanner);
    CHECK_JAVA_EXCEPTION(env, "get JDBCScanner failed")

    auto jdbc_scanner_cls = env->FindClass(JDBC_SCANNER_CLASS_NAME);
    _jdbc_scanner_cls = std::make_unique<JVMClass>(env->NewGlobalRef(jdbc_scanner_cls));
    LOCAL_REF_GUARD_ENV(env, jdbc_scanner);

    DCHECK(_jdbc_scanner_cls != nullptr);
    // init jmethod
    _scanner_has_next = env->GetMethodID(_jdbc_scanner_cls->clazz(), "hasNext", "()Z");
    DCHECK(_scanner_has_next != nullptr);
    _scanner_get_next_chunk = env->GetMethodID(_jdbc_scanner_cls->clazz(), "getNextChunk", "()Ljava/util/List;");

    _scanner_result_rows = env->GetMethodID(_jdbc_scanner_cls->clazz(), "getResultNumRows", "()I");
    DCHECK(_scanner_result_rows != nullptr);

    DCHECK(_scanner_get_next_chunk != nullptr);
    _scanner_close = env->GetMethodID(_jdbc_scanner_cls->clazz(), "close", "()V");
    DCHECK(_scanner_close != nullptr);

    // open scanner
    jmethodID scanner_open = env->GetMethodID(_jdbc_scanner_cls->clazz(), "open", "()V");
    DCHECK(scanner_open != nullptr);

    env->CallVoidMethod(_jdbc_scanner.handle(), scanner_open);
    CHECK_JAVA_EXCEPTION(env, "open JDBCScanner failed")

    return Status::OK();
}

void JDBCScanner::_init_profile() {
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "IOCounter", TUnit::UNIT);
    _profile.fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunkTime");
    _runtime_profile->add_info_string("Query", _scan_ctx.sql);
}

StatusOr<LogicalType> JDBCScanner::_precheck_data_type(const std::string& java_class, SlotDescriptor* slot_desc) {
    auto type = slot_desc->type().type;
    if (java_class == "java.lang.Short") {
        if (type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Short, please set the type to "
                                "one of tinyint,smallint,int,bigint",
                                slot_desc->col_name()));
        }
        return TYPE_SMALLINT;
    } else if (java_class == "java.lang.Integer") {
        if (type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Integer, please set the type to "
                                "one of tinyint,smallint,int,bigint",
                                slot_desc->col_name()));
        }
        return TYPE_INT;
    } else if (java_class == "java.lang.String") {
        if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is String, please set the type to varchar or char",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    } else if (java_class == "java.lang.Long") {
        if (type != TYPE_BIGINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Long, please set the type to bigint",
                    slot_desc->col_name()));
        }
        return TYPE_BIGINT;
    } else if (java_class == "java.lang.Boolean") {
        if (type != TYPE_BOOLEAN && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Boolean, please set the type to "
                                "one of boolean,smallint,int,bigint",
                                slot_desc->col_name()));
        }
        return TYPE_BOOLEAN;
    } else if (java_class == "java.lang.Float") {
        if (type != TYPE_FLOAT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Float, please set the type to float",
                    slot_desc->col_name()));
        }
        return TYPE_FLOAT;
    } else if (java_class == "java.lang.Double") {
        if (type != TYPE_DOUBLE) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Double, please set the type to double",
                    slot_desc->col_name()));
        }
        return TYPE_DOUBLE;
    } else if (java_class == "java.sql.Timestamp") {
        if (type != TYPE_DATETIME) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Timestamp, please set the type to datetime",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    } else if (java_class == "java.sql.Date") {
        if (type != TYPE_DATE) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Date, please set the type to date",
                                slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    } else if (java_class == "java.time.LocalDateTime") {
        if (type != TYPE_DATETIME) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is LocalDateTime, please set the type to datetime",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    } else if (java_class == "java.math.BigDecimal") {
        if (type != TYPE_DECIMAL32 && type != TYPE_DECIMAL64 && type != TYPE_DECIMAL128 && type != TYPE_VARCHAR) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is BigDecimal, please set the type to "
                                "decimal or varchar",
                                slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    } else {
        return Status::NotSupported(fmt::format("Type is not supported on column[{}], JDBC result type is [{}]",
                                                slot_desc->col_name(), java_class));
    }
    __builtin_unreachable();
}

Status JDBCScanner::_init_column_class_name(RuntimeState* state) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();

    jmethodID get_result_column_class_names =
            env->GetMethodID(_jdbc_scanner_cls->clazz(), "getResultColumnClassNames", "()Ljava/util/List;");
    DCHECK(get_result_column_class_names != nullptr);

    jobject column_class_names = env->CallObjectMethod(_jdbc_scanner.handle(), get_result_column_class_names);
    CHECK_JAVA_EXCEPTION(env, "get JDBC result column class name failed")
    LOCAL_REF_GUARD_ENV(env, column_class_names);

    auto& helper = JVMFunctionHelper::getInstance();
    int len = helper.list_size(column_class_names);

    _result_chunk = std::make_shared<Chunk>();
    for (int i = 0; i < len; i++) {
        jobject jelement = helper.list_get(column_class_names, i);
        LOCAL_REF_GUARD_ENV(env, jelement);
        std::string class_name = helper.to_string((jstring)(jelement));
        ASSIGN_OR_RETURN(auto ret_type, _precheck_data_type(class_name, _slot_descs[i]));
        _column_class_names.emplace_back(class_name);
        _result_column_types.emplace_back(ret_type);
        // intermediate means the result type from JDBC Scanner
        // Some types cannot be written directly to the column,
        // so we need to write directly to the intermediate type and then cast to the target type:
        // eg:
        // JDBC(java.sql.Date) -> SR(TYPE_VARCHAR) -> SR(cast(varchar as TYPE_DATE))
        auto intermediate = TypeDescriptor(ret_type);
        auto result_column = ColumnHelper::create_column(intermediate, true);
        _result_chunk->append_column(std::move(result_column), i);
        auto column_ref = _pool.add(new ColumnRef(intermediate, i));
        // TODO: add check cast status
        Expr* cast_expr =
                VectorizedCastExprFactory::from_type(intermediate, _slot_descs[i]->type(), column_ref, &_pool, true);
        _cast_exprs.push_back(_pool.add(new ExprContext(cast_expr)));
    }
    RETURN_IF_ERROR(Expr::prepare(_cast_exprs, state));
    RETURN_IF_ERROR(Expr::open(_cast_exprs, state));

    return Status::OK();
}

Status JDBCScanner::_init_jdbc_util() {
    auto* env = JVMFunctionHelper::getInstance().getEnv();

    // init JDBCUtil method
    auto jdbc_util_cls = env->FindClass(JDBC_UTIL_CLASS_NAME);
    DCHECK(jdbc_util_cls != nullptr);
    _jdbc_util_cls = std::make_unique<JVMClass>(env->NewGlobalRef(jdbc_util_cls));
    LOCAL_REF_GUARD_ENV(env, jdbc_util_cls);

    _util_format_date =
            env->GetStaticMethodID(_jdbc_util_cls->clazz(), "formatDate", "(Ljava/sql/Date;)Ljava/lang/String;");
    DCHECK(_util_format_date != nullptr);
    _util_format_localdatetime = env->GetStaticMethodID(_jdbc_util_cls->clazz(), "formatLocalDatetime",
                                                        "(Ljava/time/LocalDateTime;)Ljava/lang/String;");
    DCHECK(_util_format_localdatetime != nullptr);
    return Status::OK();
}

Status JDBCScanner::_has_next(bool* result) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    jboolean ret = env->CallBooleanMethod(_jdbc_scanner.handle(), _scanner_has_next);
    CHECK_JAVA_EXCEPTION(env, "call JDBCScanner hasNext failed")
    *result = ret;
    return Status::OK();
}

Status JDBCScanner::_get_next_chunk(jobject* chunk, size_t* num_rows) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    SCOPED_TIMER(_profile.io_timer);
    COUNTER_UPDATE(_profile.io_counter, 1);
    *chunk = env->CallObjectMethod(_jdbc_scanner.handle(), _scanner_get_next_chunk);
    CHECK_JAVA_EXCEPTION(env, "getNextChunk failed")
    *num_rows = env->CallIntMethod(_jdbc_scanner.handle(), _scanner_result_rows);
    CHECK_JAVA_EXCEPTION(env, "getResultNumRows failed")
    return Status::OK();
}

Status JDBCScanner::_close_jdbc_scanner() {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    if (_jdbc_scanner.handle() == nullptr) {
        return Status::OK();
    }
    env->CallVoidMethod(_jdbc_scanner.handle(), _scanner_close);
    CHECK_JAVA_EXCEPTION(env, "close JDBCScanner failed")

    _jdbc_scanner.clear();
    _jdbc_scan_context.clear();
    _jdbc_bridge.clear();
    _jdbc_util_cls.reset();
    _jdbc_scanner_cls.reset();
    _jdbc_bridge_cls.reset();
    return Status::OK();
}

Status JDBCScanner::_fill_chunk(jobject jchunk, size_t num_rows, ChunkPtr* chunk) {
    SCOPED_TIMER(_profile.fill_chunk_timer);
    // get result from JNI
    {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();

        COUNTER_UPDATE(_profile.rows_read_counter, num_rows);
        (*chunk)->reset();

        for (size_t i = 0; i < _slot_descs.size(); i++) {
            jobject jcolumn = helper.list_get(jchunk, i);
            LOCAL_REF_GUARD_ENV(env, jcolumn);
            auto& result_column = _result_chunk->columns()[i];
            auto st =
                    helper.get_result_from_boxed_array(_result_column_types[i], result_column.get(), jcolumn, num_rows);
            RETURN_IF_ERROR(st);
            // check data's length for string type
            auto origin_type = _slot_descs[i]->type().type;
            if (origin_type == TYPE_VARCHAR || origin_type == TYPE_CHAR) {
                DCHECK_EQ(_result_column_types[i], TYPE_VARCHAR);
                int max_len = _slot_descs[i]->type().len;
                ColumnViewer<TYPE_VARCHAR> viewer(result_column);
                for (int row = 0; row < viewer.size(); row++) {
                    if (viewer.is_null(row)) {
                        continue;
                    }
                    auto value = viewer.value(row);
                    if ((int)value.size > max_len) {
                        return Status::DataQualityError(fmt::format(
                                "Value length exceeds limit on column[{}], max length is [{}], value is [{}]",
                                _slot_descs[i]->col_name(), max_len, value));
                    }
                }
            }
            down_cast<NullableColumn*>(result_column.get())->update_has_null();
        }
    }

    // convert intermediate results type to output chunks
    // TODO: avoid the cast overhead when from type == to type
    for (size_t col_idx = 0; col_idx < _slot_descs.size(); col_idx++) {
        SlotDescriptor* slot_desc = _slot_descs[col_idx];
        // use reference, then we check the column's nullable and set the final result to the referred column.
        ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        ASSIGN_OR_RETURN(auto result, _cast_exprs[col_idx]->evaluate(_result_chunk.get()));
        // unfold const_nullable_column to avoid error down_cast.
        // unpack_and_duplicate_const_column is not suitable, we need set correct type.
        result = ColumnHelper::unfold_const_column(slot_desc->type(), num_rows, result);
        if (column->is_nullable() == result->is_nullable()) {
            column = result;
        } else if (column->is_nullable() && !result->is_nullable()) {
            column = NullableColumn::create(result, NullColumn::create(num_rows));
        } else if (!column->is_nullable() && result->is_nullable()) {
            if (result->has_null()) {
                return Status::DataQualityError(
                        fmt::format("Unexpected NULL value occurs on NOT NULL column[{}]", slot_desc->col_name()));
            }
            column = down_cast<NullableColumn*>(result.get())->data_column();
        }
    }
    return Status::OK();
}

} // namespace starrocks
