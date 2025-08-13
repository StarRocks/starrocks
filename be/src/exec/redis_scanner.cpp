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

#include "exec/redis_scanner.h"

#include <memory>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/cast_expr.h"
#include "exprs/clone_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "types/type_checker_manager.h"
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

Status RedisScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(detect_java_runtime());

    _init_profile();

    RETURN_IF_ERROR(_init_redis_scan_context(state));

    RETURN_IF_ERROR(_init_redis_scanner());

    RETURN_IF_ERROR(_init_column_class_name(state));

    return Status::OK();
}

Status RedisScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
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
    if (jchunk_rows == 0) {
        *eos = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_fill_chunk(jchunk, jchunk_rows, chunk));
    return Status::OK();
}

Status RedisScanner::close(RuntimeState* state) {
    return _close_redis_scanner();
}

Status RedisScanner::_init_redis_scan_context(RuntimeState* state) {
    // scan
    auto* env = JVMFunctionHelper::getInstance().getEnv();

    jclass scan_context_cls = env->FindClass(REDIS_SCAN_CONTEXT_CLASS_NAME);
    DCHECK(scan_context_cls != nullptr);
    LOCAL_REF_GUARD_ENV(env, scan_context_cls);

    for (int i = 0; i < _slot_descs.size(); i++) {
        switch (_slot_descs[i]->type().type) {
        case TYPE_BIGINT:
            _scan_ctx.column_types.emplace_back("java.lang.Long");
            break;
        default:
            _scan_ctx.column_types.emplace_back("java.lang.String");
        }
    }

    jmethodID constructor =
            env->GetMethodID(scan_context_cls, "<init>",
                             "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/"
                             "String;Ljava/lang/String;ILjava/util/List;Ljava/util/List;)V");
    jstring tbl_name = env->NewStringUTF(_scan_ctx.tbl_name.c_str());
    LOCAL_REF_GUARD_ENV(env, tbl_name);
    jstring db_name = env->NewStringUTF(_scan_ctx.db_name.c_str());
    LOCAL_REF_GUARD_ENV(env, db_name);
    jstring redis_url = env->NewStringUTF(_scan_ctx.redis_url.c_str());
    LOCAL_REF_GUARD_ENV(env, redis_url);
    jstring user = env->NewStringUTF(_scan_ctx.user.c_str());
    LOCAL_REF_GUARD_ENV(env, user);
    jstring passwd = env->NewStringUTF(_scan_ctx.passwd.c_str());
    LOCAL_REF_GUARD_ENV(env, passwd);
    jstring value_data_format = env->NewStringUTF(_scan_ctx.value_data_format.c_str());
    LOCAL_REF_GUARD_ENV(env, value_data_format);
    int statement_fetch_size = state->chunk_size();

    jobject column_names = ConvertVectorToJavaList(env, _scan_ctx.column_names);
    jobject column_types = ConvertVectorToJavaList(env, _scan_ctx.column_types);

    auto scan_ctx = env->NewObject(scan_context_cls, constructor, db_name, tbl_name, redis_url, user, passwd,
                                   value_data_format, statement_fetch_size, column_names, column_types);
    _redis_scan_context = env->NewGlobalRef(scan_ctx);
    LOCAL_REF_GUARD_ENV(env, scan_ctx);
    CHECK_JAVA_EXCEPTION(env, "construct RedisScanContext failed")

    return Status::OK();
}

jobject RedisScanner::ConvertVectorToJavaList(JNIEnv* env, const std::vector<std::string>& column_names) {
    jclass arrayListClass = env->FindClass("java/util/ArrayList");
    if (arrayListClass == nullptr) {
        return nullptr;
    }

    jmethodID arrayListConstructor = env->GetMethodID(arrayListClass, "<init>", "()V");
    if (arrayListConstructor == nullptr) {
        return nullptr;
    }

    jobject arrayList = env->NewObject(arrayListClass, arrayListConstructor);
    if (arrayList == nullptr) {
        return nullptr;
    }

    jmethodID arrayListAdd = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
    if (arrayListAdd == nullptr) {
        return nullptr;
    }

    for (const std::string& col : column_names) {
        jstring jstr = env->NewStringUTF(col.c_str());
        env->CallBooleanMethod(arrayList, arrayListAdd, jstr);
        env->DeleteLocalRef(jstr);
    }

    return arrayList;
}

Status RedisScanner::_init_redis_scanner() {
    auto& h = JVMFunctionHelper::getInstance();
    auto* env = h.getEnv();

    jclass redis_scanner_cls = env->FindClass(REDIS_SCANNER_CLASS_NAME);
    DCHECK(redis_scanner_cls != nullptr);
    LOCAL_REF_GUARD_ENV(env, redis_scanner_cls);
    jmethodID constructor =
            env->GetMethodID(redis_scanner_cls, "<init>", "(Lcom/starrocks/redis/reader/RedisScanContext;)V");
    auto redis_scanner = env->NewObject(redis_scanner_cls, constructor, _redis_scan_context.handle());
    _redis_scanner = env->NewGlobalRef(redis_scanner);
    LOCAL_REF_GUARD_ENV(env, redis_scanner);
    CHECK_JAVA_EXCEPTION(env, "get RedisScanner failed")

    _redis_scanner_cls = std::make_unique<JVMClass>(env->NewGlobalRef(redis_scanner_cls));
    LOCAL_REF_GUARD_ENV(env, redis_scanner);
    DCHECK(_redis_scanner_cls != nullptr);
    // init jmethod
    _scanner_has_next = env->GetMethodID(_redis_scanner_cls->clazz(), "hasNext", "()Z");
    DCHECK(_scanner_has_next != nullptr);
    _scanner_get_next_chunk = env->GetMethodID(_redis_scanner_cls->clazz(), "getNextChunk", "()Ljava/util/List;");

    _scanner_result_rows = env->GetMethodID(_redis_scanner_cls->clazz(), "getResultNumRows", "()I");
    DCHECK(_scanner_result_rows != nullptr);

    DCHECK(_scanner_get_next_chunk != nullptr);
    _scanner_close = env->GetMethodID(_redis_scanner_cls->clazz(), "close", "()V");
    DCHECK(_scanner_close != nullptr);

    // open scanner
    jmethodID scanner_open = env->GetMethodID(_redis_scanner_cls->clazz(), "open", "()V");
    DCHECK(scanner_open != nullptr);

    env->CallVoidMethod(_redis_scanner.handle(), scanner_open);
    CHECK_JAVA_EXCEPTION(env, "open RedisScanner failed")

    return Status::OK();
}

void RedisScanner::_init_profile() {
    _profile.rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _profile.io_timer = ADD_TIMER(_runtime_profile, "IOTime");
    _profile.io_counter = ADD_COUNTER(_runtime_profile, "IOCounter", TUnit::UNIT);
    _profile.fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunkTime");
}

StatusOr<LogicalType> RedisScanner::_precheck_data_type(const std::string& java_class, SlotDescriptor* slot_desc) {
    return TypeCheckerManager::getInstance().checkType(java_class, slot_desc);
}

Status RedisScanner::_init_column_class_name(RuntimeState* state) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();

    jmethodID get_result_column_class_names =
            env->GetMethodID(_redis_scanner_cls->clazz(), "getResultColumnClassNames", "()Ljava/util/List;");
    DCHECK(get_result_column_class_names != nullptr);

    jobject column_class_names = env->CallObjectMethod(_redis_scanner.handle(), get_result_column_class_names);
    CHECK_JAVA_EXCEPTION(env, "get Redis result column class name failed")
    LOCAL_REF_GUARD_ENV(env, column_class_names);

    auto& helper = JVMFunctionHelper::getInstance();
    JavaListStub list_stub(column_class_names);
    ASSIGN_OR_RETURN(int len, list_stub.size());

    _result_chunk = std::make_shared<Chunk>();
    for (int i = 0; i < len; i++) {
        ASSIGN_OR_RETURN(jobject jelement, list_stub.get(i));
        LOCAL_REF_GUARD_ENV(env, jelement);
        std::string class_name = helper.to_string((jstring)(jelement));
        ASSIGN_OR_RETURN(auto ret_type, _precheck_data_type(class_name, _slot_descs[i]));
        _result_column_types.emplace_back(ret_type);

        auto type_desc = TypeDescriptor(ret_type);
        auto result_column = ColumnHelper::create_column(type_desc, true);
        _result_chunk->append_column(std::move(result_column), i);
        auto column_ref = _pool.add(new ColumnRef(type_desc, i));
        Expr* expr = CloneExpr::from_child(column_ref, &_pool);

        _cast_exprs.push_back(_pool.add(new ExprContext(expr)));
    }
    RETURN_IF_ERROR(Expr::prepare(_cast_exprs, state));
    RETURN_IF_ERROR(Expr::open(_cast_exprs, state));

    return Status::OK();
}

Status RedisScanner::_has_next(bool* result) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    jboolean ret = env->CallBooleanMethod(_redis_scanner.handle(), _scanner_has_next);
    CHECK_JAVA_EXCEPTION(env, "call RedisScanner hasNext failed")
    *result = ret;
    return Status::OK();
}

Status RedisScanner::_get_next_chunk(jobject* chunk, size_t* num_rows) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    SCOPED_TIMER(_profile.io_timer);
    COUNTER_UPDATE(_profile.io_counter, 1);
    *chunk = env->CallObjectMethod(_redis_scanner.handle(), _scanner_get_next_chunk);
    CHECK_JAVA_EXCEPTION(env, "getNextChunk failed")
    *num_rows = env->CallIntMethod(_redis_scanner.handle(), _scanner_result_rows);
    CHECK_JAVA_EXCEPTION(env, "getResultNumRows failed")
    return Status::OK();
}

Status RedisScanner::_close_redis_scanner() {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    if (_redis_scanner.handle() == nullptr) {
        return Status::OK();
    }
    env->CallVoidMethod(_redis_scanner.handle(), _scanner_close);
    CHECK_JAVA_EXCEPTION(env, "close RedisScanner failed")

    _redis_scanner.clear();
    _redis_scan_context.clear();
    _redis_scanner_cls.reset();
    return Status::OK();
}

Status RedisScanner::_fill_chunk(jobject jchunk, size_t num_rows, ChunkPtr* chunk) {
    SCOPED_TIMER(_profile.fill_chunk_timer);
    // get result from JNI
    {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        JavaListStub list_stub(jchunk);

        COUNTER_UPDATE(_profile.rows_read_counter, num_rows);
        (*chunk)->reset();

        for (size_t i = 0; i < _slot_descs.size(); i++) {
            ASSIGN_OR_RETURN(jobject jcolumn, list_stub.get(i));
            LOCAL_REF_GUARD_ENV(env, jcolumn);
            auto& result_column = _result_chunk->columns()[i];
            auto st =
                    helper.get_result_from_boxed_array(_result_column_types[i], result_column.get(), jcolumn, num_rows);
            RETURN_IF_ERROR(st);
            down_cast<NullableColumn*>(result_column.get())->update_has_null();
        }
    }

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
