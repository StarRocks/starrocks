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

#pragma once

#include <string>

#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "common/status.h"
#include "fmt/core.h"
#include "jni.h"
#include "runtime/env/java/java_env.h"

namespace starrocks {

std::string dump_jni_exception_string(jthrowable throwable);

} // namespace starrocks

// local object reference guard.
// The object is automatically deleted when the guard leaves scope.
#define LOCAL_REF_GUARD(lref)                             \
    starrocks::DeferOp VARNAME_LINENUM(guard)([&lref]() { \
        if (lref) {                                       \
            getJNIEnv()->DeleteLocalRef(lref);            \
            lref = nullptr;                               \
        }                                                 \
    })

#define LOCAL_REF_GUARD_ENV(env, lref)                \
    starrocks::DeferOp VARNAME_LINENUM(guard)([&]() { \
        if (lref) {                                   \
            env->DeleteLocalRef(lref);                \
            lref = nullptr;                           \
        }                                             \
    })

// check JNI Exception and set error in FunctionContext
#define CHECK_UDF_CALL_EXCEPTION(env, ctx)                         \
    if (auto e = env->ExceptionOccurred()) {                       \
        LOCAL_REF_GUARD_ENV(env, e);                               \
        std::string msg = starrocks::dump_jni_exception_string(e); \
        LOG(WARNING) << "Exception: " << msg;                      \
        ctx->set_error(msg.c_str());                               \
        env->ExceptionClear();                                     \
    }

#define RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, prefix)                       \
    if (auto e = env->ExceptionOccurred()) {                                         \
        LOCAL_REF_GUARD_ENV(env, e);                                                 \
        std::string err = starrocks::dump_jni_exception_string(e);                   \
        env->ExceptionClear();                                                       \
        return starrocks::Status::InternalError(fmt::format("{}, {}", prefix, err)); \
    }

#define RETURN_ERROR_IF_JNI_EXCEPTION(env) RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "JNI Exception")

#define RETURN_IF_JNI_EXCEPTION(env, errmsg, ret)                                                   \
    if (jthrowable jthr = env->ExceptionOccurred()) {                                               \
        LOCAL_REF_GUARD_ENV(env, jthr);                                                             \
        std::string msg = fmt::format("{},{}", errmsg, starrocks::dump_jni_exception_string(jthr)); \
        LOG(WARNING) << msg;                                                                        \
        env->ExceptionClear();                                                                      \
        return ret;                                                                                 \
    }
