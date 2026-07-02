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

#include "udf/java/java_udf_context.h"

#include <fmt/core.h>

#include "base/utility/defer_op.h"
#include "udf/java/java_udf.h"
#include "udf/java/java_udf_reflection.h"

namespace starrocks {

JavaUDAFUniqueContext* get_java_udaf_context(FunctionContext* ctx) {
    if (ctx == nullptr) {
        return nullptr;
    }
    return reinterpret_cast<JavaUDAFUniqueContext*>(ctx->get_function_state(FunctionContext::THREAD_LOCAL));
}

void attach_java_udaf_context(FunctionContext* ctx, std::unique_ptr<JavaUDAFUniqueContext> udaf_ctx) {
    DCHECK(ctx != nullptr);
    DCHECK(udaf_ctx != nullptr);
    auto* old_ctx = get_java_udaf_context(ctx);
    DCHECK(old_ctx == nullptr) << "duplicate Java UDAF context attach";
    if (old_ctx != nullptr) {
        delete old_ctx;
    }
    ctx->set_function_state(FunctionContext::THREAD_LOCAL, udaf_ctx.release());
}

void clear_java_udaf_states(FunctionContext* ctx) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    if (udaf_ctx == nullptr || udaf_ctx->states == nullptr) {
        return;
    }

    auto env = JVMFunctionHelper::getInstance().getEnv();
    udaf_ctx->states->clear(ctx, env);
}

void destroy_java_udaf_context(FunctionContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    auto* udaf_ctx = get_java_udaf_context(ctx);
    if (udaf_ctx == nullptr) {
        return;
    }

    clear_java_udaf_states(ctx);
    ctx->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
    delete udaf_ctx;
}

UDAFStateList::UDAFStateList(JavaGlobalRef&& handle, JavaGlobalRef&& get, JavaGlobalRef&& batch_get,
                             JavaGlobalRef&& add, JavaGlobalRef&& remove, JavaGlobalRef&& clear)
        : _handle(std::move(handle)),
          _get_method(std::move(get)),
          _batch_get_method(std::move(batch_get)),
          _add_method(std::move(add)),
          _remove_method(std::move(remove)),
          _clear_method(std::move(clear)) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    _get_method_id = env->FromReflectedMethod(_get_method.handle());
    _batch_get_method_id = env->FromReflectedMethod(_batch_get_method.handle());
    _add_method_id = env->FromReflectedMethod(_add_method.handle());
    _remove_method_id = env->FromReflectedMethod(_remove_method.handle());
    _clear_method_id = env->FromReflectedMethod(_clear_method.handle());
}

jobject UDAFStateList::get_state(FunctionContext* ctx, JNIEnv* env, int state_handle) {
    auto obj = env->CallObjectMethod(_handle.handle(), _get_method_id, state_handle);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return obj;
}

jobject UDAFStateList::get_state(FunctionContext* ctx, JNIEnv* env, jobject state_ids) {
    auto obj = env->CallObjectMethod(_handle.handle(), _batch_get_method_id, state_ids);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return obj;
}

int UDAFStateList::add_state(FunctionContext* ctx, JNIEnv* env, jobject state) {
    auto res = env->CallIntMethod(_handle.handle(), _add_method_id, state);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return res;
}

void UDAFStateList::remove(FunctionContext* ctx, JNIEnv* env, int state_handle) {
    env->CallVoidMethod(_handle.handle(), _remove_method_id, state_handle);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

void UDAFStateList::clear(FunctionContext* ctx, JNIEnv* env) {
    env->CallVoidMethod(_handle.handle(), _clear_method_id);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

JavaUDFContext::~JavaUDFContext() = default;
JavaUDAFSharedContext::~JavaUDAFSharedContext() = default;
JavaUDAFUniqueContext::~JavaUDAFUniqueContext() = default;

int UDAFFunction::create() {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    jmethodID create = _ctx->ctx->create->get_method_id();
    auto obj = env->CallObjectMethod(_udaf_handle, create);
    LOCAL_REF_GUARD(obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return _ctx->states->add_state(_function_context, env, obj);
}

void UDAFFunction::destroy(int state) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID destory = _ctx->ctx->destory->get_method_id();
    // call destroy
    env->CallVoidMethod(_udaf_handle, destory, obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    _ctx->states->remove(_function_context, env, state);
}

jvalue UDAFFunction::finalize(int state) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID finalize = _ctx->ctx->finalize->get_method_id();
    jvalue res;
    res.l = env->CallObjectMethod(_udaf_handle, finalize, obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return res;
}

void AggBatchCallStub::batch_update_single(int num_rows, jobject state, jobject* input, int cols) {
    jvalue jni_inputs[3 + cols];
    jni_inputs[0].i = num_rows;
    jni_inputs[1].l = _caller;
    jni_inputs[2].l = state;
    for (int i = 0; i < cols; ++i) {
        jni_inputs[3 + i].l = input[i];
    }
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    env->CallStaticVoidMethodA(_stub_clazz.clazz(), env->FromReflectedMethod(_stub_method.handle()), jni_inputs);
    CHECK_UDF_CALL_EXCEPTION(env, _ctx);
}

StatusOr<jobject> BatchEvaluateStub::batch_evaluate(int num_rows, jobject* input, int cols) {
    jvalue jni_inputs[2 + cols];
    jni_inputs[0].i = num_rows;
    jni_inputs[1].l = _caller;
    for (int i = 0; i < cols; ++i) {
        jni_inputs[2 + i].l = input[i];
    }
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethodA(_stub_clazz.clazz(), env->FromReflectedMethod(_stub_method.handle()),
                                            jni_inputs);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

void UDAFFunction::update(jvalue* val) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    jmethodID update = _ctx->ctx->update->get_method_id();
    env->CallVoidMethodA(_udaf_handle, update, val);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

void UDAFFunction::merge(int state, jobject buffer) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID merge = _ctx->ctx->merge->get_method_id();
    env->CallVoidMethod(_udaf_handle, merge, obj, buffer);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

void UDAFFunction::serialize(int state, jobject buffer) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID serialize = _ctx->ctx->serialize->get_method_id();
    env->CallVoidMethod(_udaf_handle, serialize, obj, buffer);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

int UDAFFunction::serialize_size(int state) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID serialize_size = _ctx->ctx->serialize_size->get_method_id();
    int sz = env->CallIntMethod(obj, serialize_size);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return sz;
}

void UDAFFunction::reset(int state) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID reset = _ctx->ctx->reset->get_method_id();
    env->CallVoidMethod(_udaf_handle, reset, obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

jobject UDAFFunction::window_update_batch(int state, int peer_group_start, int peer_group_end, int frame_start,
                                          int frame_end, int col_sz, jobject* cols) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);

    jmethodID window_update = _ctx->ctx->window_update->get_method_id();
    jvalue jvalues[5 + col_sz];
    jvalues[0].l = obj;
    jvalues[1].j = peer_group_start;
    jvalues[2].j = peer_group_end;
    jvalues[3].j = frame_start;
    jvalues[4].j = frame_end;

    for (int i = 0; i < col_sz; ++i) {
        jvalues[i + 5].l = cols[i];
    }

    jobject res = env->CallObjectMethodA(_udaf_handle, window_update, jvalues);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return res;
}

} // namespace starrocks
