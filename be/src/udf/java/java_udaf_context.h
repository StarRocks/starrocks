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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/function_context.h"
#include "jni.h"
#include "runtime/env/java/java_env.h"
#include "udf/java/java_call_stub.h"
#include "udf/java/java_function_loader.h"
#include "udf/java/jvm_function_helper.h"

namespace starrocks {

// UDAF State Lists
// mapping a java object as a int index
// use get method to
// TODO: implement a Java binder to avoid using this class
class UDAFStateList {
public:
    static inline const char* clazz_name = "com.starrocks.udf.FunctionStates";
    UDAFStateList(JavaGlobalRef&& handle, JavaGlobalRef&& get, JavaGlobalRef&& batch_get, JavaGlobalRef&& add,
                  JavaGlobalRef&& remove, JavaGlobalRef&& clear);

    jobject handle() { return _handle.handle(); }

    // get state with index state
    jobject get_state(FunctionContext* ctx, JNIEnv* env, int state);

    // batch get states
    jobject get_state(FunctionContext* ctx, JNIEnv* env, jobject state_ids);

    // add a state to StateList
    int add_state(FunctionContext* ctx, JNIEnv* env, jobject state);

    // remove a state from StateList
    void remove(FunctionContext* ctx, JNIEnv* env, int state);

    // clear all state in StateList
    void clear(FunctionContext* ctx, JNIEnv* env);

private:
    JavaGlobalRef _handle;
    JavaGlobalRef _get_method;
    JavaGlobalRef _batch_get_method;
    JavaGlobalRef _add_method;
    JavaGlobalRef _remove_method;
    JavaGlobalRef _clear_method;
    jmethodID _get_method_id;
    jmethodID _batch_get_method_id;
    jmethodID _add_method_id;
    jmethodID _remove_method_id;
    jmethodID _clear_method_id;
};

// Shareable, cacheable UDAF class-level context.
// Contains class references and method descriptors — no per-aggregation state.
// Cached via UserFunctionCache::load_cacheable_java_udf.
struct JavaUDAFSharedContext {
    std::unique_ptr<ClassLoader> udf_classloader;

    JVMClass udaf_class = nullptr;
    JVMClass udaf_state_class = nullptr;

    std::unique_ptr<JavaMethodDescriptor> create;
    std::unique_ptr<JavaMethodDescriptor> destory;
    std::unique_ptr<JavaMethodDescriptor> update;
    std::unique_ptr<JavaMethodDescriptor> merge;
    std::unique_ptr<JavaMethodDescriptor> finalize;
    std::unique_ptr<JavaMethodDescriptor> serialize;
    std::unique_ptr<JavaMethodDescriptor> serialize_size;

    // Window function methods (optional)
    std::unique_ptr<JavaMethodDescriptor> reset;
    std::unique_ptr<JavaMethodDescriptor> window_update;
    std::unique_ptr<JavaMethodDescriptor> get_values;

    // Generated stub class/method — used to create a per-aggregator AggBatchCallStub
    JVMClass update_stub_clazz = nullptr;
    JavaGlobalRef update_stub_method = nullptr;

    // FunctionStates method objects — looked up once, cloned per aggregator instance
    JavaGlobalRef states_get_method = nullptr;
    JavaGlobalRef states_batch_get_method = nullptr;
    JavaGlobalRef states_add_method = nullptr;
    JavaGlobalRef states_remove_method = nullptr;
    JavaGlobalRef states_clear_method = nullptr;

    // Per-argument UdfTypeDesc Java objects mirroring the SQL TypeDescriptor of each
    // UDAF arg (read by `update`). Slots whose type subtree contains no STRUCT are
    // null-handle. Built once at shared-context construction by walking
    // update.getGenericParameterTypes() (with state_offset=1 for the leading State arg).
    std::vector<JavaGlobalRef> update_arg_type_descs;
    // UdfTypeDesc for `finalize`'s return type when the subtree contains a STRUCT;
    // null-handle otherwise. Used by the UDAF finalize / batch_finalize paths to
    // route through the unified UDFHelper.writeResult drain.
    JavaGlobalRef finalize_return_type_desc = nullptr;
};

// Per-aggregator UDAF context stored as FunctionContext::THREAD_LOCAL.
// Holds a reference to the shared class-level JavaUDAFSharedContext plus
// mutable per-aggregation state.
struct JavaUDAFUniqueContext;

class UDAFFunction {
public:
    UDAFFunction(jobject udaf_handle, FunctionContext* function_ctx, JavaUDAFUniqueContext* ctx)
            : _udaf_handle(udaf_handle), _function_context(function_ctx), _ctx(ctx) {}
    // create a new state for UDAF
    int create();
    // destroy state
    void destroy(int state);
    // UDAF Update Function
    void update(jvalue* val);
    // UDAF merge
    void merge(int state, jobject buffer);
    void serialize(int state, jobject buffer);
    // UDAF State serialize_size
    int serialize_size(int state);
    // UDAF finalize
    jvalue finalize(int state);

    // WindowFunction reset
    void reset(int state);

    // WindowFunction updateBatch
    jobject window_update_batch(int state, int peer_group_start, int peer_group_end, int frame_start, int frame_end,
                                int col_sz, jobject* cols);

private:
    jobject _convert_to_jobject(int state);

    // not owned udaf function handle
    jobject _udaf_handle;
    FunctionContext* _function_context;
    JavaUDAFUniqueContext* _ctx;
};

struct JavaUDAFUniqueContext {
    // Shared, possibly cached class-level context
    std::shared_ptr<JavaUDAFSharedContext> ctx;

    // Per-aggregator UDAF object instance and its batch-update stub
    JavaGlobalRef handle = nullptr;
    std::unique_ptr<AggBatchCallStub> update_batch_call_stub;

    // Per-aggregator mutable state
    std::unique_ptr<UDAFStateList> states;
    std::unique_ptr<UDAFFunction> _func;
    std::unique_ptr<DirectByteBuffer> buffer;
    std::vector<uint8_t> buffer_data;
};

// Java UDAF lifecycle management based on FunctionContext::THREAD_LOCAL state.
JavaUDAFUniqueContext* get_java_udaf_context(FunctionContext* ctx);
void attach_java_udaf_context(FunctionContext* ctx, std::unique_ptr<JavaUDAFUniqueContext> udaf_ctx);
void clear_java_udaf_states(FunctionContext* ctx);
void destroy_java_udaf_context(FunctionContext* ctx);

} // namespace starrocks
