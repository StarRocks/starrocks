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

#include "util/jvm_metrics.h"

#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "jni.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

#define CHECK_JNI_EXCEPTION(env, message)                                                          \
    if (jthrowable thr = env->ExceptionOccurred(); thr) {                                          \
        std::string jni_error_message = JVMFunctionHelper::getInstance().dumpExceptionString(thr); \
        env->ExceptionDescribe();                                                                  \
        env->ExceptionClear();                                                                     \
        env->DeleteLocalRef(thr);                                                                  \
        return Status::InternalError(fmt::format("{}, error: {}", message, jni_error_message));    \
    }

namespace starrocks {

Status JVMMetrics::init() {
    RETURN_IF_ERROR(detect_java_runtime());

    // check JNIEnv before calling JVMFunctionHelper::getInstance() to avoid crash
    if (getJNIEnv() == nullptr) {
        return Status::InternalError("get JNIEnv failed");
    }

    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();

    jclass management_factory_cls = env->FindClass("java/lang/management/ManagementFactory");
    CHECK_JNI_EXCEPTION(env, "find class ManagementFactory failed")
    _management_factory_cls = std::make_unique<JVMClass>(env->NewGlobalRef(management_factory_cls));
    LOCAL_REF_GUARD_ENV(env, management_factory_cls);

    jclass memory_mxbean_cls = env->FindClass("java/lang/management/MemoryMXBean");
    CHECK_JNI_EXCEPTION(env, "find class MemoryMXBean failed")
    LOCAL_REF_GUARD_ENV(env, memory_mxbean_cls);

    _get_memory_mxbean =
            env->GetStaticMethodID(management_factory_cls, "getMemoryMXBean", "()Ljava/lang/management/MemoryMXBean;");
    CHECK_JNI_EXCEPTION(env, "get method getMemoryMXBean failed")
    jobject memory_mxbean_obj = env->CallStaticObjectMethod(management_factory_cls, _get_memory_mxbean);
    CHECK_JNI_EXCEPTION(env, "get memory mxbean failed")
    _memory_mxbean_obj = JavaGlobalRef(env->NewGlobalRef(memory_mxbean_obj));
    LOCAL_REF_GUARD_ENV(env, memory_mxbean_obj);

    _get_heap_memory_usage =
            env->GetMethodID(memory_mxbean_cls, "getHeapMemoryUsage", "()Ljava/lang/management/MemoryUsage;");
    CHECK_JNI_EXCEPTION(env, "get method getHeapMemoryUsage failed")
    _get_non_heap_memory_usage =
            env->GetMethodID(memory_mxbean_cls, "getNonHeapMemoryUsage", "()Ljava/lang/management/MemoryUsage;");
    CHECK_JNI_EXCEPTION(env, "get method getNonHeapMemoryUsage failed")

    jclass memory_usage_cls = env->FindClass("java/lang/management/MemoryUsage");
    CHECK_JNI_EXCEPTION(env, "find class MemoryUsage failed")
    LOCAL_REF_GUARD_ENV(env, memory_usage_cls);
    _get_init = env->GetMethodID(memory_usage_cls, "getInit", "()J");
    CHECK_JNI_EXCEPTION(env, "get method getInit failed")
    _get_used = env->GetMethodID(memory_usage_cls, "getUsed", "()J");
    CHECK_JNI_EXCEPTION(env, "get method getUsed failed")
    _get_committed = env->GetMethodID(memory_usage_cls, "getCommitted", "()J");
    CHECK_JNI_EXCEPTION(env, "get method getCommitted failed")
    _get_max = env->GetMethodID(memory_usage_cls, "getMax", "()J");
    CHECK_JNI_EXCEPTION(env, "get method getMax failed")

    _get_memory_pool_mxbeans =
            env->GetStaticMethodID(management_factory_cls, "getMemoryPoolMXBeans", "()Ljava/util/List;");
    CHECK_JNI_EXCEPTION(env, "get method getMemoryPoolMXBeans failed")

    jclass memory_pool_mxbean_cls = env->FindClass("java/lang/management/MemoryPoolMXBean");
    CHECK_JNI_EXCEPTION(env, "find class MemoryPoolMXBean failed")
    LOCAL_REF_GUARD_ENV(env, memory_pool_mxbean_cls);
    _get_usage = env->GetMethodID(memory_pool_mxbean_cls, "getUsage", "()Ljava/lang/management/MemoryUsage;");
    CHECK_JNI_EXCEPTION(env, "get method getUsage failed")
    _get_peak_usage = env->GetMethodID(memory_pool_mxbean_cls, "getPeakUsage", "()Ljava/lang/management/MemoryUsage;");
    CHECK_JNI_EXCEPTION(env, "get method getPeakUsage failed")
    _get_name = env->GetMethodID(memory_pool_mxbean_cls, "getName", "()Ljava/lang/String;");
    CHECK_JNI_EXCEPTION(env, "get method getName failed")

    return Status::OK();
}

void JVMMetrics::install(MetricRegistry* registry) {
    if (!registry->register_hook("jvm_metrics", [this] {
            auto status = update();
            if (!status.ok()) {
                LOG(WARNING) << "update jvm metrics failed: " << status.to_string();
            }
        })) {
        return;
    }

#define REGISTER_JVM_METRIC(name) registry->register_metric("jvm_" #name "_bytes", &jvm_##name##_bytes)

    REGISTER_JVM_METRIC(heap_used);
    REGISTER_JVM_METRIC(heap_committed);
    REGISTER_JVM_METRIC(heap_max);
    REGISTER_JVM_METRIC(nonheap_used);
    REGISTER_JVM_METRIC(nonheap_committed);
    REGISTER_JVM_METRIC(young_used);
    REGISTER_JVM_METRIC(young_committed);
    REGISTER_JVM_METRIC(young_max);
    REGISTER_JVM_METRIC(young_peak_used);
    REGISTER_JVM_METRIC(young_peak_max);
    REGISTER_JVM_METRIC(old_used);
    REGISTER_JVM_METRIC(old_committed);
    REGISTER_JVM_METRIC(old_max);
    REGISTER_JVM_METRIC(old_peak_used);
    REGISTER_JVM_METRIC(old_peak_max);
    REGISTER_JVM_METRIC(survivor_used);
    REGISTER_JVM_METRIC(survivor_committed);
    REGISTER_JVM_METRIC(survivor_max);
    REGISTER_JVM_METRIC(survivor_peak_used);
    REGISTER_JVM_METRIC(survivor_peak_max);
    REGISTER_JVM_METRIC(perm_used);
    REGISTER_JVM_METRIC(perm_committed);
    REGISTER_JVM_METRIC(perm_max);
    REGISTER_JVM_METRIC(perm_peak_used);
    REGISTER_JVM_METRIC(perm_peak_max);

#undef REGISTER_JVM_METRIC
}

Status JVMMetrics::update() {
    ASSIGN_OR_RETURN(auto heap_memory_usage, get_heap_memory_usage());
    jvm_heap_used_bytes.set_value(heap_memory_usage.used);
    jvm_heap_committed_bytes.set_value(heap_memory_usage.committed);
    jvm_heap_max_bytes.set_value(heap_memory_usage.max);

    ASSIGN_OR_RETURN(auto non_heap_memory_usage, get_non_heap_memory_usage());
    jvm_nonheap_used_bytes.set_value(non_heap_memory_usage.used);
    jvm_nonheap_committed_bytes.set_value(non_heap_memory_usage.committed);

    ASSIGN_OR_RETURN(auto memory_pools, get_memory_pools());
    for (auto& pool : memory_pools) {
        auto name = pool.name;
        auto usage = pool.usage;
        auto peak_usage = pool.peak_usage;

#define UPDATE_METRIC(name, used, committed, max, peak_used, peak_max) \
    jvm_##name##_used_bytes.set_value(used);                           \
    jvm_##name##_committed_bytes.set_value(committed);                 \
    jvm_##name##_max_bytes.set_value(max);                             \
    jvm_##name##_peak_used_bytes.set_value(peak_used);                 \
    jvm_##name##_peak_max_bytes.set_value(peak_max)

        if (name == "Eden Space" || name == "PS Eden Space" || name == "Par Eden Space" || name == "G1 Eden Space") {
            UPDATE_METRIC(young, usage.used, usage.committed, usage.max, peak_usage.used, peak_usage.max);
        } else if (name == "Tenured Gen" || name == "PS Old Gen" || name == "CMS Old Gen" || name == "G1 Old Gen") {
            UPDATE_METRIC(old, usage.used, usage.committed, usage.max, peak_usage.used, peak_usage.max);
        } else if (name == "Survivor Space" || name == "PS Survivor Space" || name == "Par Survivor Space" ||
                   name == "G1 Survivor Space") {
            UPDATE_METRIC(survivor, usage.used, usage.committed, usage.max, peak_usage.used, peak_usage.max);
        } else if (name == "Perm Gen" || name == "Metaspace") {
            UPDATE_METRIC(perm, usage.used, usage.committed, usage.max, peak_usage.used, peak_usage.max);
        }

#undef UPDATE_METRIC
    }

    return Status::OK();
}

StatusOr<MemoryUsage> JVMMetrics::get_heap_memory_usage() {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();

    jobject memory_usage = env->CallObjectMethod(_memory_mxbean_obj.handle(), _get_heap_memory_usage);
    CHECK_JNI_EXCEPTION(env, "get heap memory usage failed")
    LOCAL_REF_GUARD_ENV(env, memory_usage);

    jlong init = env->CallLongMethod(memory_usage, _get_init);
    jlong used = env->CallLongMethod(memory_usage, _get_used);
    jlong committed = env->CallLongMethod(memory_usage, _get_committed);
    jlong max = env->CallLongMethod(memory_usage, _get_max);
    CHECK_JNI_EXCEPTION(env, "get heap memory usage details failed")

    return MemoryUsage(init, used, committed, max);
}

StatusOr<MemoryUsage> JVMMetrics::get_non_heap_memory_usage() {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();

    jobject memory_usage = env->CallObjectMethod(_memory_mxbean_obj.handle(), _get_non_heap_memory_usage);
    CHECK_JNI_EXCEPTION(env, "get non heap memory usage failed")
    LOCAL_REF_GUARD_ENV(env, memory_usage);

    jlong init = env->CallLongMethod(memory_usage, _get_init);
    jlong used = env->CallLongMethod(memory_usage, _get_used);
    jlong committed = env->CallLongMethod(memory_usage, _get_committed);
    jlong max = env->CallLongMethod(memory_usage, _get_max);
    CHECK_JNI_EXCEPTION(env, "get non heap memory usage details failed")

    return MemoryUsage(init, used, committed, max);
}

StatusOr<std::vector<MemoryPool>> JVMMetrics::get_memory_pools() {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();

    jobject memory_pools = env->CallStaticObjectMethod(_management_factory_cls->clazz(), _get_memory_pool_mxbeans);
    CHECK_JNI_EXCEPTION(env, "get memory pool mxbeans failed")
    LOCAL_REF_GUARD_ENV(env, memory_pools);

    JavaListStub list_stub(memory_pools);
    ASSIGN_OR_RETURN(auto size, list_stub.size());

    std::vector<MemoryPool> pools;
    pools.reserve(size);
    for (int i = 0; i < size; i++) {
        ASSIGN_OR_RETURN(auto memory_pool, list_stub.get(i));
        LOCAL_REF_GUARD_ENV(env, memory_pool);

        jstring name = (jstring)env->CallObjectMethod(memory_pool, _get_name);
        CHECK_JNI_EXCEPTION(env, "get memory pool name failed")
        LOCAL_REF_GUARD_ENV(env, name);
        std::string name_str = JVMFunctionHelper::getInstance().to_cxx_string(name);

        jobject usage = env->CallObjectMethod(memory_pool, _get_usage);
        CHECK_JNI_EXCEPTION(env, "get memory pool usage failed")
        LOCAL_REF_GUARD_ENV(env, usage);
        jlong init = env->CallLongMethod(usage, _get_init);
        jlong used = env->CallLongMethod(usage, _get_used);
        jlong committed = env->CallLongMethod(usage, _get_committed);
        jlong max = env->CallLongMethod(usage, _get_max);
        CHECK_JNI_EXCEPTION(env, "get memory pool usage details failed")
        MemoryUsage usage_obj(init, used, committed, max);

        jobject peak_usage = env->CallObjectMethod(memory_pool, _get_peak_usage);
        CHECK_JNI_EXCEPTION(env, "get memory pool peak usage failed")
        LOCAL_REF_GUARD_ENV(env, peak_usage);
        used = env->CallLongMethod(peak_usage, _get_used);
        max = env->CallLongMethod(peak_usage, _get_max);
        CHECK_JNI_EXCEPTION(env, "get memory pool peak usage details failed")
        MemoryUsage peak_usage_obj(0, used, 0, max);

        pools.emplace_back(std::move(name_str), usage_obj, peak_usage_obj);
    }

    return pools;
}

} // namespace starrocks
