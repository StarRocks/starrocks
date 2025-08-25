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
    }                                                                                              \

namespace starrocks {

Status JVMMetrics::init() {
    RETURN_IF_ERROR(detect_java_runtime());

    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();

    jclass management_factory_cls = env->FindClass("java/lang/management/ManagementFactory");
    DCHECK(management_factory_cls != nullptr);
    _management_factory_cls = std::make_unique<JVMClass>(env->NewGlobalRef(management_factory_cls));
    LOCAL_REF_GUARD_ENV(env, management_factory_cls);

    jclass memory_mxbean_cls = env->FindClass("java/lang/management/MemoryMXBean");
    DCHECK(memory_mxbean_cls != nullptr);
    LOCAL_REF_GUARD_ENV(env, memory_mxbean_cls);

    _get_memory_mxbean = env->GetStaticMethodID(management_factory_cls, "getMemoryMXBean", "()Ljava/lang/management/MemoryMXBean;");
    DCHECK(_get_memory_mxbean != nullptr);
    jobject memory_mxbean_obj = env->CallStaticObjectMethod(management_factory_cls, _get_memory_mxbean);
    CHECK_JNI_EXCEPTION(env, "get memory mxbean failed");
    _memory_mxbean_obj = JavaGlobalRef(env->NewGlobalRef(memory_mxbean_obj));
    LOCAL_REF_GUARD_ENV(env, memory_mxbean_obj);

    _get_heap_memory_usage = env->GetMethodID(memory_mxbean_cls, "getHeapMemoryUsage", "()Ljava/lang/management/MemoryUsage;");
    DCHECK(_get_heap_memory_usage != nullptr);
    _get_non_heap_memory_usage = env->GetMethodID(memory_mxbean_cls, "getNonHeapMemoryUsage", "()Ljava/lang/management/MemoryUsage;");
    DCHECK(_get_non_heap_memory_usage != nullptr);
    
    jclass memory_usage_cls = env->FindClass("java/lang/management/MemoryUsage");
    DCHECK(memory_usage_cls != nullptr);
    LOCAL_REF_GUARD_ENV(env, memory_usage_cls);
    _get_init = env->GetMethodID(memory_usage_cls, "getInit", "()J");
    DCHECK(_get_init != nullptr);
    _get_used = env->GetMethodID(memory_usage_cls, "getUsed", "()J");
    DCHECK(_get_used != nullptr);
    _get_committed = env->GetMethodID(memory_usage_cls, "getCommitted", "()J");
    DCHECK(_get_committed != nullptr);
    _get_max = env->GetMethodID(memory_usage_cls, "getMax", "()J");
    DCHECK(_get_max != nullptr);

    _get_memory_pool_mxbeans = env->GetStaticMethodID(management_factory_cls, "getMemoryPoolMXBeans", "()Ljava/util/List;");
    DCHECK(_get_memory_pool_mxbeans != nullptr);

    jclass memory_pool_mxbean_cls = env->FindClass("java/lang/management/MemoryPoolMXBean");
    DCHECK(memory_pool_mxbean_cls != nullptr);
    LOCAL_REF_GUARD_ENV(env, memory_pool_mxbean_cls);
    _get_usage = env->GetMethodID(memory_pool_mxbean_cls, "getUsage", "()Ljava/lang/management/MemoryUsage;");
    DCHECK(_get_usage != nullptr);
    _get_peak_usage = env->GetMethodID(memory_pool_mxbean_cls, "getPeakUsage", "()Ljava/lang/management/MemoryUsage;");
    DCHECK(_get_peak_usage != nullptr);
    _get_name = env->GetMethodID(memory_pool_mxbean_cls, "getName", "()Ljava/lang/String;");
    DCHECK(_get_name != nullptr);

    return Status::OK();
}

void JVMMetrics::install(MetricRegistry* registry) {
    if (!registry->register_hook("jvm_metrics", [this] { 
            if (!update().ok()) {
                LOG(WARNING) << "update jvm metrics failed: " << update().to_string();
            }; 
        })) {
        return;
    }

#define REGISTER_JVM_METRIC(name) \
    registry->register_metric("jvm_" #name "_bytes", &jvm_##name##_bytes);

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
};

Status JVMMetrics::update(){
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
    jvm_##name##_peak_max_bytes.set_value(peak_max);

        if (name == "Eden Space" || name == "PS Eden Space" || name == "Par Eden Space" || name == "G1 Eden Space") {
            UPDATE_METRIC(young, usage.used, usage.committed, usage.max, peak_usage.used, peak_usage.max);
        } else if (name == "Tenured Gen" || name == "PS Old Gen" || name == "CMS Old Gen" || name == "G1 Old Gen") {
            UPDATE_METRIC(old, usage.used, usage.committed, usage.max, peak_usage.used, peak_usage.max);
        } else if (name == "Survivor Space" || name == "PS Survivor Space" || name == "Par Survivor Space" || name == "G1 Survivor Space") {
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
    CHECK_JNI_EXCEPTION(env, "get heap memory usage failed");
    LOCAL_REF_GUARD_ENV(env, memory_usage);

    jlong init = env->CallLongMethod(memory_usage, _get_init);
    jlong used = env->CallLongMethod(memory_usage, _get_used);
    jlong committed = env->CallLongMethod(memory_usage, _get_committed);
    jlong max = env->CallLongMethod(memory_usage, _get_max);
    CHECK_JNI_EXCEPTION(env, "get heap memory usage details failed");

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
    CHECK_JNI_EXCEPTION(env, "get non heap memory usage details failed");

    return MemoryUsage(init, used, committed, max);
}

StatusOr<std::vector<MemoryPool>> JVMMetrics::get_memory_pools() {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();

    jobject memory_pools = env->CallStaticObjectMethod(_management_factory_cls->clazz(), _get_memory_pool_mxbeans);
    CHECK_JNI_EXCEPTION(env, "get memory pool mxbeans failed");
    LOCAL_REF_GUARD_ENV(env, memory_pools);

    JavaListStub list_stub(memory_pools);
    ASSIGN_OR_RETURN(auto size, list_stub.size());

    std::vector<MemoryPool> pools;
    pools.reserve(size);
    for (int i = 0; i < size; i ++) {
        ASSIGN_OR_RETURN(auto memory_pool, list_stub.get(i));
        LOCAL_REF_GUARD_ENV(env, memory_pool);

        jstring name = (jstring)env->CallObjectMethod(memory_pool, _get_name);
        CHECK_JNI_EXCEPTION(env, "get memory pool name failed");
        LOCAL_REF_GUARD_ENV(env, name);
        std::string name_str = JVMFunctionHelper::getInstance().to_cxx_string(name);

        jobject usage = env->CallObjectMethod(memory_pool, _get_usage);
        CHECK_JNI_EXCEPTION(env, "get memory pool usage failed");
        LOCAL_REF_GUARD_ENV(env, usage);
        jlong init = env->CallLongMethod(usage, _get_init);
        jlong used = env->CallLongMethod(usage, _get_used);
        jlong committed = env->CallLongMethod(usage, _get_committed);
        jlong max = env->CallLongMethod(usage, _get_max);
        CHECK_JNI_EXCEPTION(env, "get memory pool usage details failed");
        MemoryUsage usage_obj(init, used, committed, max);

        jobject peak_usage = env->CallObjectMethod(memory_pool, _get_peak_usage);
        CHECK_JNI_EXCEPTION(env, "get memory pool peak usage failed");
        LOCAL_REF_GUARD_ENV(env, peak_usage);
        used = env->CallLongMethod(peak_usage, _get_used);
        max = env->CallLongMethod(peak_usage, _get_max);
        CHECK_JNI_EXCEPTION(env, "get memory pool peak usage details failed");
        MemoryUsage peak_usage_obj(0, used, 0, max);

        pools.emplace_back(std::move(name_str), usage_obj, peak_usage_obj);
    }

    return pools;
}

} // namespace starrocks
