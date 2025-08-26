#pragma once

#include "common/status.h"
#include "jni.h"
#include "udf/java/java_udf.h"
#include "util/metrics.h"

namespace starrocks {

struct MemoryUsage {
    MemoryUsage(int64_t init, int64_t used, int64_t committed, int64_t max)
        : init(init), used(used), committed(committed), max(max) {}

    int64_t init;
    int64_t used;
    int64_t committed;
    int64_t max;
};

struct MemoryPool {
    MemoryPool(std::string name, MemoryUsage usage, MemoryUsage peak_usage)
        : name(std::move(name)), usage(usage), peak_usage(peak_usage) {}

    std::string name;
    MemoryUsage usage;
    MemoryUsage peak_usage;
};

class JVMMetrics {
public:
    JVMMetrics() = default;
    ~JVMMetrics() = default;

    METRIC_DEFINE_INT_GAUGE(jvm_heap_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_heap_committed_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_heap_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_nonheap_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_nonheap_committed_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_young_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_young_committed_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_young_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_young_peak_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_young_peak_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_old_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_old_committed_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_old_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_old_peak_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_old_peak_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_survivor_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_survivor_committed_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_survivor_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_survivor_peak_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_survivor_peak_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_perm_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_perm_committed_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_perm_max_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_perm_peak_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jvm_perm_peak_max_bytes, MetricUnit::BYTES);

    Status init();
    void install(MetricRegistry*);
    Status update();
    StatusOr<MemoryUsage> get_heap_memory_usage();
    StatusOr<MemoryUsage> get_non_heap_memory_usage();
    StatusOr<std::vector<MemoryPool>> get_memory_pools();

private:
    std::unique_ptr<JVMClass> _management_factory_cls = nullptr;
    JavaGlobalRef _memory_mxbean_obj = nullptr;

    jmethodID _get_memory_mxbean = nullptr;
    jmethodID _get_heap_memory_usage = nullptr;
    jmethodID _get_non_heap_memory_usage = nullptr;
    jmethodID _get_init = nullptr;
    jmethodID _get_used = nullptr;
    jmethodID _get_committed = nullptr;
    jmethodID _get_max = nullptr;
    jmethodID _get_memory_pool_mxbeans = nullptr;

    jmethodID _get_usage = nullptr;
    jmethodID _get_peak_usage = nullptr;
    jmethodID _get_name = nullptr;

};

} // namespace starrocks
