//
// Created by femi on 2025/5/14.
//

#pragma once
#include <memory>

#include "pipeline.h"

namespace starrocks {
class RuntimeProfile;
}
namespace starrocks::pipeline {

struct FragmentProfileMaterial {
    std::shared_ptr<RuntimeProfile> load_channel_profile;
    std::shared_ptr<RuntimeProfile> query_profile;
    TPipelineProfileLevel::type profile_level;

    int64_t mem_cost_bytes;
    int64_t total_cpu_cost_ns;
    int64_t total_spill_bytes;
    int64_t lifetime;
};

class profile_manager {
public:
    explicit profile_manager(const CpuUtil::CpuIds& cpuids);
    void submit(std::function<void()>&& report_task);
    static RuntimeProfile* build_merged_instance_profile(FragmentProfileMaterial& fragment_profile);

private:
    std::unique_ptr<ThreadPool> _merge_thread_pool;
    std::unique_ptr<ThreadPool> _report_thread_pool;
};

} // namespace starrocks::pipeline
