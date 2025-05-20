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
#include <memory>

#include "pipeline.h"

namespace starrocks {
class RuntimeProfile;
}
namespace starrocks::pipeline {

struct FragmentProfileMaterial {
    std::shared_ptr<RuntimeProfile> instance_profile;
    std::shared_ptr<RuntimeProfile> load_channel_profile;
    TPipelineProfileLevel::type profile_level;

    int64_t mem_cost_bytes;
    int64_t total_cpu_cost_ns;
    int64_t total_spill_bytes;
    int64_t lifetime;

    bool instance_is_done;
    TUniqueId query_id;
    int be_number;
    TQueryType::type query_type;
    TNetworkAddress fe_addr;

    FragmentProfileMaterial(std::shared_ptr<RuntimeProfile> instance_profile,
                            std::shared_ptr<RuntimeProfile> load_channel_profile,
                            TPipelineProfileLevel::type profile_level, int64_t mem_cost_bytes,
                            int64_t total_cpu_cost_ns, int64_t total_spill_bytes, int64_t lifetime,
                            bool instance_is_done, TUniqueId query_id, int be_number, TQueryType::type query_type,
                            TNetworkAddress fe_addr)
            : instance_profile(std::move(instance_profile)),
              load_channel_profile(std::move(load_channel_profile)),
              profile_level(profile_level),
              mem_cost_bytes(mem_cost_bytes),
              total_cpu_cost_ns(total_cpu_cost_ns),
              total_spill_bytes(total_spill_bytes),
              lifetime(lifetime),
              instance_is_done(instance_is_done),
              query_id(query_id),
              be_number(be_number),
              query_type(query_type),
              fe_addr(fe_addr) {}
};

class ProfileManager {
public:
    explicit ProfileManager(const CpuUtil::CpuIds& cpuids);
    void build_and_report_profile(std::shared_ptr<FragmentProfileMaterial> fragment_profile_material);

private:
    static RuntimeProfile* build_merged_instance_profile(
            std::shared_ptr<FragmentProfileMaterial> fragment_profile_material, ObjectPool* obj_pool);

    static std::unique_ptr<TFragmentProfile> create_report_profile_params(
            std::shared_ptr<FragmentProfileMaterial> fragment_profile_material,
            RuntimeProfile* merged_instance_profile);

    std::unique_ptr<ThreadPool> _merge_thread_pool;
    std::unique_ptr<ThreadPool> _report_thread_pool;
};

} // namespace starrocks::pipeline
