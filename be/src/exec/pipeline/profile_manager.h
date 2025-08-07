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
    std::shared_ptr<RuntimeProfile> _instance_profile;
    // pipeline profile/driver profile/operator profile
    std::vector<std::shared_ptr<RuntimeProfile>> _profiles_holders;
    std::shared_ptr<RuntimeProfile> _load_channel_profile;
    TPipelineProfileLevel::type _profile_level;

    int64_t _mem_cost_bytes;
    int64_t _total_cpu_cost_ns;
    int64_t _total_spill_bytes;
    int64_t _lifetime;

    bool _instance_is_done;
    TUniqueId _query_id;
    TUniqueId _instance_id;
    TQueryType::type _query_type;
    TNetworkAddress _fe_addr;

    FragmentProfileMaterial(std::shared_ptr<RuntimeProfile> instance_profile,
                            std::shared_ptr<RuntimeProfile> load_channel_profile,
                            TPipelineProfileLevel::type profile_level, int64_t mem_cost_bytes,
                            int64_t total_cpu_cost_ns, int64_t total_spill_bytes, int64_t lifetime,
                            bool instance_is_done, const TUniqueId& query_id, const TUniqueId& instance_id,
                            TQueryType::type query_type, const TNetworkAddress& fe_addr)
            : _instance_profile(std::move(instance_profile)),
              _load_channel_profile(std::move(load_channel_profile)),
              _profile_level(profile_level),
              _mem_cost_bytes(mem_cost_bytes),
              _total_cpu_cost_ns(total_cpu_cost_ns),
              _total_spill_bytes(total_spill_bytes),
              _lifetime(lifetime),
              _instance_is_done(instance_is_done),
              _query_id(query_id),
              _instance_id(instance_id),
              _query_type(query_type),
              _fe_addr(fe_addr) {}
};

class ProfileManager {
public:
    explicit ProfileManager();
    void build_and_report_profile(std::shared_ptr<FragmentProfileMaterial> fragment_profile_material);

    static RuntimeProfile* build_merged_instance_profile(
            const std::shared_ptr<FragmentProfileMaterial>& fragment_profile_material, ObjectPool* obj_pool);

private:
    static std::unique_ptr<TFragmentProfile> create_report_profile_params(
            const std::shared_ptr<FragmentProfileMaterial>& fragment_profile_material,
            RuntimeProfile* merged_instance_profile);

    std::unique_ptr<ThreadPool> _merge_thread_pool;
    std::unique_ptr<ThreadPool> _report_thread_pool;
    MemTracker* _mem_tracker;
};

} // namespace starrocks::pipeline
