//
// Created by femi on 2025/5/14.
//

#include "profile_manager.h"

namespace starrocks::pipeline {
RuntimeProfile* profile_manager::build_merged_instance_profile(FragmentProfileMaterial& fragment_profile_material) {
    ObjectPool obj_pool;
    RuntimeProfile* new_instance_profile = nullptr;
    RuntimeProfile* instance_profile = fragment_profile_material.query_profile.get();
    if (fragment_profile_material.profile_level >= TPipelineProfileLevel::type::DETAIL) {
        new_instance_profile = fragment_profile_material.query_profile.get();
    } else {
        int64_t process_raw_timer = 0;
        DeferOp defer([&new_instance_profile, &process_raw_timer]() {
            if (new_instance_profile != nullptr) {
                auto* process_timer = ADD_TIMER(new_instance_profile, "BackendProfileMergeTime");
                COUNTER_SET(process_timer, process_raw_timer);
            }
        });

        SCOPED_RAW_TIMER(&process_raw_timer);
        std::vector<RuntimeProfile*> pipeline_profiles;
        instance_profile->get_children(&pipeline_profiles);

        std::vector<RuntimeProfile*> merged_driver_profiles;
        for (auto* pipeline_profile : pipeline_profiles) {
            std::vector<RuntimeProfile*> driver_profiles;
            pipeline_profile->get_children(&driver_profiles);

            if (driver_profiles.empty()) {
                continue;
            }

            auto* merged_driver_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool, driver_profiles);

            // use the name of pipeline' profile as pipeline driver's
            merged_driver_profile->set_name(pipeline_profile->name());

            // add all the info string and counters of the pipeline's profile
            // to the pipeline driver's profile
            merged_driver_profile->copy_all_info_strings_from(pipeline_profile);
            merged_driver_profile->copy_all_counters_from(pipeline_profile);

            merged_driver_profiles.push_back(merged_driver_profile);
        }

        new_instance_profile = obj_pool.add(new RuntimeProfile(instance_profile->name()));
        new_instance_profile->copy_all_info_strings_from(instance_profile);
        new_instance_profile->copy_all_counters_from(instance_profile);
        for (auto* merged_driver_profile : merged_driver_profiles) {
            merged_driver_profile->reset_parent();
            new_instance_profile->add_child(merged_driver_profile, true, nullptr);
        }
    }

    // Add counters for query level memory and cpu usage, these two metrics will be specially handled at the frontend
    auto* query_peak_memory = new_instance_profile->add_counter(
            "QueryPeakMemoryUsage", TUnit::BYTES,
            RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
    query_peak_memory->set(fragment_profile_material.mem_cost_bytes);
    auto* query_cumulative_cpu = new_instance_profile->add_counter(
            "QueryCumulativeCpuTime", TUnit::TIME_NS,
            RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
    query_cumulative_cpu->set(fragment_profile_material.total_cpu_cost_ns);
    auto* query_spill_bytes = new_instance_profile->add_counter(
            "QuerySpillBytes", TUnit::BYTES,
            RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
    query_spill_bytes->set(fragment_profile_material.total_spill_bytes);
    // Add execution wall time
    auto* query_exec_wall_time = new_instance_profile->add_counter(
            "QueryExecutionWallTime", TUnit::TIME_NS,
            RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
    query_exec_wall_time->set(fragment_profile_material.lifetime);
}

} // namespace starrocks::pipeline