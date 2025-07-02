//
// Created by femi on 2025/5/14.
//

#include "profile_manager.h"

#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::pipeline {
RuntimeProfile* profile_manager::build_merged_instance_profile(FragmentProfileMaterial& fragment_profile_material,
                                                               ObjectPool* obj_pool) {
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

        new_instance_profile = obj_pool->add(new RuntimeProfile(instance_profile->name()));
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

profile_manager::profile_manager(const CpuUtil::CpuIds& cpuids) {
    auto status = ThreadPoolBuilder("query_profile_merge") // exec state reporter
                          .set_min_threads(1)
                          .set_max_threads(2)
                          .set_max_queue_size(1000)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                          .set_cpuids(cpuids)
                          .build(&_report_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for profile_manager's query_profile_merge: error="
                   << status.to_string();
    }

    status = ThreadPoolBuilder("query_profile_report") // priority exec state reporter with infinite queue
                     .set_min_threads(1)
                     .set_max_threads(2)
                     .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                     .set_cpuids(cpuids)
                     .build(&_merge_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for query_profile_merge's query_profile_report: error="
                   << status.to_string();
    }
}

Status profile_manager::build_and_report_profile(std::shared_ptr<FragmentProfileMaterial> fragment_profile_material) {
    auto profile_task = [=]() {
        ObjectPool obj_pool;
        RuntimeProfile* merged_instance_profile = build_merged_instance_profile(*fragment_profile_material);
        std::shared_ptr<TFragmentProfile> params =
                create_report_profile_params(fragment_profile_material, merged_instance_profile);
        auto report_task = [params]() {
            const auto& fe_addr = fragment_profile_material->fe_addr;
            int max_retry_times = config::report_exec_rpc_request_retry_num;
            while (retry_times++ < max_retry_times) {
                Status rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                        fe_addr.hostname, fe_addr.port,
                        [&](FrontendServiceConnection& client) { client->asyncProfileReport(status, *params); });
                if (!rpc_status.ok()) {
                    if (rpc_status.is_not_found()) {
                        VLOG(1) << "[Driver] Fail to report profile due to query not found";
                    } else {
                        // if it is done exec state report, we should retry
                        if (params->__isset.done && params->done) {
                            continue;
                        }
                    }
                }
                break;
            }
        };

        _report_thread_pool->submit_func(std::move(report_task));
    };

    Status status = _merge_thread_pool->submit_func(std::move(profile_task));
    if (!status.ok()) {
        profile_task();
    }
}

static std::unique_ptr<TFragmentProfile> profile_manager::create_report_profile_params(
        std::shared_ptr<FragmentProfileMaterial> fragment_profile_material, RuntimeProfile* merged_instance_profile) {
    auto res = std::make_unique<TFragmentProfile>();
    TFragmentProfile& params = *res;
    params.__set_query_id(fragment_profile_material.query_id);
    params.__set_backend_num(fragment_profile_material.be_number);
    params.__set_done(fragment_profile_material.instance_is_done);

    merged_instance_profile->to_thrift(&params.profile);
    params.__isset.profile = true;

    if (fragment_profile_material.query_type == TQueryType::LOAD) {
        fragment_profile_material.load_channel_profile->to_thrift(&params.load_channel_profile);
        params.__isset.load_channel_profile = true;
    }

    return res;
}

} // namespace starrocks::pipeline