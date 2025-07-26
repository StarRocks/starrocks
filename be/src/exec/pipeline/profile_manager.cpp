//
// Created by femi on 2025/5/14.
//

#include "profile_manager.h"

#include <utility>

#include "runtime/client_cache.h"
#include "util/starrocks_metrics.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::pipeline {
RuntimeProfile* ProfileManager::build_merged_instance_profile(
        const std::shared_ptr<FragmentProfileMaterial>& fragment_profile_material, ObjectPool* obj_pool) {
    RuntimeProfile* new_instance_profile = nullptr;
    RuntimeProfile* instance_profile = fragment_profile_material->_instance_profile.get();
    if (fragment_profile_material->_profile_level >= TPipelineProfileLevel::type::DETAIL) {
        new_instance_profile = fragment_profile_material->_instance_profile.get();
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
    query_peak_memory->set(fragment_profile_material->_mem_cost_bytes);
    auto* query_cumulative_cpu = new_instance_profile->add_counter(
            "QueryCumulativeCpuTime", TUnit::TIME_NS,
            RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
    query_cumulative_cpu->set(fragment_profile_material->_total_cpu_cost_ns);
    auto* query_spill_bytes = new_instance_profile->add_counter(
            "QuerySpillBytes", TUnit::BYTES,
            RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
    query_spill_bytes->set(fragment_profile_material->_total_spill_bytes);
    // Add execution wall time
    auto* query_exec_wall_time = new_instance_profile->add_counter(
            "QueryExecutionWallTime", TUnit::TIME_NS,
            RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
    query_exec_wall_time->set(fragment_profile_material->_lifetime);

    return new_instance_profile;
}

ProfileManager::ProfileManager() {
    int max_reporter = config::async_profile_report_thread_max_num == 0 ? CpuInfo::num_cores() / 2
                                                                        : config::async_profile_report_thread_max_num;

    auto status = ThreadPoolBuilder("query_profile_report")
                          .set_min_threads(1)
                          .set_max_threads(max_reporter)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                          .build(&_report_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for profile_manager's query_profile_merge: error="
                   << status.to_string();
    }

    REGISTER_THREAD_POOL_METRICS(async_profile_merge, _report_thread_pool);

    int max_merger = config::async_profile_merge_thread_max_num == 0 ? CpuInfo::num_cores() / 2
                                                                     : config::async_profile_merge_thread_max_num;
    status = ThreadPoolBuilder("query_profile_merge")
                     .set_min_threads(1)
                     .set_max_threads(max_merger)
                     .set_max_queue_size(1000)
                     .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                     .build(&_merge_thread_pool);
    if (!status.ok()) {
        LOG(FATAL) << "Cannot create thread pool for query_profile_merge's query_profile_report: error="
                   << status.to_string();
    }

    REGISTER_THREAD_POOL_METRICS(async_profile_report, _merge_thread_pool);

    _mem_tracker = GlobalEnv::GetInstance()->profile_mem_tracker();
}

void ProfileManager::build_and_report_profile(std::shared_ptr<FragmentProfileMaterial> fragment_profile_material) {
    auto profile_merge_task = [=, fragment_profile_material = std::move(fragment_profile_material)]() {
        SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

        ObjectPool obj_pool;
        RuntimeProfile* merged_instance_profile = build_merged_instance_profile(fragment_profile_material, &obj_pool);
        std::shared_ptr<TFragmentProfile> params =
                create_report_profile_params(fragment_profile_material, merged_instance_profile);
        auto profile_report_task = [params, fragment_profile_material, this]() {
            SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
            const auto& fe_addr = fragment_profile_material->_fe_addr;
            int max_retry_times = config::report_exec_rpc_request_retry_num;
            int retry_times = 0;
            while (retry_times++ < max_retry_times) {
                TStatus t_res;
                Status rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                        fe_addr.hostname, fe_addr.port,
                        [&](FrontendServiceConnection& client) { client->asyncProfileReport(t_res, *params); });
                Status res = Status(t_res);
                if (!rpc_status.ok() || !res.ok()) {
                    if (res.is_not_found()) {
                        VLOG(1) << "[Driver] Fail to report profile due to query not found";
                    } else {
                        LOG(WARNING) << "profile report failed once, return res:" << res.to_string()
                                     << ", rpc error:" << rpc_status.to_string()
                                     << " fragment_instance_id=" << print_id(fragment_profile_material->_instance_id)
                                     << " queryId=" << print_id(fragment_profile_material->_query_id);
                        // if it is done exec state report, we should retry
                        if (params->__isset.done && params->done) {
                            continue;
                        }
                    }
                }
                break;
            }
        };

        Status submit_report_status = _report_thread_pool->submit_func(std::move(profile_report_task));
        if (!submit_report_status.ok()) {
            profile_report_task();
        }
    };

    Status status = _merge_thread_pool->submit_func(profile_merge_task);
    if (!status.ok()) {
        LOG(WARNING) << "Cannot submit merge-profile task: " << status.to_string();
        profile_merge_task();
    }
}

std::unique_ptr<TFragmentProfile> ProfileManager::create_report_profile_params(
        const std::shared_ptr<FragmentProfileMaterial>& fragment_profile_material,
        RuntimeProfile* merged_instance_profile) {
    auto res = std::make_unique<TFragmentProfile>();
    TFragmentProfile& params = *res;
    params.__set_query_id(fragment_profile_material->_query_id);
    params.__set_fragment_instance_id(fragment_profile_material->_instance_id);
    params.__set_done(fragment_profile_material->_instance_is_done);

    merged_instance_profile->to_thrift(&params.profile);
    params.__isset.profile = true;

    if (fragment_profile_material->_query_type == TQueryType::LOAD) {
        // Load channel profile will be merged on FE
        fragment_profile_material->_load_channel_profile->to_thrift(&params.load_channel_profile);
        params.__isset.load_channel_profile = true;
    }

    return res;
}

} // namespace starrocks::pipeline