// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/fragment_context.h"
namespace starrocks::pipeline {

FragmentContext::~FragmentContext() {
    auto runtime_state_ptr = _runtime_state;
    _runtime_filter_hub.close_all_in_filters(runtime_state_ptr.get());
    close_all_pipelines();
    if (_plan != nullptr) {
        _plan->close(_runtime_state.get());
    }

    if (config::enable_pipeline_schedule_statistics) {
        std::stringstream os;
        bool has_thread_shedule_overhead = false;
        int thread_count = 0;
        int64_t process_times = 0;
        int64_t process_time_nanos = 0;
        for (int i = 0; i < _thread_shedule_time.size(); ++i) {
            process_times += _thread_shedule_frequency[i];
            process_time_nanos += _thread_shedule_time[i];

            auto milliseconds = ((double)_thread_shedule_time[i]) / 1000000;
            if (milliseconds >= config::pipeline_thread_schedule_threshold) {
                has_thread_shedule_overhead = true;
                if (thread_count > 0 && (thread_count % 3) == 0) {
                    os << "\n";
                }
                ++thread_count;
                os << "THREAD " << i << ": ";
                os << _thread_shedule_frequency[i] << "-" << milliseconds << "ms(" << (milliseconds / 1000) << "s) "
                   << ((milliseconds * 1000) / _thread_shedule_frequency[i]) << "us/frequency"
                   << "; ";
            }
        }

        if (has_thread_shedule_overhead) {
            LOG(INFO) << "[SCHEDULE OVERHEAD " << config::pipeline_thread_schedule_threshold << "ms] "
                      << "fragment_instance_id=" << print_id(fragment_instance_id()) << "\n"
                      << os.str() << "\n"
                      << "PROCESS_TIMES: " << process_times
                      << " PROCESS_TIME_NANOS: " << ((double)process_time_nanos / 1000000L) << "ms";
        }

        int64_t accumulate_chunks = 0;
        int64_t empty_moved_times = 0;
        int64_t schedule_times = 0;
        for (auto& driver : _drivers) {
            accumulate_chunks += driver->driver_acct().get_accumulated_chunk_moved();
            empty_moved_times += driver->driver_acct().get_empty_moved_times();
            schedule_times += driver->driver_acct().get_schedule_times();
        }
        LOG(INFO) << "[ACCUMULATED_CHUNK_MOVED] "
                  << "fragment_instance_id=" << print_id(fragment_instance_id()) << " NUMS: " << accumulate_chunks
                  << " EFFECTIVE_MOVED_TIMES: " << (schedule_times - empty_moved_times)
                  << " SCHEDULE_TIMES: " << schedule_times;
    }

    _drivers.clear();
}

FragmentContext* FragmentContextManager::get_or_register(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        return it->second.get();
    } else {
        auto&& ctx = std::make_unique<FragmentContext>();
        auto* raw_ctx = ctx.get();
        _fragment_contexts.emplace(fragment_id, std::move(ctx));
        return raw_ctx;
    }
}

void FragmentContextManager::register_ctx(const TUniqueId& fragment_id, FragmentContextPtr fragment_ctx) {
    std::lock_guard<std::mutex> lock(_lock);

    if (_fragment_contexts.find(fragment_id) != _fragment_contexts.end()) {
        return;
    }

    _fragment_contexts.emplace(fragment_id, std::move(fragment_ctx));
}

FragmentContextPtr FragmentContextManager::get(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void FragmentContextManager::unregister(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        it->second->_finish_promise.set_value();
        _fragment_contexts.erase(it);
    }
}

void FragmentContextManager::cancel(const Status& status) {
    std::lock_guard<std::mutex> lock(_lock);
    for (auto& _fragment_context : _fragment_contexts) {
        _fragment_context.second->cancel(status);
    }
}

} // namespace starrocks::pipeline
