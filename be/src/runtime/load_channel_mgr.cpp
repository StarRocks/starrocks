// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel_mgr.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/load_channel_mgr.h"

#include <memory>

#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"
#include "storage/lru_cache.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"

namespace starrocks {

// Calculate the memory limit for a single load job.
static int64_t calc_job_max_load_memory(int64_t mem_limit_in_req, int64_t total_mem_limit) {
    // mem_limit_in_req == -1 means no limit for single load.
    // total_mem_limit according to load_process_max_memory_limit_percent calculation
    if (mem_limit_in_req <= 0) {
        return total_mem_limit;
    }
    return std::min<int64_t>(mem_limit_in_req, total_mem_limit);
}

static int64_t calc_job_timeout_s(int64_t timeout_in_req_s) {
    int64_t load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    if (timeout_in_req_s > 0) {
        load_channel_timeout_s = std::max<int64_t>(load_channel_timeout_s, timeout_in_req_s);
    }
    return load_channel_timeout_s;
}

LoadChannelMgr::LoadChannelMgr() : _is_stopped(false) {
    REGISTER_GAUGE_STARROCKS_METRIC(load_channel_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _load_channels.size();
    });
    _lastest_success_channel = new_lru_cache(1024);
}

LoadChannelMgr::~LoadChannelMgr() {
    _is_stopped.store(true);
    if (_load_channels_clean_thread.joinable()) {
        _load_channels_clean_thread.join();
    }
    delete _lastest_success_channel;
}

Status LoadChannelMgr::init(MemTracker* mem_tracker) {
    _mem_tracker = mem_tracker;
    RETURN_IF_ERROR(_start_bg_worker());
    return Status::OK();
}

Status LoadChannelMgr::open(const PTabletWriterOpenRequest& params) {
    UniqueId load_id(params.id());
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it != _load_channels.end()) {
            channel = it->second;
        } else {
            int64_t mem_limit_in_req = params.has_load_mem_limit() ? params.load_mem_limit() : -1;
            int64_t job_max_memory = calc_job_max_load_memory(mem_limit_in_req, _mem_tracker->limit());

            int64_t timeout_in_req_s = params.has_load_channel_timeout_s() ? params.load_channel_timeout_s() : -1;
            int64_t job_timeout_s = calc_job_timeout_s(timeout_in_req_s);
            auto job_mem_tracker = std::make_unique<MemTracker>(job_max_memory, load_id.to_string(), _mem_tracker);

            channel.reset(new LoadChannel(load_id, job_timeout_s, std::move(job_mem_tracker)));
            _load_channels.insert({load_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {}

Status LoadChannelMgr::add_chunk(const PTabletWriterAddChunkRequest& request,
                                 google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
                                 int64_t* wait_lock_time_ns) {
    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it == _load_channels.end()) {
            auto* handle = _lastest_success_channel->lookup(load_id.to_string());
            // success only when eos be true
            if (handle != nullptr) {
                _lastest_success_channel->release(handle);
                if (request.has_eos() && request.eos()) {
                    return Status::OK();
                }
            }
            return Status::InternalError(
                    strings::Substitute("fail to add batch in load channel. unknown load_id=$0", load_id.to_string()));
        }
        channel = it->second;
    }

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    RETURN_IF_ERROR(channel->add_chunk(request, tablet_vec));

    // 4. handle finish
    if (channel->is_finished()) {
        LOG(INFO) << "Removing finished load channel load id=" << load_id;
        {
            std::lock_guard<std::mutex> l(_lock);
            _load_channels.erase(load_id);
            auto* handle = _lastest_success_channel->insert(load_id.to_string(), nullptr, 1, dummy_deleter);
            _lastest_success_channel->release(handle);
        }
        VLOG(1) << "Removed load channel load id=" << load_id;
    }

    return Status::OK();
}

Status LoadChannelMgr::cancel(const PTabletWriterCancelRequest& params) {
    UniqueId load_id(params.id());
    std::shared_ptr<LoadChannel> cancelled_channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_load_channels.find(load_id) != _load_channels.end()) {
            cancelled_channel = _load_channels[load_id];
            _load_channels.erase(load_id);
        }
    }

    if (cancelled_channel != nullptr) {
        cancelled_channel->cancel();
        LOG(INFO) << "Cancelled load channel load id=" << load_id;
    }

    return Status::OK();
}

Status LoadChannelMgr::_start_bg_worker() {
    _load_channels_clean_thread = std::thread([this] {
#ifdef GOOGLE_PROFILER
        ProfilerRegisterThread();
#endif

#ifndef BE_TEST
        uint32_t interval = 60;
#else
        uint32_t interval = 1;
#endif
        while (!_is_stopped.load()) {
            _start_load_channels_clean();
            sleep(interval);
        }
    });
    return Status::OK();
}

Status LoadChannelMgr::_start_load_channels_clean() {
    std::vector<std::shared_ptr<LoadChannel>> timeout_channels;
    time_t now = time(nullptr);
    {
        std::lock_guard<std::mutex> l(_lock);
        VLOG(1) << "there are " << _load_channels.size() << " running load channels";
        for (auto it = _load_channels.begin(); it != _load_channels.end(); /**/) {
            time_t last_updated_time = it->second->last_updated_time();
            if (difftime(now, last_updated_time) >= it->second->timeout()) {
                timeout_channels.emplace_back(std::move(it->second));
                it = _load_channels.erase(it);
            } else {
                ++it;
            }
        }
    }

    // we must cancel these load channels before destroying them.
    // otherwise some object may be invalid before trying to visit it.
    // eg: MemTracker in load channel
    for (auto& channel : timeout_channels) {
        channel->cancel();
        LOG(INFO) << "Deleted timedout channel. load id=" << channel->load_id() << " timeout=" << channel->timeout();
    }

    // this log print every 1 min, so that we could observe the mem consumption of load process
    // on this Backend
    LOG(INFO) << "Memory consumption(bytes) limit=" << _mem_tracker->limit()
              << " current=" << _mem_tracker->consumption() << " peak=" << _mem_tracker->peak_consumption();

    return Status::OK();
}

} // namespace starrocks
