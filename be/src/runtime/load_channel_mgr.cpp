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

#include "common/closure_guard.h"
#include "gutil/strings/substitute.h"
#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"

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
}

LoadChannelMgr::~LoadChannelMgr() {
    _is_stopped.store(true);
    if (_load_channels_clean_thread.joinable()) {
        _load_channels_clean_thread.join();
    }
}

Status LoadChannelMgr::init(MemTracker* mem_tracker) {
    _mem_tracker = mem_tracker;
    RETURN_IF_ERROR(_start_bg_worker());
    return Status::OK();
}

void LoadChannelMgr::open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request,
                          PTabletWriterOpenResult* response, google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);
    UniqueId load_id(request.id());
    scoped_refptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it != _load_channels.end()) {
            channel = it->second;
        } else if (!_mem_tracker->limit_exceeded() || config::enable_new_load_on_memory_limit_exceeded) {
            int64_t mem_limit_in_req = request.has_load_mem_limit() ? request.load_mem_limit() : -1;
            int64_t job_max_memory = calc_job_max_load_memory(mem_limit_in_req, _mem_tracker->limit());

            int64_t timeout_in_req_s = request.has_load_channel_timeout_s() ? request.load_channel_timeout_s() : -1;
            int64_t job_timeout_s = calc_job_timeout_s(timeout_in_req_s);
            auto job_mem_tracker = std::make_unique<MemTracker>(job_max_memory, load_id.to_string(), _mem_tracker);

            channel.reset(new LoadChannel(this, load_id, job_timeout_s, std::move(job_mem_tracker)));
            _load_channels.insert({load_id, channel});
        } else {
            response->mutable_status()->set_status_code(TStatusCode::MEM_LIMIT_EXCEEDED);
            response->mutable_status()->add_error_msgs(
                    "memory limit exceeded, please reduce load frequency or increase config "
                    "`load_process_max_memory_limit_percent` or `load_process_max_memory_limit_bytes` "
                    "or add more BE nodes");
            return;
        }
    }
    channel->open(cntl, request, response, done_guard.release());
}

void LoadChannelMgr::add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                               PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) {
    VLOG(2) << "Current memory usage=" << _mem_tracker->consumption() << " limit=" << _mem_tracker->limit();
    ClosureGuard done_guard(done);
    UniqueId load_id(request.id());
    auto channel = _find_load_channel(load_id);
    if (channel != nullptr) {
        channel->add_chunk(cntl, request, response, done_guard.release());
    } else {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("no associated load channel");
    }
}

void LoadChannelMgr::cancel(brpc::Controller* cntl, const PTabletWriterCancelRequest& request,
                            PTabletWriterCancelResult* response, google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);
    UniqueId load_id(request.id());
    if (auto channel = remove_load_channel(load_id); channel != nullptr) {
        channel->cancel();
        channel->abort();
    }
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
    Thread::set_thread_name(_load_channels_clean_thread, "load_chan_clean");
    return Status::OK();
}

Status LoadChannelMgr::_start_load_channels_clean() {
    std::vector<scoped_refptr<LoadChannel>> timeout_channels;

    time_t now = time(nullptr);
    {
        std::lock_guard<std::mutex> l(_lock);
        for (auto it = _load_channels.begin(); it != _load_channels.end(); /**/) {
            if (difftime(now, it->second->last_updated_time()) >= it->second->timeout()) {
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
    }
    for (auto& channel : timeout_channels) {
        channel->abort();
        LOG(INFO) << "Deleted timeout channel. load id=" << channel->load_id() << " timeout=" << channel->timeout();
    }

    // this log print every 1 min, so that we could observe the mem consumption of load process
    // on this Backend
    LOG(INFO) << "Memory consumption(bytes) limit=" << _mem_tracker->limit()
              << " current=" << _mem_tracker->consumption() << " peak=" << _mem_tracker->peak_consumption();

    return Status::OK();
}

scoped_refptr<LoadChannel> LoadChannelMgr::_find_load_channel(const UniqueId& load_id) {
    std::lock_guard l(_lock);
    auto it = _load_channels.find(load_id);
    return (it != _load_channels.end()) ? it->second : nullptr;
}

scoped_refptr<LoadChannel> LoadChannelMgr::remove_load_channel(const UniqueId& load_id) {
    std::lock_guard l(_lock);
    if (auto it = _load_channels.find(load_id); it != _load_channels.end()) {
        auto ret = it->second;
        _load_channels.erase(it);
        return ret;
    }
    return nullptr;
}

} // namespace starrocks
