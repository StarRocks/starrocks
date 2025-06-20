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

#include "cache/mem_space_monitor.h"

#include "cache/object_cache/page_cache.h"
#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/await.h"
#include "util/gc_helper.h"
#include "util/thread.h"

namespace starrocks {

void MemSpaceMonitor::start() {
    _adjust_datacache_thread = std::thread([this] { _adjust_datacache_callback(); });
    Thread::set_thread_name(_adjust_datacache_thread, "adjust_mem_cache");
}

void MemSpaceMonitor::stop() {
    _stopped.store(true, std::memory_order_release);
    if (_adjust_datacache_thread.joinable()) {
        _adjust_datacache_thread.join();
    }
}

void MemSpaceMonitor::_evict_datacache(int64_t bytes_to_dec) {
    if (bytes_to_dec > 0) {
        int64_t bytes = bytes_to_dec;
        while (bytes >= GCBYTES_ONE_STEP) {
            // Evicting 1GB of data takes about 1 second, check if process have been canceled.
            if (UNLIKELY(_stopped)) {
                return;
            }
            _datacache->adjust_mem_capacity(-GCBYTES_ONE_STEP, kcacheMinSize);
            bytes -= GCBYTES_ONE_STEP;
        }
        if (bytes > 0) {
            _datacache->adjust_mem_capacity(-bytes, kcacheMinSize);
        }
    }
}

void MemSpaceMonitor::_adjust_datacache_callback() {
    int64_t cur_period = config::datacache_mem_adjust_period;
    int64_t cur_interval = config::datacache_mem_adjust_interval_seconds;
    std::unique_ptr<GCHelper> dec_advisor = std::make_unique<GCHelper>(cur_period, cur_interval, MonoTime::Now());
    std::unique_ptr<GCHelper> inc_advisor = std::make_unique<GCHelper>(cur_period, cur_interval, MonoTime::Now());
    while (!_stopped.load(std::memory_order_consume)) {
        int64_t kWaitTimeout = cur_interval * 1000 * 1000;
        static const int64_t kCheckInterval = 1000 * 1000;
        auto cond = [this]() { return _stopped.load(std::memory_order_acquire); };
        auto wait_ret = Awaitility().timeout(kWaitTimeout).interval(kCheckInterval).until(cond);
        if (wait_ret) {
            break;
        }

        if (!config::enable_datacache_mem_auto_adjust) {
            continue;
        }
        if (!config::datacache_enable) {
            continue;
        }
        MemTracker* memtracker = GlobalEnv::GetInstance()->process_mem_tracker();
        if (memtracker == nullptr || !memtracker->has_limit()) {
            continue;
        }
        if (UNLIKELY(cur_period != config::datacache_mem_adjust_period ||
                     cur_interval != config::datacache_mem_adjust_interval_seconds)) {
            cur_period = config::datacache_mem_adjust_period;
            cur_interval = config::datacache_mem_adjust_interval_seconds;
            dec_advisor = std::make_unique<GCHelper>(cur_period, cur_interval, MonoTime::Now());
            inc_advisor = std::make_unique<GCHelper>(cur_period, cur_interval, MonoTime::Now());
            // We re-initialized advisor, just continue.
            continue;
        }

        // Check config valid
        int64_t memory_urgent_level = config::memory_urgent_level;
        int64_t memory_high_level = config::memory_high_level;
        if (UNLIKELY(!(memory_urgent_level > memory_high_level && memory_high_level >= 1 &&
                       memory_urgent_level <= 100))) {
            LOG(ERROR) << "memory water level config is illegal: memory_urgent_level=" << memory_urgent_level
                       << " memory_high_level=" << memory_high_level;
            continue;
        }

        int64_t memory_urgent = memtracker->limit() * memory_urgent_level / 100;
        int64_t delta_urgent = memtracker->consumption() - memory_urgent;
        int64_t memory_high = memtracker->limit() * memory_high_level / 100;
        if (delta_urgent > 0) {
            // Memory usage exceeds memory_urgent_level, reduce size immediately.
            _datacache->adjust_mem_capacity(-delta_urgent, kcacheMinSize);
            size_t bytes_to_dec = dec_advisor->bytes_should_gc(MonoTime::Now(), memory_urgent - memory_high);
            _evict_datacache(static_cast<int64_t>(bytes_to_dec));
            continue;
        }

        int64_t delta_high = memtracker->consumption() - memory_high;
        if (delta_high > 0) {
            size_t bytes_to_dec = dec_advisor->bytes_should_gc(MonoTime::Now(), delta_high);
            _evict_datacache(static_cast<int64_t>(bytes_to_dec));
        } else {
            auto ret = _datacache->get_storage_page_cache_limit();
            if (!ret.ok()) {
                LOG(ERROR) << "Failed to get storage page size: " << ret.status();
                continue;
            }
            int64_t max_cache_size = std::max(ret.value(), kcacheMinSize);
            int64_t cur_cache_size = _datacache->get_mem_capacity();
            if (cur_cache_size >= max_cache_size) {
                continue;
            }
            int64_t delta_cache = std::min(max_cache_size - cur_cache_size, std::abs(delta_high));
            size_t bytes_to_inc = inc_advisor->bytes_should_gc(MonoTime::Now(), delta_cache);
            if (bytes_to_inc > 0) {
                _datacache->adjust_mem_capacity(bytes_to_inc, kcacheMinSize);
            }
        }
    }
}

} // namespace starrocks