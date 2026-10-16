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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

#include "base/debug/trace.h"
#include "base/testutil/sync_point.h"
#include "common/statusor.h"

namespace starrocks::lake {

// FIFO permits for publish admission and transient SST reads. Publish callers acquire before
// tablet/shard locks; pressure reduces their concurrency to one. Admitted work can finish and
// release memory for the oldest waiter.
class PkIndexPublishLimiter {
public:
    class Permit {
    public:
        ~Permit() {
            if (_owner) _owner->release();
        }
        Permit(const Permit&) = delete;
        Permit& operator=(const Permit&) = delete;

    private:
        friend class PkIndexPublishLimiter;
        explicit Permit(PkIndexPublishLimiter* owner) : _owner(owner) {}
        PkIndexPublishLimiter* _owner;
    };

    // Optional preloading must not queue ahead of a publish or steal its update workspace.
    // OFF callers still hold a permit so a later ON transition drains in-flight preloads.
    std::unique_ptr<Permit> try_acquire(size_t concurrency, bool respect_queue = true) {
        auto permit = std::unique_ptr<Permit>(new Permit(nullptr));
        std::lock_guard<std::mutex> lock(_mutex);
        if (_active >= concurrency || (respect_queue && !_queue.empty())) return nullptr;
        ++_active;
        permit->_owner = this;
        return permit;
    }

    StatusOr<std::unique_ptr<Permit>> acquire(int64_t timeout_ms, const std::function<size_t()>& concurrency,
                                              const std::function<bool()>& cancelled, bool sst_read = false) {
        TRACE_COUNTER_SCOPE_LATENCY_US(sst_read ? "pindex_sst_memory_admission_us" : "pindex_publish_admission_us");
        auto permit = std::unique_ptr<Permit>(new Permit(nullptr));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        std::unique_lock<std::mutex> lock(_mutex);
        const uint64_t ticket = _next_ticket++;
        _queue.push_back(ticket);
        TEST_SYNC_POINT_CALLBACK("PkIndexPublishLimiter::queued", nullptr);
        auto remove_waiter = [&] {
            _queue.erase(std::find(_queue.begin(), _queue.end(), ticket));
            _cv.notify_all();
        };
        bool waited = false;
        for (;;) {
            if (cancelled()) {
                remove_waiter();
                TRACE_COUNTER_INCREMENT(
                        sst_read ? "pindex_sst_memory_admission_cancelled" : "pindex_publish_admission_cancelled", 1);
                return Status::Cancelled("PK publish admission cancelled");
            }
            if (_queue.front() == ticket && _active < concurrency()) {
                _queue.pop_front();
                ++_active;
                _cv.notify_all();
                TRACE_COUNTER_INCREMENT(sst_read ? "pindex_sst_memory_admitted" : "pindex_publish_admitted", 1);
                permit->_owner = this;
                return permit;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                remove_waiter();
                TRACE_COUNTER_INCREMENT(
                        sst_read ? "pindex_sst_memory_admission_timeout" : "pindex_publish_admission_timeout", 1);
                return Status::TimedOut("PK publish admission exceeded its bounded wait");
            }
            if (!waited) {
                TRACE_COUNTER_INCREMENT(
                        sst_read ? "pindex_sst_memory_admission_wait_count" : "pindex_publish_admission_wait_count", 1);
                waited = true;
            }
            // Also observe shutdown, pressure changes and dynamic switch rollback without needing
            // a completion notification. A released permit wakes the queue immediately.
            _cv.wait_until(lock, std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(100)));
        }
    }

private:
    void release() {
        std::lock_guard<std::mutex> lock(_mutex);
        --_active;
        _cv.notify_all();
    }

    std::mutex _mutex;
    std::condition_variable _cv;
    std::deque<uint64_t> _queue;
    uint64_t _next_ticket = 0;
    size_t _active = 0;
};

} // namespace starrocks::lake
