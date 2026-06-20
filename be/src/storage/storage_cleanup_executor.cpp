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

#include "storage/storage_cleanup_executor.h"

#include <algorithm>
#include <utility>

#include "base/time/monotime.h"
#include "common/config_storage_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

constexpr int kStorageCleanupMaxQueueSize = 40960;

int calc_storage_cleanup_worker_count() {
    return config::storage_cleanup_worker_count > 0 ? config::storage_cleanup_worker_count
                                                    : std::max(static_cast<int>(CpuInfo::num_cores() / 2), 1);
}

std::future<Status> completed_future(Status value) {
    std::promise<Status> promise;
    promise.set_value(std::move(value));
    return promise.get_future();
}

} // namespace

StorageCleanupExecutor::~StorageCleanupExecutor() {
    shutdown(0);
}

Status StorageCleanupExecutor::init() {
    return ThreadPoolBuilder("storage_cleanup")
            .set_min_threads(1)
            .set_max_threads(calc_storage_cleanup_worker_count())
            .set_max_queue_size(kStorageCleanupMaxQueueSize)
            .build(&_thread_pool);
}

Status StorageCleanupExecutor::submit(std::function<void()> task) {
    if (!_begin_pool_op()) {
        return Status::ServiceUnavailable("storage cleanup executor is shutting down");
    }

    Status status;
    if (_thread_pool == nullptr) {
        status = Status::InternalError("storage cleanup executor is not initialized");
    } else {
        status = _thread_pool->submit_func(std::move(task));
    }

    _finish_pool_op();
    return status;
}

std::future<Status> StorageCleanupExecutor::submit_callable(std::function<Status()> task) {
    auto packaged_task = std::make_shared<std::packaged_task<Status()>>(std::move(task));
    auto future = packaged_task->get_future();
    auto status = submit([packaged_task]() { (*packaged_task)(); });
    if (!status.ok()) {
        return completed_future(std::move(status));
    }
    return future;
}

void StorageCleanupExecutor::wait() {
    if (_thread_pool != nullptr) {
        _thread_pool->wait();
    }
}

void StorageCleanupExecutor::shutdown(int64_t drain_timeout_ms) {
    {
        std::unique_lock l(_mutex);
        if (_shutting_down) {
            return;
        }
        _shutting_down = true;
        _cv.wait(l, [this] { return _active_pool_ops == 0; });
    }

    if (_thread_pool == nullptr) {
        return;
    }

    const auto timeout_ms = std::max<int64_t>(drain_timeout_ms, 0);
    if (!_thread_pool->wait_for(MonoDelta::FromMilliseconds(timeout_ms))) {
        LOG(WARNING) << "storage cleanup executor shutdown timed out after " << timeout_ms
                     << "ms, queued_tasks=" << _thread_pool->num_queued_tasks()
                     << ", active_threads=" << _thread_pool->active_threads()
                     << ". Queued cleanup tasks may be cancelled";
    }
    _thread_pool->shutdown();
}

Status StorageCleanupExecutor::update_max_threads() {
    if (!_begin_pool_op()) {
        return Status::ServiceUnavailable("storage cleanup executor is shutting down");
    }

    Status status;
    if (_thread_pool == nullptr) {
        status = Status::InternalError("storage cleanup executor is not initialized");
    } else {
        status = _thread_pool->update_max_threads(calc_storage_cleanup_worker_count());
    }

    _finish_pool_op();
    return status;
}

int StorageCleanupExecutor::num_queued_tasks() const {
    return _thread_pool == nullptr ? 0 : _thread_pool->num_queued_tasks();
}

int StorageCleanupExecutor::active_threads() const {
    return _thread_pool == nullptr ? 0 : _thread_pool->active_threads();
}

bool StorageCleanupExecutor::_begin_pool_op() {
    std::lock_guard l(_mutex);
    if (_shutting_down) {
        return false;
    }
    ++_active_pool_ops;
    return true;
}

void StorageCleanupExecutor::_finish_pool_op() {
    std::lock_guard l(_mutex);
    --_active_pool_ops;
    if (_shutting_down && _active_pool_ops == 0) {
        _cv.notify_all();
    }
}

} // namespace starrocks
