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

#include "common/util/bthreads/executor.h"

#include "common/config_diagnostic_fwd.h"
#include "common/thread/threadpool.h"

namespace starrocks::bthreads {

ThreadPoolExecutor::~ThreadPoolExecutor() {
    if (_ownership == kTakesOwnership) delete _thread_pool;
}

int ThreadPoolExecutor::submit(void* (*fn)(void*), void* args) {
    Status st;
    while (true) {
        st = _thread_pool->submit_func([=]() { fn(args); });
        if (!st.is_service_unavailable()) break;

        // There are two scenarios that will return service unaviable
        // 1. The first scenario is that there is a lot of concurrency and tablet,
        //    write speed is slower than send speed,
        //    so that we can sleep here back pressure sender(rpc request memory consume will trace by MemTrack)
        // 2. The second scenrio is that storage hang, task will be blocked continuously,
        //    in which case we stop retrying after be_exit_after_disk_write_hang_second.
        MonoTime now_timestamp = MonoTime::Now();
        if (now_timestamp.GetDeltaSince(_thread_pool->last_active_timestamp())
                    .MoreThan(MonoDelta::FromSeconds(starrocks::config::be_exit_after_disk_write_hang_second))) {
            LOG(WARNING) << "async_delta_writer thread pool hang after "
                         << starrocks::config::be_exit_after_disk_write_hang_second << " second, err=" << st;
            break;
        }
        VLOG(2) << "async_delta_writer is busy, retry after " << _busy_sleep_ms << "ms";
        bthread_usleep(_busy_sleep_ms * 1000);
    }
    if (!st.ok()) {
        if (starrocks::config::enable_load_fail_fast_when_disk_write_hang) {
            // The thread pool is saturated, most likely because of a slow or hung disk.
            // Instead of taking the whole BE down (which kills writes/queries on every
            // healthy disk too), run the task inline so the already-queued work still
            // makes progress, matching brpc's own fallback when it fails to offload
            // _execute_tasks. New writes are shed earlier via is_overloaded() in the
            // load write path, so they fail fast with a retryable error rather than
            // piling up here.
            fn(args);
        } else {
            // Legacy behavior: a long disk write hang is treated as fatal so the storage
            // error becomes visible and the long tail of writes is eliminated.
            LOG(FATAL) << "BE exit since submit write fail err=" << st;
        }
    }
    return 0;
}

bool ThreadPoolExecutor::is_overloaded() const {
    if (!starrocks::config::enable_load_fail_fast_when_disk_write_hang || _thread_pool == nullptr) {
        return false;
    }
    // Only treat the pool as overloaded once the queue is full: a non-full queue can still
    // accept work, and an idle pool (a stale last_active_timestamp with an empty queue) must
    // not be mistaken for a hang.
    if (_thread_pool->num_queued_tasks() < _thread_pool->max_queue_size()) {
        return false;
    }
    return MonoTime::Now()
            .GetDeltaSince(_thread_pool->last_active_timestamp())
            .MoreThan(MonoDelta::FromSeconds(starrocks::config::be_exit_after_disk_write_hang_second));
}

} // namespace starrocks::bthreads
