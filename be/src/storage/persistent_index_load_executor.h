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

#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace starrocks {

class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;

// PersistentIndexLoadExecutor is responsible for asynchronous loading of the persistent index
// for primary key tablet.
//
// It maintains a thread pool to submit and execute the persistent index load tasks,
// and allows waiting for the task to finish within a specified timeout.
// - Returns OK status if the task finishes within the specified timeout.
// - Returns TIMEOUT status if the timeout expires before the task finishes.
//
// If the persistent index does not exist, it will be rebuilt.
//
// This class is thread-safe.
class PersistentIndexLoadExecutor {
public:
    PersistentIndexLoadExecutor() = default;
    ~PersistentIndexLoadExecutor() = default;

    Status init();
    void shutdown();

    Status refresh_max_thread_num();

    Status submit_task_and_wait_for(const TabletSharedPtr& tablet, int32_t wait_seconds);

    ThreadPool* TEST_get_load_pool() { return _load_pool.get(); }
    void TEST_reset_load_pool() { _load_pool.reset(); }

private:
    StatusOr<std::shared_ptr<CountDownLatch>> submit_task(const TabletSharedPtr& tablet);

    std::unique_ptr<ThreadPool> _load_pool;
    std::mutex _lock;
    std::unordered_map<int64_t, std::shared_ptr<CountDownLatch>> _running_tablets;
};

} // namespace starrocks
