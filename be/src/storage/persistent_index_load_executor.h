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

// PersistentIndexLoadExecutor is responsible for loading the persistent index
// for primary key tablet.
//
// The submit_task function returns CountDownLatch.
// The caller can wait until the persistent index load is finished
// or wait for a certain period if the persistent index is still loading.
//
// If the persistent index does not exist, it will be rebuilt.
class PersistentIndexLoadExecutor {
public:
    PersistentIndexLoadExecutor() = default;
    ~PersistentIndexLoadExecutor() = default;

    Status init();
    void shutdown();

    Status refresh_max_thread_num();

    StatusOr<std::shared_ptr<CountDownLatch>> submit_task(const TabletSharedPtr& tablet);

private:
    std::unique_ptr<ThreadPool> _load_pool;
    std::mutex _lock;
    std::unordered_map<int64_t, std::shared_ptr<CountDownLatch>> _running_tablets;
};

} // namespace starrocks
