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

#include <memory>

#include "common/status.h"
#include "common/thread/threadpool.h"

namespace starrocks {

class ThreadPoolToken;

class LoadSpillBlockMergeExecutor {
public:
    LoadSpillBlockMergeExecutor() = default;
    ~LoadSpillBlockMergeExecutor() = default;
    Status init();

    ThreadPool* get_thread_pool() { return _merge_pool.get(); }
    ThreadPool* get_tablet_internal_parallel_merge_thread_pool() { return _tablet_internal_parallel_merge_pool.get(); }
    Status refresh_max_thread_num();

    std::unique_ptr<ThreadPoolToken> create_token();

    std::unique_ptr<ThreadPoolToken> create_tablet_internal_parallel_merge_token();

private:
    // The _merge_pool is used for executing merge tasks at the tablet level.
    // For large tablets, tasks within a single tablet are further subdivided,
    // and the _tablet_internal_parallel_merge_pool is responsible for executing
    // these internal-tablet sub-tasks.
    //
    // The reason these two thread pools are not unified is that tasks in _merge_pool
    // depend on the completion of tasks in _tablet_internal_parallel_merge_pool.
    // Merging them into a single pool would create a circular dependency,
    // leading to potential deadlocks.
    std::unique_ptr<ThreadPool> _merge_pool;
    // ThreadPool for internal-tablet parallel merge
    std::unique_ptr<ThreadPool> _tablet_internal_parallel_merge_pool;
};

} // namespace starrocks
