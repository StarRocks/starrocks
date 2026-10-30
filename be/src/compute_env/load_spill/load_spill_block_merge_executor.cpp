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

#include "compute_env/load_spill/load_spill_block_merge_executor.h"

#include <algorithm>

#include "common/config_ingest_fwd.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"

namespace starrocks {

namespace {

int calc_max_merge_blocks_thread() {
#ifndef BE_TEST
    // The starting point for setting the maximum number of threads for load spill:
    // 1. Meet the memory limit requirements (by config::load_spill_merge_memory_limit_percent) for load spill.
    // 2. Each thread can use 1GB(by config::load_spill_memory_usage_per_merge) of memory.
    // 3. The maximum number of threads is limited by config::load_spill_merge_max_thread.
    int64_t load_spill_merge_memory_limit_bytes = RuntimeEnv::GetInstance()->process_mem_tracker()->limit() *
                                                  config::load_spill_merge_memory_limit_percent / (int64_t)100;
    int max_merge_blocks_thread = load_spill_merge_memory_limit_bytes / config::load_spill_memory_usage_per_merge;
#else
    int max_merge_blocks_thread = 1;
#endif

    return std::max<int>(1, std::min<int>(max_merge_blocks_thread, config::load_spill_merge_max_thread));
}

} // namespace

Status LoadSpillBlockMergeExecutor::init() {
    RETURN_IF_ERROR(ThreadPoolBuilder("ld_spill_merge")
                            .set_min_threads(1)
                            .set_max_threads(calc_max_merge_blocks_thread())
                            .set_max_queue_size(40960 /*a random chosen number that should big enough*/)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(/*5 minutes=*/5 * 60 * 1000))
                            .build(&_merge_pool));
    return ThreadPoolBuilder("tablet_par_mrg")
            .set_min_threads(1)
            .set_max_threads(calc_max_merge_blocks_thread())
            .set_max_queue_size(40960 /*a random chosen number that should big enough*/)
            .set_idle_timeout(MonoDelta::FromMilliseconds(/*5 minutes=*/5 * 60 * 1000))
            .build(&_tablet_internal_parallel_merge_pool);
}

Status LoadSpillBlockMergeExecutor::refresh_max_thread_num() {
    if (_merge_pool != nullptr) {
        RETURN_IF_ERROR(_merge_pool->update_max_threads(calc_max_merge_blocks_thread()));
    }
    if (_tablet_internal_parallel_merge_pool != nullptr) {
        RETURN_IF_ERROR(_tablet_internal_parallel_merge_pool->update_max_threads(calc_max_merge_blocks_thread()));
    }
    return Status::OK();
}

std::unique_ptr<ThreadPoolToken> LoadSpillBlockMergeExecutor::create_token() {
    return _merge_pool->new_token(ThreadPool::ExecutionMode::SERIAL);
}

std::unique_ptr<ThreadPoolToken> LoadSpillBlockMergeExecutor::create_tablet_internal_parallel_merge_token() {
    return _tablet_internal_parallel_merge_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
}

} // namespace starrocks
