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

#include "storage/lake/load_spill_block_manager.h"

#include <vector>

#include "exec/spill/file_block_manager.h"
#include "exec/spill/hybird_block_manager.h"
#include "exec/spill/log_block_manager.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "runtime/exec_env.h"

namespace starrocks::lake {

static int calc_max_merge_blocks_thread() {
#ifndef BE_TEST
    // The starting point for setting the maximum number of threads for load spill:
    // 1. Meet the memory limit requirements (by config::load_spill_merge_memory_limit_percent) for load spill.
    // 2. Each thread can use 1GB(by config::load_spill_max_merge_bytes) of memory.
    // 3. The maximum number of threads is limited by config::load_spill_merge_max_thread.
    int64_t load_spill_merge_memory_limit_bytes = GlobalEnv::GetInstance()->process_mem_tracker()->limit() *
                                                  config::load_spill_merge_memory_limit_percent / (int64_t)100;
    int max_merge_blocks_thread = load_spill_merge_memory_limit_bytes / config::load_spill_max_merge_bytes;
#else
    int max_merge_blocks_thread = 1;
#endif

    return std::max<int>(1, std::min<int>(max_merge_blocks_thread, config::load_spill_merge_max_thread));
}

Status LoadSpillBlockMergeExecutor::init() {
    RETURN_IF_ERROR(ThreadPoolBuilder("load_spill_block_merge")
                            .set_min_threads(1)
                            .set_max_threads(calc_max_merge_blocks_thread())
                            .set_max_queue_size(40960 /*a random chosen number that should big enough*/)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(/*5 minutes=*/5 * 60 * 1000))
                            .build(&_merge_pool));
    return Status::OK();
}

Status LoadSpillBlockMergeExecutor::refresh_max_thread_num() {
    if (_merge_pool != nullptr) {
        return _merge_pool->update_max_threads(calc_max_merge_blocks_thread());
    }
    return Status::OK();
}

void LoadSpillBlockContainer::append_block(const spill::BlockPtr& block) {
    std::lock_guard guard(_mutex);
    _block_groups.back().append(block);
}

void LoadSpillBlockContainer::create_block_group() {
    std::lock_guard guard(_mutex);
    _block_groups.emplace_back(spill::BlockGroup());
}

bool LoadSpillBlockContainer::empty() {
    std::lock_guard guard(_mutex);
    return _block_groups.empty();
}

spill::BlockPtr LoadSpillBlockContainer::get_block(size_t gid, size_t bid) {
    return _block_groups[gid].blocks()[bid];
}

LoadSpillBlockManager::~LoadSpillBlockManager() {
    // release blocks before block manager
    _block_container.reset();
}

Status LoadSpillBlockManager::init() {
    // init remote block manager
    std::vector<std::shared_ptr<spill::Dir>> remote_dirs;
    // Remote FS can also use data cache to speed up.
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_remote_spill_path));
    RETURN_IF_ERROR(fs->create_dir_if_missing(_remote_spill_path));
    auto dir = std::make_shared<spill::RemoteDir>(_remote_spill_path, std::move(fs), nullptr, INT64_MAX);
    remote_dirs.emplace_back(dir);
    _remote_dir_manager = std::make_unique<spill::DirManager>(remote_dirs);

    // init block manager
    auto local_block_manager =
            std::make_unique<spill::LogBlockManager>(_load_id, ExecEnv::GetInstance()->spill_dir_mgr());
    auto remote_block_manager = std::make_unique<spill::FileBlockManager>(_load_id, _remote_dir_manager.get());
    _block_manager = std::make_unique<spill::HyBirdBlockManager>(_load_id, std::move(local_block_manager),
                                                                 std::move(remote_block_manager));
    // init block container
    _block_container = std::make_unique<LoadSpillBlockContainer>();
    _initialized = true;
    return Status::OK();
}

// acquire Block from BlockManager
StatusOr<spill::BlockPtr> LoadSpillBlockManager::acquire_block(size_t block_size) {
    spill::AcquireBlockOptions opts;
    opts.query_id = _load_id; // load id as query id
    opts.fragment_instance_id =
            UniqueId(_tablet_id, _txn_id).to_thrift(); // use tablet id + txn id to generate fragment instance id
    opts.plan_node_id = 0;
    opts.name = "load_spill";
    opts.block_size = block_size;
    return _block_manager->acquire_block(opts);
}

// return Block to BlockManager
Status LoadSpillBlockManager::release_block(spill::BlockPtr block) {
    return _block_manager->release_block(std::move(block));
}

} // namespace starrocks::lake