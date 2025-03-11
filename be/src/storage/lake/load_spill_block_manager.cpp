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
    return Status::OK();
}

// acquire Block from BlockManager
StatusOr<spill::BlockPtr> LoadSpillBlockManager::acquire_block(int64_t tablet_id, int64_t txn_id, size_t block_size) {
    spill::AcquireBlockOptions opts;
    opts.query_id = _load_id; // load id as query id
    opts.fragment_instance_id =
            UniqueId(tablet_id, txn_id).to_thrift(); // use tablet id + txn id to generate fragment instance id
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