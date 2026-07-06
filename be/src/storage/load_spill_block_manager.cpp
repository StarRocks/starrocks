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

#include "storage/load_spill_block_manager.h"

#include <vector>

#include "base/uid_util.h"
#include "compute_env/spill/file_block_manager.h"
#include "compute_env/spill/hybird_block_manager.h"
#include "compute_env/spill/log_block_manager.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "storage/storage_env.h"

namespace starrocks {

void LoadSpillBlockContainer::append_block(spill::BlockGroup* block_group, const spill::BlockPtr& block) {
    // Move append outside the lock to reduce lock contention
    block_group->append(block);
    std::lock_guard guard(_mutex);
    _total_bytes += block->size();
}

// Create a new block group tagged with the given slot_idx
// @param slot_idx: slot index from flush token, used to track submission order
// @return: pointer to the newly created block group
spill::BlockGroup* LoadSpillBlockContainer::create_block_group(int64_t slot_idx) {
    std::lock_guard guard(_mutex);
    _block_groups.emplace_back(BlockGroupPtrWithSlot{std::make_shared<spill::BlockGroup>(), slot_idx});
    return _block_groups.back().block_group.get();
}

bool LoadSpillBlockContainer::empty() {
    std::lock_guard guard(_mutex);
    return _block_groups.empty();
}

spill::BlockPtr LoadSpillBlockContainer::get_block(size_t gid, size_t bid) {
    return _block_groups[gid].block_group->blocks()[bid];
}

LoadSpillBlockManager::~LoadSpillBlockManager() {
    // release blocks before block manager
    _block_container.reset();
}

Status LoadSpillBlockManager::clear_parent_path() {
    // _remote_dir_manager is initialized in init(), skip cleanup if init() was not called or failed
    Status status = Status::OK();
    if (_remote_dir_manager != nullptr) {
        spill::AcquireDirOptions acquire_dir_opts;
        acquire_dir_opts.data_size = 1; // just need acquire a dir
        auto dir_st = _remote_dir_manager->acquire_writable_dir(acquire_dir_opts);
        if (!dir_st.ok()) {
            LOG(WARNING) << "Failed to acquire dir for clearing load spill parent path, load_id=" << print_id(_load_id)
                         << ", error=" << dir_st.status();
            return dir_st.status();
        }
        auto dir = std::move(dir_st).value();
        std::string parent_path = dir->dir() + "/" + print_id(_load_id);
        status = dir->fs()->delete_dir(parent_path);
        if (!status.ok() && !status.is_not_found()) {
            LOG(WARNING) << "Failed to clear load spill parent path, load_id=" << print_id(_load_id)
                         << ", error=" << status;
        }
    }
    return status;
}

Status LoadSpillBlockManager::init() {
    // init remote block manager
    std::vector<std::shared_ptr<spill::Dir>> remote_dirs;
    // Remote FS can also use data cache to speed up.

    if (!_fs) {
        ASSIGN_OR_RETURN(_fs, FileSystemFactory::CreateSharedFromString(_remote_spill_path));
    }
    if (_fs->type() != FileSystem::Type::STARLET) {
        // in starlet fs, there is opt create_missing_parent and it will create the parent dir if not exists.
        // but in other fs, we need to create the parent dir manually.
        RETURN_IF_ERROR(_fs->create_dir_if_missing(_remote_spill_path));
    }
    auto dir = std::make_shared<spill::RemoteDir>(_remote_spill_path, std::move(_fs), nullptr, INT64_MAX);
    remote_dirs.emplace_back(dir);
    _remote_dir_manager = std::make_unique<spill::DirManager>(remote_dirs);

    // init block manager
    auto* local_spill_dir_mgr = StorageEnv::GetInstance()->spill_dir_mgr();
    if (local_spill_dir_mgr == nullptr) {
        return Status::InternalError("LoadSpillBlockManager requires local spill dir manager");
    }
    auto local_block_manager = std::make_unique<spill::LogBlockManager>(_load_id, local_spill_dir_mgr);
    auto remote_block_manager = std::make_unique<spill::FileBlockManager>(_load_id, _remote_dir_manager.get());
    _block_manager = std::make_unique<spill::HyBirdBlockManager>(_load_id, std::move(local_block_manager),
                                                                 std::move(remote_block_manager));
    // init block container
    _block_container = std::make_unique<LoadSpillBlockContainer>();
    _initialized = true;
    return Status::OK();
}

// acquire Block from BlockManager
StatusOr<spill::BlockPtr> LoadSpillBlockManager::acquire_block(size_t block_size, bool force_remote) {
    spill::AcquireBlockOptions opts;
    opts.query_id = _load_id; // load id as query id
    opts.fragment_instance_id = _fragment_instance_id;
    opts.plan_node_id = 0;
    opts.name = "load_spill";
    opts.block_size = block_size;
    opts.force_remote = force_remote;
    // Parent path deletion is handled explicitly by the caller via clear_parent_path(),
    // so skip it at the block layer to avoid races with files still in use.
    opts.skip_parent_path_deletion = true;
    // In flat layout, per-file deletion is delegated to the merge hot-delete path
    // and vacuum_full (eliminating S3 DeleteObject on the write hot path).
    opts.skip_file_deletion = _enable_flat_layout;
    if (_enable_flat_layout) {
        // Encode <txn_id_hex>_<load_id>_<frag_id> into the file name so vacuum can
        // reclaim by parsing the leading hex segment alone. See class-level comment.
        opts.flat_layout = true;
        opts.flat_name_prefix =
                fmt::format("{:016x}_{}_{}", _txn_id, print_id(_load_id), print_id(_fragment_instance_id));
    }
    return _block_manager->acquire_block(opts);
}

// return Block to BlockManager
Status LoadSpillBlockManager::release_block(spill::BlockPtr block) {
    return _block_manager->release_block(std::move(block));
}

} // namespace starrocks
