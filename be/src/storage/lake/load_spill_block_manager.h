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

#include "exec/spill/block_manager.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/input_stream.h"
#include "util/threadpool.h"

namespace starrocks {

class ThreadPoolToken;

namespace lake {

class LoadSpillBlockMergeExecutor {
public:
    LoadSpillBlockMergeExecutor() {}
    ~LoadSpillBlockMergeExecutor() {}
    Status init();

    ThreadPool* get_thread_pool() { return _merge_pool.get(); }
    Status refresh_max_thread_num();

    std::unique_ptr<ThreadPoolToken> create_token();

private:
    // ThreadPool for merge.
    std::unique_ptr<ThreadPool> _merge_pool;
};

class LoadSpillBlockContainer {
public:
    void append_block(const spill::BlockPtr& block);
    void create_block_group();
    bool empty();
    // No thread safe, UT only
    spill::BlockPtr get_block(size_t gid, size_t bid);
    std::vector<spill::BlockGroup>& block_groups() { return _block_groups; }

private:
    // Mutex for the container.
    std::mutex _mutex;
    // Blocks generated when loading. Each block group contains multiple blocks which are ordered.
    std::vector<spill::BlockGroup> _block_groups;
};

class LoadSpillBlockManager {
public:
    // Constructor that initializes the LoadSpillBlockManager with a query ID and remote spill path.
    LoadSpillBlockManager(const TUniqueId& load_id, int64_t tablet_id, int64_t txn_id,
                          const std::string& remote_spill_path)
            : _load_id(load_id), _tablet_id(tablet_id), _txn_id(txn_id) {
        _remote_spill_path = remote_spill_path + "/load_spill/";
    }

    // Default destructor.
    ~LoadSpillBlockManager();

    int64_t tablet_id() const { return _tablet_id; }
    int64_t txn_id() const { return _txn_id; }

    // Initializes the LoadSpillBlockManager.
    Status init();

    bool is_initialized() const { return _initialized; }

    // acquire Block from BlockManager
    StatusOr<spill::BlockPtr> acquire_block(size_t block_size);
    // return Block to BlockManager
    Status release_block(spill::BlockPtr block);

    spill::BlockManager* block_manager() { return _block_manager.get(); }
    LoadSpillBlockContainer* block_container() { return _block_container.get(); }

    bool has_spill_block() const { return _block_container != nullptr && !_block_container->empty(); }

private:
    TUniqueId _load_id;                                        // Unique ID for the load.
    int64_t _tablet_id;                                        // ID for the tablet.
    int64_t _txn_id;                                           // ID for the transaction.
    std::string _remote_spill_path;                            // Path for remote spill storage.
    std::unique_ptr<spill::DirManager> _remote_dir_manager;    // Manager for remote directories.
    std::unique_ptr<spill::BlockManager> _block_manager;       // Manager for blocks.
    std::unique_ptr<LoadSpillBlockContainer> _block_container; // Container for blocks.
    bool _initialized = false;                                 // Whether the manager is initialized.
};

} // namespace lake
} // namespace starrocks