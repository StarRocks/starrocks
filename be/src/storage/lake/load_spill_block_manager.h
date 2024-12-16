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

namespace starrocks {

namespace lake {

class LoadSpillBlockContainer {
public:
    LoadSpillBlockContainer() {}
    ~LoadSpillBlockContainer() = default;

private:
    std::vector<spill::BlockPtr> _blocks; // Blocks generated when loading.
};

class LoadSpillBlockManager {
public:
    // Constructor that initializes the LoadSpillBlockManager with a query ID and remote spill path.
    LoadSpillBlockManager(const TUniqueId& load_id, const std::string& remote_spill_path) : _load_id(load_id) {
        _remote_spill_path = remote_spill_path + "/load_spill/";
    }

    // Default destructor.
    ~LoadSpillBlockManager();

    // Initializes the LoadSpillBlockManager.
    Status init();

    // acquire Block from BlockManager
    StatusOr<spill::BlockPtr> acquire_block(int64_t tablet_id, int64_t txn_id, size_t block_size);
    // return Block to BlockManager
    Status release_block(spill::BlockPtr block);

    spill::BlockManager* block_manager() { return _block_manager.get(); }
    LoadSpillBlockContainer* block_container() { return _block_container.get(); }

private:
    TUniqueId _load_id;                                        // Unique ID for the load.
    std::string _remote_spill_path;                            // Path for remote spill storage.
    std::unique_ptr<spill::DirManager> _remote_dir_manager;    // Manager for remote directories.
    std::unique_ptr<spill::BlockManager> _block_manager;       // Manager for blocks.
    std::unique_ptr<LoadSpillBlockContainer> _block_container; // Container for blocks.
};

} // namespace lake
} // namespace starrocks